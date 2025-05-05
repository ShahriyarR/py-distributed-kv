import asyncio
import logging
import time
from typing import Dict, Optional

import requests

from pydistributedkv.configurator.settings.base import API_TIMEOUT, HEARTBEAT_INTERVAL, HEARTBEAT_TIMEOUT

logger = logging.getLogger(__name__)


class HeartbeatService:
    """
    Service for sending and monitoring heartbeats between servers in the cluster.
    """

    def __init__(self, service_name: str, server_id: str, server_url: str):
        self.service_name = service_name
        self.server_id = server_id
        self.server_url = server_url
        self.servers: Dict[str, Dict] = {}  # server_id -> {url, last_heartbeat, status}
        self._background_tasks = set()
        self._monitor_running = False
        self._send_running = False

    def register_server(self, server_id: str, server_url: str) -> None:
        """Register a server to be monitored"""
        current_time = time.time()
        self.servers[server_id] = {"url": server_url, "last_heartbeat": current_time, "status": "healthy"}
        logger.info(f"{self.service_name}: Registered server {server_id} at {server_url}")

    def deregister_server(self, server_id: str) -> None:
        """Deregister a server from monitoring"""
        if server_id in self.servers:
            del self.servers[server_id]
            logger.info(f"{self.service_name}: Deregistered server {server_id}")

    def record_heartbeat(self, server_id: str) -> None:
        """Record that a heartbeat was received from a server"""
        if server_id not in self.servers:
            logger.warning(f"{self.service_name}: Received heartbeat from unknown server {server_id}")
            return

        current_time = time.time()
        self.servers[server_id]["last_heartbeat"] = current_time

        # If server was previously down, mark it as healthy
        if self.servers[server_id]["status"] != "healthy":
            self.servers[server_id]["status"] = "healthy"
            logger.info(f"{self.service_name}: Server {server_id} is now healthy")

    def get_server_status(self, server_id: str) -> Optional[Dict]:
        """Get the status of a specific server"""
        return self.servers.get(server_id)

    def get_all_statuses(self) -> Dict:
        """Get the status of all servers"""
        return {
            server_id: {
                "url": info["url"],
                "status": info["status"],
                "last_heartbeat": info["last_heartbeat"],
                "seconds_since_last_heartbeat": time.time() - info["last_heartbeat"],
            }
            for server_id, info in self.servers.items()
        }

    def get_healthy_servers(self) -> Dict[str, str]:
        """Get a dictionary of healthy server IDs and URLs"""
        return {server_id: info["url"] for server_id, info in self.servers.items() if info["status"] == "healthy"}

    async def start_monitoring(self) -> None:
        """Start monitoring heartbeats from registered servers"""
        if self._monitor_running:
            return

        self._monitor_running = True
        task = asyncio.create_task(self._monitor_heartbeats())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        logger.info(f"{self.service_name}: Started heartbeat monitoring")

    async def start_sending(self) -> None:
        """Start sending heartbeats to registered servers"""
        if self._send_running:
            return

        self._send_running = True
        task = asyncio.create_task(self._send_heartbeats())
        self._background_tasks.add(task)
        task.add_done_callback(self._background_tasks.discard)
        logger.info(f"{self.service_name}: Started sending heartbeats")

    async def stop(self) -> None:
        """Stop all heartbeat activities"""
        self._monitor_running = False
        self._send_running = False
        # Wait for tasks to complete
        for task in self._background_tasks:
            task.cancel()
        logger.info(f"{self.service_name}: Stopped heartbeat service")

    async def _monitor_heartbeats(self) -> None:
        """Monitor heartbeats and mark servers as down if they miss heartbeats"""
        while self._monitor_running:
            current_time = time.time()
            self._check_server_heartbeats(current_time)
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    def _check_server_heartbeats(self, current_time: float) -> None:
        """Check each server's heartbeat and update status if needed"""
        for server_id, info in self.servers.items():
            if info["status"] == "down":
                continue

            time_since_last_heartbeat = current_time - info["last_heartbeat"]
            self._update_server_status(server_id, info, time_since_last_heartbeat)

    def _update_server_status(self, server_id: str, info: Dict, elapsed_time: float) -> None:
        """Update server status based on heartbeat timeout"""
        if elapsed_time <= HEARTBEAT_TIMEOUT:
            return

        info["status"] = "down"
        logger.warning(f"{self.service_name}: Server {server_id} marked as down. " f"No heartbeat for {elapsed_time:.1f}s")

    async def _send_heartbeats(self) -> None:
        """Send periodic heartbeats to all registered servers"""
        while self._send_running:
            await self._send_heartbeats_to_all_servers()
            await asyncio.sleep(HEARTBEAT_INTERVAL)

    async def _send_heartbeats_to_all_servers(self) -> None:
        """Send heartbeats to all registered servers in parallel"""
        for server_id, info in list(self.servers.items()):
            # Send heartbeat even to servers marked as down (to detect recovery)
            self._schedule_heartbeat(server_id, info)

    def _schedule_heartbeat(self, server_id: str, info: dict) -> None:
        """Schedule a non-blocking heartbeat to a specific server"""
        try:
            server_url = info["url"]
            asyncio.create_task(self._send_single_heartbeat(server_id, server_url))
        except Exception as e:
            logger.error(f"{self.service_name}: Error preparing heartbeat to {server_id}: {str(e)}")

    async def _send_single_heartbeat(self, server_id: str, server_url: str) -> None:
        """Send a single heartbeat to a specific server"""
        try:
            response = await asyncio.to_thread(
                requests.post, f"{server_url}/heartbeat", json={"server_id": self.server_id, "timestamp": time.time()}, timeout=API_TIMEOUT
            )

            if response.status_code == 200:
                logger.debug(f"{self.service_name}: Heartbeat sent to {server_id}")
            else:
                logger.warning(f"{self.service_name}: Heartbeat to {server_id} failed with status {response.status_code}")
        except requests.RequestException as e:
            logger.warning(f"{self.service_name}: Failed to send heartbeat to {server_id}: {str(e)}")
