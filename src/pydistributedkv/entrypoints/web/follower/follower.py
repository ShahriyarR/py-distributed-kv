import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query

from pydistributedkv.configurator.settings.base import API_TIMEOUT, compaction_interval, HEARTBEAT_INTERVAL, MAX_SEGMENT_SIZE
from pydistributedkv.domain.models import ClientRequest, LogEntry, OperationType, ReplicationRequest, WAL
from pydistributedkv.service.compaction import LogCompactionService
from pydistributedkv.service.heartbeat import HeartbeatService
from pydistributedkv.service.request_deduplication import RequestDeduplicationService
from pydistributedkv.service.storage import KeyValueStorage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize WAL and storage
wal = WAL(os.getenv("WAL_PATH", "data/follower/wal.log"), max_segment_size=MAX_SEGMENT_SIZE)
storage = KeyValueStorage(wal)

# Initialize compaction service
compaction_service = LogCompactionService(storage, compaction_interval=compaction_interval)

# Request deduplication service
request_deduplication = RequestDeduplicationService(service_name="follower")

# Leader connection info
leader_url = os.getenv("LEADER_URL", "http://localhost:8000")
follower_id = os.getenv("FOLLOWER_ID", "follower-1")
follower_url = os.getenv("FOLLOWER_URL", "http://localhost:8001")

# Create heartbeat service
heartbeat_service = HeartbeatService(service_name="follower", server_id=follower_id, server_url=follower_url)

# Replication state
last_applied_id = wal.get_last_id()  # Initialize with the current last ID in WAL


@app.on_event("startup")
async def startup_event():
    # Register the leader with the heartbeat service
    heartbeat_service.register_server("leader", leader_url)

    # Start heartbeat monitoring and sending
    await heartbeat_service.start_monitoring()
    await heartbeat_service.start_sending()

    # Start compaction service
    await compaction_service.start()

    # Register with leader
    try:
        response = requests.post(
            f"{leader_url}/register_follower",
            json={"id": follower_id, "url": follower_url, "last_applied_id": last_applied_id},
            timeout=API_TIMEOUT,
        )
        response_data = response.json()

        # If leader has entries we don't, fetch them
        leader_last_id = response_data.get("last_log_id", 0)
        if leader_last_id > last_applied_id:
            await sync_with_leader()
    except requests.RequestException as e:
        # In production, you'd implement retry logic
        logger.error(f"Failed to register with leader at {leader_url}: {str(e)}")


@app.on_event("shutdown")
async def shutdown_event():
    # Stop heartbeat service
    await heartbeat_service.stop()

    # Stop compaction service
    await compaction_service.stop()

    logger.info("Follower server shutting down")


async def sync_with_leader():
    """Synchronize the follower with the leader by fetching and applying new log entries."""
    global last_applied_id  # This global declaration is necessary here
    try:
        entries = await fetch_entries_from_leader()
        if not entries:
            return

        new_entries = append_entries_to_wal(entries)
        if new_entries:
            last_applied_id = apply_entries_to_storage(new_entries)
    except requests.RequestException:
        print("Failed to sync with leader")


async def fetch_entries_from_leader() -> list[LogEntry]:
    """Fetch new log entries from the leader."""
    response = requests.get(f"{leader_url}/log_entries/{last_applied_id}", timeout=API_TIMEOUT)
    data = response.json()
    return _parse_and_validate_entries(data.get("entries", []), source="leader")


def _parse_and_validate_entries(entry_data_list: list[dict], source: str = "") -> list[LogEntry]:
    """Parse and validate log entries from the provided data."""
    valid_entries = []

    for entry_data in entry_data_list:
        entry = _create_valid_entry(entry_data, source)
        if entry:
            valid_entries.append(entry)

    return valid_entries


def _create_valid_entry(entry_data: dict, source: str = "") -> LogEntry | None:
    """Create and validate a single log entry."""
    try:
        entry = LogEntry(**entry_data)
        if not entry.validate_crc():
            print(f"Warning: Received entry with ID {entry.id} with invalid CRC from {source}")
            return None
        return entry
    except ValueError as e:
        print(f"Error parsing entry from {source}: {str(e)}")
        return None


def append_entries_to_wal(entries: list[LogEntry]) -> list[LogEntry]:
    """Append new entries to the WAL and return only the newly added ones."""
    new_entries = []
    for entry in entries:
        # Ensure we only add entries with valid CRC
        if entry.validate_crc() and not wal.has_entry(entry.id):
            wal.append_entry(entry)
            new_entries.append(entry)
    return new_entries


def apply_entries_to_storage(entries: list[LogEntry]) -> int:
    """Apply entries to storage and return the last applied ID."""
    return storage.apply_entries(entries)


@app.post("/replicate")
async def replicate(req: ReplicationRequest):
    global last_applied_id  # This global declaration is necessary here

    # Use the existing helper to parse and validate entries
    entries = _parse_and_validate_entries(req.entries, source="replication request")

    # Process and apply new entries
    new_entries = _process_new_entries(entries)

    # Update the last applied ID if we have new entries
    if new_entries:
        last_id = storage.apply_entries(new_entries)
        last_applied_id = max(last_applied_id, last_id)

    return {"status": "ok", "last_applied_id": last_applied_id}


def _process_new_entries(entries: list[LogEntry]) -> list[LogEntry]:
    """Process and store only entries that don't exist in the WAL."""
    if not entries:
        return []

    new_entries = []
    for entry in entries:
        if not wal.has_entry(entry.id):
            wal.append_entry(entry)
            new_entries.append(entry)
    return new_entries


@app.get("/key/{key}")
def get_key(
    key: str,
    version: Optional[int] = Query(None, description="Specific version to retrieve"),
    client_id: Optional[str] = Query(None),
    request_id: Optional[str] = Query(None),
):
    """Handle GET request for a specific key with deduplication support"""
    # Check for cached response from duplicate request
    cached_response = _check_request_cache(client_id, request_id, key, OperationType.GET)
    if cached_response:
        return cached_response

    # Get the value and prepare response
    result = _get_value_from_storage(key, version)
    if result is None:
        response = {"status": "error", "message": f"Key not found: {key}"}
        if version:
            response["message"] = f"Key not found or version {version} not available: {key}"
        _cache_response_if_needed(client_id, request_id, key, OperationType.GET, response)
        raise HTTPException(status_code=404, detail=response["message"])

    value, actual_version = result

    # Create success response
    response = {"key": key, "value": value, "version": actual_version}

    # Cache the response if client tracking is enabled
    _cache_response_if_needed(client_id, request_id, key, OperationType.GET, response)

    return response


def _check_request_cache(client_id: Optional[str], request_id: Optional[str], key: str, operation: OperationType) -> Optional[Dict]:
    """Check if this is a duplicate request with a cached response"""
    if not client_id or not request_id:
        logger.info(f"GET request for key={key} (no client ID)")
        return None

    logger.info(f"GET request for key={key} from client={client_id}, request={request_id}")
    previous_response = request_deduplication.get_processed_result(client_id, request_id, operation)

    if previous_response is not None:
        logger.info(f"✅ Returning cached response for GET key={key}, client={client_id}, request={request_id}")
        return previous_response

    return None


def _get_value_from_storage(key: str, version: Optional[int] = None) -> Optional[Tuple[Any, int]]:
    """Get a value and its version from storage"""
    return storage.get_with_version(key, version)


def _cache_response_if_needed(
    client_id: Optional[str], request_id: Optional[str], key: str, operation: OperationType, response: Dict
) -> None:
    """Cache the response if client tracking is enabled"""
    if not client_id or not request_id:
        return

    client_request = ClientRequest(client_id=client_id, request_id=request_id, operation=operation, key=key)
    request_deduplication.mark_request_processed(client_request, response)

    status = "error" if "status" in response and response["status"] == "error" else "success"
    logger.info(f"Cached {status} response for GET key={key}, client={client_id}, request={request_id}")


@app.get("/status")
def get_status():
    return {"follower_id": follower_id, "last_applied_id": last_applied_id, "leader_url": leader_url}


@app.get("/segments")
def get_segments():
    """Return information about the WAL segments"""
    segments = wal.get_segment_files()
    active_segment = wal.get_active_segment()

    segment_info = []
    for segment in segments:
        try:
            size = os.path.getsize(segment)
            segment_info.append({"path": segment, "size": size, "is_active": segment == active_segment})
        except FileNotFoundError:
            pass

    return {"segments": segment_info, "total_segments": len(segment_info), "max_segment_size": MAX_SEGMENT_SIZE}


@app.get("/keys")
def get_all_keys():
    """Return all keys in the storage"""
    keys = storage.get_all_keys()
    return {"keys": keys, "count": len(keys)}


@app.get("/request_status")
def get_request_status(client_id: str, request_id: str, operation: Optional[str] = Query(None)):
    """Check if a client request has been processed"""
    logger.info(f"Checking status for client={client_id}, request={request_id}, operation={operation}")
    # Pass operation type to get_processed_result if provided
    result = request_deduplication.get_processed_result(client_id, request_id, operation)
    if result:
        logger.info(f"Found cached result for client={client_id}, request={request_id}, operation={operation}")
        return {"processed": True, "result": result}
    else:
        logger.info(f"No cached result found for client={client_id}, request={request_id}, operation={operation}")
        return {"processed": False}


@app.get("/deduplication_stats")
def get_deduplication_stats():
    """Return statistics about the request deduplication service"""
    stats = request_deduplication.get_stats()
    logger.info(f"Returning deduplication stats: duplicates detected={stats['total_duplicates_detected']}")
    return stats


@app.post("/heartbeat")
def receive_heartbeat(data: dict):
    """Handle heartbeat from leader or other servers"""
    server_id = data.get("server_id")
    timestamp = data.get("timestamp", time.time())

    if not server_id:
        return {"status": "error", "message": "Missing server_id"}

    heartbeat_service.record_heartbeat(server_id)
    logger.debug(f"Received heartbeat from {server_id} at {timestamp}")

    return {"status": "ok", "server_id": follower_id, "timestamp": time.time()}


@app.get("/cluster_status")
def get_cluster_status():
    """Get status of the leader from this follower's perspective"""
    leader_status = heartbeat_service.get_server_status("leader")

    return {
        "follower": {"id": follower_id, "url": follower_url, "status": "healthy"},  # Follower always reports itself as healthy
        "leader": leader_status,
        "heartbeat_interval": HEARTBEAT_INTERVAL,
    }


@app.get("/key/{key}/history")
def get_key_history(key: str):
    """Get the version history of a key"""
    history = storage.get_version_history(key)

    if history is None:
        raise HTTPException(status_code=404, detail=f"Key not found: {key}")

    versions = sorted(history.keys())

    return {"key": key, "versions": versions, "history": [{"version": v, "value": history[v]} for v in versions]}


@app.get("/key/{key}/versions")
def get_key_versions(key: str):
    """Get available versions for a key"""
    history = storage.get_version_history(key)

    if history is None:
        raise HTTPException(status_code=404, detail=f"Key not found: {key}")

    return {"key": key, "versions": sorted(history.keys()), "latest_version": storage.get_latest_version(key)}


@app.post("/compaction/run")
async def run_compaction(force: bool = Query(False, description="Force compaction even if minimum interval hasn't passed")):
    """Manually trigger log compaction"""
    try:
        segments_compacted, entries_removed = await compaction_service.run_compaction(force=force)
        return {
            "status": "success",
            "segments_compacted": segments_compacted,
            "entries_removed": entries_removed,
        }
    except Exception as e:
        logger.error(f"Error during manual compaction: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Compaction error: {str(e)}") from e


@app.get("/compaction/status")
def get_compaction_status():
    """Get the status of the log compaction service"""
    return compaction_service.get_status()


@app.post("/compaction/configure")
def configure_compaction(
    enabled: Optional[bool] = Query(None, description="Enable or disable compaction"),
    interval: Optional[int] = Query(None, description="Compaction interval in seconds"),
):
    """Configure the log compaction service"""
    changes = {}

    if enabled is not None:
        result = compaction_service.set_enabled(enabled)
        changes["enabled"] = result

    if interval is not None:
        result = compaction_service.set_compaction_interval(interval)
        changes["interval"] = result

    return {"status": "success", "changes": changes}
