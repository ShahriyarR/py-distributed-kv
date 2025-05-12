import asyncio
import logging
import time
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from pydistributedkv.service.storage import KeyValueStorage

logger = logging.getLogger(__name__)


class LogCompactionService:
    """Service to handle periodic log compaction"""

    def __init__(
        self,
        storage: KeyValueStorage,
        compaction_interval: int = 3600,  # Default: run compaction every hour
        min_compaction_interval: int = 600,  # Minimum time between compactions (10 minutes)
        enabled: bool = True,
    ):
        self.storage = storage
        self.compaction_interval = compaction_interval
        self.min_compaction_interval = min_compaction_interval
        self.enabled = enabled
        self.last_compaction: Optional[datetime] = None
        self.compaction_running = False
        self.compaction_task = None
        self.compaction_history: List[dict] = []

    async def start(self) -> None:
        """Start the compaction service"""
        if not self._can_start():
            return

        self.compaction_task = asyncio.create_task(self._compaction_loop())
        logger.info(f"Started log compaction service (interval: {self.compaction_interval}s)")

    def _can_start(self) -> bool:
        """Check if the service can start"""
        if self.compaction_task is not None:
            logger.warning("Compaction service already running")
            return False

        if not self.enabled:
            logger.info("Compaction service is disabled")
            return False

        return True

    async def stop(self) -> None:
        """Stop the compaction service"""
        if self.compaction_task:
            self.compaction_task.cancel()
            try:
                await self.compaction_task
            except asyncio.CancelledError:
                pass
            self.compaction_task = None
            logger.info("Stopped log compaction service")

    async def _compaction_loop(self) -> None:
        """Main compaction loop that runs periodically"""
        while True:
            try:
                # Sleep first to avoid immediate compaction on startup
                await asyncio.sleep(self.compaction_interval)
                await self._run_if_not_running()
            except asyncio.CancelledError:
                logger.info("Compaction loop cancelled")
                break
            except Exception as e:
                self._handle_loop_error(e)

    async def _run_if_not_running(self) -> None:
        """Run compaction if not already running"""
        if not self.compaction_running:
            await self.run_compaction()

    def _handle_loop_error(self, error: Exception) -> None:
        """Handle errors in the compaction loop"""
        logger.error(f"Error in compaction loop: {str(error)}")

    async def run_compaction(self, force: bool = False) -> Tuple[int, int]:
        """Run log compaction process

        Args:
            force: If True, runs compaction even if minimum interval hasn't passed

        Returns:
            Tuple of (segments_compacted, entries_removed)
        """
        # Early returns for conditions where compaction should be skipped
        if self.compaction_running:
            logger.warning("Compaction already in progress, skipping")
            return 0, 0

        if not force and self._is_too_soon_for_compaction():
            return 0, 0

        return await self._execute_compaction()

    def _is_too_soon_for_compaction(self) -> bool:
        """Check if it's too soon since the last compaction"""
        if not self.last_compaction:
            return False

        now = datetime.now()
        time_since_last = (now - self.last_compaction).total_seconds()

        if time_since_last < self.min_compaction_interval:
            logger.info(f"Skipping compaction, last run was {time_since_last:.1f}s ago " f"(min interval: {self.min_compaction_interval}s)")
            return True

        return False

    async def _execute_compaction(self) -> Tuple[int, int]:
        """Execute the actual compaction process"""
        self.compaction_running = True
        start_time = time.time()
        now = datetime.now()

        try:
            logger.info("Starting log compaction")
            result = self.storage.compact_log()
            self._record_compaction_result(result, start_time, now)
            return result
        except Exception as e:
            logger.error(f"Error during compaction: {str(e)}")
            raise
        finally:
            self.compaction_running = False

    def _record_compaction_result(self, result: Tuple[int, int], start_time: float, timestamp: datetime) -> None:
        """Record the result of a compaction operation"""
        segments_compacted, entries_removed = result
        duration = time.time() - start_time
        self.last_compaction = timestamp

        compaction_data = {
            "timestamp": timestamp.isoformat(),
            "duration_seconds": duration,
            "segments_compacted": segments_compacted,
            "entries_removed": entries_removed,
        }

        self._update_compaction_history(compaction_data)

        logger.info(
            f"Compaction completed in {duration:.2f}s: " f"compacted {segments_compacted} segments, removed {entries_removed} entries"
        )

    def _update_compaction_history(self, compaction_data: Dict[str, Any]) -> None:
        """Update the compaction history, keeping only the latest entries"""
        self.compaction_history.append(compaction_data)
        if len(self.compaction_history) > 10:  # Keep only last 10 entries
            self.compaction_history.pop(0)

    def get_status(self) -> dict:
        """Get the current status of the compaction service"""
        return {
            "enabled": self.enabled,
            "compaction_interval_seconds": self.compaction_interval,
            "min_compaction_interval_seconds": self.min_compaction_interval,
            "last_compaction": self.last_compaction.isoformat() if self.last_compaction else None,
            "compaction_running": self.compaction_running,
            "compaction_history": self.compaction_history,
        }

    def set_enabled(self, enabled: bool) -> bool:
        """Enable or disable the compaction service"""
        self.enabled = enabled
        return self.enabled

    def set_compaction_interval(self, interval: int) -> int:
        """Set the compaction interval in seconds"""
        if interval < 60:
            interval = 60  # Minimum 1 minute
        self.compaction_interval = interval
        return self.compaction_interval
