import asyncio
import datetime
import logging
from unittest import mock

import pytest

from pydistributedkv.domain.models import WAL
from pydistributedkv.service.compaction import LogCompactionService
from pydistributedkv.service.storage import KeyValueStorage


@pytest.fixture
def mock_storage():
    """Create a mock storage object"""
    storage = mock.MagicMock(spec=KeyValueStorage)
    storage.compact_log.return_value = (3, 100)  # Default: 3 segments compacted, 100 entries removed
    return storage


@pytest.fixture
def compaction_service(mock_storage):
    """Create a compaction service with a mock storage"""
    return LogCompactionService(
        storage=mock_storage,
        compaction_interval=1,  # Use short interval for testing
        min_compaction_interval=0.1,  # Use short min interval for testing
        enabled=True,
    )


class TestLogCompactionService:
    def test_initialization(self, mock_storage):
        """Test that the service initializes with correct parameters"""
        # Default initialization
        service = LogCompactionService(storage=mock_storage)
        assert service.storage == mock_storage
        assert service.compaction_interval == 3600  # Default: 1 hour
        assert service.min_compaction_interval == 600  # Default: 10 minutes
        assert service.enabled is True
        assert service.last_compaction is None
        assert service.compaction_running is False
        assert service.compaction_task is None
        assert service.compaction_history == []

        # Custom initialization
        service = LogCompactionService(storage=mock_storage, compaction_interval=300, min_compaction_interval=60, enabled=False)
        assert service.compaction_interval == 300
        assert service.min_compaction_interval == 60
        assert service.enabled is False

    def test_set_enabled(self, compaction_service):
        """Test enabling and disabling the service"""
        # Initially enabled
        assert compaction_service.enabled is True

        # Disable
        result = compaction_service.set_enabled(False)
        assert result is False
        assert compaction_service.enabled is False

        # Enable
        result = compaction_service.set_enabled(True)
        assert result is True
        assert compaction_service.enabled is True

    def test_set_compaction_interval(self, compaction_service):
        """Test setting the compaction interval"""
        # Initial interval
        assert compaction_service.compaction_interval == 1

        # Set to 120 seconds
        result = compaction_service.set_compaction_interval(120)
        assert result == 120
        assert compaction_service.compaction_interval == 120

        # Test minimum limit (should be clamped to 60)
        result = compaction_service.set_compaction_interval(30)
        assert result == 60
        assert compaction_service.compaction_interval == 60

    @pytest.mark.asyncio
    async def test_start_already_running(self, compaction_service):
        """Test starting the service when it's already running"""
        # Mock the compaction task
        compaction_service.compaction_task = mock.MagicMock()

        # Try to start it again - use the module-level logger from the compaction service module
        with mock.patch("pydistributedkv.service.compaction.logger.warning") as mock_warning:
            await compaction_service.start()
            mock_warning.assert_called_once_with("Compaction service already running")

        # Task should not have been changed
        assert compaction_service.compaction_task is not None

    @pytest.mark.asyncio
    async def test_start_disabled(self, compaction_service):
        """Test starting the service when it's disabled"""
        compaction_service.enabled = False

        # Fix: Use the module-level logger from the compaction service module
        with mock.patch("pydistributedkv.service.compaction.logger.info") as mock_info:
            await compaction_service.start()
            mock_info.assert_called_once_with("Compaction service is disabled")

        # No task should be created
        assert compaction_service.compaction_task is None

    @pytest.mark.asyncio
    async def test_start_and_stop(self, compaction_service):
        """Test starting and stopping the compaction service"""
        # Start the service
        with mock.patch("pydistributedkv.service.compaction.logger.info") as mock_info:
            await compaction_service.start()
            mock_info.assert_called_with(f"Started log compaction service (interval: {compaction_service.compaction_interval}s)")

        # Task should have been created
        assert compaction_service.compaction_task is not None

        # Stop the service
        with mock.patch("pydistributedkv.service.compaction.logger.info") as mock_info:
            await compaction_service.stop()
            mock_info.assert_called_with("Stopped log compaction service")

        # Task should be None after stopping
        assert compaction_service.compaction_task is None

    @pytest.mark.asyncio
    async def test_compaction_loop(self, compaction_service):
        """Test the compaction loop logic"""
        # Mock the run_compaction method
        compaction_service.run_compaction = mock.AsyncMock()

        # Create a mock task that will be cancelled
        async def mock_loop():
            try:
                # Sleep first
                await asyncio.sleep(compaction_service.compaction_interval)
                # Run compaction
                await compaction_service.run_compaction()
                # Raise CancelledError to simulate the loop being cancelled
                raise asyncio.CancelledError()
            except asyncio.CancelledError:
                raise

        # Patch the _compaction_loop method to use our mock
        with mock.patch.object(compaction_service, "_compaction_loop", mock_loop):
            # Start the service
            await compaction_service.start()
            # Let the mock loop run
            await asyncio.sleep(compaction_service.compaction_interval * 1.5)
            # Stop the service
            await compaction_service.stop()

        # Check if run_compaction was called
        compaction_service.run_compaction.assert_called_once()

    @pytest.mark.asyncio
    async def test_run_compaction_already_running(self, compaction_service):
        """Test that run_compaction skips when already running"""
        # Set compaction_running to True
        compaction_service.compaction_running = True

        # Use the module-level logger instead of the global logging module
        with mock.patch("pydistributedkv.service.compaction.logger.warning") as mock_warning:
            result = await compaction_service.run_compaction()
            mock_warning.assert_called_once_with("Compaction already in progress, skipping")

        # Should return (0, 0) without doing anything
        assert result == (0, 0)

    @pytest.mark.asyncio
    async def test_run_compaction_too_soon(self, compaction_service):
        """Test that run_compaction skips when run too soon after the last run"""
        # Set last compaction time to now
        compaction_service.last_compaction = datetime.datetime.now()

        # Use the module-level logger instead of the global logging module
        with mock.patch("pydistributedkv.service.compaction.logger.info") as mock_info:
            result = await compaction_service.run_compaction()
            # Check that appropriate message was logged
            assert "Skipping compaction" in mock_info.call_args[0][0]

        # Should return (0, 0) without doing anything
        assert result == (0, 0)

    @pytest.mark.asyncio
    async def test_run_compaction_force(self, compaction_service):
        """Test that run_compaction with force=True ignores minimum interval"""
        # Set last compaction time to now
        compaction_service.last_compaction = datetime.datetime.now()

        # Ensure storage.compact_log returns a known value
        mock_result = (5, 200)  # 5 segments, 200 entries
        compaction_service.storage.compact_log.return_value = mock_result

        result = await compaction_service.run_compaction(force=True)

        # Should call compact_log and return its result
        compaction_service.storage.compact_log.assert_called_once()
        assert result == mock_result

        # Should update history
        assert len(compaction_service.compaction_history) == 1
        assert compaction_service.compaction_history[0]["segments_compacted"] == mock_result[0]
        assert compaction_service.compaction_history[0]["entries_removed"] == mock_result[1]

    @pytest.mark.asyncio
    async def test_run_compaction_normal(self, compaction_service):
        """Test normal compaction run"""
        # No previous compaction
        assert compaction_service.last_compaction is None

        # Ensure storage.compact_log returns a known value
        mock_result = (2, 50)  # 2 segments, 50 entries
        compaction_service.storage.compact_log.return_value = mock_result

        # Use the module-level logger instead of the global logging module
        with mock.patch("pydistributedkv.service.compaction.logger.info") as mock_info:
            result = await compaction_service.run_compaction()

            # Check that start and completion logs were made
            log_messages = [call[0][0] for call in mock_info.call_args_list]
            assert any("Starting log compaction" in msg for msg in log_messages)
            assert any("Compaction completed" in msg for msg in log_messages)

        # Should return storage.compact_log result
        assert result == mock_result

        # Should have updated last_compaction
        assert compaction_service.last_compaction is not None

        # Should have updated history
        assert len(compaction_service.compaction_history) == 1
        assert compaction_service.compaction_history[0]["segments_compacted"] == mock_result[0]
        assert compaction_service.compaction_history[0]["entries_removed"] == mock_result[1]
        assert "duration_seconds" in compaction_service.compaction_history[0]
        assert "timestamp" in compaction_service.compaction_history[0]

    @pytest.mark.asyncio
    async def test_run_compaction_error(self, compaction_service):
        """Test compaction run with error"""
        # Make storage.compact_log raise an exception
        compaction_service.storage.compact_log.side_effect = ValueError("Test error")

        # Should propagate the error
        with pytest.raises(ValueError, match="Test error"):
            await compaction_service.run_compaction()

        # compaction_running should be reset to False
        assert compaction_service.compaction_running is False

        # No history should be added
        assert len(compaction_service.compaction_history) == 0

    @pytest.mark.asyncio
    async def test_compaction_history_limit(self, compaction_service):
        """Test compaction history is limited to 10 entries"""
        # Make 12 compactions
        for i in range(12):
            compaction_service.storage.compact_log.return_value = (i, i * 10)
            await compaction_service.run_compaction(force=True)

        # History should be limited to 10 entries
        assert len(compaction_service.compaction_history) == 10

        # The oldest 2 entries should have been removed
        assert compaction_service.compaction_history[0]["segments_compacted"] == 2
        assert compaction_service.compaction_history[0]["entries_removed"] == 20

    def test_get_status(self, compaction_service):
        """Test getting status information"""
        # Set some data
        compaction_service.last_compaction = datetime.datetime(2023, 1, 1, 12, 0, 0)
        compaction_service.compaction_history.append(
            {"timestamp": "2023-01-01T12:00:00", "duration_seconds": 0.5, "segments_compacted": 3, "entries_removed": 100}
        )

        status = compaction_service.get_status()

        assert status["enabled"] == compaction_service.enabled
        assert status["compaction_interval_seconds"] == compaction_service.compaction_interval
        assert status["min_compaction_interval_seconds"] == compaction_service.min_compaction_interval
        assert status["last_compaction"] == "2023-01-01T12:00:00"
        assert status["compaction_running"] == compaction_service.compaction_running
        assert len(status["compaction_history"]) == 1
