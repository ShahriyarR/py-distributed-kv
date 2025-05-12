import asyncio
import os
import tempfile
import time

import pytest

from pydistributedkv.domain.models import OperationType, WAL
from pydistributedkv.service.compaction import LogCompactionService
from pydistributedkv.service.storage import KeyValueStorage


@pytest.fixture
def temp_dir():
    """Create a temporary directory for test files"""
    with tempfile.TemporaryDirectory() as tmpdirname:
        yield tmpdirname


@pytest.fixture
def wal(temp_dir):
    """Create a WAL with a small segment size for testing"""
    log_path = os.path.join(temp_dir, "test_log")
    # Use a very small segment size to trigger multiple segments
    return WAL(log_path, max_segment_size=100)


@pytest.fixture
def storage(wal):
    """Create a storage with the test WAL"""
    return KeyValueStorage(wal)


@pytest.fixture
def compaction_service(storage):
    """Create a compaction service with test settings"""
    return LogCompactionService(
        storage=storage,
        compaction_interval=0.5,  # Run every 0.5 seconds
        min_compaction_interval=0.1,  # Allow compaction after 0.1 seconds
        enabled=True,
    )


class TestCompactionIntegration:

    @pytest.mark.asyncio
    async def test_compaction_with_real_storage(self, compaction_service, storage):
        """Test that compaction works with real storage and WAL"""
        # Add many entries to create multiple segments
        for i in range(100):
            key = f"key_{i}"
            value = f"value_{i}"
            storage.set(key, value)

            # Overwrite some keys to create redundancy
            if i % 3 == 0:  # Every third key gets overwritten
                storage.set(key, f"updated_{value}")

            # Delete some keys
            if i % 7 == 0:  # Every seventh key gets deleted
                storage.delete(key)

        # Check we have data and segments
        assert len(storage.get_all_keys()) > 0
        segments_before = len(storage.wal.get_segment_files())
        assert segments_before > 1, "Test needs multiple segments to be meaningful"

        # Run compaction manually
        segments_compacted, entries_removed = await compaction_service.run_compaction(force=True)

        # Verify compaction results
        assert segments_compacted > 0, "No segments were compacted"
        assert entries_removed > 0, "No entries were removed"

        # Check history was updated
        assert len(compaction_service.compaction_history) == 1
        assert compaction_service.compaction_history[0]["segments_compacted"] == segments_compacted
        assert compaction_service.compaction_history[0]["entries_removed"] == entries_removed

        # Verify status returns correct information
        status = compaction_service.get_status()
        assert status["compaction_history"][0]["segments_compacted"] == segments_compacted

        # Verify data integrity - all accessible keys should still have correct values
        for key in storage.get_all_keys():
            if key.startswith("key_"):
                i = int(key.split("_")[1])
                expected_value = None

                if i % 7 == 0:
                    # These keys should have been deleted
                    continue
                elif i % 3 == 0:
                    # These keys should have updated values
                    expected_value = f"updated_value_{i}"
                else:
                    # These keys should have original values
                    expected_value = f"value_{i}"

                actual_value = storage.get(key)
                assert actual_value == expected_value, f"Key {key} has incorrect value"

    @pytest.mark.asyncio
    async def test_compaction_service_lifecycle(self, compaction_service):
        """Test the full lifecycle of a compaction service"""
        # Start the service
        await compaction_service.start()

        # Verify it's running
        assert compaction_service.compaction_task is not None

        # Let it run for a bit to complete at least one compaction cycle
        await asyncio.sleep(1.0)

        # Stop the service
        await compaction_service.stop()

        # Verify it's stopped
        assert compaction_service.compaction_task is None

        # Check if at least one compaction happened
        assert len(compaction_service.compaction_history) > 0
