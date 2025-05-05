import os
import tempfile
from unittest.mock import patch

import pytest

from pydistributedkv.domain.models import LogEntry, OperationType, VersionedValue, WAL
from pydistributedkv.service.storage import KeyValueStorage


class TestStorageVersioning:
    @pytest.fixture
    def wal(self):
        """Create a temporary WAL for testing"""
        with tempfile.TemporaryDirectory() as tmp_dir:
            wal_path = os.path.join(tmp_dir, "test_wal.log")
            yield WAL(wal_path)

    @pytest.fixture
    def storage(self, wal):
        """Create a storage with the test WAL"""
        return KeyValueStorage(wal)

    def test_set_new_key(self, storage):
        """Test setting a new key creates version 1"""
        entry, version = storage.set("key1", "value1")

        assert version == 1
        assert storage.get("key1") == "value1"
        assert storage.get_latest_version("key1") == 1

    def test_set_existing_key_increments_version(self, storage):
        """Test setting an existing key increments the version"""
        storage.set("key1", "value1")
        entry, version = storage.set("key1", "value2")

        assert version == 2
        assert storage.get("key1") == "value2"
        assert storage.get_latest_version("key1") == 2

    def test_setting_specific_version_for_new_key(self, storage):
        """Test setting a specific version for a new key"""
        entry, version = storage.set("key1", "value1", version=5)

        assert version == 5
        assert storage.get("key1") == "value1"
        assert storage.get_latest_version("key1") == 5

    def test_version_conflict_returns_none(self, storage):
        """Test setting with an outdated version returns None and current version"""
        storage.set("key1", "value1")  # Version 1
        storage.set("key1", "value2")  # Version 2

        # Try to set with version 1 which is outdated
        entry, version = storage.set("key1", "value3", version=1)

        assert entry is None
        assert version == 2  # Current version
        assert storage.get("key1") == "value2"  # Value unchanged

    def test_get_with_version(self, storage):
        """Test getting a specific version of a key"""
        storage.set("key1", "value1")  # Version 1
        storage.set("key1", "value2")  # Version 2
        storage.set("key1", "value3")  # Version 3

        assert storage.get("key1", version=1) == "value1"
        assert storage.get("key1", version=2) == "value2"
        assert storage.get("key1", version=3) == "value3"
        assert storage.get("key1") == "value3"  # Latest version
        assert storage.get("key1", version=4) is None  # Non-existent version

    def test_get_with_version_returns_tuple(self, storage):
        """Test get_with_version returns both value and version"""
        storage.set("key1", "value1")  # Version 1
        storage.set("key1", "value2")  # Version 2

        result = storage.get_with_version("key1", version=1)
        assert result == ("value1", 1)

        result = storage.get_with_version("key1")  # Latest version
        assert result == ("value2", 2)

    def test_get_version_history(self, storage):
        """Test getting the version history of a key"""
        storage.set("key1", "value1")  # Version 1
        storage.set("key1", "value2")  # Version 2
        storage.set("key1", "value3")  # Version 3

        history = storage.get_version_history("key1")

        assert history == {1: "value1", 2: "value2", 3: "value3"}

    def test_get_version_history_nonexistent_key(self, storage):
        """Test getting history for a non-existent key returns None"""
        assert storage.get_version_history("nonexistent") is None

    def test_replay_log_with_versions(self, wal):
        """Test replaying a WAL with versioned entries"""
        # Create entries in the WAL
        wal.append(OperationType.SET, "key1", "value1", version=1)
        wal.append(OperationType.SET, "key1", "value2", version=2)
        wal.append(OperationType.SET, "key2", "value-a", version=1)

        # Create a new storage that will replay the WAL
        storage = KeyValueStorage(wal)

        assert storage.get("key1") == "value2"
        assert storage.get_latest_version("key1") == 2

        assert storage.get("key2") == "value-a"
        assert storage.get_latest_version("key2") == 1

        # Check version histories
        assert storage.get_version_history("key1") == {1: "value1", 2: "value2"}
        assert storage.get_version_history("key2") == {1: "value-a"}

    def test_delete_removes_all_versions(self, storage):
        """Test that delete removes a key and all its versions"""
        storage.set("key1", "value1")  # Version 1
        storage.set("key1", "value2")  # Version 2

        entry = storage.delete("key1")

        assert entry is not None
        assert storage.get("key1") is None
        assert storage.get_version_history("key1") is None
        assert storage.get_latest_version("key1") is None
