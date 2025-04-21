# test_distributed_kv.py
import os
import shutil
import signal
import subprocess
import tempfile
import time
from pathlib import Path

import pytest
import requests

from pydistributedkv.domain.models import OperationType, WAL
from pydistributedkv.service.storage import KeyValueStorage


# Test the WAL component
def test_wal():
    temp_dir = tempfile.mkdtemp()
    try:
        log_path = os.path.join(temp_dir, "test_wal.log")

        # Import here to avoid import errors

        wal = WAL(log_path)

        # Test appending entries
        entry1 = wal.append(OperationType.SET, "key1", "value1")
        assert entry1.id == 1
        assert entry1.operation == OperationType.SET
        assert entry1.key == "key1"
        assert entry1.value == "value1"

        entry2 = wal.append(OperationType.SET, "key2", "value2")
        assert entry2.id == 2

        entry3 = wal.append(OperationType.DELETE, "key1")
        assert entry3.id == 3
        assert entry3.operation == OperationType.DELETE
        assert entry3.key == "key1"

        # Test reading entries
        entries = wal.read_from(0)
        assert len(entries) == 3

        entries = wal.read_from(2)
        assert len(entries) == 2
        assert entries[0].id == 2

        # Test persistence and reloading
        wal2 = WAL(log_path)
        assert wal2.get_last_id() == 3

        entries = wal2.read_from(0)
        assert len(entries) == 3
    finally:
        shutil.rmtree(temp_dir)


# Test the storage component
def test_storage():
    temp_dir = tempfile.mkdtemp()
    try:
        log_path = os.path.join(temp_dir, "test_storage.log")

        wal = WAL(log_path)
        storage = KeyValueStorage(wal)

        # Test basic operations
        storage.set("key1", "value1")
        assert storage.get("key1") == "value1"

        storage.set("key2", {"nested": "value"})
        assert storage.get("key2") == {"nested": "value"}

        storage.delete("key1")
        assert storage.get("key1") is None

        # Test durability: recreate the storage with the same WAL
        storage2 = KeyValueStorage(wal)
        assert storage2.get("key1") is None
        assert storage2.get("key2") == {"nested": "value"}
    finally:
        shutil.rmtree(temp_dir)
