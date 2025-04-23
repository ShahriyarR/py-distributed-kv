import json
import os
import shutil
import tempfile

from pydistributedkv.domain.models import LogEntry, OperationType, WAL


def test_log_entry_crc_calculation():
    # Create a log entry and verify CRC calculation works
    entry = LogEntry(id=1, operation=OperationType.SET, key="test_key", value="test_value")

    # Initially CRC is None
    assert entry.crc is None

    # Calculate CRC
    crc = entry.calculate_crc()
    assert isinstance(crc, int)

    # Set CRC and validate
    entry.crc = crc
    assert entry.validate_crc() is True

    # Modify entry and verify CRC becomes invalid
    entry.value = "modified_value"
    assert entry.validate_crc() is False

    # Recalculate CRC after modification
    new_crc = entry.calculate_crc()
    assert new_crc != crc

    entry.crc = new_crc
    assert entry.validate_crc() is True


def test_wal_skips_invalid_crc():
    temp_dir = tempfile.mkdtemp()
    try:
        log_path = os.path.join(temp_dir, "wal.log")

        # Create a WAL and add some entries
        wal = WAL(log_path)
        entry1 = wal.append(OperationType.SET, "key1", "value1")
        entry2 = wal.append(OperationType.SET, "key2", "value2")

        # Get the actual segment file path that WAL is using
        active_segment = wal.get_active_segment()

        # Manually corrupt the second entry in the log file
        with open(active_segment, "r") as f:
            lines = f.readlines()

        # Parse the second entry, modify its value but keep the old CRC
        corrupted_entry = json.loads(lines[1])
        corrupted_entry["value"] = "corrupted_value"

        # Write back the corrupted log
        with open(active_segment, "w") as f:
            f.write(lines[0])  # Write the first entry unchanged
            f.write(json.dumps(corrupted_entry) + "\n")

        # Create a new WAL instance to load from the corrupted file
        wal2 = WAL(log_path)

        # Only the valid entry should be loaded
        assert wal2.get_last_id() == 1
        assert 1 in wal2.existing_ids
        assert 2 not in wal2.existing_ids

        # Reading entries should only return the valid one
        entries = wal2.read_from(0)
        assert len(entries) == 1
        assert entries[0].id == 1

    finally:
        shutil.rmtree(temp_dir)


def test_wal_handles_missing_crc():
    temp_dir = tempfile.mkdtemp()
    try:
        log_path = os.path.join(temp_dir, "wal.log")

        # Create a WAL to initialize the segment structure
        wal = WAL(log_path)
        active_segment = wal.get_active_segment()

        # Clear the segment file to start fresh
        open(active_segment, "w").close()

        # Manually create a log file with an entry missing CRC
        entry_without_crc = {"id": 1, "operation": "SET", "key": "test_key", "value": "test_value"}

        with open(active_segment, "w") as f:
            f.write(json.dumps(entry_without_crc) + "\n")

        # Load the WAL and verify it skips the entry without CRC
        wal = WAL(log_path)

        # The entry should be loaded despite missing CRC (legacy compatibility)
        assert wal.get_last_id() == 1
        assert 1 in wal.existing_ids

    finally:
        shutil.rmtree(temp_dir)


def test_append_entry_recalculates_invalid_crc():
    temp_dir = tempfile.mkdtemp()
    try:
        log_path = os.path.join(temp_dir, "wal.log")
        wal = WAL(log_path)

        # Create an entry with an invalid CRC
        entry = LogEntry(id=1, operation=OperationType.SET, key="key1", value="value1", crc=12345)

        # Append the entry - WAL should recalculate the CRC
        appended_entry = wal.append_entry(entry)

        # The entry should have a valid CRC now
        assert appended_entry.validate_crc() is True
        assert appended_entry.crc != 12345  # CRC should be recalculated

        # Load the WAL again to verify the entry was stored with valid CRC
        wal2 = WAL(log_path)
        entries = wal2.read_from(0)

        assert len(entries) == 1
        assert entries[0].validate_crc() is True

    finally:
        shutil.rmtree(temp_dir)


def test_duplicate_entry_handling():
    temp_dir = tempfile.mkdtemp()
    try:
        log_path = os.path.join(temp_dir, "test_duplicate.log")
        wal = WAL(log_path)

        # Add an entry
        original_entry = wal.append(OperationType.SET, "key1", "value1")

        # Try to add a duplicate entry with the same ID but different content
        duplicate_entry = LogEntry(id=1, operation=OperationType.SET, key="duplicate_key", value="duplicate_value")
        duplicate_entry.crc = duplicate_entry.calculate_crc()

        # WAL should ignore the duplicate entry
        wal.append_entry(duplicate_entry)

        # Verify the original entry wasn't replaced
        entries = wal.read_from(0)
        assert len(entries) == 1
        assert entries[0].key == "key1"
        assert entries[0].value == "value1"

    finally:
        shutil.rmtree(temp_dir)


def test_corrupted_json_handling():
    temp_dir = tempfile.mkdtemp()
    try:
        log_path = os.path.join(temp_dir, "test_corrupted_json.log")

        # Create a WAL with some valid entries
        wal = WAL(log_path)
        wal.append(OperationType.SET, "key1", "value1")
        wal.append(OperationType.SET, "key2", "value2")

        # Get the active segment
        active_segment = wal.get_active_segment()

        # Append some corrupted JSON to the log file
        with open(active_segment, "a") as f:
            f.write("{this is not valid JSON}\n")

            # Add a valid entry after the corrupted one - the WAL should process this correctly
            valid_entry = LogEntry(id=3, operation=OperationType.SET, key="key3", value="value3")
            valid_entry.crc = valid_entry.calculate_crc()
            f.write(json.dumps(valid_entry.model_dump()) + "\n")

        # Load the WAL again
        wal2 = WAL(log_path)

        # It should have loaded all valid entries (including entry 3) and skipped the corrupted one
        valid_entries = wal2.read_from(0)
        assert len(valid_entries) == 3
        assert valid_entries[0].id == 1
        assert valid_entries[1].id == 2
        assert valid_entries[2].id == 3

    finally:
        shutil.rmtree(temp_dir)
