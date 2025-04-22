import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from pydistributedkv.domain.models import OperationType, WAL
from pydistributedkv.service.storage import KeyValueStorage


class TestStorageWithSegmentedWAL(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test WAL files
        self.temp_dir = tempfile.mkdtemp()
        self.wal_path = os.path.join(self.temp_dir, "wal.log")

        # Use a small segment size for testing
        self.small_segment_size = 200  # bytes

    def tearDown(self):
        # Clean up the temporary directory after tests
        shutil.rmtree(self.temp_dir)

    def test_storage_replay_after_restart(self):
        """Test that storage correctly rebuilds state after restart with segmented WAL"""
        # Create initial storage with some data
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)
        storage = KeyValueStorage(wal)

        # Add enough data to span multiple segments
        test_data = {}
        for i in range(20):
            key = f"test-key-{i}"
            value = f"test-value-{i}" * 3  # Make the value larger to reach segment limit faster
            storage.set(key, value)
            test_data[key] = value

        # Delete some keys to verify delete operations are also replayed
        for i in range(0, 20, 4):  # Delete every 4th key
            key = f"test-key-{i}"
            storage.delete(key)
            if key in test_data:
                del test_data[key]

        # Verify we have multiple segments
        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 1, "Expected multiple segments for this test")

        # Create a new storage instance that will replay the log
        new_wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Create a new storage with the existing WAL
        with patch("builtins.print"):  # suppress print statements during replay
            new_storage = KeyValueStorage(new_wal)

        # Verify all data was correctly replayed
        for key, expected_value in test_data.items():
            self.assertEqual(new_storage.get(key), expected_value, f"Replayed value for {key} doesn't match expected value")

        # Verify deleted keys are not present
        for i in range(0, 20, 4):
            self.assertIsNone(new_storage.get(f"test-key-{i}"), f"Key test-key-{i} should have been deleted")

    def test_storage_with_complex_operations(self):
        """Test storage with complex operations across multiple segments"""
        # Create a WAL with small segment size
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)
        storage = KeyValueStorage(wal)

        # Test complex data types
        complex_data = [
            {"key": "dict-key", "value": {"nested": "value", "list": [1, 2, 3], "number": 42}},
            {"key": "list-key", "value": [1, "string", {"nested": "object"}, None]},
            {"key": "number-key", "value": 12345.6789},
            {"key": "bool-key", "value": True},
            {"key": "null-key", "value": None},
            {"key": "string-key", "value": "This is a longer string value that should take up more space in the log."},
        ]

        # Set the values
        expected_data = {}
        for item in complex_data:
            storage.set(item["key"], item["value"])
            expected_data[item["key"]] = item["value"]

        # Update some values to create more log entries
        for i in range(10):
            storage.set("counter", i)
            expected_data["counter"] = i

        # Delete and recreate a key multiple times
        for i in range(5):
            storage.set("temp-key", f"temp-value-{i}")
            if i % 2 == 0:  # Delete on even iterations
                storage.delete("temp-key")
                if "temp-key" in expected_data:
                    del expected_data["temp-key"]
            else:
                expected_data["temp-key"] = f"temp-value-{i}"

        # Verify multiple segments were created
        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 1, f"Expected multiple segments but got {len(segments)}")

        # Create a new storage instance to test replay
        new_wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)
        with patch("builtins.print"):  # suppress print statements
            new_storage = KeyValueStorage(new_wal)

        # Verify all data was correctly replayed
        for key, expected_value in expected_data.items():
            actual_value = new_storage.get(key)

            # For complex objects, compare serialized JSON to handle floating-point differences
            if isinstance(expected_value, (dict, list)):
                self.assertEqual(
                    json.dumps(actual_value, sort_keys=True),
                    json.dumps(expected_value, sort_keys=True),
                    f"Replayed value for {key} doesn't match expected value",
                )
            else:
                self.assertEqual(actual_value, expected_value, f"Replayed value for {key} doesn't match expected value")

    def test_wal_with_huge_values(self):
        """Test WAL segmentation with large values that exceed segment size"""
        # Create a WAL with small segment size
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)
        storage = KeyValueStorage(wal)

        # Add a value that is larger than the segment size
        large_value = "x" * (self.small_segment_size * 2)  # Value larger than segment size
        storage.set("large-key", large_value)

        # Add some more normal values
        storage.set("key1", "value1")
        storage.set("key2", "value2")

        # Verify we have at least two segments (since large value should force segment rollover)
        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 1, "Expected multiple segments after adding large value")

        # Create a new storage instance to verify replay works
        new_wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)
        new_storage = KeyValueStorage(new_wal)

        # Verify all values, including the large one, were replayed correctly
        self.assertEqual(new_storage.get("large-key"), large_value)
        self.assertEqual(new_storage.get("key1"), "value1")
        self.assertEqual(new_storage.get("key2"), "value2")

    def test_wal_truncated_segment(self):
        """Test handling of truncated segment files"""
        # Create a WAL with small segment size
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Add enough entries to create at least 2 segments
        for i in range(20):
            wal.append(OperationType.SET, f"key{i}", f"value{i}" * 5)

        # Verify we have multiple segments
        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 1, "Expected multiple segments for this test")

        # Truncate the last segment file (remove the last few bytes)
        with open(segments[-1], "r+") as f:
            content = f.read()
            # Truncate the file to half its size
            truncated_content = content[: len(content) // 2]
            f.seek(0)
            f.write(truncated_content)
            f.truncate()

        # Create a new WAL instance with the truncated segment
        # It should still load successfully but skip the corrupted entries
        with patch("builtins.print"):  # suppress print statements
            new_wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Verify we can still read entries from the WAL
        entries = new_wal.read_from(0)
        self.assertGreater(len(entries), 0, "Expected some valid entries despite truncation")
