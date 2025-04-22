import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from pydistributedkv.domain.models import LogEntry, OperationType, WAL
from pydistributedkv.service.storage import KeyValueStorage


class TestWALSegmentation(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test WAL files
        self.temp_dir = tempfile.mkdtemp()
        self.wal_path = os.path.join(self.temp_dir, "wal.log")

        # Use a small segment size for testing
        self.small_segment_size = 100  # bytes

    def tearDown(self):
        # Clean up the temporary directory after tests
        shutil.rmtree(self.temp_dir)

    def test_segment_creation(self):
        """Test that segments are created with correct naming pattern"""
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Verify initial segment
        segments = wal.get_segment_files()
        self.assertEqual(len(segments), 1)
        self.assertTrue(segments[0].endswith("wal.log.segment.1"))

        # Verify active segment
        active_segment = wal.get_active_segment()
        self.assertEqual(active_segment, segments[0])

    def test_segment_rollover(self):
        """Test that new segments are created when size limit is reached"""
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Add entries to force segment rollover
        # Each JSON entry will be ~50-70 bytes, so a few entries should trigger rollover
        for i in range(10):
            wal.append(OperationType.SET, f"key{i}", f"value{i}" * 5)  # Make value larger to exceed segment size quicker

        # Verify multiple segments were created
        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 1, f"Expected multiple segments but got {len(segments)}")

        # Verify segments have sequential numbering
        segment_numbers = [int(seg.split(".")[-1]) for seg in segments]
        self.assertEqual(segment_numbers, list(range(1, len(segments) + 1)))

        # Verify the active segment is the last one
        active_segment = wal.get_active_segment()
        self.assertEqual(active_segment, segments[-1])

    def test_segment_size_limit(self):
        """Test that segments respect the configured size limit"""
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Add entries to force segment rollover
        for i in range(20):
            wal.append(OperationType.SET, f"key{i}", f"value{i}" * 5)

        segments = wal.get_segment_files()

        # Check each non-active segment size doesn't exceed the configured limit
        for segment in segments[:-1]:  # Exclude active segment which might not be full yet
            size = os.path.getsize(segment)
            self.assertLessEqual(
                size,
                self.small_segment_size + 10,  # Allow small buffer for potential alignment
                f"Segment {segment} size {size} exceeds limit {self.small_segment_size}",
            )

    def test_replay_across_segments(self):
        """Test that log replay works correctly across multiple segments"""
        # Create a WAL with small segments and add many entries
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Add entries that will span multiple segments
        expected_data = {}
        for i in range(30):
            key = f"key{i}"
            value = f"value{i}"

            if i % 3 == 0 and i > 0:  # Delete some keys periodically
                prev_key = f"key{i-3}"
                wal.append(OperationType.DELETE, prev_key)
                if prev_key in expected_data:
                    del expected_data[prev_key]
            else:
                wal.append(OperationType.SET, key, value)
                expected_data[key] = value

        # Verify we have multiple segments
        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 2, "Expected at least 3 segments for this test")

        # Create a new WAL instance that will replay the log
        new_wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Create a storage that will replay the log
        storage = KeyValueStorage(new_wal)

        # Verify the replayed data matches expectations
        for key, expected_value in expected_data.items():
            self.assertEqual(storage.get(key), expected_value, f"Replayed value for {key} doesn't match expected value")

        # Verify keys that should be deleted are not present
        for i in range(30):
            if i % 3 == 0 and i > 0:
                self.assertIsNone(storage.get(f"key{i-3}"), f"Key key{i-3} should have been deleted")

    def test_read_from_specific_id(self):
        """Test that reading from a specific ID works across segments"""
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Add entries that will span multiple segments
        entries_by_id = {}
        for i in range(30):
            entry = wal.append(OperationType.SET, f"key{i}", f"value{i}")
            entries_by_id[entry.id] = entry

        # Pick a starting ID in the middle
        start_id = wal.get_last_id() // 2

        # Read entries from that ID
        entries = wal.read_from(start_id)

        # Verify we got the correct entries
        self.assertEqual(len(entries), len(entries_by_id) - start_id + 1)
        for entry in entries:
            self.assertGreaterEqual(entry.id, start_id)
            self.assertEqual(entry.key, entries_by_id[entry.id].key)
            self.assertEqual(entry.value, entries_by_id[entry.id].value)
            self.assertEqual(entry.operation, entries_by_id[entry.id].operation)

    def test_data_integrity_across_segments(self):
        """Test that CRC validation works across segments"""
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Add entries that will span multiple segments
        for i in range(20):
            wal.append(OperationType.SET, f"key{i}", f"value{i}")

        # Verify we have multiple segments
        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 1, "Expected multiple segments for this test")

        # Read all entries
        entries = wal.read_from(0)

        # Verify all entries have valid CRCs
        for entry in entries:
            self.assertTrue(entry.validate_crc(), f"Entry {entry.id} failed CRC validation")

    def test_integrity_after_corruption(self):
        """Test that corrupted entries are handled properly during replay"""
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Add entries that will span multiple segments
        for i in range(15):
            wal.append(OperationType.SET, f"key{i}", f"value{i}")

        # Verify we have multiple segments
        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 1, "Expected multiple segments for this test")

        # Corrupt the middle segment
        middle_segment = segments[len(segments) // 2]
        with open(middle_segment, "r+") as f:
            content = f.read()
            # Corrupt some data by replacing characters
            corrupted = content.replace("value", "XXXXX", 1)
            f.seek(0)
            f.write(corrupted)
            f.truncate()

        # Create a new WAL instance that will replay the log
        with patch("builtins.print") as mock_print:  # Capture print statements
            new_wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Verify warning was printed about CRC validation failure
        crc_warning_printed = any("CRC validation failure" in str(call) for call in mock_print.call_args_list)
        self.assertTrue(crc_warning_printed, "Expected CRC validation warning")

        # But the WAL should still initialize and contain valid entries
        entries = new_wal.read_from(0)
        self.assertGreater(len(entries), 0, "Expected some valid entries despite corruption")
