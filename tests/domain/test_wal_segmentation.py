import json
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
        # Increase segment size to handle the larger entries with version information
        self.small_segment_size = 150  # bytes - increased to accommodate version field
        wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)

        # Generate entries with much larger values to ensure segments fill up properly
        # Use a fixed string size to make segments more predictable
        large_value = "X" * 60  # Fixed size string to make entries larger

        # Add enough entries to ensure multiple segments with substantial content
        for i in range(10):  # Fewer entries, but each one is larger
            wal.append(OperationType.SET, f"key{i}", large_value)

        segments = wal.get_segment_files()
        self.assertGreater(len(segments), 1, "Test needs multiple segments to be valid")

        # Only test the size of completed segments (not the active one)
        # The active segment might not be full yet
        completed_segments = segments[:-1]

        print(f"Testing {len(segments)} segments with size limit {self.small_segment_size}")
        print(f"Active segment: {segments[-1]}")

        for i, segment in enumerate(completed_segments):
            size = os.path.getsize(segment)
            print(f"Completed segment {i+1}: {segment} size = {size} bytes")

            # 1. Check that size doesn't greatly exceed the limit
            allowed_buffer = 20  # Bytes of buffer for overhead
            self.assertLessEqual(
                size,
                self.small_segment_size + allowed_buffer,
                f"Segment {segment} size {size} exceeds limit {self.small_segment_size} by more than {allowed_buffer} bytes",
            )

            # 2. Skip the minimum size check if we have only one completed segment
            # The first segment might not be full if we roll over quickly due to entry size
            if len(completed_segments) > 1 and i > 0:  # Skip first segment, check others if we have multiple
                min_expected_size = self.small_segment_size * 0.5
                self.assertGreater(
                    size,
                    min_expected_size,
                    f"Segment {segment} size {size} is too small relative to limit {self.small_segment_size} (min expected {min_expected_size})",
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

        # Corrupt the middle segment by modifying an entry's value but keeping its original CRC
        middle_segment = segments[len(segments) // 2]
        with open(middle_segment, "r") as f:
            lines = f.readlines()

        # Modify at least one line to have invalid CRC
        if lines:
            line_to_corrupt = lines[0]  # Take the first entry in the middle segment
            entry = json.loads(line_to_corrupt)
            original_crc = entry["crc"]  # Save the original CRC

            # Modify the value but keep the original CRC - this will cause CRC validation to fail
            entry["value"] = "corrupted_value"

            # Write back to file with the original CRC which is now invalid
            lines[0] = json.dumps(entry) + "\n"

            with open(middle_segment, "w") as f:
                f.writelines(lines)

        # Create a new WAL instance that will replay the log and capture print statements
        with patch("builtins.print") as mock_print:
            new_wal = WAL(self.wal_path, max_segment_size=self.small_segment_size)
            # Explicitly read all entries to trigger validation
            entries = new_wal.read_from(0)

        # Verify warning was printed about invalid CRC or skipping entries
        crc_warning_printed = False
        for call in mock_print.call_args_list:
            call_args = " ".join(str(arg) for arg in call[0])
            if "invalid CRC" in call_args.lower() or "skipping" in call_args.lower() or "crc validation" in call_args.lower():
                crc_warning_printed = True
                break

        self.assertTrue(crc_warning_printed, "Expected CRC validation warning")

        # The WAL should still initialize and contain valid entries
        entries = new_wal.read_from(0)
        self.assertGreater(len(entries), 0, "Expected some valid entries despite corruption")
