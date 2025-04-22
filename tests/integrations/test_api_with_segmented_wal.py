import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

from fastapi.testclient import TestClient

from pydistributedkv.domain.models import WAL
from pydistributedkv.service.storage import KeyValueStorage


class TestAPIWithSegmentedWAL(unittest.TestCase):
    def setUp(self):
        # Create a temporary directory for test WAL files
        self.temp_dir = tempfile.mkdtemp()
        self.wal_path = os.path.join(self.temp_dir, "wal.log")

        # Use a small segment size for testing
        self.small_segment_size = 200  # bytes

    def tearDown(self):
        # Clean up the temporary directory after tests
        shutil.rmtree(self.temp_dir)

    def test_leader_api_segments_endpoint(self):
        """Test the /segments endpoint of the leader API"""
        # This requires mocking environment variables for the WAL path
        with patch.dict(os.environ, {"WAL_PATH": self.wal_path}):
            # Import the app here to use the mocked WAL_PATH
            from pydistributedkv.entrypoints.web.leader.leader import app, wal

            # Set a small segment size
            wal.max_segment_size = self.small_segment_size

            client = TestClient(app)

            # Add enough data to create multiple segments
            for i in range(20):
                response = client.put(f"/key/key{i}", json={"value": f"value{i}" * 10})
                self.assertEqual(response.status_code, 200)

            # Get segments info
            response = client.get("/segments")
            self.assertEqual(response.status_code, 200)

            segments_info = response.json()
            self.assertIn("segments", segments_info)
            self.assertIn("total_segments", segments_info)
            self.assertIn("max_segment_size", segments_info)

            # Verify we have multiple segments
            self.assertGreater(segments_info["total_segments"], 1, "Expected multiple segments after adding data")

            # Verify segment size info
            for segment in segments_info["segments"]:
                self.assertIn("path", segment)
                self.assertIn("size", segment)
                self.assertIn("is_active", segment)

                # Non-active segments should be close to the max size
                if not segment["is_active"]:
                    self.assertLessEqual(segment["size"], self.small_segment_size + 10)

    def test_follower_api_segments_endpoint(self):
        """Test the /segments endpoint of the follower API"""
        # Create a WAL and set up necessary mocks
        with patch.dict(
            os.environ,
            {
                "WAL_PATH": self.wal_path,
                "LEADER_URL": "http://localhost:8000",
                "FOLLOWER_ID": "test-follower",
                "FOLLOWER_URL": "http://localhost:8001",
            },
        ):
            # Import the app here to use the mocked environment variables
            with patch("requests.post"):  # Mock the requests to leader
                from pydistributedkv.entrypoints.web.follower.follower import app, wal

                # Set a small segment size
                wal.max_segment_size = self.small_segment_size

                client = TestClient(app)

                # Add enough data to create multiple segments by direct WAL manipulation
                # (Since we mocked the requests to leader)
                for i in range(20):
                    storage = KeyValueStorage(wal)
                    storage.set(f"key{i}", f"value{i}" * 10)

                # Get segments info
                response = client.get("/segments")
                self.assertEqual(response.status_code, 200)

                segments_info = response.json()
                self.assertIn("segments", segments_info)
                self.assertIn("total_segments", segments_info)
                self.assertIn("max_segment_size", segments_info)

                # Verify we have multiple segments
                self.assertGreater(segments_info["total_segments"], 1, "Expected multiple segments after adding data")

    def test_replication_with_segmented_wal(self):
        """Test that replication works properly with segmented WAL"""
        # This would be an integration test that's challenging to set up with unittest
        # We'll instead test a simplified version focusing on the follower's ability
        # to process replicated entries

        # Create leader and follower WAL files
        leader_wal_path = os.path.join(self.temp_dir, "leader_wal.log")
        follower_wal_path = os.path.join(self.temp_dir, "follower_wal.log")

        # Create a leader WAL with some entries
        leader_wal = WAL(leader_wal_path, max_segment_size=self.small_segment_size)
        leader_storage = KeyValueStorage(leader_wal)

        # Add enough entries to create multiple segments
        entries = []
        for i in range(20):
            entry = leader_storage.set(f"key{i}", f"value{i}" * 5)
            entries.append(entry)

        # Verify leader has multiple segments
        leader_segments = leader_wal.get_segment_files()
        self.assertGreater(len(leader_segments), 1, "Expected multiple segments for leader")

        # Create follower WAL
        follower_wal = WAL(follower_wal_path, max_segment_size=self.small_segment_size)
        follower_storage = KeyValueStorage(follower_wal)

        # Mock replication by applying the same entries to the follower
        for entry in entries:
            follower_wal.append_entry(entry)

        # Verify follower created segments too
        follower_segments = follower_wal.get_segment_files()
        self.assertGreater(len(follower_segments), 1, "Expected multiple segments for follower")

        # Verify the data in follower matches leader
        for i in range(20):
            key = f"key{i}"
            expected = f"value{i}" * 5
            self.assertEqual(follower_storage.get(key), expected)
