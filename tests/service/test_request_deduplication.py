import time
from unittest.mock import patch

import pytest

from pydistributedkv.domain.models import ClientRequest, OperationType
from pydistributedkv.service.request_deduplication import RequestDeduplicationService


class TestRequestDeduplicationService:
    """Tests for the request deduplication service"""

    @pytest.fixture
    def dedup_service(self):
        """Create a fresh deduplication service for each test"""
        return RequestDeduplicationService(max_cache_size=10, expiry_seconds=1)

    def test_basic_deduplication(self, dedup_service):
        """Test that a request is correctly identified as a duplicate"""
        # First request
        client_request = ClientRequest(client_id="client1", request_id="req1", operation=OperationType.GET, key="test_key")
        result = {"key": "test_key", "value": "test_value"}
        dedup_service.mark_request_processed(client_request, result)

        # Same request should be identified as duplicate
        cached_result = dedup_service.get_processed_result("client1", "req1", OperationType.GET)
        assert cached_result == result
        assert dedup_service.total_duplicates_detected == 1
        assert dedup_service.same_operation_duplicates == 1

    def test_operation_type_differentiation(self, dedup_service):
        """Test that different operation types with the same request ID are treated as different requests"""
        # Mark a GET request as processed
        get_request = ClientRequest(client_id="client1", request_id="req1", operation=OperationType.GET, key="test_key")
        get_result = {"key": "test_key", "value": "test_value"}
        dedup_service.mark_request_processed(get_request, get_result)

        # Different operation with the same request ID should NOT be considered a duplicate
        set_result = dedup_service.get_processed_result("client1", "req1", OperationType.SET)
        assert set_result is None
        assert dedup_service.different_operation_duplicates == 1

        # But the GET operation should still be considered a duplicate
        get_result_again = dedup_service.get_processed_result("client1", "req1", OperationType.GET)
        assert get_result_again == get_result
        assert dedup_service.same_operation_duplicates == 1

    def test_different_clients_same_request_id(self, dedup_service):
        """Test that the same request ID from different clients is treated as different requests"""
        # First client request
        client1_request = ClientRequest(client_id="client1", request_id="req1", operation=OperationType.GET, key="test_key")
        client1_result = {"key": "test_key", "value": "client1_value"}
        dedup_service.mark_request_processed(client1_request, client1_result)

        # Second client with same request ID
        client2_request = ClientRequest(client_id="client2", request_id="req1", operation=OperationType.GET, key="test_key")
        client2_result = {"key": "test_key", "value": "client2_value"}
        dedup_service.mark_request_processed(client2_request, client2_result)

        # Both clients should get their own results
        cached_result1 = dedup_service.get_processed_result("client1", "req1", OperationType.GET)
        cached_result2 = dedup_service.get_processed_result("client2", "req1", OperationType.GET)

        assert cached_result1 == client1_result
        assert cached_result2 == client2_result
        assert cached_result1 != cached_result2

    def test_expiry_of_cached_results(self, dedup_service):
        """Test that cached results expire after the configured time"""
        # Set a very short expiry time for testing
        dedup_service.expiry_seconds = 0.1

        # Cache a request
        client_request = ClientRequest(client_id="client1", request_id="req1", operation=OperationType.GET, key="test_key")
        result = {"key": "test_key", "value": "test_value"}
        dedup_service.mark_request_processed(client_request, result)

        # Request should be cached initially
        assert dedup_service.get_processed_result("client1", "req1", OperationType.GET) == result

        # Wait for the cache to expire
        time.sleep(0.2)

        # After expiry, the result should be gone
        assert dedup_service.get_processed_result("client1", "req1", OperationType.GET) is None

    @patch("time.time")
    def test_cache_cleanup_on_access(self, mock_time, dedup_service):
        """Test that expired entries are cleaned up when accessing the cache"""
        # Set up mock times
        mock_time.return_value = 1000  # Starting time

        # Cache a request
        client_request = ClientRequest(client_id="client1", request_id="req1", operation=OperationType.GET, key="test_key")
        result = {"key": "test_key", "value": "test_value"}
        dedup_service.mark_request_processed(client_request, result)

        # Add another entry
        client_request2 = ClientRequest(client_id="client1", request_id="req2", operation=OperationType.GET, key="test_key2")
        result2 = {"key": "test_key2", "value": "test_value2"}
        dedup_service.mark_request_processed(client_request2, result2)

        # Advance time beyond expiry
        mock_time.return_value = 1000 + dedup_service.expiry_seconds + 1

        # Access the cache, which should trigger cleanup
        dedup_service.get_processed_result("client1", "req1", OperationType.GET)

        # All entries should be gone
        assert len(dedup_service.processed_requests) == 0
        assert dedup_service.total_cache_cleanups >= 1

    def test_same_request_different_operations(self, dedup_service):
        """Test handling multiple operations on the same request ID"""
        # First operation: SET
        set_request = ClientRequest(
            client_id="client1", request_id="req1", operation=OperationType.SET, key="test_key", value="initial_value"
        )
        set_result = {"status": "ok", "id": 1}
        dedup_service.mark_request_processed(set_request, set_result)

        # Second operation: DELETE
        delete_request = ClientRequest(client_id="client1", request_id="req1", operation=OperationType.DELETE, key="test_key")
        delete_result = {"status": "ok", "id": 2}
        dedup_service.mark_request_processed(delete_request, delete_result)

        # Each operation should have its own cached result
        cached_set_result = dedup_service.get_processed_result("client1", "req1", OperationType.SET)
        cached_delete_result = dedup_service.get_processed_result("client1", "req1", OperationType.DELETE)

        assert cached_set_result == set_result
        assert cached_delete_result == delete_result
        assert cached_set_result != cached_delete_result

    def test_get_stats(self, dedup_service):
        """Test the statistics collection feature"""
        # Cache a few different requests
        for i in range(3):
            client_request = ClientRequest(client_id="client1", request_id=f"req{i}", operation=OperationType.GET, key=f"key{i}")
            dedup_service.mark_request_processed(client_request, {"value": f"value{i}"})

        # Create some duplicates
        dedup_service.get_processed_result("client1", "req0", OperationType.GET)
        dedup_service.get_processed_result("client1", "req1", OperationType.GET)
        dedup_service.get_processed_result("client1", "req0", OperationType.SET)  # Different operation

        # Check stats
        stats = dedup_service.get_stats()

        assert stats["total_requests_cached"] == 3
        assert stats["total_duplicates_detected"] == 2
        assert stats["same_operation_duplicates"] == 2
        assert stats["different_operation_duplicates"] == 1
        assert stats["current_cache_size"] == 3
        assert stats["unique_request_ids"] == 3
        assert stats["total_client_count"] == 1
