import logging
import time
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

from pydistributedkv.domain.models import ClientRequest

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RequestDeduplicationService:
    """Service to track processed client requests and prevent duplicate processing"""

    def __init__(self, max_cache_size: int = 10000, expiry_seconds: int = 3600, service_name: str = "deduplication"):
        """
        Initialize the request deduplication service.

        Args:
            max_cache_size: Maximum number of client requests to track
            expiry_seconds: Time in seconds after which cached requests should expire
            service_name: Name of the service using this deduplication (for logging)
        """
        # Change to store operation type as part of the key
        # Structure: {client_id: {(request_id, operation): (timestamp, result)}}
        self.processed_requests: Dict[str, Dict[Tuple[str, str], Tuple[float, Any]]] = defaultdict(dict)
        self.max_cache_size = max_cache_size
        self.expiry_seconds = expiry_seconds
        self.service_name = service_name

        # Statistics
        self.total_requests_cached = 0
        self.total_duplicates_detected = 0
        self.total_cache_cleanups = 0

        # Track different types of duplicates
        self.same_operation_duplicates = 0
        self.different_operation_duplicates = 0

        logger.info(
            f"[{service_name}] Request deduplication service initialized with max_cache_size={max_cache_size}, expiry_seconds={expiry_seconds}"
        )

    def mark_request_processed(self, client_request: ClientRequest, result: Any):
        """Mark a client request as processed with its result"""
        self._clean_expired_requests()

        client_id = client_request.client_id
        request_id = client_request.request_id
        operation = client_request.operation or "UNKNOWN"
        key = client_request.key or "N/A"

        # Use a tuple of (request_id, operation) as the cache key
        cache_key = (request_id, str(operation))

        # Store the result with the current timestamp
        self.processed_requests[client_id][cache_key] = (time.time(), result)
        self.total_requests_cached += 1

        logger.info(f"[{self.service_name}] Cached result for client={client_id}, request={request_id}, operation={operation}, key={key}")

        # If we've exceeded our cache size, remove the oldest entries
        if len(self.processed_requests) > self.max_cache_size:
            self._clean_oldest_requests()

    def get_processed_result(self, client_id: str, request_id: str, operation: Optional[str] = None) -> Optional[Any]:
        """
        Check if a request has been processed and return its result if found.
        Now takes into account the operation type.

        Args:
            client_id: Client identifier
            request_id: Request identifier
            operation: Operation type (GET, SET, DELETE) - must match for cache hit

        Returns:
            The stored result if the request was already processed with the same operation, None otherwise
        """
        self._clean_expired_requests()

        if client_id not in self.processed_requests:
            return None

        # Check for exact operation match
        result = self._check_exact_operation_match(client_id, request_id, operation)
        if result is not None:
            return result

        # Log different operations with same request ID (no result returned)
        self._log_different_operations(client_id, request_id, operation)

        return None

    def _check_exact_operation_match(self, client_id: str, request_id: str, operation: Optional[str]) -> Optional[Any]:
        """Check if we have an exact match for request ID and operation"""
        if not operation:
            return None

        cache_key = (request_id, str(operation))
        if cache_key not in self.processed_requests[client_id]:
            return None

        timestamp, result = self.processed_requests[client_id][cache_key]
        self.total_duplicates_detected += 1
        self.same_operation_duplicates += 1

        # Calculate how long ago this request was first processed
        time_since_original = time.time() - timestamp

        logger.warning(
            f"[{self.service_name}] DUPLICATE REQUEST DETECTED: client={client_id}, request={request_id}, "
            f"operation={operation}, originally processed {time_since_original:.2f} seconds ago"
        )
        return result

    def _log_different_operations(self, client_id: str, request_id: str, operation: Optional[str]) -> None:
        """Log when different operations are attempted with the same request ID"""
        if not operation:
            return

        for (req_id, op), (_, _) in list(self.processed_requests[client_id].items()):
            if req_id == request_id and op != str(operation):
                self.different_operation_duplicates += 1
                logger.warning(
                    f"[{self.service_name}] DIFFERENT OPERATION ATTEMPTED: client={client_id}, request={request_id}, "
                    f"previous_op={op}, current_op={operation}"
                )
                break

    def _clean_expired_requests(self):
        """Remove expired entries from the cache"""
        current_time = time.time()
        expired_count = self._remove_expired_entries(current_time)
        self._remove_empty_clients()

        if expired_count > 0:
            logger.info(f"[{self.service_name}] Cleaned up {expired_count} expired cache entries")
            self.total_cache_cleanups += 1

    def _remove_expired_entries(self, current_time: float) -> int:
        """Remove expired request entries and return count of removed entries"""
        expired_count = 0

        for _, requests in list(self.processed_requests.items()):
            # Find and remove expired requests for this client
            expired_requests = [req_key for req_key, (timestamp, _) in requests.items() if current_time - timestamp > self.expiry_seconds]

            # Remove expired requests
            for req_key in expired_requests:
                del requests[req_key]
                expired_count += 1

        return expired_count

    def _remove_empty_clients(self) -> None:
        """Remove client entries that have no requests"""
        empty_clients = [client_id for client_id, requests in self.processed_requests.items() if not requests]

        for client_id in empty_clients:
            del self.processed_requests[client_id]

    def _clean_oldest_requests(self):
        """Remove the oldest entries when the cache exceeds max size"""
        # Calculate how many entries to remove
        total_entries = self._count_total_entries()
        entries_to_remove = max(0, total_entries - self.max_cache_size)

        if entries_to_remove <= 0:
            return

        logger.info(f"[{self.service_name}] Cache size limit reached, removing {entries_to_remove} oldest entries")

        # Get sorted entries by age (oldest first)
        oldest_entries = self._get_entries_sorted_by_age()

        # Remove oldest entries
        self._remove_oldest_entries(oldest_entries, entries_to_remove)
        self.total_cache_cleanups += 1

    def _count_total_entries(self) -> int:
        """Count total entries across all clients"""
        return sum(len(requests) for requests in self.processed_requests.values())

    def _get_entries_sorted_by_age(self) -> List[Tuple[float, str, Tuple[str, str]]]:
        """Get all entries sorted by timestamp (oldest first)"""
        all_entries = []
        for client_id, requests in self.processed_requests.items():
            for req_key, (timestamp, _) in requests.items():
                all_entries.append((timestamp, client_id, req_key))

        all_entries.sort()  # Sort by timestamp (oldest first)
        return all_entries

    def _remove_oldest_entries(self, sorted_entries: List[Tuple[float, str, Tuple[str, str]]], count: int) -> None:
        """Remove the specified number of oldest entries"""
        entries_to_remove = sorted_entries[: min(count, len(sorted_entries))]

        for _, client_id, req_key in entries_to_remove:
            self._remove_entry_if_exists(client_id, req_key)

    def _remove_entry_if_exists(self, client_id: str, req_key: Tuple[str, str]) -> None:
        """Remove a single entry if it exists and clean up empty client entries"""
        if client_id not in self.processed_requests:
            return

        client_requests = self.processed_requests[client_id]
        if req_key in client_requests:
            del client_requests[req_key]

            # Clean up empty client entry
            if not client_requests:
                del self.processed_requests[client_id]

    def get_stats(self) -> dict:
        """Return statistics about the deduplication service"""
        total_cached = 0
        unique_request_ids = set()

        for _, requests in self.processed_requests.items():
            total_cached += len(requests)
            for req_id, _ in requests.keys():
                unique_request_ids.add(req_id)

        return {
            "service_name": self.service_name,
            "current_cache_size": total_cached,
            "unique_request_ids": len(unique_request_ids),
            "total_client_count": len(self.processed_requests),
            "total_requests_cached": self.total_requests_cached,
            "total_duplicates_detected": self.total_duplicates_detected,
            "same_operation_duplicates": self.same_operation_duplicates,
            "different_operation_duplicates": self.different_operation_duplicates,
            "total_cache_cleanups": self.total_cache_cleanups,
        }
