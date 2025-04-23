import logging
import time
from collections import defaultdict
from typing import Any, Dict, Optional, Tuple

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
        self.processed_requests: Dict[str, Dict[str, Tuple[float, Any]]] = defaultdict(dict)
        self.max_cache_size = max_cache_size
        self.expiry_seconds = expiry_seconds
        self.service_name = service_name

        # Statistics
        self.total_requests_cached = 0
        self.total_duplicates_detected = 0
        self.total_cache_cleanups = 0

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

        # Store the result with the current timestamp
        self.processed_requests[client_id][request_id] = (time.time(), result)
        self.total_requests_cached += 1

        logger.info(f"[{self.service_name}] Cached result for client={client_id}, request={request_id}, operation={operation}, key={key}")

        # If we've exceeded our cache size, remove the oldest entries
        if len(self.processed_requests) > self.max_cache_size:
            self._clean_oldest_requests()

    def get_processed_result(self, client_id: str, request_id: str) -> Optional[Any]:
        """
        Check if a request has been processed and return its result if found

        Returns:
            The stored result if the request was already processed, None otherwise
        """
        self._clean_expired_requests()

        if client_id in self.processed_requests and request_id in self.processed_requests[client_id]:
            timestamp, result = self.processed_requests[client_id][request_id]
            self.total_duplicates_detected += 1

            # Calculate how long ago this request was first processed
            time_since_original = time.time() - timestamp

            logger.warning(
                f"[{self.service_name}] DUPLICATE REQUEST DETECTED: client={client_id}, request={request_id}, "
                f"originally processed {time_since_original:.2f} seconds ago"
            )
            return result

        return None

    def _clean_expired_requests(self):
        """Remove expired entries from the cache"""
        current_time = time.time()
        clients_to_remove = []
        expired_count = 0

        for client_id, requests in self.processed_requests.items():
            # Find expired requests for this client
            expired_requests = [req_id for req_id, (timestamp, _) in requests.items() if current_time - timestamp > self.expiry_seconds]

            # Remove expired requests
            for req_id in expired_requests:
                del requests[req_id]
                expired_count += 1

            # If client has no more requests, mark for removal
            if not requests:
                clients_to_remove.append(client_id)

        # Remove empty client entries
        for client_id in clients_to_remove:
            del self.processed_requests[client_id]

        if expired_count > 0:
            logger.info(f"[{self.service_name}] Cleaned up {expired_count} expired cache entries")
            self.total_cache_cleanups += 1

    def _clean_oldest_requests(self):
        """Remove the oldest entries when the cache exceeds max size"""
        # Flatten all requests with their timestamps
        all_requests = []
        for client_id, requests in self.processed_requests.items():
            for request_id, (timestamp, _) in requests.items():
                all_requests.append((timestamp, client_id, request_id))

        # Sort by timestamp (oldest first)
        all_requests.sort()

        # Remove oldest entries until we're within limits
        entries_to_remove = max(0, len(all_requests) - self.max_cache_size)

        if entries_to_remove > 0:
            logger.info(f"[{self.service_name}] Cache size limit reached, removing {entries_to_remove} oldest entries")

            for i in range(entries_to_remove):
                _, client_id, request_id = all_requests[i]
                if client_id in self.processed_requests and request_id in self.processed_requests[client_id]:
                    del self.processed_requests[client_id][request_id]

                    # If client has no more requests, remove the client entry
                    if not self.processed_requests[client_id]:
                        del self.processed_requests[client_id]

            self.total_cache_cleanups += 1

    def get_stats(self) -> dict:
        """Return statistics about the deduplication service"""
        total_cached = 0
        for client_id, requests in self.processed_requests.items():
            total_cached += len(requests)

        return {
            "service_name": self.service_name,
            "current_cache_size": total_cached,
            "total_client_count": len(self.processed_requests),
            "total_requests_cached": self.total_requests_cached,
            "total_duplicates_detected": self.total_duplicates_detected,
            "total_cache_cleanups": self.total_cache_cleanups,
        }
