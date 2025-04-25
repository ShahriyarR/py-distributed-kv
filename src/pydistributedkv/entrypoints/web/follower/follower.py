import logging
import os
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query

from pydistributedkv.configurator.settings.base import API_TIMEOUT, MAX_SEGMENT_SIZE
from pydistributedkv.domain.models import ClientRequest, LogEntry, OperationType, ReplicationRequest, WAL
from pydistributedkv.service.request_deduplication import RequestDeduplicationService
from pydistributedkv.service.storage import KeyValueStorage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize WAL and storage
wal = WAL(os.getenv("WAL_PATH", "data/follower/wal.log"), max_segment_size=MAX_SEGMENT_SIZE)
storage = KeyValueStorage(wal)

# Request deduplication service
request_deduplication = RequestDeduplicationService(service_name="follower")

# Leader connection info
leader_url = os.getenv("LEADER_URL", "http://localhost:8000")
follower_id = os.getenv("FOLLOWER_ID", "follower-1")
follower_url = os.getenv("FOLLOWER_URL", "http://localhost:8001")

# Replication state
last_applied_id = wal.get_last_id()  # Initialize with the current last ID in WAL


@app.on_event("startup")
async def startup_event():
    global last_applied_id  # noqa: F824
    # Register with leader
    try:
        response = requests.post(
            f"{leader_url}/register_follower",
            json={"id": follower_id, "url": follower_url, "last_applied_id": last_applied_id},
            timeout=API_TIMEOUT,
        )
        response_data = response.json()

        # If leader has entries we don't, fetch them
        leader_last_id = response_data.get("last_log_id", 0)
        if leader_last_id > last_applied_id:
            await sync_with_leader()
    except requests.RequestException as e:
        # In production, you'd implement retry logic
        print(f"Failed to register with leader at {leader_url}: {str(e)}")


async def sync_with_leader():
    """Synchronize the follower with the leader by fetching and applying new log entries."""
    global last_applied_id
    try:
        entries = await fetch_entries_from_leader()
        if not entries:
            return

        new_entries = append_entries_to_wal(entries)
        if new_entries:
            last_applied_id = apply_entries_to_storage(new_entries)
    except requests.RequestException:
        print("Failed to sync with leader")


async def fetch_entries_from_leader() -> list[LogEntry]:
    """Fetch new log entries from the leader."""
    response = requests.get(f"{leader_url}/log_entries/{last_applied_id}", timeout=API_TIMEOUT)
    data = response.json()
    return _parse_and_validate_entries(data.get("entries", []), source="leader")


def _parse_and_validate_entries(entry_data_list: list[dict], source: str = "") -> list[LogEntry]:
    """Parse and validate log entries from the provided data."""
    valid_entries = []

    for entry_data in entry_data_list:
        entry = _create_valid_entry(entry_data, source)
        if entry:
            valid_entries.append(entry)

    return valid_entries


def _create_valid_entry(entry_data: dict, source: str = "") -> LogEntry | None:
    """Create and validate a single log entry."""
    try:
        entry = LogEntry(**entry_data)
        if not entry.validate_crc():
            print(f"Warning: Received entry with ID {entry.id} with invalid CRC from {source}")
            return None
        return entry
    except ValueError as e:
        print(f"Error parsing entry from {source}: {str(e)}")
        return None


def append_entries_to_wal(entries: list[LogEntry]) -> list[LogEntry]:
    """Append new entries to the WAL and return only the newly added ones."""
    new_entries = []
    for entry in entries:
        # Ensure we only add entries with valid CRC
        if entry.validate_crc() and not wal.has_entry(entry.id):
            wal.append_entry(entry)
            new_entries.append(entry)
    return new_entries


def apply_entries_to_storage(entries: list[LogEntry]) -> int:
    """Apply entries to storage and return the last applied ID."""
    return storage.apply_entries(entries)


@app.post("/replicate")
async def replicate(req: ReplicationRequest):
    global last_applied_id

    # Use the existing helper to parse and validate entries
    entries = _parse_and_validate_entries(req.entries, source="replication request")

    # Process and apply new entries
    new_entries = _process_new_entries(entries)

    # Update the last applied ID if we have new entries
    if new_entries:
        last_id = storage.apply_entries(new_entries)
        last_applied_id = max(last_applied_id, last_id)

    return {"status": "ok", "last_applied_id": last_applied_id}


def _process_new_entries(entries: list[LogEntry]) -> list[LogEntry]:
    """Process and store only entries that don't exist in the WAL."""
    if not entries:
        return []

    new_entries = []
    for entry in entries:
        if not wal.has_entry(entry.id):
            wal.append_entry(entry)
            new_entries.append(entry)
    return new_entries


@app.get("/key/{key}")
def get_key(key: str, client_id: Optional[str] = Query(None), request_id: Optional[str] = Query(None)):
    """Handle GET request for a specific key with deduplication support"""
    # Check for cached response from duplicate request
    cached_response = _check_request_cache(client_id, request_id, key, OperationType.GET)
    if cached_response:
        return cached_response

    # Get the value and prepare response
    value, status_code, message = _get_value_from_storage(key)

    # Handle error case
    if status_code != 200:
        _cache_response_if_needed(client_id, request_id, key, OperationType.GET, {"status": "error", "message": message})
        raise HTTPException(status_code=status_code, detail=message)

    # Create success response
    response = {"key": key, "value": value}

    # Cache the response if client tracking is enabled
    _cache_response_if_needed(client_id, request_id, key, OperationType.GET, response)

    return response


def _check_request_cache(client_id: Optional[str], request_id: Optional[str], key: str, operation: OperationType) -> Optional[Dict]:
    """Check if this is a duplicate request with a cached response"""
    if not client_id or not request_id:
        logger.info(f"GET request for key={key} (no client ID)")
        return None

    logger.info(f"GET request for key={key} from client={client_id}, request={request_id}")
    previous_response = request_deduplication.get_processed_result(client_id, request_id, operation)

    if previous_response is not None:
        logger.info(f"✅ Returning cached response for GET key={key}, client={client_id}, request={request_id}")
        return previous_response

    return None


def _get_value_from_storage(key: str) -> Tuple[Any, int, str]:
    """Get a value from storage, returning value, status code and message"""
    value = storage.get(key)

    if value is None:
        error_msg = f"Key not found: {key}"
        logger.warning(error_msg)
        return None, 404, error_msg

    return value, 200, "OK"


def _cache_response_if_needed(
    client_id: Optional[str], request_id: Optional[str], key: str, operation: OperationType, response: Dict
) -> None:
    """Cache the response if client tracking is enabled"""
    if not client_id or not request_id:
        return

    client_request = ClientRequest(client_id=client_id, request_id=request_id, operation=operation, key=key)
    request_deduplication.mark_request_processed(client_request, response)

    status = "error" if "status" in response and response["status"] == "error" else "success"
    logger.info(f"Cached {status} response for GET key={key}, client={client_id}, request={request_id}")


@app.get("/status")
def get_status():
    return {"follower_id": follower_id, "last_applied_id": last_applied_id, "leader_url": leader_url}


@app.get("/segments")
def get_segments():
    """Return information about the WAL segments"""
    segments = wal.get_segment_files()
    active_segment = wal.get_active_segment()

    segment_info = []
    for segment in segments:
        try:
            size = os.path.getsize(segment)
            segment_info.append({"path": segment, "size": size, "is_active": segment == active_segment})
        except FileNotFoundError:
            pass

    return {"segments": segment_info, "total_segments": len(segment_info), "max_segment_size": MAX_SEGMENT_SIZE}


@app.get("/keys")
def get_all_keys():
    """Return all keys in the storage"""
    keys = storage.get_all_keys()
    return {"keys": keys, "count": len(keys)}


@app.get("/request_status")
def get_request_status(client_id: str, request_id: str, operation: Optional[str] = Query(None)):
    """Check if a client request has been processed"""
    logger.info(f"Checking status for client={client_id}, request={request_id}, operation={operation}")
    # Pass operation type to get_processed_result if provided
    result = request_deduplication.get_processed_result(client_id, request_id, operation)
    if result:
        logger.info(f"Found cached result for client={client_id}, request={request_id}, operation={operation}")
        return {"processed": True, "result": result}
    else:
        logger.info(f"No cached result found for client={client_id}, request={request_id}, operation={operation}")
        return {"processed": False}


@app.get("/deduplication_stats")
def get_deduplication_stats():
    """Return statistics about the request deduplication service"""
    stats = request_deduplication.get_stats()
    logger.info(f"Returning deduplication stats: duplicates detected={stats['total_duplicates_detected']}")
    return stats
