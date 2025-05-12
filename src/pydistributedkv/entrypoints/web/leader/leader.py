import logging
import os
import time
from typing import Any, Dict, Optional, Tuple

import requests
from fastapi import FastAPI, HTTPException, Query

from pydistributedkv.configurator.settings.base import API_TIMEOUT, compaction_interval, HEARTBEAT_INTERVAL, MAX_SEGMENT_SIZE
from pydistributedkv.domain.models import ClientRequest, FollowerRegistration, KeyValue, OperationType, WAL
from pydistributedkv.service.compaction import LogCompactionService
from pydistributedkv.service.heartbeat import HeartbeatService
from pydistributedkv.service.request_deduplication import RequestDeduplicationService
from pydistributedkv.service.storage import KeyValueStorage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize WAL and storage
wal = WAL(os.getenv("WAL_PATH", "data/leader/wal.log"), max_segment_size=MAX_SEGMENT_SIZE)
storage = KeyValueStorage(wal)

# Initialize compaction service
compaction_service = LogCompactionService(storage, compaction_interval=compaction_interval)

# Request deduplication service
request_deduplication = RequestDeduplicationService(service_name="leader")

# Track followers and their replication status
followers: dict[str, str] = {}  # follower_id -> url
replication_status: dict[str, int] = {}  # follower_id -> last_replicated_id

# Create heartbeat service
leader_id = "leader"
leader_url = os.getenv("LEADER_URL", "http://localhost:8000")
heartbeat_service = HeartbeatService(service_name="leader", server_id=leader_id, server_url=leader_url)


@app.on_event("startup")
async def startup_event():
    # Start heartbeat monitoring and sending tasks
    await heartbeat_service.start_monitoring()
    await heartbeat_service.start_sending()

    # Start compaction service
    await compaction_service.start()

    logger.info("Leader server started with heartbeat and compaction services")


@app.on_event("shutdown")
async def shutdown_event():
    # Stop heartbeat service
    await heartbeat_service.stop()

    # Stop compaction service
    await compaction_service.stop()

    logger.info("Leader server shutting down")


@app.get("/key/{key}")
def get_key(
    key: str,
    version: Optional[int] = Query(None, description="Specific version to retrieve"),
    client_id: Optional[str] = Query(None),
    request_id: Optional[str] = Query(None),
):
    """Handle GET request for a specific key with deduplication support"""
    # Check for cached response if client tracking is enabled
    cached_response = _check_request_cache(client_id, request_id, key, OperationType.GET)
    if cached_response:
        return cached_response

    # Get value from storage and handle errors
    result = _get_value_from_storage(key, version)
    if result is None:
        response = {"status": "error", "message": f"Key not found: {key}"}
        if version:
            response["message"] = f"Key not found or version {version} not available: {key}"
        _cache_response_if_needed(client_id, request_id, key, OperationType.GET, response)
        raise HTTPException(status_code=404, detail=response["message"])

    value, actual_version = result

    # Create success response
    response = {"key": key, "value": value, "version": actual_version}

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


def _get_value_from_storage(key: str, version: Optional[int] = None) -> Optional[Tuple[Any, int]]:
    """Get a value and its version from storage"""
    return storage.get_with_version(key, version)


def _cache_response_if_needed(
    client_id: Optional[str], request_id: Optional[str], key: str, operation: OperationType, response: Dict, value: Any = None
) -> None:
    """Cache the response if client tracking is enabled"""
    if not client_id or not request_id:
        return

    client_request = ClientRequest(client_id=client_id, request_id=request_id, operation=operation, key=key, value=value)
    request_deduplication.mark_request_processed(client_request, response)

    status_type = "error" if "status" in response and response["status"] == "error" else "success"
    logger.info(f"Cached {status_type} response for {operation.name} key={key}, client={client_id}, request={request_id}")


@app.put("/key/{key}")
def set_key(key: str, kv: KeyValue, client_id: Optional[str] = Query(None), request_id: Optional[str] = Query(None)):
    """Handle PUT request to set a specific key with deduplication support"""
    # Check for cached response if client tracking is enabled
    cached_response = _check_request_cache(client_id, request_id, key, OperationType.SET)
    if cached_response:
        return cached_response

    # Process the request and get the resulting entry
    entry, version = _process_set_key_request(key, kv.value, kv.version)

    # Handle version conflict
    if entry is None:
        response = {
            "status": "error",
            "message": f"Version conflict: provided version {kv.version} is outdated",
            "current_version": version,
        }
        _cache_response_if_needed(client_id, request_id, key, OperationType.SET, response)
        raise HTTPException(status_code=409, detail=response["message"])

    # Create and cache the response
    response = {"status": "ok", "id": entry.id, "key": key, "version": version}
    _cache_response_if_needed(client_id, request_id, key, OperationType.SET, response, kv.value)

    return response


def _process_set_key_request(key: str, value: Any, version: Optional[int] = None):
    """Process the key-value storage operation and handle replication"""
    # Store the value
    entry, actual_version = storage.set(key, value, version)

    if entry is None:
        return None, actual_version

    logger.info(f"Added SET entry id={entry.id} for key={key}, version={actual_version}")

    # Replicate to followers asynchronously
    _replicate_to_followers(entry)

    return entry, actual_version


@app.delete("/key/{key}")
def delete_key(key: str, client_id: Optional[str] = Query(None), request_id: Optional[str] = Query(None)):
    """Handle DELETE request for a specific key with deduplication support"""
    # Check for cached response if client tracking is enabled
    cached_response = _check_request_cache(client_id, request_id, key, OperationType.DELETE)
    if cached_response:
        return cached_response

    # Process the delete request
    entry, status_code, error_msg = _process_delete_request(key)

    # Handle error case
    if status_code != 200:
        response = {"status": "error", "message": error_msg}
        _cache_response_if_needed(client_id, request_id, key, OperationType.DELETE, response)
        raise HTTPException(status_code=status_code, detail=error_msg)

    # Create success response
    response = {"status": "ok", "id": entry.id}

    # Cache the response if client tracking is enabled
    _cache_response_if_needed(client_id, request_id, key, OperationType.DELETE, response)

    return response


def _process_delete_request(key: str) -> Tuple[Any, int, Optional[str]]:
    """Process the key deletion operation and handle replication"""
    # Delete the key from storage
    entry = storage.delete(key)

    if entry is None:
        error_msg = f"Key not found: {key}"
        logger.warning(error_msg)
        return None, 404, error_msg

    logger.info(f"Added DELETE entry id={entry.id} for key={key}")

    # Replicate to followers asynchronously
    _replicate_to_followers(entry)

    return entry, 200, None


def _replicate_to_followers(entry):
    """Helper method to replicate an entry to all followers"""
    # Only replicate to healthy followers
    healthy_followers = heartbeat_service.get_healthy_servers()

    for follower_id, follower_url in healthy_followers.items():
        try:
            logger.info(f"Replicating entry id={entry.id} to follower {follower_id}")
            requests.post(
                f"{follower_url}/replicate",
                json={"entries": [entry.model_dump()]},
                timeout=API_TIMEOUT,
            )
            replication_status[follower_id] = entry.id
        except requests.RequestException as e:
            logger.error(f"Failed to replicate entry id={entry.id} to follower {follower_id}: {str(e)}")
            # In production, you'd want better error handling and retry logic


@app.post("/register_follower")
def register_follower(follower_data: FollowerRegistration):
    follower_id = follower_data.id
    follower_url = follower_data.url
    last_applied_id = follower_data.last_applied_id

    followers[follower_id] = follower_url
    replication_status[follower_id] = last_applied_id

    # Register follower with heartbeat service
    heartbeat_service.register_server(follower_id, follower_url)

    return {"status": "ok", "last_log_id": wal.get_last_id()}


@app.get("/log_entries/{last_id}")
def get_log_entries(last_id: int):
    entries = wal.read_from(last_id + 1)
    return {"entries": [entry.model_dump() for entry in entries]}


@app.get("/follower_status")
def get_follower_status():
    return {
        "followers": [{"id": f_id, "url": url, "last_replicated_id": replication_status.get(f_id, 0)} for f_id, url in followers.items()]
    }


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


@app.post("/heartbeat")
def receive_heartbeat(data: dict):
    """Handle heartbeat from followers"""
    server_id = data.get("server_id")
    timestamp = data.get("timestamp", time.time())

    if not server_id:
        return {"status": "error", "message": "Missing server_id"}

    heartbeat_service.record_heartbeat(server_id)
    logger.debug(f"Received heartbeat from {server_id} at {timestamp}")

    return {"status": "ok", "server_id": leader_id, "timestamp": time.time()}


@app.get("/cluster_status")
def get_cluster_status():
    """Get status of all servers in the cluster"""
    return {
        "leader": {"id": leader_id, "url": leader_url, "status": "healthy"},  # Leader always reports itself as healthy
        "followers": heartbeat_service.get_all_statuses(),
        "heartbeat_interval": HEARTBEAT_INTERVAL,
    }


@app.get("/key/{key}/history")
def get_key_history(key: str):
    """Get the version history of a key"""
    history = storage.get_version_history(key)

    if history is None:
        raise HTTPException(status_code=404, detail=f"Key not found: {key}")

    versions = sorted(history.keys())

    return {"key": key, "versions": versions, "history": [{"version": v, "value": history[v]} for v in versions]}


@app.get("/key/{key}/versions")
def get_key_versions(key: str):
    """Get available versions for a key"""
    history = storage.get_version_history(key)

    if history is None:
        raise HTTPException(status_code=404, detail=f"Key not found: {key}")

    return {"key": key, "versions": sorted(history.keys()), "latest_version": storage.get_latest_version(key)}


@app.post("/compaction/run")
async def run_compaction(force: bool = Query(False, description="Force compaction even if minimum interval hasn't passed")):
    """Manually trigger log compaction"""
    try:
        segments_compacted, entries_removed = await compaction_service.run_compaction(force=force)
        return {
            "status": "success",
            "segments_compacted": segments_compacted,
            "entries_removed": entries_removed,
        }
    except Exception as e:
        logger.error(f"Error during manual compaction: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Compaction error: {str(e)}") from e


@app.get("/compaction/status")
def get_compaction_status():
    """Get the status of the log compaction service"""
    return compaction_service.get_status()


@app.post("/compaction/configure")
def configure_compaction(
    enabled: Optional[bool] = Query(None, description="Enable or disable compaction"),
    interval: Optional[int] = Query(None, description="Compaction interval in seconds"),
):
    """Configure the log compaction service"""
    changes = {}

    if enabled is not None:
        result = compaction_service.set_enabled(enabled)
        changes["enabled"] = result

    if interval is not None:
        result = compaction_service.set_compaction_interval(interval)
        changes["interval"] = result

    return {"status": "success", "changes": changes}
