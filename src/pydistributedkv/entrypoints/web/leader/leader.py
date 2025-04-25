import logging
import os
from typing import Optional

import requests
from fastapi import FastAPI, HTTPException, Query

from pydistributedkv.configurator.settings.base import API_TIMEOUT, MAX_SEGMENT_SIZE
from pydistributedkv.domain.models import ClientRequest, FollowerRegistration, KeyValue, OperationType, WAL
from pydistributedkv.service.request_deduplication import RequestDeduplicationService
from pydistributedkv.service.storage import KeyValueStorage

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()

# Initialize WAL and storage
wal = WAL(os.getenv("WAL_PATH", "data/leader/wal.log"), max_segment_size=MAX_SEGMENT_SIZE)
storage = KeyValueStorage(wal)

# Request deduplication service
request_deduplication = RequestDeduplicationService(service_name="leader")

# Track followers and their replication status
followers: dict[str, str] = {}  # follower_id -> url
replication_status: dict[str, int] = {}  # follower_id -> last_replicated_id


@app.get("/key/{key}")
def get_key(key: str, client_id: Optional[str] = Query(None), request_id: Optional[str] = Query(None)):
    # Track request with client identifiers if provided
    if client_id and request_id:
        logger.info(f"GET request for key={key} from client={client_id}, request={request_id}")
        # Pass operation type to get_processed_result
        previous_response = request_deduplication.get_processed_result(client_id, request_id, OperationType.GET)
        if previous_response is not None:
            logger.info(f"✅ Returning cached response for GET key={key}, client={client_id}, request={request_id}")
            return previous_response
    else:
        logger.info(f"GET request for key={key} (no client ID)")

    value = storage.get(key)
    if value is None:
        error_msg = f"Key not found: {key}"
        logger.warning(error_msg)

        response = {"status": "error", "message": error_msg}

        # Cache error responses too
        if client_id and request_id:
            client_request = ClientRequest(client_id=client_id, request_id=request_id, operation=OperationType.GET, key=key)
            request_deduplication.mark_request_processed(client_request, response)

        raise HTTPException(status_code=404, detail=error_msg)

    # Create response
    response = {"key": key, "value": value}

    # Store response if client identifiers provided
    if client_id and request_id:
        client_request = ClientRequest(client_id=client_id, request_id=request_id, operation=OperationType.GET, key=key)
        request_deduplication.mark_request_processed(client_request, response)

    return response


@app.put("/key/{key}")
def set_key(key: str, kv: KeyValue, client_id: Optional[str] = Query(None), request_id: Optional[str] = Query(None)):
    # If client_id and request_id are provided, check for duplicate request
    if client_id and request_id:
        logger.info(f"SET request for key={key} from client={client_id}, request={request_id}")
        # Pass operation type to get_processed_result
        previous_response = request_deduplication.get_processed_result(client_id, request_id, OperationType.SET)
        if previous_response is not None:
            logger.info(f"✅ Returning cached response for SET key={key}, client={client_id}, request={request_id}")
            return previous_response
    else:
        logger.info(f"SET request for key={key} (no client ID)")

    # Process the request normally
    entry = storage.set(key, kv.value)
    logger.info(f"Added SET entry id={entry.id} for key={key}")

    # Replicate to followers asynchronously
    _replicate_to_followers(entry)

    # Create response
    response = {"status": "ok", "id": entry.id}

    # If client tracking info was provided, store the response
    if client_id and request_id:
        client_request = ClientRequest(client_id=client_id, request_id=request_id, operation=OperationType.SET, key=key, value=kv.value)
        request_deduplication.mark_request_processed(client_request, response)
        logger.info(f"Cached response for SET key={key}, client={client_id}, request={request_id}")

    return response


@app.delete("/key/{key}")
def delete_key(key: str, client_id: Optional[str] = Query(None), request_id: Optional[str] = Query(None)):
    # Check for duplicate request
    if client_id and request_id:
        logger.info(f"DELETE request for key={key} from client={client_id}, request={request_id}")
        # Pass operation type to get_processed_result
        previous_response = request_deduplication.get_processed_result(client_id, request_id, OperationType.DELETE)
        if previous_response is not None:
            logger.info(f"✅ Returning cached response for DELETE key={key}, client={client_id}, request={request_id}")
            return previous_response
    else:
        logger.info(f"DELETE request for key={key} (no client ID)")

    # Process the request
    entry = storage.delete(key)
    if entry is None:
        error_msg = f"Key not found: {key}"
        logger.warning(error_msg)

        response = {"status": "error", "message": error_msg}
        status_code = 404

        # Cache the error response too if client tracking is enabled
        if client_id and request_id:
            client_request = ClientRequest(client_id=client_id, request_id=request_id, operation=OperationType.DELETE, key=key)
            request_deduplication.mark_request_processed(client_request, response)
            logger.info(f"Cached error response for DELETE key={key}, client={client_id}, request={request_id}")

        raise HTTPException(status_code=status_code, detail=error_msg)

    logger.info(f"Added DELETE entry id={entry.id} for key={key}")

    # Replicate to followers
    _replicate_to_followers(entry)

    # Create response
    response = {"status": "ok", "id": entry.id}

    # If client tracking info was provided, store the response
    if client_id and request_id:
        client_request = ClientRequest(client_id=client_id, request_id=request_id, operation=OperationType.DELETE, key=key)
        request_deduplication.mark_request_processed(client_request, response)
        logger.info(f"Cached response for DELETE key={key}, client={client_id}, request={request_id}")

    return response


def _replicate_to_followers(entry):
    """Helper method to replicate an entry to all followers"""
    for follower_id, follower_url in followers.items():
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
