import os

import requests
from fastapi import FastAPI, HTTPException

from pydistributedkv.domain.models import FollowerRegistration, KeyValue, WAL
from pydistributedkv.service.storage import KeyValueStorage

app = FastAPI()

# Initialize WAL and storage
wal = WAL(os.getenv("WAL_PATH", "data/leader/wal.log"))
storage = KeyValueStorage(wal)

# Track followers and their replication status
followers: dict[str, str] = {}  # follower_id -> url
replication_status: dict[str, int] = {}  # follower_id -> last_replicated_id


@app.get("/key/{key}")
def get_key(key: str):
    value = storage.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": value}


@app.put("/key/{key}")
def set_key(key: str, kv: KeyValue):
    entry = storage.set(key, kv.value)

    # Replicate to followers asynchronously
    # In a production system, you'd want to handle this more robustly
    # (e.g., with a background task queue)
    for follower_id, follower_url in followers.items():
        try:
            requests.post(
                f"{follower_url}/replicate",
                json={"entries": [entry.model_dump()]},
            )
            replication_status[follower_id] = entry.id
        except requests.RequestException:
            # In production, you'd want better error handling and retry logic
            pass

    return {"status": "ok", "id": entry.id}


@app.delete("/key/{key}")
def delete_key(key: str):
    entry = storage.delete(key)
    if entry is None:
        raise HTTPException(status_code=404, detail="Key not found")

    # Replicate deletion to followers
    for follower_id, follower_url in followers.items():
        try:
            requests.post(
                f"{follower_url}/replicate",
                json={"entries": [entry.model_dump()]},
            )
            replication_status[follower_id] = entry.id
        except requests.RequestException:
            # In production, you'd want better error handling and retry logic
            pass

    return {"status": "ok", "id": entry.id}


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
