import os

import requests
from fastapi import FastAPI, HTTPException

from pydistributedkv.domain.models import LogEntry, ReplicationRequest, WAL
from pydistributedkv.service.storage import KeyValueStorage

app = FastAPI()

# Initialize WAL and storage
wal = WAL(os.getenv("WAL_PATH", "data/follower/wal.log"))
storage = KeyValueStorage(wal)

# Leader connection info
leader_url = os.getenv("LEADER_URL", "http://localhost:8000")
follower_id = os.getenv("FOLLOWER_ID", "follower-1")
follower_url = os.getenv("FOLLOWER_URL", "http://localhost:8001")

# Replication state
last_applied_id = 0


@app.on_event("startup")
async def startup_event():
    # Register with leader
    try:
        response = requests.post(
            f"{leader_url}/register_follower",
            json={"id": follower_id, "url": follower_url},
        )
        response_data = response.json()

        # If leader has entries we don't, fetch them
        leader_last_id = response_data.get("last_log_id", 0)
        if leader_last_id > last_applied_id:
            await sync_with_leader()
    except requests.RequestException:
        # In production, you'd implement retry logic
        print(f"Failed to register with leader at {leader_url}")


async def sync_with_leader():
    global last_applied_id
    try:
        response = requests.get(f"{leader_url}/log_entries/{last_applied_id}")
        data = response.json()

        entries = [LogEntry(**entry) for entry in data.get("entries", [])]
        if entries:
            # Append entries to the follower's WAL
            for entry in entries:
                wal.append(entry.operation, entry.key, entry.value)
            last_id = storage.apply_entries(entries)
            last_applied_id = last_id
    except requests.RequestException:
        print("Failed to sync with leader")


@app.post("/replicate")
async def replicate(req: ReplicationRequest):
    global last_applied_id
    entries = [LogEntry(**entry) for entry in req.entries]
    if entries:
        # Append entries to the follower's WAL
        for entry in entries:
            wal.append(entry.operation, entry.key, entry.value)

        # Apply entries to the in-memory state
        last_id = storage.apply_entries(entries)
        last_applied_id = max(last_applied_id, last_id)
    return {"status": "ok", "last_applied_id": last_applied_id}


@app.get("/key/{key}")
def get_key(key: str):
    # Followers can serve read requests directly
    value = storage.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": value}


@app.get("/status")
def get_status():
    return {"follower_id": follower_id, "last_applied_id": last_applied_id, "leader_url": leader_url}
