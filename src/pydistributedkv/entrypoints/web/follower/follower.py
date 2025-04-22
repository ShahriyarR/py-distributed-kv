import os

import requests
from fastapi import FastAPI, HTTPException

from pydistributedkv.configurator.settings.base import API_TIMEOUT
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
    entries = []

    for entry_data in data.get("entries", []):
        try:
            entry = LogEntry(**entry_data)
            if entry.validate_crc():
                entries.append(entry)
            else:
                print(f"Warning: Received entry with ID {entry.id} with invalid CRC from leader")
        except ValueError as e:
            print(f"Error parsing entry from leader: {str(e)}")

    return entries


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
    entries = []

    # Create and validate entries
    for entry_data in req.entries:
        try:
            entry = LogEntry(**entry_data)
            if entry.validate_crc():
                entries.append(entry)
            else:
                print(f"Warning: Received entry with ID {entry.id} with invalid CRC")
        except ValueError as e:
            print(f"Error parsing entry: {str(e)}")

    new_entries = _process_new_entries(entries)

    # Apply only new entries to the in-memory state
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
def get_key(key: str):
    # Followers can serve read requests directly
    value = storage.get(key)
    if value is None:
        raise HTTPException(status_code=404, detail="Key not found")
    return {"key": key, "value": value}


@app.get("/status")
def get_status():
    return {"follower_id": follower_id, "last_applied_id": last_applied_id, "leader_url": leader_url}
