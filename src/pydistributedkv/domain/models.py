import json
import os
from enum import Enum
from typing import Any, Optional, Set

from pydantic import BaseModel


class OperationType(str, Enum):
    SET = "SET"
    DELETE = "DELETE"


class LogEntry(BaseModel):
    id: int
    operation: OperationType
    key: str
    value: Optional[Any] = None


class KeyValue(BaseModel):
    value: Any


class ReplicationStatus(BaseModel):
    follower_id: str
    last_replicated_id: int


class ReplicationRequest(BaseModel):
    entries: list[dict[str, Any]]


class FollowerRegistration(BaseModel):
    id: str
    url: str
    last_applied_id: int = 0


class WAL:
    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path
        self.current_id = 0
        self.existing_ids: Set[int] = set()
        self._ensure_log_file_exists()
        self._load_last_id()

    def _ensure_log_file_exists(self):
        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, "w"):
                pass  # Create empty file

    def _load_last_id(self):
        """Load the last entry ID from the log file and populate existing IDs set."""
        try:
            with open(self.log_file_path, "r") as f:
                self._process_log_entries(f)
        except FileNotFoundError:
            self.current_id = 0

    def _process_log_entries(self, file_handle):
        """Process each line in the log file to extract entry IDs."""
        for line in file_handle:
            self._process_log_entry(line)

    def _process_log_entry(self, line):
        """Process a single log entry line and update tracking data."""
        try:
            entry = json.loads(line)
            entry_id = entry["id"]
            self.existing_ids.add(entry_id)
            if entry_id > self.current_id:
                self.current_id = entry_id
        except (json.JSONDecodeError, KeyError):
            pass

    def append(self, operation: OperationType, key: str, value: Optional[Any] = None) -> LogEntry:
        self.current_id += 1
        entry = LogEntry(id=self.current_id, operation=operation, key=key, value=value)
        return self.append_entry(entry)

    def append_entry(self, entry: LogEntry) -> LogEntry:
        """Append a pre-created entry, used for replication"""
        # Skip if entry already exists
        if entry.id in self.existing_ids:
            return entry

        # Update current_id if needed
        if entry.id > self.current_id:
            self.current_id = entry.id

        with open(self.log_file_path, "a") as f:
            f.write(json.dumps(entry.model_dump()) + "\n")

        self.existing_ids.add(entry.id)
        return entry

    def has_entry(self, entry_id: int) -> bool:
        """Check if an entry with the given ID already exists in the WAL"""
        return entry_id in self.existing_ids

    def read_from(self, start_id: int = 0) -> list[LogEntry]:
        entries = []
        try:
            with open(self.log_file_path, "r") as f:
                for line in f:
                    entry_dict = json.loads(line)
                    entry = LogEntry(**entry_dict)
                    if entry.id >= start_id:
                        entries.append(entry)
        except (json.JSONDecodeError, FileNotFoundError):
            pass

        return entries

    def get_last_id(self) -> int:
        return self.current_id
