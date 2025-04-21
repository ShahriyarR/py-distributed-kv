import json
import os
from enum import Enum
from typing import Any, Optional

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


class WAL:
    def __init__(self, log_file_path: str):
        self.log_file_path = log_file_path
        self.current_id = 0
        self._ensure_log_file_exists()
        self._load_last_id()

    def _ensure_log_file_exists(self):
        os.makedirs(os.path.dirname(self.log_file_path), exist_ok=True)
        if not os.path.exists(self.log_file_path):
            with open(self.log_file_path, "w") as f:
                pass  # Create empty file

    def _load_last_id(self):
        try:
            with open(self.log_file_path, "r") as f:
                lines = f.readlines()
                if lines:
                    last_entry = json.loads(lines[-1])
                    self.current_id = last_entry["id"]
        except (json.JSONDecodeError, IndexError, KeyError):
            self.current_id = 0

    def append(self, operation: OperationType, key: str, value: Optional[Any] = None) -> LogEntry:
        self.current_id += 1
        entry = LogEntry(id=self.current_id, operation=operation, key=key, value=value)

        with open(self.log_file_path, "a") as f:
            f.write(json.dumps(entry.model_dump()) + "\n")

        return entry

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
