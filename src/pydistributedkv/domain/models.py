import json
import os
import zlib
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
    crc: Optional[int] = None

    def calculate_crc(self) -> int:
        """Calculate CRC for this entry based on its content except the CRC itself."""
        # Create a copy of the entry without the CRC field
        data_for_crc = self.model_dump()
        data_for_crc.pop("crc", None)
        # Convert to a stable string representation and calculate CRC32
        json_str = json.dumps(data_for_crc, sort_keys=True)
        return zlib.crc32(json_str.encode())

    def validate_crc(self) -> bool:
        """Validate that the stored CRC matches the calculated one."""
        if self.crc is None:
            return False
        return self.crc == self.calculate_crc()


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
            # Skip invalid entries (with incorrect CRC)
            if "crc" in entry:
                log_entry = LogEntry(**entry)
                if not log_entry.validate_crc():
                    print(f"Warning: Entry with ID {entry_id} has invalid CRC, skipping")
                    return

            self.existing_ids.add(entry_id)
            if entry_id > self.current_id:
                self.current_id = entry_id
        except (json.JSONDecodeError, KeyError):
            pass

    def append(self, operation: OperationType, key: str, value: Optional[Any] = None) -> LogEntry:
        self.current_id += 1
        entry = LogEntry(id=self.current_id, operation=operation, key=key, value=value)
        # Calculate and set CRC
        entry.crc = entry.calculate_crc()
        return self.append_entry(entry)

    def append_entry(self, entry: LogEntry) -> LogEntry:
        """Append a pre-created entry, used for replication"""
        # Skip if entry already exists
        if entry.id in self.existing_ids:
            return entry

        # Update current_id if needed
        if entry.id > self.current_id:
            self.current_id = entry.id

        # Ensure entry has valid CRC
        if entry.crc is None:
            entry.crc = entry.calculate_crc()
        elif not entry.validate_crc():
            # Recalculate CRC if invalid
            entry.crc = entry.calculate_crc()

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
                    try:
                        entry_dict = json.loads(line)
                        entry = LogEntry(**entry_dict)

                        # Skip entries with invalid CRC
                        if not entry.validate_crc():
                            print(f"Warning: Skipping entry with ID {entry.id} due to CRC validation failure")
                            continue

                        if entry.id >= start_id:
                            entries.append(entry)
                    except (json.JSONDecodeError, ValueError) as e:
                        print(f"Error parsing log entry: {str(e)}")
                        continue
        except FileNotFoundError:
            pass

        return entries

    def get_last_id(self) -> int:
        return self.current_id
