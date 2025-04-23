import glob
import json
import os
import zlib
from enum import Enum
from typing import Any, List, Optional, Set

from pydantic import BaseModel


class OperationType(str, Enum):
    SET = "SET"
    DELETE = "DELETE"
    GET = "GET"


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


class ClientRequest(BaseModel):
    """Request from a client with unique identifiers to enable idempotent processing"""

    client_id: str
    request_id: str
    operation: Optional[OperationType] = None
    key: Optional[str] = None
    value: Optional[Any] = None


class ReplicationRequest(BaseModel):
    entries: list[dict[str, Any]]


class FollowerRegistration(BaseModel):
    id: str
    url: str
    last_applied_id: int = 0


class WAL:
    def __init__(self, log_file_path: str, max_segment_size: int = 1024 * 1024):  # Default 1MB per segment
        self.log_dir = os.path.dirname(log_file_path)
        self.base_name = os.path.basename(log_file_path)
        self.max_segment_size = max_segment_size
        self.current_id = 0
        self.existing_ids: Set[int] = set()
        self.active_segment_path = ""

        self._ensure_log_dir_exists()
        self._initialize_segments()

    def _ensure_log_dir_exists(self):
        """Ensure that the directory for log files exists."""
        os.makedirs(self.log_dir, exist_ok=True)

    def _initialize_segments(self):
        """Initialize segments, find existing ones, and determine the active segment."""
        segments = self._get_all_segments()

        if not segments:
            # No segments yet, create the first one
            self.active_segment_path = self._create_segment_path(1)
            with open(self.active_segment_path, "w"):
                pass  # Create empty file
        else:
            # Find the highest segment
            latest_segment = segments[-1]
            self.active_segment_path = latest_segment

        self._load_all_entries()

    def _get_all_segments(self) -> List[str]:
        """Get all segment files sorted by segment number."""
        segment_pattern = os.path.join(self.log_dir, f"{self.base_name}.segment.*")
        segments = sorted(glob.glob(segment_pattern), key=self._extract_segment_number)
        return segments

    def _extract_segment_number(self, segment_path: str) -> int:
        """Extract the segment number from a segment file path."""
        try:
            return int(segment_path.split(".")[-1])
        except (ValueError, IndexError):
            return 0

    def _create_segment_path(self, segment_number: int) -> str:
        """Create a path for a new segment file with the given number."""
        return os.path.join(self.log_dir, f"{self.base_name}.segment.{segment_number}")

    def _load_all_entries(self):
        """Load all entries from all segments to populate existing IDs and determine current ID."""
        segments = self._get_all_segments()
        for segment in segments:
            self._load_entries_from_file(segment)

    def _load_entries_from_file(self, file_path: str):
        """Load entries from a specific file."""
        try:
            with open(file_path, "r") as f:
                self._process_log_entries(f)
        except FileNotFoundError:
            pass

    def _process_log_entries(self, file_handle):
        """Process each line in the log file to extract entry IDs."""
        for line in file_handle:
            self._process_log_entry(line)

    def _process_log_entry(self, line):
        """Process a single log entry line and update tracking data."""
        try:
            entry = json.loads(line)
            entry_id = entry["id"]

            if not self._is_valid_entry(entry, entry_id):
                return

            self._update_tracking_data(entry_id)
        except (json.JSONDecodeError, KeyError):
            pass

    def _is_valid_entry(self, entry, entry_id):
        """Check if an entry has valid CRC."""
        if "crc" not in entry:
            return True

        log_entry = LogEntry(**entry)
        if not log_entry.validate_crc():
            print(f"Warning: Entry with ID {entry_id} has invalid CRC, skipping")
            return False
        return True

    def _update_tracking_data(self, entry_id):
        """Update tracking data for a valid entry."""
        self.existing_ids.add(entry_id)
        if entry_id > self.current_id:
            self.current_id = entry_id

    def _get_next_segment_number(self) -> int:
        """Get the next segment number based on the active segment."""
        current_segment_num = self._extract_segment_number(self.active_segment_path)
        return current_segment_num + 1

    def _roll_segment_if_needed(self):
        """Roll over to a new segment file if the current one exceeds the size limit."""
        try:
            if os.path.getsize(self.active_segment_path) >= self.max_segment_size:
                next_segment_num = self._get_next_segment_number()
                self.active_segment_path = self._create_segment_path(next_segment_num)
                # Create the new empty segment file
                with open(self.active_segment_path, "w"):
                    pass
                print(f"Rolled over to new segment: {self.active_segment_path}")
        except OSError:
            # If there's an issue checking the file size, just continue with the current segment
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

        # Check if we need to roll over to a new segment
        self._roll_segment_if_needed()

        # Append entry to the active segment
        with open(self.active_segment_path, "a") as f:
            f.write(json.dumps(entry.model_dump()) + "\n")

        self.existing_ids.add(entry.id)
        return entry

    def has_entry(self, entry_id: int) -> bool:
        """Check if an entry with the given ID already exists in the WAL"""
        return entry_id in self.existing_ids

    def read_from(self, start_id: int = 0) -> list[LogEntry]:
        """Read log entries with ID >= start_id from all segments."""
        entries = []

        # Get all segments
        segments = self._get_all_segments()

        # Process each segment
        for segment in segments:
            try:
                self._append_entries(entries, segment, start_id)
            except FileNotFoundError:
                continue

        # Sort entries by ID to ensure correct order
        entries.sort(key=lambda e: e.id)
        return entries

    def _append_entries(self, entries, segment, start_id):
        with open(segment, "r") as f:
            for line in f:
                try:
                    self._append_single_entry(entries, line, start_id)
                except (json.JSONDecodeError, ValueError) as e:
                    print(f"Error parsing log entry: {str(e)}")
                    continue

    def _append_single_entry(self, entries, line, start_id):
        entry = self._parse_log_entry(line)
        if entry and self._should_include_entry(entry, start_id):
            entries.append(entry)

    def _parse_log_entry(self, line: str) -> Optional[LogEntry]:
        """Parse a log entry from a line in the log file."""
        try:
            entry_dict = json.loads(line)
            entry = LogEntry(**entry_dict)

            # Skip entries with invalid CRC
            if not entry.validate_crc():
                print(f"Warning: Skipping entry with ID {entry.id} due to CRC validation failure")
                return None

            return entry
        except (json.JSONDecodeError, ValueError) as e:
            print(f"Error parsing log entry: {str(e)}")
            return None

    def _should_include_entry(self, entry: LogEntry, start_id: int) -> bool:
        """Check if an entry should be included based on its ID."""
        return entry.id >= start_id

    def get_last_id(self) -> int:
        return self.current_id

    def get_segment_files(self) -> List[str]:
        """Get a list of all segment files."""
        return self._get_all_segments()

    def get_active_segment(self) -> str:
        """Get the path of the currently active segment."""
        return self.active_segment_path
