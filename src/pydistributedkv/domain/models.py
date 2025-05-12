import glob
import json
import os
import zlib
from enum import Enum
from typing import Any, Dict, List, Optional, Set, Tuple

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
    version: Optional[int] = None  # Version number for the key

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
    version: Optional[int] = None  # Optional version for specific version retrieval


class VersionedValue(BaseModel):
    """Model representing a value with version history"""

    current_version: int  # Latest version number
    value: Any  # Current value
    history: Optional[Dict[int, Any]] = None  # Version -> Value mapping

    def get_value(self, version: Optional[int] = None) -> Optional[Any]:
        """Get value at specific version, or latest if version is None"""
        if version is None:
            return self.value

        if version == self.current_version:
            return self.value

        if self.history and version in self.history:
            return self.history[version]

        return None

    def update(self, value: Any, version: int) -> None:
        """Update with a new value and version"""
        if version <= self.current_version:
            # Ignore updates with older versions
            return

        # Save current value to history before updating
        if self.history is None:
            self.history = {}

        # Always keep history of previous versions
        self.history[self.current_version] = self.value

        self.value = value
        self.current_version = version


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
    version: Optional[int] = None  # Specific version for GET or precondition for SET


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
            if os.path.exists(self.active_segment_path) and os.path.getsize(self.active_segment_path) >= self.max_segment_size:
                next_segment_num = self._get_next_segment_number()
                self.active_segment_path = self._create_segment_path(next_segment_num)
                # Create the new empty segment file
                with open(self.active_segment_path, "w"):
                    pass
                print(f"Rolled over to new segment: {self.active_segment_path}")
        except OSError:
            # If there's an issue checking the file size, just continue with the current segment
            pass

    def append(self, operation: OperationType, key: str, value: Optional[Any] = None, version: Optional[int] = None) -> LogEntry:
        self.current_id += 1
        entry = LogEntry(id=self.current_id, operation=operation, key=key, value=value, version=version)
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

    def compact_segments(self) -> Tuple[int, int]:
        """Compact segments by keeping only the latest operation for each key.

        Returns:
            Tuple containing:
            - Number of segments compacted
            - Number of entries removed
        """
        segments = self._get_segments_for_compaction()
        if not segments:
            return 0, 0

        # Read all entries from segments marked for compaction
        entries = self._read_entries_from_segments(segments)
        if not entries:
            return 0, 0

        # Filter to get only the latest operation for each key
        entries_to_keep = self._filter_latest_entries(entries)

        # Calculate entries removed
        entries_removed = len(entries) - len(entries_to_keep)

        # Write the compacted entries and handle cleanup
        self._write_compacted_entries(segments, entries_to_keep)

        return len(segments), entries_removed

    def _get_segments_for_compaction(self) -> List[str]:
        """Get segments that should be compacted (all except the active one)"""
        segments = self._get_all_segments()

        # Don't compact if we have only one segment (which is the active one)
        if len(segments) <= 1:
            return []

        # The last segment is the active one, we won't compact it
        return segments[:-1]

    def _read_entries_from_segments(self, segments: List[str]) -> List[LogEntry]:
        """Read all entries from the given segments"""
        entries = []
        for segment in segments:
            segment_entries = self._read_entries_from_segment(segment)
            entries.extend(segment_entries)
        return entries

    def _filter_latest_entries(self, entries: List[LogEntry]) -> List[LogEntry]:
        """Filter entries to keep only the latest operation for each key"""
        key_to_latest_entry: Dict[str, LogEntry] = {}

        # Track latest operation for each key
        for entry in sorted(entries, key=lambda e: e.id):
            if entry.operation in [OperationType.SET, OperationType.DELETE]:
                key_to_latest_entry[entry.key] = entry

        # Extract the values and sort by ID
        result = list(key_to_latest_entry.values())
        result.sort(key=lambda e: e.id)
        return result

    def _write_compacted_entries(self, segments_to_remove: List[str], entries: List[LogEntry]) -> None:
        """Write compacted entries to a new segment and clean up old segments"""
        # Create a new compacted segment
        compacted_segment_path = self._create_compacted_segment()

        # Write entries to the compacted segment
        with open(compacted_segment_path, "w") as f:
            for entry in entries:
                f.write(json.dumps(entry.model_dump()) + "\n")

        # Delete old segments after successful compaction
        self._delete_segments(segments_to_remove)

        # Update segment numbers to be continuous
        self._renumber_segments()

    def _delete_segments(self, segments: List[str]) -> None:
        """Delete the given segment files"""
        for segment in segments:
            try:
                os.remove(segment)
            except OSError as e:
                print(f"Error removing segment {segment}: {e}")

    def _read_entries_from_segment(self, segment_path: str) -> List[LogEntry]:
        """Read all entries from a segment file."""
        if not self._is_valid_segment_file(segment_path):
            return []

        entries = []
        for line in self._read_segment_lines(segment_path):
            entry = self._parse_entry_from_line(line)
            if entry:
                entries.append(entry)

        return entries

    def _is_valid_segment_file(self, segment_path: str) -> bool:
        """Check if the segment file exists and is readable."""
        return os.path.exists(segment_path)

    def _read_segment_lines(self, segment_path: str) -> List[str]:
        """Read all lines from a segment file, handling errors."""
        lines = []
        try:
            with open(segment_path, "r") as f:
                lines = f.readlines()
        except OSError:
            print(f"Error accessing segment file {segment_path}")
        return lines

    def _parse_entry_from_line(self, line: str) -> Optional[LogEntry]:
        """Parse a single entry from a line in the log file."""
        try:
            entry_dict = json.loads(line)
            entry = LogEntry(**entry_dict)

            if entry.crc and not entry.validate_crc():
                return None

            return entry
        except (json.JSONDecodeError, ValueError):
            # Skip invalid entries
            return None

    def _create_compacted_segment(self) -> str:
        """Create a new segment file for compacted entries."""
        return os.path.join(self.log_dir, f"{self.base_name}.compacted.temp")

    def _renumber_segments(self):
        """Rename segments to ensure contiguous numbering after compaction."""
        segments = self._get_all_segments()
        compacted_path = os.path.join(self.log_dir, f"{self.base_name}.compacted.temp")

        # Check if compacted file exists
        if not os.path.exists(compacted_path):
            return

        # Mark the active segment
        self._set_active_segment(segments)

        # Prepare for renumbering by renaming segments to temporary names
        self._prepare_segments_for_renumbering(segments)

        # Move the compacted file to be the first segment
        self._rename_compacted_to_first_segment(compacted_path)

        # Rename the remaining segments to have contiguous numbers
        self._rename_remaining_segments(segments)

        # Update the active segment path
        self._update_active_segment_path()

    def _set_active_segment(self, segments):
        """Mark the active segment (the last one)."""
        if segments:
            # The last segment is the active one
            self.active_segment_path = segments[-1]

    def _prepare_segments_for_renumbering(self, segments):
        """Rename existing segments to temporary names to avoid conflicts."""
        for segment in segments[:-1]:  # Skip the last (active) segment
            try:
                temp_path = f"{segment}.tmp"
                if os.path.exists(segment):
                    os.rename(segment, temp_path)
            except OSError as e:
                print(f"Error renaming segment {segment}: {e}")

    def _rename_compacted_to_first_segment(self, compacted_path):
        """Rename the compacted file to be the first segment."""
        try:
            new_first_segment = self._create_segment_path(1)
            os.rename(compacted_path, new_first_segment)
        except OSError as e:
            print(f"Error renaming compacted file: {e}")

    def _rename_remaining_segments(self, segments):
        """Rename remaining segments to have contiguous numbers."""
        # Start from 2 because compacted file is now segment 1
        for i, segment in enumerate(segments[:-1], 2):
            try:
                temp_path = f"{segment}.tmp"
                if os.path.exists(temp_path):
                    new_path = self._create_segment_path(i)
                    os.rename(temp_path, new_path)
            except OSError as e:
                print(f"Error renaming segment {temp_path}: {e}")

    def _update_active_segment_path(self):
        """Update the active segment path to be the highest segment."""
        segments = self._get_all_segments()
        if segments:
            self.active_segment_path = segments[-1]
