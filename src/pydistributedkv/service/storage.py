from typing import Any, Dict, List, Optional, Tuple

from pydistributedkv.domain.models import LogEntry, OperationType, VersionedValue, WAL


class KeyValueStorage:
    def __init__(self, wal: WAL):
        self.wal = wal
        # Changed from Dict[str, Any] to Dict[str, VersionedValue]
        self.data: Dict[str, VersionedValue] = {}
        self._replay_log()

    def _replay_log(self):
        """Replay the WAL to rebuild the in-memory state"""
        entries = self.wal.read_from(0)
        entries_count = len(entries)
        print(f"Replaying {entries_count} entries from WAL...")

        for i, entry in enumerate(entries):
            self._apply_log_entry(entry)

            # Log progress for larger datasets
            if entries_count > 1000 and i % 1000 == 0:
                print(f"Replayed {i}/{entries_count} entries...")

        print(f"Finished replaying {entries_count} entries, data store contains {len(self.data)} keys")

    def _apply_log_entry(self, entry: LogEntry) -> None:
        """Apply a single log entry to the in-memory state"""
        if entry.operation == OperationType.SET:
            self._apply_set_operation(entry)
        elif entry.operation == OperationType.DELETE:
            self._apply_delete_operation(entry)

    def _apply_set_operation(self, entry: LogEntry) -> None:
        """Apply SET operation to the in-memory state"""
        version = entry.version if entry.version is not None else 1

        if entry.key in self.data:
            self.data[entry.key].update(entry.value, version)
        else:
            self.data[entry.key] = VersionedValue(current_version=version, value=entry.value)

    def _apply_delete_operation(self, entry: LogEntry) -> None:
        """Apply DELETE operation to the in-memory state"""
        if entry.key in self.data:
            del self.data[entry.key]

    def set(self, key: str, value: Any, version: Optional[int] = None) -> Tuple[LogEntry | None, int]:
        """Set a key-value pair and log the operation

        Returns:
            Tuple[LogEntry, int]: The log entry and the actual version used
        """
        # Determine version and check for conflicts
        next_version, conflict = self._determine_version(key, version)
        if conflict:
            current_version = self.data[key].current_version
            return None, current_version

        # Create and log the entry with version
        entry = self.wal.append(OperationType.SET, key, value, version=next_version)

        # Update in-memory state
        self._update_in_memory_state(key, value, next_version)

        return entry, next_version

    def _determine_version(self, key: str, requested_version: Optional[int]) -> Tuple[int, bool]:
        """Determine the appropriate version and check for conflicts

        Returns:
            Tuple[int, bool]: (next_version, has_conflict)
        """
        if key not in self.data:
            return self._handle_new_key(requested_version)

        return self._handle_existing_key(key, requested_version)

    def _handle_new_key(self, requested_version: Optional[int]) -> Tuple[int, bool]:
        """Handle version determination for a new key"""
        next_version = requested_version if requested_version and requested_version > 1 else 1
        return next_version, False

    def _handle_existing_key(self, key: str, requested_version: Optional[int]) -> Tuple[int, bool]:
        """Handle version determination for an existing key"""
        current_version = self.data[key].current_version
        if requested_version and requested_version <= current_version:
            return current_version, True
        return current_version + 1, False

    def _update_in_memory_state(self, key: str, value: Any, version: int) -> None:
        """Update the in-memory state with the new value and version"""
        if key in self.data:
            self.data[key].update(value, version)
        else:
            self.data[key] = VersionedValue(current_version=version, value=value)

    def get(self, key: str, version: Optional[int] = None) -> Optional[Any]:
        """Get a value by key and optional version"""
        if key not in self.data:
            return None

        return self.data[key].get_value(version)

    def get_with_version(self, key: str, version: Optional[int] = None) -> Optional[Tuple[Any, int]]:
        """Get a value and its version by key"""
        if key not in self.data:
            return None

        versioned_value = self.data[key]
        value = versioned_value.get_value(version)

        if value is None:
            return None

        actual_version = version if version is not None else versioned_value.current_version
        return (value, actual_version)

    def get_version_history(self, key: str) -> Optional[Dict[int, Any]]:
        """Get the version history for a key"""
        if key not in self.data:
            return None

        versioned_value = self.data[key]
        # Start with the current version
        history = {versioned_value.current_version: versioned_value.value}

        # Add all historical versions if they exist
        if versioned_value.history:
            history.update(versioned_value.history)

        return history

    def delete(self, key: str) -> Optional[LogEntry]:
        """Delete a key and log the operation"""
        if key in self.data:
            entry = self.wal.append(OperationType.DELETE, key)
            del self.data[key]
            return entry
        return None

    def apply_entries(self, entries: List[LogEntry]) -> int:
        """Apply multiple log entries and return the last applied ID"""
        last_id = 0
        for entry in entries:
            self._apply_log_entry(entry)
            last_id = entry.id
        return last_id

    def get_all_keys(self) -> List[str]:
        """Get a list of all keys in the storage"""
        return list(self.data.keys())

    def get_latest_version(self, key: str) -> Optional[int]:
        """Get the latest version number for a key"""
        if key not in self.data:
            return None
        return self.data[key].current_version

    def compact_log(self) -> Tuple[int, int]:
        """Compact the write-ahead log by removing redundant entries.

        Returns:
            Tuple containing (segments_compacted, entries_removed)
        """
        segments_compacted, entries_removed = self.wal.compact_segments()
        return segments_compacted, entries_removed
