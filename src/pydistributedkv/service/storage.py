from typing import Any, Dict, List, Optional

from pydistributedkv.domain.models import LogEntry, OperationType, WAL


class KeyValueStorage:
    def __init__(self, wal: WAL):
        self.wal = wal
        self.data: Dict[str, Any] = {}
        self._replay_log()

    def _replay_log(self):
        """Replay the WAL to rebuild the in-memory state"""
        entries = self.wal.read_from(0)
        for entry in entries:
            self._apply_log_entry(entry)

    def _apply_log_entry(self, entry: LogEntry) -> None:
        """Apply a single log entry to the in-memory state"""
        if entry.operation == OperationType.SET:
            self.data[entry.key] = entry.value
        elif entry.operation == OperationType.DELETE:
            if entry.key in self.data:
                del self.data[entry.key]

    def set(self, key: str, value: Any) -> LogEntry:
        """Set a key-value pair and log the operation"""
        entry = self.wal.append(OperationType.SET, key, value)
        self.data[key] = value
        return entry

    def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        return self.data.get(key)

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
