import logging

import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, AsyncMock
import requests

from pydistributedkv.entrypoints.web.follower.follower import (
    app, wal, storage, compaction_service, heartbeat_service,
    request_deduplication, startup_event, shutdown_event,
    sync_with_leader, fetch_entries_from_leader,
    _parse_and_validate_entries,
    _create_valid_entry, append_entries_to_wal, apply_entries_to_storage
)
from pydistributedkv.domain.models import LogEntry

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture(autouse=True)
def reset_global_state():
    """Reset global state before each test"""
    import pydistributedkv.entrypoints.web.follower.follower as follower_module
    original_value = follower_module.last_applied_id
    follower_module.last_applied_id = 0

    yield

    follower_module.last_applied_id = original_value


@pytest.fixture
def mock_log_entry():
    """Create a properly mocked LogEntry"""
    entry = MagicMock(spec=LogEntry)
    entry.id = 1
    entry.operation = "SET"
    entry.key = "test"
    entry.value = "value"
    entry.crc = 123
    entry.validate_crc.return_value = True
    return entry


# Replication Tests
def test_replicate_endpoint_success(client, mock_log_entry):
    """Test the /replicate endpoint successfully processes new entries."""
    entries_data = [{"id": 1, "operation": "SET", "key": "test",
                    "value": "value", "crc": 123}]

    with patch('pydistributedkv.entrypoints.web.follower.follower.'
               '_parse_and_validate_entries',
               return_value=[mock_log_entry]), \
         patch.object(wal, 'has_entry', return_value=False), \
         patch.object(wal, 'append_entry', return_value=None), \
         patch.object(storage, 'apply_entries', return_value=1):

        response = client.post("/replicate", json={"entries": entries_data})
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "last_applied_id": 1}


def test_replicate_endpoint_duplicate(client, mock_log_entry):
    """Test the /replicate endpoint skips duplicate entries."""
    entries_data = [{"id": 1, "operation": "SET", "key": "test",
                    "value": "value", "crc": 123}]

    with patch('pydistributedkv.entrypoints.web.follower.follower.'
               '_parse_and_validate_entries',
               return_value=[mock_log_entry]), \
         patch.object(wal, 'has_entry', return_value=True), \
         patch.object(wal, 'append_entry',
                      return_value=None) as mock_append, \
         patch.object(storage, 'apply_entries', return_value=0):

        response = client.post("/replicate", json={"entries": entries_data})
        assert response.status_code == 200
        mock_append.assert_not_called()


# Key-Value Operation Tests
def test_get_key_success(client):
    """Test retrieving a key with a specific value and version."""
    with patch.object(storage, 'get_with_version',
                      return_value=("value", 1)):
        response = client.get("/key/test")
        assert response.status_code == 200
        assert response.json() == {"key": "test",
                                   "value": "value", "version": 1}


def test_get_key_with_version(client):
    """Test retrieving a key with a specific version."""
    with patch.object(storage, 'get_with_version',
                      return_value=("old_value", 2)):
        response = client.get("/key/test?version=2")
        assert response.status_code == 200
        assert response.json() == {"key": "test",
                                   "value": "old_value", "version": 2}


def test_get_key_not_found(client):
    """Test retrieving a non-existent key."""
    with patch.object(storage, 'get_with_version', return_value=None):
        response = client.get("/key/nonexistent")
        assert response.status_code == 404
        assert "Key not found" in response.json()["detail"]


# Compaction Service Tests
def test_manual_compaction_success(client):
    """Test manually triggering compaction."""
    with patch.object(compaction_service, 'run_compaction',
                      new_callable=AsyncMock, return_value=(2, 10)):
        response = client.post("/compaction/run?force=true")
        assert response.status_code == 200
        assert response.json() == {"status": "success",
                                   "segments_compacted": 2,
                                   "entries_removed": 10}


def test_compaction_status(client):
    """Test retrieving compaction service status."""
    with patch.object(compaction_service, 'get_status',
                      return_value={"enabled": True, "interval": 300}):
        response = client.get("/compaction/status")
        assert response.status_code == 200
        assert response.json() == {"enabled": True, "interval": 300}


def test_configure_compaction(client):
    """Test configuring compaction service."""
    with patch.object(compaction_service, 'set_enabled',
                      return_value=True), \
         patch.object(compaction_service,
                      'set_compaction_interval', return_value=600):
        url = "/compaction/configure?enabled=true&interval=600"
        response = client.post(url)
        assert response.status_code == 200
        assert response.json()["status"] == "success"
        assert response.json()["changes"]["enabled"] is True
        assert response.json()["changes"]["interval"] == 600


# Heartbeat Service Tests
def test_receive_heartbeat(client):
    """Test receiving a heartbeat from the leader."""
    with patch.object(heartbeat_service, 'record_heartbeat') as mock_record:
        response = client.post("/heartbeat", json={"server_id": "leader",
                                                   "timestamp": 1234567890})
        assert response.status_code == 200
        assert response.json()["status"] == "ok"
        assert "server_id" in response.json()
        mock_record.assert_called_with("leader")


def test_receive_heartbeat_missing_server_id(client):
    """Test heartbeat with missing server_id."""
    response = client.post("/heartbeat", json={"timestamp": 1234567890})
    assert response.status_code == 200
    assert response.json()["status"] == "error"
    assert "Missing server_id" in response.json()["message"]


def test_cluster_status(client):
    """Test retrieving cluster status."""
    with patch.object(heartbeat_service, 'get_server_status',
                      return_value={"status": "healthy"}):
        response = client.get("/cluster_status")
        assert response.status_code == 200
        assert response.json()["follower"]["status"] == "healthy"
        assert response.json()["leader"] == {"status": "healthy"}


# Request Deduplication Tests
def test_get_key_deduplication(client):
    """Test deduplication of GET requests."""
    # First request - not cached
    with patch.object(storage, 'get_with_version',
                      return_value=("value", 1)), \
         patch.object(request_deduplication,
                      'get_processed_result', return_value=None), \
         patch.object(request_deduplication,
                      'mark_request_processed') as mock_mark:
        url = "/key/test?client_id=client1&request_id=req1"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == {"key": "test",
                                   "value": "value", "version": 1}
        mock_mark.assert_called_once()

    # Second request - cached response
    cached_response = {"key": "test", "value": "value", "version": 1}
    with patch.object(request_deduplication, 'get_processed_result',
                      return_value=cached_response):
        response = client.get(url)
        assert response.status_code == 200
        assert response.json() == cached_response


def test_request_status_endpoint(client):
    """Test the request status endpoint."""
    with patch.object(request_deduplication, 'get_processed_result',
                      return_value={"key": "test", "value": "value"}):
        url = ("/request_status?client_id=client1&request_id=req1"
               "&operation=GET")
        response = client.get(url)

        assert response.status_code == 200
        assert response.json()["processed"] is True
        assert "result" in response.json()

    with patch.object(request_deduplication, 'get_processed_result',
                      return_value=None):
        url = "/request_status?client_id=client1&request_id=req1"
        response = client.get(url)
        assert response.status_code == 200
        assert response.json()["processed"] is False


def test_deduplication_stats(client):
    """Test getting deduplication statistics."""
    stats = {"total_duplicates_detected": 5, "total_requests": 10}
    with patch.object(request_deduplication, 'get_stats',
                      return_value=stats):
        response = client.get("/deduplication_stats")
        assert response.status_code == 200
        assert response.json() == stats


# Status and Utility Endpoint Tests
def test_get_status(client):
    """Test retrieving follower status."""
    response = client.get("/status")
    assert response.status_code == 200
    json_response = response.json()
    assert "follower_id" in json_response
    assert "last_applied_id" in json_response
    assert "leader_url" in json_response


def test_get_segments(client):
    """Test retrieving WAL segment information."""
    with patch.object(wal, 'get_segment_files',
                      return_value=["seg1.log", "seg2.log"]), \
         patch.object(wal, 'get_active_segment', return_value="seg2.log"), \
         patch('os.path.getsize', return_value=1024):

        response = client.get("/segments")
        assert response.status_code == 200
        json_response = response.json()
        assert len(json_response["segments"]) == 2
        assert json_response["segments"][1]["is_active"] is True
        assert json_response["total_segments"] == 2


def test_get_all_keys(client):
    """Test retrieving all keys from storage."""
    with patch.object(storage, 'get_all_keys',
                      return_value=["key1", "key2"]):
        response = client.get("/keys")
        assert response.status_code == 200
        assert response.json() == {"keys": ["key1", "key2"], "count": 2}


# Key History and Versions Tests
def test_get_key_history(client):
    """Test retrieving a key's version history."""
    with patch.object(storage, 'get_version_history',
                      return_value={1: "value1", 2: "value2"}):
        response = client.get("/key/test/history")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["key"] == "test"
        assert json_response["versions"] == [1, 2]
        assert len(json_response["history"]) == 2


def test_get_key_versions(client):
    """Test retrieving available versions for a key."""
    with patch.object(storage, 'get_version_history',
                      return_value={1: "value1", 2: "value2"}), \
         patch.object(storage, 'get_latest_version', return_value=2):

        response = client.get("/key/test/versions")
        assert response.status_code == 200
        json_response = response.json()
        assert json_response["key"] == "test"
        assert json_response["versions"] == [1, 2]
        assert json_response["latest_version"] == 2


# Startup and Shutdown Tests
@pytest.mark.asyncio
async def test_startup_event():
    """Test the startup event registers with the leader and starts services."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"last_log_id": 0}

    with patch('requests.post', return_value=mock_response), \
         patch.object(heartbeat_service, 'register_server'), \
         patch.object(heartbeat_service, 'start_monitoring',
                      new_callable=AsyncMock), \
         patch.object(heartbeat_service, 'start_sending',
                      new_callable=AsyncMock), \
         patch.object(compaction_service, 'start',
                      new_callable=AsyncMock), \
         patch('pydistributedkv.entrypoints.web.follower.follower.'
               'sync_with_leader', new_callable=AsyncMock) as mock_sync:
        await startup_event()
        heartbeat_service.start_monitoring.assert_called_once()
        heartbeat_service.start_sending.assert_called_once()
        compaction_service.start.assert_called_once()


@pytest.mark.asyncio
async def test_startup_event_with_sync(client):
    """Test startup event when leader has newer entries."""
    mock_response = MagicMock()
    mock_response.json.return_value = {"last_log_id": 5}  # Leader ahead

    with patch('requests.post', return_value=mock_response), \
         patch.object(heartbeat_service, 'register_server'), \
         patch.object(heartbeat_service, 'start_monitoring',
                      new_callable=AsyncMock), \
         patch.object(heartbeat_service, 'start_sending',
                      new_callable=AsyncMock), \
         patch.object(compaction_service, 'start',
                      new_callable=AsyncMock), \
         patch('pydistributedkv.entrypoints.web.follower.follower.'
               'sync_with_leader', new_callable=AsyncMock) as mock_sync:

        await startup_event()
        mock_sync.assert_called_once()


@pytest.mark.asyncio
async def test_startup_event_leader_unreachable():
    """Test startup event when leader is unreachable."""
    with patch('requests.post',
               side_effect=requests.RequestException("Connection failed")), \
         patch.object(heartbeat_service, 'register_server'), \
         patch.object(heartbeat_service, 'start_monitoring',
                      new_callable=AsyncMock), \
         patch.object(heartbeat_service, 'start_sending',
                      new_callable=AsyncMock), \
         patch.object(compaction_service, 'start',
                      new_callable=AsyncMock):

        # Should not raise exception, just log error
        await startup_event()
        heartbeat_service.start_monitoring.assert_called_once()


@pytest.mark.asyncio
async def test_shutdown_event():
    """Test the shutdown event stops services."""
    with patch.object(heartbeat_service, 'stop', new_callable=AsyncMock), \
         patch.object(compaction_service, 'stop', new_callable=AsyncMock):

        await shutdown_event()
        heartbeat_service.stop.assert_called_once()
        compaction_service.stop.assert_called_once()


# Sync Functionality Tests
@pytest.mark.asyncio
async def test_sync_with_leader(mock_log_entry):
    """Test syncing with leader functionality."""
    with patch('pydistributedkv.entrypoints.web.follower.follower.'
               'fetch_entries_from_leader', new_callable=AsyncMock,
               return_value=[mock_log_entry]), \
         patch('pydistributedkv.entrypoints.web.follower.follower.'
               'append_entries_to_wal', return_value=[mock_log_entry]), \
         patch('pydistributedkv.entrypoints.web.follower.follower.'
               'apply_entries_to_storage', return_value=1):

        await sync_with_leader()
        # Test passes if no exception is raised


@pytest.mark.asyncio
async def test_fetch_entries_from_leader(mock_log_entry):
    """Test fetching entries from leader."""
    mock_response = MagicMock()
    mock_response.json.return_value = {
        "entries": [{"id": 1, "operation": "SET", "key": "test",
                     "value": "value", "crc": 123}]
    }

    with patch('requests.get', return_value=mock_response), \
         patch('pydistributedkv.entrypoints.web.follower.follower.'
               '_parse_and_validate_entries',
               return_value=[mock_log_entry]):

        entries = await fetch_entries_from_leader()
        assert len(entries) == 1
        assert entries[0] == mock_log_entry


def test_parse_and_validate_entries():
    """Test parsing and validating entries."""
    entry_data = [{"id": 1, "operation": "SET", "key": "test",
                   "value": "value", "crc": 123}]

    with patch('pydistributedkv.entrypoints.web.follower.follower.'
               '_create_valid_entry') as mock_create:
        mock_entry = MagicMock(spec=LogEntry)
        mock_create.return_value = mock_entry

        result = _parse_and_validate_entries(entry_data, "test")
        assert len(result) == 1
        assert result[0] == mock_entry


def test_create_valid_entry():
    """Test creating and validating a single log entry."""
    entry_data = {"id": 1, "operation": "SET", "key": "test",
                  "value": "value", "crc": 123}

    with patch('pydistributedkv.entrypoints.web.follower.follower.LogEntry') \
    as mock_log_cls:
        mock_entry = MagicMock(spec=LogEntry)
        mock_entry.validate_crc.return_value = True
        mock_log_cls.return_value = mock_entry

        result = _create_valid_entry(entry_data, "test")
        assert result == mock_entry
        mock_log_cls.assert_called_once_with(**entry_data)


def test_create_valid_entry_invalid_crc():
    """Test creating an entry with invalid CRC."""
    entry_data = {"id": 1, "operation": "SET", "key": "test",
                  "value": "value", "crc": 123}

    with patch('pydistributedkv.domain.models.LogEntry') as mock_log_cls:
        mock_entry = MagicMock(spec=LogEntry)
        mock_entry.validate_crc.return_value = False
        mock_log_cls.return_value = mock_entry

        result = _create_valid_entry(entry_data, "test")
        assert result is None


def test_create_valid_entry_value_error():
    """Test creating an entry that raises ValueError."""
    entry_data = {"id": 1, "operation": "INVALID", "key": "test",
                  "value": "value", "crc": 123}

    with patch('pydistributedkv.domain.models.LogEntry',
               side_effect=ValueError("Invalid operation")):
        result = _create_valid_entry(entry_data, "test")
        assert result is None


def test_append_entries_to_wal(mock_log_entry):
    """Test appending entries to WAL."""
    entries = [mock_log_entry]

    with patch.object(wal, 'has_entry', return_value=False), \
         patch.object(wal, 'append_entry') as mock_append:

        result = append_entries_to_wal(entries)
        assert len(result) == 1
        assert result[0] == mock_log_entry
        mock_append.assert_called_once_with(mock_log_entry)


def test_append_entries_to_wal_duplicate(mock_log_entry):
    """Test appending duplicate entries to WAL."""
    entries = [mock_log_entry]

    with patch.object(wal, 'has_entry', return_value=True), \
         patch.object(wal, 'append_entry') as mock_append:

        result = append_entries_to_wal(entries)
        assert len(result) == 0
        mock_append.assert_not_called()


def test_append_entries_to_wal_invalid_crc(mock_log_entry):
    """Test appending entries with invalid CRC to WAL."""
    mock_log_entry.validate_crc.return_value = False
    entries = [mock_log_entry]

    with patch.object(wal, 'has_entry', return_value=False), \
         patch.object(wal, 'append_entry') as mock_append:

        result = append_entries_to_wal(entries)
        assert len(result) == 0
        mock_append.assert_not_called()


def test_apply_entries_to_storage(mock_log_entry):
    """Test applying entries to storage."""
    entries = [mock_log_entry]

    with patch.object(storage, 'apply_entries',
                      return_value=1) as mock_apply:
        result = apply_entries_to_storage(entries)
        assert result == 1
        mock_apply.assert_called_once_with(entries)


# Error Handling Tests
def test_get_key_history_not_found(client):
    """Test retrieving history for a non-existent key."""
    with patch.object(storage, 'get_version_history', return_value=None):
        response = client.get("/key/nonexistent/history")
        assert response.status_code == 404
        assert "Key not found" in response.json()["detail"]


def test_get_key_versions_not_found(client):
    """Test retrieving versions for a non-existent key."""
    with patch.object(storage, 'get_version_history', return_value=None):
        response = client.get("/key/nonexistent/versions")
        assert response.status_code == 404
        assert "Key not found" in response.json()["detail"]


def test_manual_compaction_error(client):
    """Test error handling during manual compaction."""
    with patch.object(compaction_service, 'run_compaction',
                      new_callable=AsyncMock,
                      side_effect=Exception("Compaction failed")):
        response = client.post("/compaction/run")
        assert response.status_code == 500
        assert "Compaction error" in response.json()["detail"]


# Additional edge case tests
def test_get_key_with_client_tracking_error_response(client):
    """Test GET request error response with client tracking."""
    with patch.object(storage, 'get_with_version', return_value=None), \
         patch.object(request_deduplication,
                      'get_processed_result', return_value=None), \
         patch.object(request_deduplication,
                      'mark_request_processed') as mock_mark:

        url = "/key/nonexistent?client_id=client1&request_id=req1"
        response = client.get(url)
        assert response.status_code == 404
        # Should still cache the error response
        mock_mark.assert_called_once()


def test_get_segments_file_not_found(client):
    """Test segment info when files don't exist."""
    with patch.object(wal, 'get_segment_files',
                      return_value=["missing.log"]), \
         patch.object(wal, 'get_active_segment',
                      return_value="missing.log"), \
         patch('os.path.getsize', side_effect=FileNotFoundError):

        response = client.get("/segments")
        assert response.status_code == 200
        # Should handle FileNotFoundError gracefully
        assert response.json()["total_segments"] == 0


def test_sync_with_leader_no_entries():
    """Test sync when no new entries are available."""
    import asyncio

    async def run_test():
        with patch('pydistributedkv.entrypoints.web.follower.follower.'
                   'fetch_entries_from_leader', new_callable=AsyncMock,
                   return_value=[]):
            await sync_with_leader()
            # Should complete without error

    asyncio.run(run_test())


def test_sync_with_leader_request_exception():
    """Test sync when request to leader fails."""
    import asyncio

    async def run_test():
        with patch('pydistributedkv.entrypoints.web.follower.follower.'
                   'fetch_entries_from_leader', new_callable=AsyncMock,
                   side_effect=requests.RequestException("Network error")):
            # Should not raise exception, just print error
            await sync_with_leader()

    asyncio.run(run_test())


def test_fetch_entries_from_leader_empty_response():
    """Test fetching entries when leader returns empty response."""
    import asyncio

    async def run_test():
        mock_response = MagicMock()
        mock_response.json.return_value = {"entries": []}

        with patch('requests.get', return_value=mock_response), \
             patch('pydistributedkv.entrypoints.web.follower.follower.'
                   '_parse_and_validate_entries', return_value=[]):

            entries = await fetch_entries_from_leader()
            assert len(entries) == 0

    asyncio.run(run_test())


def test_replicate_endpoint_empty_entries(client):
    """Test the /replicate endpoint with empty entries list."""
    with patch('pydistributedkv.entrypoints.web.follower.follower.'
               '_parse_and_validate_entries', return_value=[]), \
         patch.object(storage, 'apply_entries', return_value=0):

        response = client.post("/replicate", json={"entries": []})
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "last_applied_id": 0}


def test_get_key_with_version_not_found(client):
    """Test retrieving a specific version that doesn't exist."""
    with patch.object(storage, 'get_with_version', return_value=None):
        response = client.get("/key/test?version=999")
        assert response.status_code == 404
        expected_msg = "Key not found or version 999 not available"
        assert expected_msg in response.json()["detail"]