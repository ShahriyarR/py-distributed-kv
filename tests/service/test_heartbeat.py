import asyncio
import time
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from pydistributedkv.service.heartbeat import HeartbeatService

# Test constants
SERVICE_NAME = "test-service"
SERVER_ID = "test-server"
SERVER_URL = "http://localhost:8000"
OTHER_SERVER_ID = "other-server"
OTHER_SERVER_URL = "http://localhost:8001"


@pytest.fixture
def heartbeat_service():
    """Create a HeartbeatService instance for testing"""
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)
    return service


def test_init(heartbeat_service):
    """Test initialization of HeartbeatService"""
    assert heartbeat_service.service_name == SERVICE_NAME
    assert heartbeat_service.server_id == SERVER_ID
    assert heartbeat_service.server_url == SERVER_URL
    assert heartbeat_service.servers == {}
    assert not heartbeat_service._monitor_running
    assert not heartbeat_service._send_running


def test_register_server(heartbeat_service):
    """Test registering a server"""
    # Given
    assert len(heartbeat_service.servers) == 0

    # When
    heartbeat_service.register_server(OTHER_SERVER_ID, OTHER_SERVER_URL)

    # Then
    assert len(heartbeat_service.servers) == 1
    assert OTHER_SERVER_ID in heartbeat_service.servers
    assert heartbeat_service.servers[OTHER_SERVER_ID]["url"] == OTHER_SERVER_URL
    assert heartbeat_service.servers[OTHER_SERVER_ID]["status"] == "healthy"
    assert "last_heartbeat" in heartbeat_service.servers[OTHER_SERVER_ID]


def test_deregister_server(heartbeat_service):
    """Test deregistering a server"""
    # Given
    heartbeat_service.register_server(OTHER_SERVER_ID, OTHER_SERVER_URL)
    assert len(heartbeat_service.servers) == 1

    # When
    heartbeat_service.deregister_server(OTHER_SERVER_ID)

    # Then
    assert len(heartbeat_service.servers) == 0
    assert OTHER_SERVER_ID not in heartbeat_service.servers


def test_deregister_nonexistent_server(heartbeat_service):
    """Test deregistering a server that doesn't exist"""
    # Given
    assert "nonexistent" not in heartbeat_service.servers

    # When/Then - should not raise exception
    heartbeat_service.deregister_server("nonexistent")


def test_record_heartbeat(heartbeat_service):
    """Test recording a heartbeat from a server"""
    # Given
    heartbeat_service.register_server(OTHER_SERVER_ID, OTHER_SERVER_URL)
    initial_heartbeat = heartbeat_service.servers[OTHER_SERVER_ID]["last_heartbeat"]

    # Wait briefly to ensure timestamp changes
    time.sleep(0.001)

    # When
    heartbeat_service.record_heartbeat(OTHER_SERVER_ID)

    # Then
    assert heartbeat_service.servers[OTHER_SERVER_ID]["last_heartbeat"] > initial_heartbeat


def test_record_heartbeat_unknown_server(heartbeat_service):
    """Test recording a heartbeat from an unknown server"""
    # When/Then - Should log warning but not raise exception
    heartbeat_service.record_heartbeat("unknown-server")
    # Verify no server was added
    assert "unknown-server" not in heartbeat_service.servers


def test_record_heartbeat_recovers_down_server(heartbeat_service):
    """Test that a down server becomes healthy when heartbeat is received"""
    # Given
    heartbeat_service.register_server(OTHER_SERVER_ID, OTHER_SERVER_URL)
    heartbeat_service.servers[OTHER_SERVER_ID]["status"] = "down"
    assert heartbeat_service.servers[OTHER_SERVER_ID]["status"] == "down"

    # When
    heartbeat_service.record_heartbeat(OTHER_SERVER_ID)

    # Then
    assert heartbeat_service.servers[OTHER_SERVER_ID]["status"] == "healthy"


def test_get_server_status(heartbeat_service):
    """Test getting status of a specific server"""
    # Given
    heartbeat_service.register_server(OTHER_SERVER_ID, OTHER_SERVER_URL)

    # When
    status = heartbeat_service.get_server_status(OTHER_SERVER_ID)

    # Then
    assert status is not None
    assert status["url"] == OTHER_SERVER_URL
    assert status["status"] == "healthy"


def test_get_server_status_unknown(heartbeat_service):
    """Test getting status of an unknown server"""
    # When
    status = heartbeat_service.get_server_status("unknown-server")

    # Then
    assert status is None


def test_get_all_statuses(heartbeat_service):
    """Test getting status of all servers"""
    # Given
    heartbeat_service.register_server("server1", "http://server1:8001")
    heartbeat_service.register_server("server2", "http://server2:8002")

    # When
    statuses = heartbeat_service.get_all_statuses()

    # Then
    assert len(statuses) == 2
    assert "server1" in statuses
    assert "server2" in statuses
    assert "url" in statuses["server1"]
    assert "status" in statuses["server1"]
    assert "last_heartbeat" in statuses["server1"]
    assert "seconds_since_last_heartbeat" in statuses["server1"]


def test_get_healthy_servers(heartbeat_service):
    """Test getting only healthy servers"""
    # Given
    heartbeat_service.register_server("server1", "http://server1:8001")
    heartbeat_service.register_server("server2", "http://server2:8002")
    heartbeat_service.servers["server2"]["status"] = "down"

    # When
    healthy_servers = heartbeat_service.get_healthy_servers()

    # Then
    assert len(healthy_servers) == 1
    assert "server1" in healthy_servers
    assert "server2" not in healthy_servers
    assert healthy_servers["server1"] == "http://server1:8001"


@pytest.mark.asyncio
async def test_start_monitoring():
    """Test starting heartbeat monitoring"""
    # Create the service
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)

    # We need to mock the _monitor_heartbeats method in a way that
    # allows us to verify it was called without actually running it
    with patch.object(service, "_monitor_heartbeats") as mock_coroutine:
        # Configure the mock to return a coroutine that can be awaited
        # but doesn't actually do anything
        mock_coroutine.return_value = asyncio.sleep(0)

        # When
        await service.start_monitoring()

        # Then
        assert service._monitor_running is True
        assert len(service._background_tasks) > 0

        # Verify the mock was called when creating the task
        mock_coroutine.assert_called_once()

        # Cleanup
        await service.stop()


@pytest.mark.asyncio
async def test_start_sending():
    """Test starting heartbeat sending"""
    # Create the service
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)

    # Similar approach to test_start_monitoring
    with patch.object(service, "_send_heartbeats") as mock_coroutine:
        # Configure the mock to return a coroutine that can be awaited
        mock_coroutine.return_value = asyncio.sleep(0)

        # When
        await service.start_sending()

        # Then
        assert service._send_running is True
        assert len(service._background_tasks) > 0

        # Verify the mock was called when creating the task
        mock_coroutine.assert_called_once()

        # Cleanup
        await service.stop()


@pytest.mark.asyncio
async def test_stop():
    """Test stopping the heartbeat service"""
    # Given
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)

    # Create mocks that just return immediately awaitable coroutines
    with (
        patch.object(service, "_monitor_heartbeats", return_value=asyncio.sleep(0)),
        patch.object(service, "_send_heartbeats", return_value=asyncio.sleep(0)),
    ):

        # Start both services
        await service.start_monitoring()
        await service.start_sending()

        # Confirm they started correctly
        assert service._monitor_running is True
        assert service._send_running is True
        assert len(service._background_tasks) == 2

        # When
        await service.stop()

        # Then
        assert service._monitor_running is False
        assert service._send_running is False


@pytest.mark.asyncio
async def test_monitor_heartbeats_marks_server_down():
    """Test that monitor detects and marks servers as down after timeout"""
    # Mock HEARTBEAT_TIMEOUT for testing
    with patch("pydistributedkv.service.heartbeat.HEARTBEAT_TIMEOUT", 0.1):
        service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)
        service.register_server(OTHER_SERVER_ID, OTHER_SERVER_URL)

        # Set last_heartbeat far in the past to trigger the timeout
        service.servers[OTHER_SERVER_ID]["last_heartbeat"] = time.time() - 1.0

        # Directly call the check method to test its behavior
        service._check_server_heartbeats(time.time())

        # Verify server is marked down
        assert service.servers[OTHER_SERVER_ID]["status"] == "down"


@pytest.mark.asyncio
async def test_send_heartbeats_to_all_servers():
    """Test sending heartbeats to all registered servers"""
    # Given
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)
    service.register_server("server1", "http://server1:8001")
    service.register_server("server2", "http://server2:8002")

    # Mock _schedule_heartbeat to test if it's called for each server
    with patch.object(service, "_schedule_heartbeat") as mock_schedule:
        # When
        await service._send_heartbeats_to_all_servers()

        # Then
        assert mock_schedule.call_count == 2
        # Check that it was called for each server
        calls = mock_schedule.call_args_list
        servers_called = {call.args[0] for call in calls}
        assert servers_called == {"server1", "server2"}


@pytest.mark.asyncio
async def test_send_single_heartbeat_success():
    """Test sending a single heartbeat successfully"""
    # Given
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)

    # Create a mock response
    mock_response = MagicMock()
    mock_response.status_code = 200

    # Mock asyncio.to_thread to return our mock response
    with patch("asyncio.to_thread", return_value=mock_response) as mock_to_thread:
        # When
        await service._send_single_heartbeat(OTHER_SERVER_ID, OTHER_SERVER_URL)

        # Then
        mock_to_thread.assert_called_once()
        # Check that the heartbeat URL is correct
        call_args = mock_to_thread.call_args
        assert f"{OTHER_SERVER_URL}/heartbeat" in call_args.args[1]
        # Check that the server_id is included in the payload
        assert call_args.kwargs["json"]["server_id"] == SERVER_ID


@pytest.mark.asyncio
async def test_send_single_heartbeat_failure():
    """Test sending a single heartbeat with HTTP error"""
    # Given
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)

    # Create a mock response with error status
    mock_response = MagicMock()
    mock_response.status_code = 500

    # Mock asyncio.to_thread to return our mock response
    with patch("asyncio.to_thread", return_value=mock_response) as mock_to_thread:
        # When
        await service._send_single_heartbeat(OTHER_SERVER_ID, OTHER_SERVER_URL)

        # Then
        mock_to_thread.assert_called_once()
        # No exception should be raised


@pytest.mark.asyncio
async def test_send_single_heartbeat_network_error():
    """Test sending a single heartbeat with network error"""
    # Given
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)

    # Mock asyncio.to_thread to raise a RequestException
    import requests

    with patch("asyncio.to_thread", side_effect=requests.RequestException("Network error")):
        # When/Then - should not raise exception outside
        await service._send_single_heartbeat(OTHER_SERVER_ID, OTHER_SERVER_URL)
        # No assertion needed - we're testing that no exception is raised


@pytest.mark.asyncio
async def test_schedule_heartbeat():
    """Test scheduling a heartbeat to a server"""
    # Given
    service = HeartbeatService(SERVICE_NAME, SERVER_ID, SERVER_URL)

    # Mock asyncio.create_task to verify it's called with a coroutine
    mock_task = MagicMock()

    with patch("asyncio.create_task", return_value=mock_task) as mock_create_task:
        # Mock _send_single_heartbeat to verify it's called
        with patch.object(service, "_send_single_heartbeat", return_value=AsyncMock()):
            # When
            service._schedule_heartbeat(OTHER_SERVER_ID, {"url": OTHER_SERVER_URL})

            # Then
            mock_create_task.assert_called_once()
