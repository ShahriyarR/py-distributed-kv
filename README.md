# Distributed Key-Value Store written in Python

This repository contains a simple distributed key-value store written in Python.

It is designed to be a learning project for understanding distributed systems, replication, and persistence.

Each version tag represents a different stage of development, with increasing complexity and features.

See [Development Stages](#development-stages) for more details.

> Disclaimer: This project is not intended for production use. It is a learning exercise and should not be used in any critical applications.

## Development Stages

### v1.0.0

> **Note:** This version is not production-ready and is intended for educational purposes only.

* This version is a simple key-value store with basic replication and persistence.
* It uses a write-ahead log (WAL) for durability and a simple HTTP API for replication using FastAPI.
* In this version of WAL, there is no checkpointing, compression, compaction, snapshots or sharding.
* There is no leader election or conflict resolution, and the system is not fault-tolerant.
* Leader election is done manually by specifying the leader's address in the configuration file.
* Followers each have a copy of the WAL and can apply updates from the leader, followers config is done manually.

> Few reading material about WAL:
> - [Write-Ahead Logging](https://en.wikipedia.org/wiki/Write-ahead_logging)
> - [Write-Ahead Logging in PostgreSQL](https://www.postgresql.org/docs/current/wal-intro.html)
> - [Oplog the WAL version of Mongo](https://www.mongodb.com/docs/manual/core/replica-set-oplog/)

To test this version of the implementation, checkout to the v1.0.0 branch.

- Install the dependencies:

```bash
python3.12 -m venv .venv
make install-flit
make install-dev
```

- Run the leader:

```
python src/pydistributedkv/entrypoints/cli/run_leader.py
```

- Run the followers:

```
python src/pydistributedkv/entrypoints/cli/run_follower.py follower-1
python src/pydistributedkv/entrypoints/cli/run_follower.py follower-2
python src/pydistributedkv/entrypoints/cli/run_follower.py follower-3
```

- After successful start of the leader and followers, here is the folder structure:

```bash
tree data/
data/
├── followers
│   ├── follower-1
│   │   └── wal.log
│   ├── follower-2
│   │   └── wal.log
│   ├── follower-3
│   │   └── wal.log
│   └── follower-4
└── leader
    └── wal.log

7 directories, 4 files

```

- SET the key-value pair:

```bash
curl -X PUT "http://localhost:8000/key/key6" -H "Content-Type: application/json" -d '{"value": "myvalue"}'

{"status":"ok","id":14}  # id may vary
```

- GET the key-value pair from leader:

```bash
curl  "http://localhost:8000/key/key6"

{"key":"key6","value":"myvalue"}
```

- GET the key-value pair from followers:

```bash
curl  "http://localhost:8001/key/key6"

{"key":"key6","value":"myvalue"}

curl  "http://localhost:8002/key/key6"

{"key":"key6","value":"myvalue"}
```

- DELETE the key-value pair from leader:

```bash
curl -X DELETE "http://localhost:8000/key/key6"

{"status":"ok","id":15}

curl  "http://localhost:8002/key/key6"

{"detail":"Key not found"}
```

- Current structure of the WAL file, with monotonic increasing IDs:

```bash
...
{"id": 12, "operation": "SET", "key": "key5", "value": "myvalue"}
{"id": 13, "operation": "DELETE", "key": "mykey", "value": null}
{"id": 14, "operation": "SET", "key": "key6", "value": "myvalue"}
{"id": 15, "operation": "DELETE", "key": "key6", "value": null}
```

### v1.1.0

> **Added feature:** Data integrity with CRC validation.

This version adds data integrity features to ensure the reliability of stored data across the distributed system:

* **CRC32 Checksums:** Each log entry now includes a Cyclic Redundancy Check (CRC32) to detect data corruption.
* **Validation on Read:** When entries are read from the WAL, their CRCs are verified to ensure data hasn't been corrupted.
* **Skip Invalid Entries:** Corrupted entries are automatically detected and skipped during log replay and replication.
* **Auto-correction:** When an entry with an invalid CRC is appended, the system automatically recalculates the correct CRC.

These improvements ensure:
* Data integrity is maintained during storage and replication
* The system can detect and handle corruption without crashing
* Followers won't apply corrupted data received from the leader
* Better reliability for long-term data storage

How it works:
1. When a log entry is created, a CRC32 checksum is calculated based on all fields except the CRC itself
2. Upon reading entries from disk, the CRC is validated by recalculating and comparing
3. During replication, entries are validated before being applied to followers' state

Example of a WAL entry with CRC (the CRC value is a 32-bit integer):

```json
{"id": 14, "operation": "SET", "key": "key6", "value": "myvalue", "crc": 3641357794}
```

To test this version, checkout to the v1.1.0 branch and follow the same setup instructions as v1.0.0.

### v1.2.0

> **Added feature:** Segmented Log for better performance and reliability.

This version introduces segmented logs to improve the performance and maintainability of the distributed key-value store:

* **Log Segmentation:** The Write-Ahead Log (WAL) is now split into multiple segment files of configurable size.
* **Automatic Log Rolling:** When a segment reaches its configured size limit, a new segment is created automatically.
* **Improved Scalability:** Segmented logs allow the system to handle much larger datasets without performance degradation.
* **Segment Management:** New API endpoints added to monitor and manage log segments.

Benefits of segmented logs:
* Prevents unbounded log growth in a single file
* Reduces I/O pressure on the system
* Makes log maintenance operations like compaction and pruning more efficient
* Enables faster startup times by allowing parallel processing of segments

How it works:
1. The WAL is split into multiple segment files with sequential numbering (e.g., `wal.log.segment.1`, `wal.log.segment.2`)
2. Each segment has a configurable maximum size (default: 1MB)
3. When a segment reaches its size limit, the system automatically rolls over to a new segment
4. Read operations gather data from all segments in the correct order

To test this version:
1. Set the segment size with the environment variable `MAX_SEGMENT_SIZE` (in bytes). Default is 1MB.
2. Use `/segments` endpoint on leader or followers to view segment information.

Example segment structure:

```bash
data/
├── followers
│   └── follower-1
│       ├── wal.log.segment.1
│       ├── wal.log.segment.2
│       └── wal.log.segment.3
└── leader
    ├── wal.log.segment.1
    └── wal.log.segment.2
```

To view segment information:

```bash
curl http://localhost:8000/segments

{
  "segments": [
    {"path": "data/leader/wal.log.segment.1", "size": 1048576, "is_active": false},
    {"path": "data/leader/wal.log.segment.2", "size": 325420, "is_active": true}
  ],
  "total_segments": 2,
  "max_segment_size": 1048576
}
```

### v1.3.0

> **Added feature:** Idempotent Receiver pattern for reliable client request handling.

This version implements the Idempotent Receiver pattern to handle duplicate client requests and ensure operations are only applied once:

* **Client Request Identification:** Each client request can now include a unique client ID and request ID.
* **Operation-Aware Deduplication:** The system tracks client requests by operation type (GET/SET/DELETE) to properly handle different operations with the same request ID.
* **Request Caching:** Responses are cached for a configurable period to immediately respond to duplicate requests.
* **Request Statistics:** New endpoints provide visibility into duplicate detection and request handling patterns.
* **Improved Reliability:** Clients can retry requests without worry of duplicate processing.

Benefits of the Idempotent Receiver pattern:
* Eliminates duplicate mutations when clients retry requests due to network issues
* Prevents accidental duplicate operations from users clicking twice
* Provides exactly-once semantics in an eventually consistent system
* Improves system robustness during network partitions or client timeouts

How it works:
1. Clients add `client_id` and `request_id` parameters to their requests
2. When the system first processes a request, it stores the result along with these identifiers
3. If the same request arrives again (same client_id, request_id, and operation), the cached result is returned
4. Different operations with the same request ID are tracked separately
5. Cached results expire after a configurable time period (default: 1 hour)

Example usage:

```bash
# First time - request is processed normally
curl -X PUT "http://localhost:8000/key/testkey?client_id=client1&request_id=req123" \
    -H "Content-Type: application/json" -d '{"value": "testvalue"}'

# Same request again - returns cached result without processing
curl -X PUT "http://localhost:8000/key/testkey?client_id=client1&request_id=req123" \
    -H "Content-Type: application/json" -d '{"value": "testvalue"}'

# Different operation type with same request ID - processed separately
curl -X DELETE "http://localhost:8000/key/testkey?client_id=client1&request_id=req123"

# Check deduplication statistics
curl http://localhost:8000/deduplication_stats
```

View request deduplication statistics:

```json
{
  "service_name": "leader",
  "current_cache_size": 2,
  "unique_request_ids": 1,
  "total_client_count": 1,
  "total_requests_cached": 2,
  "total_duplicates_detected": 1,
  "same_operation_duplicates": 1,
  "different_operation_duplicates": 1,
  "total_cache_cleanups": 0
}
```

### v1.4.0

> **Added feature:** Heartbeat mechanism for server health monitoring and failure detection.

This version implements a robust heartbeat mechanism to detect server failures and maintain cluster health awareness:

* **Periodic Heartbeats:** Servers exchange periodic heartbeat messages to indicate they're alive and functioning.
* **Bidirectional Monitoring:** Both leader and followers monitor each other's health through heartbeats.
* **Automatic Failure Detection:** Servers that miss heartbeats are automatically marked as "down" after a configurable timeout.
* **Smart Replication:** The leader only attempts to replicate data to healthy followers, avoiding timeouts from down servers.
* **Health Recovery:** When a server comes back online, heartbeats automatically restore its "healthy" status.
* **Cluster Status API:** New endpoints provide visibility into the health status of all servers in the cluster.

Benefits of the heartbeat mechanism:
* Fast detection of server failures for improved reliability
* Reduced timeout delays when interacting with unavailable servers
* Better visibility into cluster health status
* Automatic recovery detection when servers come back online
* Foundation for more advanced fault tolerance features

How it works:
1. Each server sends periodic heartbeat messages to all other servers it knows about
2. The heartbeat interval is configurable (default: 10 seconds)
3. If a server doesn't receive a heartbeat from another server within the timeout period (default: 3x interval), it marks that server as "down"
4. When a heartbeat is received from a previously down server, it's automatically marked as "healthy" again
5. The leader only replicates data to followers marked as "healthy"

Configuration options:
* `HEARTBEAT_INTERVAL`: Time between heartbeats in seconds (default: 10)
* `HEARTBEAT_TIMEOUT`: Time to wait before marking a server as down (default: 3x interval)

Example usage:

```bash
# Check cluster status from the leader's perspective
curl http://localhost:8000/cluster_status

# Check cluster status from a follower's perspective
curl http://localhost:8001/cluster_status
```

Example cluster status response:

```json
{
  "leader": {
    "id": "leader",
    "url": "http://localhost:8000",
    "status": "healthy"
  },
  "followers": {
    "follower-1": {
      "url": "http://localhost:8001",
      "status": "healthy",
      "last_heartbeat": 1649853215.3456,
      "seconds_since_last_heartbeat": 2.5
    },
    "follower-2": {
      "url": "http://localhost:8002",
      "status": "down",
      "last_heartbeat": 1649853175.1234,
      "seconds_since_last_heartbeat": 42.7
    },
    "follower-3": {
      "url": "http://localhost:8003",
      "status": "healthy",
      "last_heartbeat": 1649853214.7890,
      "seconds_since_last_heartbeat": 3.1
    }
  },
  "heartbeat_interval": 10
}
```

To test failure detection:
1. Start the leader and multiple followers
2. Observe the cluster status showing all servers as "healthy"
3. Stop one of the followers
4. After the heartbeat timeout period, observe the stopped follower being marked as "down" in the cluster status
5. Start the follower again and observe it automatically returning to "healthy" status

### v1.5.0

> **Added feature:** Versioned Value pattern to track value history and handle concurrent updates.

This version implements the Versioned Value pattern to track the history of values and provide a robust mechanism for handling concurrent updates:

* **Version Tracking:** Each value stored now includes a version number that increments with every update.
* **Historical Values:** The system maintains a history of previous values, allowing clients to access historical data.
* **Optimistic Concurrency Control:** Clients can provide version numbers with updates to prevent overwriting newer data.
* **Conflict Detection:** When a client tries to update with an outdated version, the operation is rejected with a conflict error.
* **Version Query API:** New endpoints for retrieving specific versions and inspecting version history.

Benefits of the Versioned Value pattern:
* **Audit Trail:** Track how values have changed over time
* **Time Travel:** Access historical states of any key
* **Conflict Prevention:** Detect and handle concurrent update conflicts gracefully
* **Data Reliability:** Prevent older values from overwriting newer ones
* **Debugging:** Diagnose issues by examining the history of changes

How it works:
1. Each key stores its current value, current version, and a history of previous values
2. Every update increments the version number for that key
3. Clients can request a specific version when reading values
4. When updating, clients can specify a version as a precondition to prevent conflicts
5. If a version conflict occurs, the server returns the current version so clients can update appropriately

API Extensions:
* Version parameter for GET operations: `GET /key/{key}?version=3`
* Version parameter for PUT operations: `PUT /key/{key}` with body `{"value": "new-value", "version": 2}`
* Version history endpoint: `GET /key/{key}/history`
* Available versions endpoint: `GET /key/{key}/versions`

Example usage:

```bash
# Set initial value for a key
curl -X PUT "http://localhost:8000/key/config" \
  -H "Content-Type: application/json" -d '{"value": "initial-setting"}'
# Response: {"status":"ok","id":1,"key":"config","version":1}

# Update the value
curl -X PUT "http://localhost:8000/key/config" \
  -H "Content-Type: application/json" -d '{"value": "updated-setting"}'
# Response: {"status":"ok","id":2,"key":"config","version":2}

# Get the latest value
curl "http://localhost:8000/key/config"
# Response: {"key":"config","value":"updated-setting","version":2}

# Get a specific version
curl "http://localhost:8000/key/config?version=1"
# Response: {"key":"config","value":"initial-setting","version":1}

# Try updating with an outdated version (conflict)
curl -X PUT "http://localhost:8000/key/config" \
  -H "Content-Type: application/json" -d '{"value": "conflict-value", "version": 1}'
# Response: {"status":"error","message":"Version conflict: provided version 1 is outdated","current_version":2}

# View version history
curl "http://localhost:8000/key/config/history"
# Response: {"key":"config","versions":[1,2],"history":[{"version":1,"value":"initial-setting"},{"version":2,"value":"updated-setting"}]}

# View available versions
curl "http://localhost:8000/key/config/versions"
# Response: {"key":"config","versions":[1,2],"latest_version":2}
```

This feature helps maintain data consistency in distributed environments by:
* Ensuring newer data isn't accidentally overwritten by stale updates
* Providing a mechanism for clients to detect and resolve conflicts
* Preserving the complete history of changes for auditing and recovery

The Versioned Value pattern is particularly valuable in distributed systems where nodes may receive updates in different orders or experience network partitions.

### v1.6.0

> **Added feature:** Log Compaction for optimizing storage usage and improving performance.

This version adds log compaction to optimize storage and improve system performance for long-running deployments:

* **Automatic Compaction:** The system periodically compacts the write-ahead log to reduce its size while preserving the current state.
* **Redundant Entry Elimination:** Compaction removes redundant operations, keeping only the latest operation for each key.
* **Configurable Scheduling:** Compaction can be configured to run at specific intervals or triggered manually.
* **Compaction Monitoring:** New endpoints provide visibility into compaction history and status.
* **Safe Execution:** Compaction runs safely alongside normal operations without disrupting service.

Benefits of log compaction:
* **Reduced Storage Requirements:** Minimizes disk space usage by removing redundant entries
* **Improved Performance:** Faster startup times due to smaller logs that need to be replayed
* **Enhanced Scalability:** Enables the system to manage more data over longer periods
* **Better Resource Utilization:** Reduces I/O load by working with optimized logs
* **Graceful Degradation:** Prevents performance decline as the system runs for extended periods

How it works:
1. The system periodically examines inactive log segments (all segments except the active one)
2. For each key in these segments, only the latest operation (SET or DELETE) is retained
3. A new compacted segment is created containing only these latest operations
4. Once compaction is successful, old segments are removed and segment numbering is updated
5. The in-memory state remains unchanged as compaction preserves the logical state

Configuration options:
* `compaction_interval`: Time between automatic compactions in seconds (default: 3600, 1 hour)
* `min_compaction_interval`: Minimum time between compactions to prevent too frequent runs (default: 600, 10 minutes)
* `enabled`: Toggle to enable/disable automatic compaction (default: true)

Example usage:

```bash
# Manually trigger log compaction
curl -X POST "http://localhost:8000/compaction/run"
# Response: {"status":"success","segments_compacted":2,"entries_removed":150}

# View compaction status and history
curl "http://localhost:8000/compaction/status"
# Response:
{
  "enabled": true,
  "compaction_interval_seconds": 3600,
  "min_compaction_interval_seconds": 600,
  "last_compaction": "2023-08-25T14:30:45.123456",
  "compaction_running": false,
  "compaction_history": [
    {
      "timestamp": "2023-08-25T14:30:45.123456",
      "duration_seconds": 0.75,
      "segments_compacted": 2,
      "entries_removed": 150
    }
  ]
}

# Configure compaction settings
curl -X POST "http://localhost:8000/compaction/configure?enabled=true&interval=7200"
# Response: {"status":"success","changes":{"enabled":true,"interval":7200}}
```

Example of log compaction effect:

Before compaction (many redundant entries):
```
{"id": 1, "operation": "SET", "key": "user1", "value": "initial-value", "crc": 123456}
{"id": 2, "operation": "SET", "key": "user2", "value": "hello", "crc": 234567}
{"id": 3, "operation": "SET", "key": "user1", "value": "updated-value", "crc": 345678}
{"id": 4, "operation": "DELETE", "key": "user2", "value": null, "crc": 456789}
{"id": 5, "operation": "SET", "key": "user3", "value": "new-entry", "crc": 567890}
{"id": 6, "operation": "SET", "key": "user1", "value": "final-value", "crc": 678901}
```

After compaction (only latest operations preserved):
```
{"id": 5, "operation": "SET", "key": "user3", "value": "new-entry", "crc": 567890}
{"id": 6, "operation": "SET", "key": "user1", "value": "final-value", "crc": 678901}
```

Note that compaction preserves the original entry IDs and CRCs while removing redundant entries, maintaining data integrity and consistency.

To test this version, checkout to the v1.6.0 branch and follow the same setup instructions as previous versions.

## TODO

- [x] Checksums: Add checksums to the WAL entries to ensure data integrity.
- [x] Segmented Log: Split WAL into multiple segment files that roll over after configured size limit.
- [x] Idempotent Receiver Pattern: Implement unique request IDs to handle duplicate client requests properly.
- [x] Fault Tolerance: 
  - [x] Implement heartbeats for failure detection
  - [ ] Add automatic retries for transient failures
  - [ ] Implement leader election (Raft or similar consensus algorithm)
- [x] Versioned Value: Track version history for each key and implement optimistic concurrency control.
- [x] Log Compaction: Automatically compact logs to optimize storage space by removing redundant entries.
- [ ] Conflict Resolution: Add mechanisms to resolve conflicts when multiple updates happen simultaneously.
- [ ] Persistent Connection: Replace the HTTP-based replication with more efficient TCP connections.
- [ ] Transaction Support: Implement support for transactions to ensure atomicity and consistency.
- [ ] Sharding: Implement data partitioning to scale horizontally.
- [ ] Security: Add authentication and encryption for API calls and replication.

## Current limitations & future improvements

Each iteration will state the missing parts and the future improvements.

#### Authors:  [Shahriyar Rzayev](https://www.linkedin.com/in/shahriyar-rzayev/)

`