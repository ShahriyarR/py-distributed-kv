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


## TODO

- [x] Checksums: Add checksums to the WAL entries to ensure data integrity.
- [x] Segmented Log: Split WAL into multiple segment files that roll over after configured size limit.
- [ ] Compression: Add support for compressing the WAL files to save disk space.
- [ ] Conflict Resolution: Add mechanisms to resolve conflicts when multiple updates happen simultaneously.
- [ ] Fault Tolerance: 
  - [ ] Implement heartbeats 
  - [ ] Retries
  - [ ] Failure detection.
  - [ ] Consensus algorithms like Raft or Paxos.
- [ ] Persistent Connection: Replace the HTTP-based replication with more efficient TCP connections.
- [ ] Transaction Support: Implement support for transactions to ensure atomicity and consistency.
- [ ] Sharding: Implement data partitioning to scale horizontally.
- [ ] Security: Add authentication and encryption for API calls and replication.

## Current limitations & future improvements

Each iteration will state the missing parts and the future improvements.

#### Authors:  [Shahriyar Rzayev](https://www.linkedin.com/in/shahriyar-rzayev/)

