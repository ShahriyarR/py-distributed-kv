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
│   ├── follower-1
│   │   └── wal.log
│   ├── follower-2
│   │   └── wal.log
│   ├── follower-3
│   │   └── wal.log
│   └── follower-4
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

Missing critical parts, such as:

- Leader Election: Implement a consensus algorithm like Raft to elect a leader automatically.
- Conflict Resolution: Add mechanisms to resolve conflicts when multiple updates happen simultaneously.
- Fault Tolerance: Implement heartbeats, retries, and failure detection.
- Persistent Connection: Replace the HTTP-based replication with more efficient TCP connections.
- Sharding: Implement data partitioning to scale horizontally.
- Compression: Add support for compressing the WAL files to save disk space.
- Security: Add authentication and encryption for API calls and replication.


## Current limitations & future improvements

Each iteration will state the missing parts and the future improvements.

#### Authors:  [Shahriyar Rzayev](https://www.linkedin.com/in/shahriyar-rzayev/)


