# This is a Python Project Template

You need to use this template for all Python projects. (Change me...)

## Setup

(Change me...)

Create virtualenv in the project root directory named ".venv"

### Commands

`make install-flit`

`make install-dev`

`make run` or `make test`


## Flow

(Change me...)

## Development Stages

### v1.0.0

This version is a simple key-value store with basic replication and persistence.
It uses a write-ahead log (WAL) for durability and a simple HTTP API for replication.
There is no leader election or conflict resolution, and the system is not fault-tolerant.

Missing critical parts, such as:

- Leader Election: Implement a consensus algorithm like Raft to elect a leader automatically.
- Conflict Resolution: Add mechanisms to resolve conflicts when multiple updates happen simultaneously.
- Fault Tolerance: Implement heartbeats, retries, and failure detection.
- Persistent Connection: Replace the HTTP-based replication with more efficient TCP connections.
- Sharding: Implement data partitioning to scale horizontally.
- Compression: Add support for compressing the WAL files to save disk space.
- Security: Add authentication and encryption for API calls and replication.


## Current limitations & future improvements

(Change me...)

#### Authors:  (Change me...)


