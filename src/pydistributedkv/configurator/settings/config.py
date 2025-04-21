# config.py
import os

# Leader config
LEADER_CONFIG = {"host": "0.0.0.0", "port": 8000, "wal_path": "data/leader/wal.log"}

# Follower config
FOLLOWER_CONFIGS = {
    "follower-1": {
        "host": "0.0.0.0",
        "port": 8001,
        "wal_path": "data/followers/follower-1/wal.log",
        "leader_url": "http://localhost:8000",
        "follower_url": "http://localhost:8001",
    },
    "follower-2": {
        "host": "0.0.0.0",
        "port": 8002,
        "wal_path": "data/followers/follower-2/wal.log",
        "leader_url": "http://localhost:8000",
        "follower_url": "http://localhost:8002",
    },
    "follower-3": {
        "host": "0.0.0.0",
        "port": 8003,
        "wal_path": "data/followers/follower-3/wal.log",
        "leader_url": "http://localhost:8000",
        "follower_url": "http://localhost:8003",
    },
    "follower-4": {
        "host": "0.0.0.0",
        "port": 8004,
        "wal_path": "data/followers/follower-4/wal.log",
        "leader_url": "http://localhost:8000",
        "follower_url": "http://localhost:8004",
    },
}

# Create directories for WAL files
for config in [LEADER_CONFIG] + list(FOLLOWER_CONFIGS.values()):
    os.makedirs(os.path.dirname(config["wal_path"]), exist_ok=True)
