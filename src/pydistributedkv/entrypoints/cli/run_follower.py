import os
import sys

import uvicorn

from pydistributedkv.configurator.settings.config import FOLLOWER_CONFIGS

if __name__ == "__main__":
    if len(sys.argv) != 2 or sys.argv[1] not in FOLLOWER_CONFIGS:
        print("Usage: python run_follower.py <follower_id>")
        print(f"Available follower IDs: {list(FOLLOWER_CONFIGS.keys())}")
        sys.exit(1)

    follower_id = sys.argv[1]
    config = FOLLOWER_CONFIGS[follower_id]

    os.environ["WAL_PATH"] = config["wal_path"]
    os.environ["LEADER_URL"] = config["leader_url"]
    os.environ["FOLLOWER_ID"] = follower_id
    os.environ["FOLLOWER_URL"] = config["follower_url"]

    uvicorn.run("pydistributedkv.entrypoints.web.follower.follower:app", host=config["host"], port=config["port"], reload=True)
