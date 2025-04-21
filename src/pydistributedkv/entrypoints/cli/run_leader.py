import os

import uvicorn

from pydistributedkv.configurator.settings.config import LEADER_CONFIG

os.environ["WAL_PATH"] = LEADER_CONFIG["wal_path"]

if __name__ == "__main__":
    uvicorn.run("leader.server:app", host=LEADER_CONFIG["host"], port=LEADER_CONFIG["port"], reload=True)
