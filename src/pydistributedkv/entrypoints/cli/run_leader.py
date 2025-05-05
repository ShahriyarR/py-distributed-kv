import os

import uvicorn

from pydistributedkv.configurator.settings.config import LEADER_CONFIG

os.environ["WAL_PATH"] = LEADER_CONFIG["wal_path"]
os.environ["LEADER_URL"] = LEADER_CONFIG["leader_url"]

if __name__ == "__main__":
    uvicorn.run("pydistributedkv.entrypoints.web.leader.leader:app", host=LEADER_CONFIG["host"], port=LEADER_CONFIG["port"], reload=True)
