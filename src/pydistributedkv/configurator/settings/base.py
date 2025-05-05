import os

# API timeouts in seconds
API_TIMEOUT = int(os.getenv("API_TIMEOUT", "5"))

# Log segmentation settings
# Default max segment size: 1MB
MAX_SEGMENT_SIZE = int(os.getenv("MAX_SEGMENT_SIZE", str(1024 * 1024)))

# Heartbeat configuration
HEARTBEAT_INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", 10))  # seconds
HEARTBEAT_TIMEOUT = HEARTBEAT_INTERVAL * 3  # After this many seconds with no heartbeat, mark server as down
