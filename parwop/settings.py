import os
import sys
import socket


NODE_ID = os.environ.get("NODE_ID", socket.gethostname())
IS_COORDINATOR = os.environ.get("IS_COORDINATOR", "False").lower() == "true"
IS_WORKER = os.environ.get("IS_WORKER", "False").lower() == "true"
COORDINATOR_ADDRESS = os.environ.get("COORDINATOR_ADDRESS", None)
WORKER_ADDRESS = os.environ.get("WORKER_ADDRESS", f"http://{NODE_ID}")
WORKER_EXTERNAL_ADDRESS = os.environ.get("WORKER_EXTERNAL_ADDRESS", None)

DUMPS_PATH = os.environ.get("DUMPS_PATH", "/tmp/parwop_dumps")
DEFAULT_DUMP_BATCH_SIZE = 10_000

HEARTBEAT_INTERVAL = os.environ.get("HEARTBEAT_INTERVAL", 20)
HEARTBEATS_SKIP_TO_DIE = os.environ.get("HEARTBEATS_SKIP_TO_DIE", 10)

IS_LINUX = sys.platform == "linux"
DEBUG = os.environ.get("DEBUG", "false").lower() == "true"

MAX_CPU = os.environ.get("MAX_CPU", 8)

RABBITMQ_HOST = os.environ.get("RABBITMQ_HOST", None)
RABBITMQ_PORT = int(os.environ.get("RABBITMQ_PORT", "5672"))
