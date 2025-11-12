import time
from typing import Optional
from fastapi import APIRouter, Body
from fastapi_utils.tasks import repeat_every

from parwop.server.discovery import discovery_service, NodeStatus
from parwop.settings import HEARTBEAT_INTERVAL, HEARTBEATS_SKIP_TO_DIE

router = APIRouter()

HEARTBEAT_SECONDS_TO_DIE = HEARTBEATS_SKIP_TO_DIE * HEARTBEAT_INTERVAL
HEARTBEAT_TIME_TO_DIE = HEARTBEAT_SECONDS_TO_DIE * 1000


@router.get("/status/")
def list_node_statuses() -> dict[str, dict[str, NodeStatus] | dict[str, Optional[str] | int]]:
    return discovery_service.get_status()


@repeat_every(seconds=HEARTBEAT_INTERVAL)
def check_heartbeats():
    current_time = int(time.time())

    for node_id, node_status in discovery_service.nodes.items():
        if current_time - node_status.last_heartbeat > HEARTBEAT_TIME_TO_DIE:
            node_status.is_active = False


@router.post("/")
def heartbeat_receive(
        node_id: str = Body(...),
        address: str = Body(...),
        external_address: Optional[str] = Body(None),
) -> dict[str, dict[str, NodeStatus] | dict[str, Optional[str] | int]]:

    node_status = NodeStatus(
        node_id=node_id,
        address=address,
        external_address=external_address,
        is_active=True,
        last_heartbeat=int(time.time()),
    )

    if node_id not in discovery_service.nodes:
        print(f"WARNING! Add new node: {node_status}")

    discovery_service.update_status({node_status.node_id: node_status})

    return discovery_service.get_status()
