import os
import time
from pydantic import BaseModel
from typing import Optional

from parwop.settings import (
    IS_COORDINATOR, NODE_ID, RABBITMQ_HOST, RABBITMQ_PORT,
    COORDINATOR_ADDRESS, WORKER_ADDRESS, WORKER_EXTERNAL_ADDRESS,
)
from parwop.server.common.exceptions import WorkerIsDead, WorkerNotFound


class HeartbeatRequest(BaseModel):
    node_id: str
    address: Optional[str] = None
    external_address: Optional[str] = None


class NodeStatus(HeartbeatRequest):
    last_heartbeat: int
    is_active: bool = True


class DiscoveryService:

    def __init__(self):
        self.nodes: dict[str, NodeStatus] = dict()
        self.rabbitmq_host: Optional[str] = RABBITMQ_HOST
        self.rabbitmq_port: int = RABBITMQ_PORT
        self.coordinator_address: Optional[str] = COORDINATOR_ADDRESS

        if not IS_COORDINATOR:
            self.nodes[NODE_ID] = NodeStatus(
                node_id=NODE_ID,
                address=WORKER_ADDRESS,
                external_address=WORKER_EXTERNAL_ADDRESS,
                is_active=True,
                last_heartbeat=int(time.time()),
            )

    def update_status(self, nodes: dict[str, NodeStatus]) -> None:
        self.nodes.update(nodes)

    def update_rabbitmq(self, host: Optional[str], port: Optional[int]) -> None:
        self.rabbitmq_host = host
        self.rabbitmq_port = port

    @property
    def this_node_status(self) -> NodeStatus:
        return self.nodes[NODE_ID]

    def get_status(self) -> dict[str, dict[str, NodeStatus] | dict[str, Optional[str] | int]]:
        return {
            "nodes": self.nodes,
            "rabbitmq": {
                "host": self.rabbitmq_host,
                "port": self.rabbitmq_port,
            }
        }

    def validate_node(self, node_id: str) -> "NodeStatus":
        if (node_status := self.nodes.get(node_id, None)) is None:
            raise WorkerNotFound(f"Node {node_id} not found")

        if not node_status.is_active:
            raise WorkerIsDead(f"Node {node_id} is not active")

        return node_status


discovery_service = DiscoveryService()
