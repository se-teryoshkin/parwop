import traceback
from json import JSONDecodeError

import requests
from fastapi import APIRouter
from fastapi_utils.tasks import repeat_every

from parwop.server.discovery import discovery_service, NodeStatus
from parwop.settings import NODE_ID, HEARTBEAT_INTERVAL

router = APIRouter()


@repeat_every(seconds=HEARTBEAT_INTERVAL)
def heartbeat() -> None:
    if (coordinator_address := discovery_service.coordinator_address) is not None:
        try:

            this_worker: NodeStatus = discovery_service.this_node_status

            response = requests.post(
                url=f"{coordinator_address}/coordinator/heartbeat/",
                json={
                    "node_id": NODE_ID,
                    "address": this_worker.address,
                    "external_address": this_worker.external_address,
                },
                timeout=5,  # TODO: Move to settings
            )

            if response.status_code != 200:
                try:
                    detail = response.json()
                except JSONDecodeError:
                    detail = response.content
                print(f"WARNING! Heartbeat failed. Reason: {detail}")
            else:
                data = response.json()
                # data = {
                #     "nodes": {
                #       "node_id_1": NodeStatus,
                #       "node_id_2": NodeStatus,
                #     },
                #     "rabbitmq": {
                #         "host": "hostname",
                #         "port": 5672,
                #     }
                # }
                discovery_service.update_status({
                    node_id: NodeStatus.model_validate(node_info)
                    for node_id, node_info in data["nodes"].items()
                })
                discovery_service.update_rabbitmq(host=data["rabbitmq"]["host"], port=data["rabbitmq"]["port"])

        except Exception as e:
            print(f"WARNING! {e}")
            print(traceback.format_exc())
