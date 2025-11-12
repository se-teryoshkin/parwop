import json
import traceback

import pika
from typing import TYPE_CHECKING, Optional

from parwop.server.discovery import discovery_service


if TYPE_CHECKING:
    from pika.adapters.blocking_connection import BlockingChannel
    from pika import BlockingConnection



class MQService:

    @staticmethod
    def get_channel(
            ignore_error: bool = False,
            extra_connection_params: Optional[dict] = None
    ) -> tuple[Optional["BlockingChannel"], Optional["BlockingConnection"]]:

        connection = None
        channel = None

        try:
            if discovery_service.rabbitmq_host is not None:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host=discovery_service.rabbitmq_host,
                        port=discovery_service.rabbitmq_port,
                        **(dict() if extra_connection_params is None else extra_connection_params)
                    )
                )
                channel = connection.channel()
        except Exception as e:
            error_text = traceback.format_exc()
            print(f"WARNING! Error while create channel/connection to RabbitMQ: {error_text}")
            if not ignore_error:
                raise e

        return channel, connection

    @staticmethod
    def send_msg_to_consumers(
            topic: str,
            msg: bytes | dict,
            channel: Optional["BlockingChannel"] = None,
    ):

        if isinstance(msg, dict):
            msg = json.dumps(msg).encode("utf-8")
        elif not isinstance(msg, bytes):
            raise ValueError(f"Message must be of type bytes or dict, not {type(msg)}")


        is_channel_provided: bool = channel is not None
        connection = None

        try:

            if channel is None:
                channel, connection = MQService.get_channel()

            if channel is not None:
                channel.queue_declare(queue=topic)
                channel.basic_publish(
                    exchange="",
                    routing_key=topic,
                    body=msg
                )

        except Exception as e:
            print(e)
            print(f"WARNING! Error while send message to queue: {str(e)}")
        finally:
            if not is_channel_provided:
                if channel is not None:
                    channel.close()
                if connection is not None:
                    connection.close()


mq_service = MQService()
