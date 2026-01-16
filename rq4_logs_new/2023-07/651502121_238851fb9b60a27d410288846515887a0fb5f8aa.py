from settings import (
    RABBITMQ_DEFAULT_HOST,
    RABBITMQ_DEFAULT_USER,
    RABBITMQ_DEFAULT_PASS,
    RABBITMQ_MAIN_QUEUE,
)
from pika import (
    BlockingConnection,
    ConnectionParameters,
    PlainCredentials,
    BasicProperties,
)
from pika.spec import PERSISTENT_DELIVERY_MODE
import json


class RabbitMQPublisher:
    def __init__(self, queue_name: str = RABBITMQ_MAIN_QUEUE) -> None:
        self.credentials = PlainCredentials(
            RABBITMQ_DEFAULT_USER, RABBITMQ_DEFAULT_PASS
        )
        self.connection = BlockingConnection(
            ConnectionParameters(RABBITMQ_DEFAULT_HOST, credentials=self.credentials)
        )
        self.channel = self.connection.channel()
        self.queue_name = queue_name
        self.channel.queue_declare(self.queue_name, durable=False)

    def publish_failed_pids(self, failed_pids: list):
        self.channel.basic_publish(
            exchange="",
            routing_key=self.queue_name,
            body=json.dumps(failed_pids),
            properties=BasicProperties(delivery_mode=PERSISTENT_DELIVERY_MODE),
        )
        self.channel.basic_qos(
            prefetch_count=1
        )  # garante o envio de apenas uma mensagem por vez a cada consumidor
        self.connection.close()