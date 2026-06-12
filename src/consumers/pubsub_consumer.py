import json
import logging

from google.cloud import pubsub_v1

from .base_consumer import BaseConsumer
from ..schema.er_record import ERRecord

logger = logging.getLogger(__name__)


class PubSubConsumer(BaseConsumer):
    def __init__(self, project_id: str, subscription_id: str, mongo_uri: str, db_name: str):
        super().__init__(mongo_uri, db_name)
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(project_id, subscription_id)
        self.dlq_topic = f"projects/{project_id}/topics/er-insight-dlq"
        self.publisher = pubsub_v1.PublisherClient()

    def listen(self, max_messages: int = 100):
        response = self.subscriber.pull(
            request={"subscription": self.subscription_path, "max_messages": max_messages}
        )
        ack_ids = []
        for msg in response.received_messages:
            payload = json.loads(msg.message.data.decode())
            message_id = msg.message.message_id
            self.process(message_id, payload)
            ack_ids.append(msg.ack_id)

        if ack_ids:
            self.subscriber.acknowledge(
                request={"subscription": self.subscription_path, "ack_ids": ack_ids}
            )
        logger.info("Processed %d messages from Pub/Sub", len(ack_ids))

    def handle(self, payload: dict):
        record = ERRecord.from_dict(payload)
        self.db["er_records"].insert_one(record.to_mongo())

    def send_to_dlq(self, message_id: str, payload: dict, error: str):
        data = json.dumps({"message_id": message_id, "payload": payload, "error": error}).encode()
        self.publisher.publish(self.dlq_topic, data)
        logger.warning("Sent message %s to DLQ: %s", message_id, error)
