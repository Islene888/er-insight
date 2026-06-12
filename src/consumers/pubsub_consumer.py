import json
import logging

from google.cloud import pubsub_v1

from .base_consumer import BaseConsumer
from ..schema.er_record import ERRecord
from ..storage.mongo_writer import MongoWriter

logger = logging.getLogger(__name__)


class PubSubConsumer(BaseConsumer):
    source = "pubsub"

    def __init__(self, project_id: str, subscription_id: str,
                 mongo_uri: str, db_name: str, writer: MongoWriter):
        super().__init__(mongo_uri, db_name)
        self.writer = writer
        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscription_path = self.subscriber.subscription_path(
            project_id, subscription_id
        )
        self.publisher = pubsub_v1.PublisherClient()
        self.dlq_topic = f"projects/{project_id}/topics/er-insight-dlq"

    def listen(self, max_messages: int = 100):
        response = self.subscriber.pull(
            request={
                "subscription": self.subscription_path,
                "max_messages": max_messages,
            }
        )
        ack_ids = []
        for msg in response.received_messages:
            try:
                payload = json.loads(msg.message.data.decode())
            except (json.JSONDecodeError, UnicodeDecodeError) as e:
                logger.error("Malformed Pub/Sub message %s: %s", msg.message.message_id, e)
                ack_ids.append(msg.ack_id)  # ack to avoid infinite loop
                continue

            self.process(msg.message.message_id, payload)
            ack_ids.append(msg.ack_id)

        if ack_ids:
            self.subscriber.acknowledge(
                request={"subscription": self.subscription_path, "ack_ids": ack_ids}
            )
        logger.info("Pub/Sub: processed %d messages", len(ack_ids))

    def handle(self, payload: dict):
        record = ERRecord.from_dict(payload)
        self.writer.write(record.to_mongo())

    def send_to_dlq(self, message_id: str, payload: dict, error: str):
        data = json.dumps({
            "message_id": message_id,
            "payload": payload,
            "error": error,
        }).encode()
        self.publisher.publish(self.dlq_topic, data)
        logger.warning("Pub/Sub DLQ: %s — %s", message_id, error)
