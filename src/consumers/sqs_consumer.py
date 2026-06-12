import json
import logging

import boto3

from .base_consumer import BaseConsumer
from ..schema.er_record import ERRecord

logger = logging.getLogger(__name__)


class SQSConsumer(BaseConsumer):
    def __init__(self, queue_url: str, dlq_url: str, mongo_uri: str, db_name: str,
                 region: str = "us-east-1"):
        super().__init__(mongo_uri, db_name)
        self.sqs = boto3.client("sqs", region_name=region)
        self.queue_url = queue_url
        self.dlq_url = dlq_url

    def listen(self, max_messages: int = 10):
        response = self.sqs.receive_message(
            QueueUrl=self.queue_url,
            MaxNumberOfMessages=max_messages,
            WaitTimeSeconds=5,
            AttributeNames=["MessageId"],
        )
        messages = response.get("Messages", [])
        for msg in messages:
            payload = json.loads(msg["Body"])
            message_id = msg["MessageId"]
            success = self.process(message_id, payload)
            if success:
                self.sqs.delete_message(
                    QueueUrl=self.queue_url,
                    ReceiptHandle=msg["ReceiptHandle"],
                )
        logger.info("Processed %d messages from SQS", len(messages))

    def handle(self, payload: dict):
        record = ERRecord.from_dict(payload)
        self.db["er_records"].insert_one(record.to_mongo())

    def send_to_dlq(self, message_id: str, payload: dict, error: str):
        body = json.dumps({"message_id": message_id, "payload": payload, "error": error})
        self.sqs.send_message(QueueUrl=self.dlq_url, MessageBody=body)
        logger.warning("Sent message %s to DLQ: %s", message_id, error)
