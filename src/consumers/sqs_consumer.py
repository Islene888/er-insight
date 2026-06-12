import json
import logging

import boto3
from botocore.exceptions import ClientError

from .base_consumer import BaseConsumer
from ..schema.er_record import ERRecord
from ..storage.mongo_writer import MongoWriter

logger = logging.getLogger(__name__)


class SQSConsumer(BaseConsumer):
    source = "sqs"

    def __init__(self, queue_url: str, dlq_url: str,
                 mongo_uri: str, db_name: str, writer: MongoWriter,
                 region: str = "us-east-1"):
        super().__init__(mongo_uri, db_name)
        self.writer = writer
        self.sqs = boto3.client("sqs", region_name=region)
        self.queue_url = queue_url
        self.dlq_url = dlq_url

    def listen(self, max_messages: int = 10):
        try:
            response = self.sqs.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=max_messages,
                WaitTimeSeconds=5,
                AttributeNames=["MessageId"],
                MessageAttributeNames=["All"],
            )
        except ClientError as e:
            logger.error("SQS receive error: %s", e)
            return

        messages = response.get("Messages", [])
        for msg in messages:
            try:
                payload = json.loads(msg["Body"])
            except json.JSONDecodeError as e:
                logger.error("Malformed SQS message %s: %s", msg["MessageId"], e)
                self._delete(msg["ReceiptHandle"])
                continue

            success = self.process(msg["MessageId"], payload)
            if success:
                self._delete(msg["ReceiptHandle"])
            # on failure, message visibility times out and returns to queue
            # until max receive count is reached, then SQS routes to DLQ

        logger.info("SQS: processed %d messages", len(messages))

    def _delete(self, receipt_handle: str):
        self.sqs.delete_message(
            QueueUrl=self.queue_url, ReceiptHandle=receipt_handle
        )

    def handle(self, payload: dict):
        record = ERRecord.from_dict(payload)
        self.writer.write(record.to_mongo())

    def send_to_dlq(self, message_id: str, payload: dict, error: str):
        body = json.dumps({
            "message_id": message_id,
            "payload": payload,
            "error": error,
        })
        try:
            self.sqs.send_message(QueueUrl=self.dlq_url, MessageBody=body)
        except ClientError as e:
            logger.error("Failed to send %s to DLQ: %s", message_id, e)
        logger.warning("SQS DLQ: %s — %s", message_id, error)
