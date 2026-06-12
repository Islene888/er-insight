import hashlib
import logging
from abc import ABC, abstractmethod
from typing import Any

from pymongo import MongoClient

logger = logging.getLogger(__name__)


class BaseConsumer(ABC):
    """
    Idempotent base consumer. Tracks processed message IDs in MongoDB
    to guarantee exactly-once delivery even across retries.
    """

    def __init__(self, mongo_uri: str, db_name: str):
        self.client = MongoClient(mongo_uri)
        self.db = self.client[db_name]
        self.processed = self.db["processed_messages"]
        self.processed.create_index("message_id", unique=True)

    def _is_duplicate(self, message_id: str) -> bool:
        return self.processed.find_one({"message_id": message_id}) is not None

    def _mark_processed(self, message_id: str):
        try:
            self.processed.insert_one({"message_id": message_id})
        except Exception:
            pass  # duplicate key — already processed

    def _fingerprint(self, payload: dict) -> str:
        raw = str(sorted(payload.items())).encode()
        return hashlib.sha256(raw).hexdigest()

    def process(self, message_id: str, payload: dict) -> bool:
        if self._is_duplicate(message_id):
            logger.debug("Skipping duplicate message %s", message_id)
            return False
        try:
            self.handle(payload)
            self._mark_processed(message_id)
            return True
        except Exception as e:
            logger.error("Failed to process %s: %s", message_id, e)
            self.send_to_dlq(message_id, payload, error=str(e))
            return False

    @abstractmethod
    def handle(self, payload: dict) -> Any:
        """Business logic for processing a single record."""

    @abstractmethod
    def send_to_dlq(self, message_id: str, payload: dict, error: str):
        """Route failed messages to dead-letter queue."""
