import hashlib
import logging
import time
from abc import ABC, abstractmethod
from typing import Any

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from ..metrics.prometheus_metrics import (
    duplicates_skipped,
    messages_failed,
    messages_processed,
    processing_duration,
)

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 0.5  # seconds, doubles each attempt


class BaseConsumer(ABC):
    """
    Idempotent base consumer.

    - Deduplicates via a processed_messages index in MongoDB.
    - Retries transient failures with exponential backoff.
    - Routes unrecoverable messages to a dead-letter queue.
    """

    source: str = "unknown"

    def __init__(self, mongo_uri: str, db_name: str):
        client = MongoClient(mongo_uri)
        db = client[db_name]
        self._processed = db["processed_messages"]
        self._processed.create_index("message_id", unique=True)

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def process(self, message_id: str, payload: dict) -> bool:
        if self._is_duplicate(message_id):
            duplicates_skipped.labels(source=self.source).inc()
            logger.debug("Duplicate skipped: %s", message_id)
            return False

        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                with processing_duration.labels(source=self.source).time():
                    self.handle(payload)
                self._mark_processed(message_id)
                messages_processed.labels(source=self.source).inc()
                return True
            except Exception as e:
                error_type = type(e).__name__
                if attempt == _MAX_RETRIES:
                    logger.error(
                        "Message %s failed after %d attempts: %s", message_id, attempt, e
                    )
                    messages_failed.labels(source=self.source, error_type=error_type).inc()
                    self.send_to_dlq(message_id, payload, error=str(e))
                    return False
                delay = _RETRY_BASE_DELAY * (2 ** (attempt - 1))
                logger.warning(
                    "Attempt %d/%d failed for %s (%s), retrying in %.1fs",
                    attempt, _MAX_RETRIES, message_id, e, delay,
                )
                time.sleep(delay)

        return False

    # ------------------------------------------------------------------
    # Idempotency helpers
    # ------------------------------------------------------------------

    def _is_duplicate(self, message_id: str) -> bool:
        return self._processed.find_one({"message_id": message_id}) is not None

    def _mark_processed(self, message_id: str):
        try:
            self._processed.insert_one({"message_id": message_id})
        except DuplicateKeyError:
            pass  # concurrent consumer already marked it

    @staticmethod
    def fingerprint(payload: dict) -> str:
        raw = str(sorted(payload.items())).encode()
        return hashlib.sha256(raw).hexdigest()

    # ------------------------------------------------------------------
    # Abstract interface
    # ------------------------------------------------------------------

    @abstractmethod
    def handle(self, payload: dict) -> Any:
        """Process a single record. Raise on failure to trigger retry."""

    @abstractmethod
    def send_to_dlq(self, message_id: str, payload: dict, error: str):
        """Route an unrecoverable message to the dead-letter queue."""
