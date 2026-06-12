import logging
import time
from abc import ABC, abstractmethod
from typing import Any, Optional

from pymongo import MongoClient
from pymongo.errors import DuplicateKeyError

from ..metrics.prometheus_metrics import (
    duplicates_skipped,
    messages_failed,
    messages_processed,
    processing_duration,
)
from ..retry.cloud_tasks_queue import CloudTasksRetryQueue, make_retry_queue

logger = logging.getLogger(__name__)

_MAX_RETRIES = 3
_RETRY_BASE_DELAY = 0.5


class BaseConsumer(ABC):
    """
    Exactly-once idempotent consumer.

    Strategy: atomically INSERT the message_id with status='processing' BEFORE
    calling handle(). This is the distributed lock — if two instances race,
    only one insert succeeds (unique index), the other gets DuplicateKeyError
    and bails out. After handle() completes we update status to 'done'.
    On final failure we update to 'failed' and route to DLQ.

    This prevents the TOCTOU race of the find-then-insert pattern.
    """

    source: str = "unknown"

    def __init__(self, mongo_uri: str, db_name: str,
                 retry_queue: Optional[CloudTasksRetryQueue] = None):
        client = MongoClient(mongo_uri)
        db = client[db_name]
        self._processed = db["processed_messages"]
        self._processed.create_index("message_id", unique=True)
        # Optional Cloud Tasks queue for out-of-process delayed retries.
        # Falls back to immediate DLQ if not configured.
        self._retry_queue = retry_queue or make_retry_queue()

    # ------------------------------------------------------------------
    # Public entry point
    # ------------------------------------------------------------------

    def process(self, message_id: str, payload: dict) -> bool:
        # Step 1: atomically claim the message. This is our distributed lock.
        try:
            self._processed.insert_one({
                "message_id": message_id,
                "status": "processing",
                "source": self.source,
            })
        except DuplicateKeyError:
            # Another instance already claimed or completed this message.
            duplicates_skipped.labels(source=self.source).inc()
            logger.debug("Duplicate skipped: %s", message_id)
            return False

        # Step 2: process with retry. We own the message.
        for attempt in range(1, _MAX_RETRIES + 1):
            try:
                with processing_duration.labels(source=self.source).time():
                    self.handle(payload)

                self._processed.update_one(
                    {"message_id": message_id},
                    {"$set": {"status": "done"}},
                )
                messages_processed.labels(source=self.source).inc()
                return True

            except Exception as e:
                error_type = type(e).__name__
                if attempt == _MAX_RETRIES:
                    logger.error(
                        "Message %s failed after %d attempts: %s",
                        message_id, attempt, e,
                    )
                    self._processed.update_one(
                        {"message_id": message_id},
                        {"$set": {"status": "failed", "error": str(e)}},
                    )
                    messages_failed.labels(source=self.source, error_type=error_type).inc()
                    # Prefer Cloud Tasks delayed retry over immediate DLQ.
                    # Cloud Tasks will call back /retry with increasing delays.
                    # If not configured or max attempts exceeded, fall back to DLQ.
                    cloud_tasks_attempt = self._get_cloud_tasks_attempt(message_id)
                    if self._retry_queue and self._retry_queue.enqueue(
                        message_id, payload, attempt=cloud_tasks_attempt
                    ):
                        logger.info(
                            "Message %s queued for Cloud Tasks retry (attempt %d)",
                            message_id, cloud_tasks_attempt,
                        )
                    else:
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
    # Abstract interface
    # ------------------------------------------------------------------

    def _get_cloud_tasks_attempt(self, message_id: str) -> int:
        doc = self._processed.find_one({"message_id": message_id})
        return (doc or {}).get("cloud_tasks_attempt", 0) + 1

    @abstractmethod
    def handle(self, payload: dict) -> Any:
        """Process a single record. Raise on failure to trigger retry."""

    @abstractmethod
    def send_to_dlq(self, message_id: str, payload: dict, error: str):
        """Route an unrecoverable message to the dead-letter queue."""
