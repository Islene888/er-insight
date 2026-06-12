import logging
import time
from typing import List

from pymongo import MongoClient, UpdateOne
from pymongo.errors import BulkWriteError

from ..metrics.prometheus_metrics import batch_write_size

logger = logging.getLogger(__name__)

_INDEXES = [
    ("record_type", 1),
    ("patient_id", 1),
    ("source_region", 1),
    ("ingested_at", -1),
]


class MongoWriter:
    """
    Batched MongoDB writer using bulk_write with upsert semantics.
    Each record is upserted on _id to tolerate duplicate delivery.
    """

    def __init__(self, uri: str, db_name: str, collection: str = "er_records",
                 batch_size: int = 100, flush_interval: float = 2.0):
        self.client = MongoClient(uri)
        self.collection = self.client[db_name][collection]
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._buffer: List[dict] = []
        self._last_flush = time.monotonic()
        self._ensure_indexes()

    def _ensure_indexes(self):
        for field, direction in _INDEXES:
            self.collection.create_index([(field, direction)], background=True)
        # compound index for common query pattern
        self.collection.create_index(
            [("source_region", 1), ("ingested_at", -1)], background=True
        )

    def write(self, record: dict):
        self._buffer.append(record)
        if len(self._buffer) >= self.batch_size or self._should_flush():
            self.flush()

    def flush(self):
        if not self._buffer:
            return
        batch = self._buffer[:]
        self._buffer.clear()
        self._last_flush = time.monotonic()

        ops = [
            UpdateOne({"_id": doc["_id"]}, {"$setOnInsert": doc}, upsert=True)
            for doc in batch
        ]
        try:
            self.collection.bulk_write(ops, ordered=False)
            batch_write_size.observe(len(batch))
            logger.info("Flushed batch of %d records to MongoDB", len(batch))
        except BulkWriteError as e:
            # upsert duplicates are expected — only log genuine errors
            write_errors = [
                err for err in e.details.get("writeErrors", [])
                if err.get("code") != 11000  # E11000 = duplicate key
            ]
            if write_errors:
                logger.error("Bulk write errors: %s", write_errors)

    def _should_flush(self) -> bool:
        return time.monotonic() - self._last_flush >= self.flush_interval

    def close(self):
        self.flush()
        self.client.close()
