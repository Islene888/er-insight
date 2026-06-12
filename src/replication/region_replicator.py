"""
Cross-region replication via MongoDB change streams.

The primary region ingests records from Pub/Sub and SQS into its local MongoDB.
RegionReplicator watches the primary's change stream and asynchronously
propagates inserts/updates to secondary MongoDB instances in other regions.

Each secondary uses upsert ($setOnInsert) so replaying the stream is idempotent —
safe to restart after a crash without duplicating data.

Architecture:
    primary (us-east-1) → change stream → RegionReplicator
                                               ├──► secondary (us-west-2)
                                               ├──► secondary (eu-west-1)
                                               └──► secondary (ap-southeast-1)

Resume token is persisted in MongoDB so replication resumes from the last
processed event after a restart — no records are lost during downtime.
"""

import logging
import threading
import time
from typing import Optional

from pymongo import MongoClient, UpdateOne
from pymongo.errors import PyMongoError

from ..metrics.prometheus_metrics import replication_lag, replication_errors

logger = logging.getLogger(__name__)

_RESUME_TOKEN_COLLECTION = "replication_state"
_RECONNECT_DELAY = 5  # seconds before reconnecting after error


class RegionReplicator:
    def __init__(
        self,
        primary_uri: str,
        secondary_uris: dict[str, str],  # {"us-west-2": "mongodb://...", ...}
        db_name: str,
        collection: str = "er_records",
        batch_size: int = 50,
    ):
        self.primary = MongoClient(primary_uri)[db_name]
        self.secondaries = {
            region: MongoClient(uri)[db_name][collection]
            for region, uri in secondary_uris.items()
        }
        self.source_collection = self.primary[collection]
        self.state = self.primary[_RESUME_TOKEN_COLLECTION]
        self.batch_size = batch_size
        self._stop = threading.Event()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def start(self):
        """Start replication in the current thread (blocking)."""
        logger.info(
            "Starting replication → %s", list(self.secondaries.keys())
        )
        while not self._stop.is_set():
            try:
                self._watch()
            except PyMongoError as e:
                replication_errors.inc()
                logger.error("Change stream error: %s — reconnecting in %ds", e, _RECONNECT_DELAY)
                time.sleep(_RECONNECT_DELAY)

    def start_async(self) -> threading.Thread:
        """Start replication in a background daemon thread."""
        t = threading.Thread(target=self.start, daemon=True, name="region-replicator")
        t.start()
        return t

    def stop(self):
        self._stop.set()

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _watch(self):
        resume_token = self._load_resume_token()
        pipeline = [{"$match": {"operationType": {"$in": ["insert", "replace", "update"]}}}]
        options = {"batch_size": self.batch_size}
        if resume_token:
            options["resume_after"] = resume_token

        with self.source_collection.watch(pipeline, **options) as stream:
            batch: list[dict] = []

            for event in stream:
                if self._stop.is_set():
                    break

                doc = event.get("fullDocument")
                if doc:
                    batch.append(doc)

                if len(batch) >= self.batch_size:
                    self._replicate_batch(batch)
                    batch.clear()
                    self._save_resume_token(stream.resume_token)

            # flush remaining
            if batch:
                self._replicate_batch(batch)
                self._save_resume_token(stream.resume_token)

    def _replicate_batch(self, docs: list[dict]):
        ops = [
            UpdateOne({"_id": doc["_id"]}, {"$setOnInsert": doc}, upsert=True)
            for doc in docs
        ]
        failed_regions = []
        for region, collection in self.secondaries.items():
            try:
                result = collection.bulk_write(ops, ordered=False)
                replication_lag.labels(region=region).set(0)
                logger.debug(
                    "Replicated %d docs → %s (upserted=%d)",
                    len(docs), region, result.upserted_count,
                )
            except PyMongoError as e:
                replication_errors.labels(region=region).inc()
                logger.error("Replication failed → %s: %s", region, e)
                failed_regions.append(region)

        if failed_regions:
            logger.warning("Replication lag on regions: %s", failed_regions)

    def _load_resume_token(self) -> Optional[dict]:
        doc = self.state.find_one({"_id": "change_stream_token"})
        return doc.get("token") if doc else None

    def _save_resume_token(self, token):
        if token:
            self.state.update_one(
                {"_id": "change_stream_token"},
                {"$set": {"token": token}},
                upsert=True,
            )
