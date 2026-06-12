import json
import logging
import os
import signal
import threading

from ..consumers.pubsub_consumer import PubSubConsumer
from ..consumers.sqs_consumer import SQSConsumer
from ..metrics.prometheus_metrics import pipeline_active, start_metrics_server
from ..replication.region_replicator import RegionReplicator
from ..storage.mongo_writer import MongoWriter

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Orchestrates parallel ingestion from GCP Pub/Sub and AWS SQS.
    Both consumers share a single MongoWriter for batched bulk_write.
    """

    def __init__(self):
        mongo_uri = os.environ["MONGO_URI"]
        db_name = os.environ.get("MONGO_DB", "er_insight")

        self.writer = MongoWriter(
            uri=mongo_uri,
            db_name=db_name,
            batch_size=int(os.environ.get("BATCH_SIZE", "100")),
            flush_interval=float(os.environ.get("FLUSH_INTERVAL", "2.0")),
        )

        self.pubsub = PubSubConsumer(
            project_id=os.environ["GCP_PROJECT_ID"],
            subscription_id=os.environ["PUBSUB_SUBSCRIPTION"],
            mongo_uri=mongo_uri,
            db_name=db_name,
            writer=self.writer,
        )

        self.sqs = SQSConsumer(
            queue_url=os.environ["SQS_QUEUE_URL"],
            dlq_url=os.environ["SQS_DLQ_URL"],
            mongo_uri=mongo_uri,
            db_name=db_name,
            writer=self.writer,
            region=os.environ.get("AWS_REGION", "us-east-1"),
        )

        # Cross-region replication (optional, configured via env)
        self.replicator = self._build_replicator(mongo_uri, db_name)

        self._stop_event = threading.Event()

        for sig in (signal.SIGINT, signal.SIGTERM):
            signal.signal(sig, self._handle_shutdown)

    @staticmethod
    def _build_replicator(mongo_uri: str, db_name: str) -> RegionReplicator | None:
        raw = os.environ.get("SECONDARY_REGIONS")
        if not raw:
            return None
        try:
            secondary_uris = json.loads(raw)  # '{"us-west-2": "mongodb://...", ...}'
        except json.JSONDecodeError:
            logger.warning("Invalid SECONDARY_REGIONS JSON, replication disabled")
            return None
        return RegionReplicator(
            primary_uri=mongo_uri,
            secondary_uris=secondary_uris,
            db_name=db_name,
        )

    def run(self):
        metrics_port = int(os.environ.get("METRICS_PORT", "8000"))
        start_metrics_server(metrics_port)
        logger.info("Metrics server started on :%d", metrics_port)

        pipeline_active.set(1)
        logger.info("ER-Insight pipeline starting")

        threads = [
            threading.Thread(target=self._run_loop, args=(self.pubsub.listen, "pubsub"), daemon=True),
            threading.Thread(target=self._run_loop, args=(self.sqs.listen, "sqs"), daemon=True),
        ]
        if self.replicator:
            threads.append(self.replicator.start_async())
            logger.info("Cross-region replication enabled")

        for t in threads:
            t.start()

        self._stop_event.wait()

        logger.info("Shutdown signal received — flushing writer")
        self.writer.close()
        pipeline_active.set(0)
        logger.info("ER-Insight pipeline stopped")

    def _run_loop(self, listen_fn, name: str):
        logger.info("%s consumer started", name)
        while not self._stop_event.is_set():
            try:
                listen_fn()
            except Exception as e:
                logger.error("%s consumer error: %s", name, e)

    def _handle_shutdown(self, signum, frame):
        logger.info("Received signal %d, initiating graceful shutdown", signum)
        self._stop_event.set()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    IngestionPipeline().run()
