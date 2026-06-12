import logging
import os
import threading

from ..consumers.pubsub_consumer import PubSubConsumer
from ..consumers.sqs_consumer import SQSConsumer

logger = logging.getLogger(__name__)


class IngestionPipeline:
    """
    Orchestrates parallel ingestion from GCP Pub/Sub and AWS SQS,
    writing normalized ER records into MongoDB.
    """

    def __init__(self):
        mongo_uri = os.environ["MONGO_URI"]
        db_name = os.environ.get("MONGO_DB", "er_insight")

        self.pubsub = PubSubConsumer(
            project_id=os.environ["GCP_PROJECT_ID"],
            subscription_id=os.environ["PUBSUB_SUBSCRIPTION"],
            mongo_uri=mongo_uri,
            db_name=db_name,
        )
        self.sqs = SQSConsumer(
            queue_url=os.environ["SQS_QUEUE_URL"],
            dlq_url=os.environ["SQS_DLQ_URL"],
            mongo_uri=mongo_uri,
            db_name=db_name,
            region=os.environ.get("AWS_REGION", "us-east-1"),
        )

    def run(self):
        logger.info("Starting ER-Insight ingestion pipeline")
        pubsub_thread = threading.Thread(target=self._run_pubsub, daemon=True)
        sqs_thread = threading.Thread(target=self._run_sqs, daemon=True)
        pubsub_thread.start()
        sqs_thread.start()
        pubsub_thread.join()
        sqs_thread.join()

    def _run_pubsub(self):
        while True:
            try:
                self.pubsub.listen()
            except Exception as e:
                logger.error("Pub/Sub error: %s", e)

    def _run_sqs(self):
        while True:
            try:
                self.sqs.listen()
            except Exception as e:
                logger.error("SQS error: %s", e)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    IngestionPipeline().run()
