"""
Cloud Tasks retry queue.

When a message exhausts in-process retries (3 attempts), instead of
permanently dropping it we enqueue a Cloud Tasks task with an
exponential delay. Cloud Tasks calls back our /retry endpoint
(retry_handler.py) after the delay, giving the message another chance
with a fresh process instance — no shared state, no blocking the main consumer.

Retry schedule (configurable via CLOUD_TASKS_MAX_ATTEMPTS env):
  attempt 1 → 30s delay
  attempt 2 → 2m delay
  attempt 3 → 10m delay
  attempt 4 → 1h delay
  beyond    → permanent DLQ
"""

import json
import logging
import os
from datetime import datetime, timedelta, timezone

from google.cloud import tasks_v2
from google.protobuf import timestamp_pb2

logger = logging.getLogger(__name__)

_RETRY_DELAYS_SECONDS = [30, 120, 600, 3600]  # 30s, 2m, 10m, 1h


class CloudTasksRetryQueue:
    def __init__(
        self,
        project: str,
        location: str,
        queue: str,
        handler_url: str,
    ):
        self.client = tasks_v2.CloudTasksClient()
        self.queue_path = self.client.queue_path(project, location, queue)
        self.handler_url = handler_url  # e.g. https://pipeline.internal/retry

    def enqueue(self, message_id: str, payload: dict, attempt: int) -> bool:
        """
        Schedule a retry task. Returns False if max attempts exceeded
        (caller should route to permanent DLQ instead).
        """
        if attempt > len(_RETRY_DELAYS_SECONDS):
            logger.warning(
                "Message %s exceeded max Cloud Tasks retries (%d), sending to permanent DLQ",
                message_id, len(_RETRY_DELAYS_SECONDS),
            )
            return False

        delay_seconds = _RETRY_DELAYS_SECONDS[attempt - 1]
        schedule_time = datetime.now(timezone.utc) + timedelta(seconds=delay_seconds)

        ts = timestamp_pb2.Timestamp()
        ts.FromDatetime(schedule_time)

        body = json.dumps({
            "message_id": message_id,
            "payload": payload,
            "attempt": attempt,
        }).encode()

        task = tasks_v2.Task(
            http_request=tasks_v2.HttpRequest(
                http_method=tasks_v2.HttpMethod.POST,
                url=f"{self.handler_url}/retry",
                headers={"Content-Type": "application/json"},
                body=body,
            ),
            schedule_time=ts,
            name=f"{self.queue_path}/tasks/retry-{message_id}-attempt-{attempt}",
        )

        try:
            self.client.create_task(
                request={"parent": self.queue_path, "task": task}
            )
            logger.info(
                "Enqueued Cloud Tasks retry for %s (attempt %d, delay %ds)",
                message_id, attempt, delay_seconds,
            )
            return True
        except Exception as e:
            logger.error("Failed to enqueue Cloud Tasks retry for %s: %s", message_id, e)
            return False


def make_retry_queue() -> CloudTasksRetryQueue | None:
    """Build from environment variables. Returns None if not configured."""
    required = ["GCP_PROJECT_ID", "CLOUD_TASKS_LOCATION", "CLOUD_TASKS_QUEUE", "RETRY_HANDLER_URL"]
    if not all(os.environ.get(k) for k in required):
        return None
    return CloudTasksRetryQueue(
        project=os.environ["GCP_PROJECT_ID"],
        location=os.environ["CLOUD_TASKS_LOCATION"],
        queue=os.environ["CLOUD_TASKS_QUEUE"],
        handler_url=os.environ["RETRY_HANDLER_URL"],
    )
