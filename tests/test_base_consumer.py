from unittest.mock import MagicMock, patch

import pytest

from src.consumers.base_consumer import BaseConsumer


class _FakeConsumer(BaseConsumer):
    source = "test"

    def __init__(self):
        # skip real MongoDB connection
        self._processed = MagicMock()
        self._processed.find_one.return_value = None
        self.handled = []
        self.dlq = []

    def handle(self, payload: dict):
        if payload.get("fail"):
            raise RuntimeError("intentional failure")
        self.handled.append(payload)

    def send_to_dlq(self, message_id, payload, error):
        self.dlq.append((message_id, payload, error))


@pytest.fixture
def consumer():
    return _FakeConsumer()


def test_successful_process(consumer):
    result = consumer.process("msg-1", {"data": "ok"})
    assert result is True
    assert len(consumer.handled) == 1


def test_duplicate_skipped(consumer):
    consumer._processed.find_one.return_value = {"message_id": "msg-1"}
    result = consumer.process("msg-1", {"data": "ok"})
    assert result is False
    assert len(consumer.handled) == 0


def test_failure_routes_to_dlq(consumer):
    with patch("src.consumers.base_consumer.time.sleep"):  # skip retry delays
        result = consumer.process("msg-bad", {"fail": True})
    assert result is False
    assert len(consumer.dlq) == 1
    assert consumer.dlq[0][0] == "msg-bad"


def test_retry_succeeds_on_second_attempt(consumer):
    attempts = {"n": 0}

    def flaky_handle(payload):
        attempts["n"] += 1
        if attempts["n"] < 2:
            raise RuntimeError("transient")

    consumer.handle = flaky_handle
    with patch("src.consumers.base_consumer.time.sleep"):
        result = consumer.process("msg-flaky", {})
    assert result is True
    assert attempts["n"] == 2
