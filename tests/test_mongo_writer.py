from unittest.mock import MagicMock, patch

import pytest

from src.storage.mongo_writer import MongoWriter


@pytest.fixture
def writer():
    with patch("src.storage.mongo_writer.MongoClient") as mock_client:
        mock_collection = MagicMock()
        mock_client.return_value.__getitem__.return_value.__getitem__.return_value = mock_collection
        w = MongoWriter(uri="mongodb://localhost", db_name="test", batch_size=3, flush_interval=999)
        w.collection = mock_collection
        yield w


def test_flush_triggered_on_batch_size(writer):
    for i in range(3):
        writer.write({"_id": str(i), "data": i})
    writer.collection.bulk_write.assert_called_once()


def test_no_flush_before_batch_size(writer):
    for i in range(2):
        writer.write({"_id": str(i), "data": i})
    writer.collection.bulk_write.assert_not_called()


def test_flush_empties_buffer(writer):
    writer.write({"_id": "x", "data": 1})
    writer.flush()
    assert writer._buffer == []


def test_close_flushes_remaining(writer):
    writer.write({"_id": "y", "data": 2})
    writer.close()
    writer.collection.bulk_write.assert_called_once()
