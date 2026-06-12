from unittest.mock import MagicMock, call, patch

import pytest

from src.replication.region_replicator import RegionReplicator


@pytest.fixture
def replicator():
    with patch("src.replication.region_replicator.MongoClient") as mock_client:
        mock_db = MagicMock()
        mock_client.return_value.__getitem__.return_value = mock_db

        r = RegionReplicator(
            primary_uri="mongodb://primary",
            secondary_uris={
                "us-west-2": "mongodb://secondary-west",
                "eu-west-1": "mongodb://secondary-eu",
            },
            db_name="er_insight",
            batch_size=2,
        )
        # Manually wire up mocks for assertions
        r.source_collection = MagicMock()
        r.state = MagicMock()
        r.state.find_one.return_value = None

        secondary_col = MagicMock()
        r.secondaries = {
            "us-west-2": secondary_col,
            "eu-west-1": secondary_col,
        }
        yield r, secondary_col


def test_replicate_batch_calls_bulk_write(replicator):
    r, secondary = replicator
    docs = [{"_id": "1", "data": "a"}, {"_id": "2", "data": "b"}]
    r._replicate_batch(docs)
    assert secondary.bulk_write.call_count == 2  # once per secondary region


def test_replicate_batch_uses_upsert(replicator):
    r, secondary = replicator
    docs = [{"_id": "x", "data": "y"}]
    r._replicate_batch(docs)
    ops = secondary.bulk_write.call_args[0][0]
    assert len(ops) == 1
    # Verify it's an upsert UpdateOne (not insert)
    assert ops[0]._filter == {"_id": "x"}
    assert ops[0]._doc == {"$setOnInsert": {"_id": "x", "data": "y"}}


def test_resume_token_saved(replicator):
    r, _ = replicator
    token = {"_data": "abc123"}
    r._save_resume_token(token)
    r.state.update_one.assert_called_once()
    call_args = r.state.update_one.call_args
    assert call_args[1]["upsert"] is True


def test_resume_token_loaded(replicator):
    r, _ = replicator
    r.state.find_one.return_value = {"_id": "change_stream_token", "token": {"_data": "tok"}}
    token = r._load_resume_token()
    assert token == {"_data": "tok"}


def test_no_resume_token_returns_none(replicator):
    r, _ = replicator
    r.state.find_one.return_value = None
    assert r._load_resume_token() is None
