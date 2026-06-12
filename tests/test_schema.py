import pytest

from src.schema.er_record import ERRecord, RecordType


def _base_payload():
    return {
        "record_id": "rec-001",
        "record_type": "admission",
        "patient_id": "pat-42",
        "source_region": "us-east-1",
        "payload": {"ward": "ICU", "bed": 3},
    }


def test_from_dict_valid():
    record = ERRecord.from_dict(_base_payload())
    assert record.record_id == "rec-001"
    assert record.record_type == RecordType.ADMISSION
    assert record.patient_id == "pat-42"
    assert record.checksum is not None
    assert record.checksum.startswith("sha256:")


def test_to_mongo_sets_id():
    doc = ERRecord.from_dict(_base_payload()).to_mongo()
    assert doc["_id"] == "rec-001"
    assert doc["record_type"] == "admission"


def test_missing_record_id_raises():
    data = _base_payload()
    del data["record_id"]
    with pytest.raises(ValueError, match="record_id"):
        ERRecord.from_dict(data)


def test_unknown_record_type_raises():
    data = {**_base_payload(), "record_type": "xray"}
    with pytest.raises(ValueError, match="record_type"):
        ERRecord.from_dict(data)


def test_checksum_deterministic():
    r1 = ERRecord.from_dict(_base_payload())
    r2 = ERRecord.from_dict(_base_payload())
    assert r1.checksum == r2.checksum


def test_all_record_types():
    for rt in RecordType:
        data = {**_base_payload(), "record_type": rt.value}
        record = ERRecord.from_dict(data)
        assert record.record_type == rt
