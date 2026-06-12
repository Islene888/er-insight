import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Optional


class RecordType(str, Enum):
    ADMISSION = "admission"
    DISCHARGE = "discharge"
    LAB_RESULT = "lab_result"
    MEDICATION = "medication"
    DIAGNOSIS = "diagnosis"


class SourceRegion(str, Enum):
    US_EAST = "us-east-1"
    US_WEST = "us-west-2"
    EU_WEST = "eu-west-1"
    AP_SOUTHEAST = "ap-southeast-1"


@dataclass
class ERRecord:
    record_id: str
    record_type: RecordType
    patient_id: str
    source_region: str
    payload: dict[str, Any]
    ingested_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    schema_version: str = "1.0"
    checksum: Optional[str] = None

    def __post_init__(self):
        if self.checksum is None:
            self.checksum = self._compute_checksum()

    def _compute_checksum(self) -> str:
        raw = json.dumps(self.payload, sort_keys=True).encode()
        return "sha256:" + hashlib.sha256(raw).hexdigest()

    @classmethod
    def from_dict(cls, data: dict) -> "ERRecord":
        if "record_id" not in data:
            raise ValueError("Missing required field: record_id")
        if "record_type" not in data:
            raise ValueError("Missing required field: record_type")
        if "patient_id" not in data:
            raise ValueError("Missing required field: patient_id")

        try:
            record_type = RecordType(data["record_type"])
        except ValueError:
            raise ValueError(f"Unknown record_type: {data['record_type']!r}")

        return cls(
            record_id=data["record_id"],
            record_type=record_type,
            patient_id=data["patient_id"],
            source_region=data.get("source_region", SourceRegion.US_EAST),
            payload=data.get("payload", {}),
            checksum=data.get("checksum"),
        )

    def to_mongo(self) -> dict:
        return {
            "_id": self.record_id,
            "record_type": self.record_type.value,
            "patient_id": self.patient_id,
            "source_region": self.source_region,
            "payload": self.payload,
            "ingested_at": self.ingested_at,
            "schema_version": self.schema_version,
            "checksum": self.checksum,
        }
