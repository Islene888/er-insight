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

    @classmethod
    def from_dict(cls, data: dict) -> "ERRecord":
        return cls(
            record_id=data["record_id"],
            record_type=RecordType(data["record_type"]),
            patient_id=data["patient_id"],
            source_region=data.get("source_region", "unknown"),
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
