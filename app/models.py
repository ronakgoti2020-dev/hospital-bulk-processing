from __future__ import annotations

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, Field


def _utcnow() -> datetime:
    return datetime.now(timezone.utc)


class BatchStatus(str, Enum):
    PENDING = "pending"
    PROCESSING = "processing"
    ACTIVATING = "activating"
    COMPLETED = "completed"
    PARTIAL = "partial"
    FAILED = "failed"


class HospitalInput(BaseModel):
    """Parsed row from CSV — used internally and for resume capability."""

    row: int
    name: str
    address: str
    phone: Optional[str] = None


class HospitalResult(BaseModel):
    """Per-hospital outcome included in every batch response."""

    row: int
    hospital_id: Optional[int] = None
    name: str
    status: str  # "created_and_activated" | "failed" | "pending"
    error: Optional[str] = None


class BatchProcessingResult(BaseModel):
    """Returned by POST /hospitals/bulk and GET /hospitals/bulk/{batch_id}."""

    batch_id: str
    total_hospitals: int
    processed_hospitals: int
    failed_hospitals: int
    processing_time_seconds: float
    batch_activated: bool
    status: BatchStatus
    hospitals: List[HospitalResult]
    created_at: datetime
    message: Optional[str] = None


class CSVValidationResult(BaseModel):
    """Returned by POST /hospitals/bulk/validate."""

    valid: bool
    total_rows: int
    errors: List[str]
    warnings: List[str]
    preview: List[Dict[str, Any]]


class BatchProgressUpdate(BaseModel):
    """Pushed over WebSocket for real-time progress."""

    batch_id: str
    status: BatchStatus
    total: int
    processed: int
    failed: int
    percent_complete: float
    current_hospital: Optional[str] = None


class InternalBatchState(BaseModel):
    """Full mutable state kept in in-memory storage."""

    batch_id: str
    status: BatchStatus = BatchStatus.PENDING
    total_hospitals: int = 0
    processed_hospitals: int = 0
    failed_hospitals: int = 0
    batch_activated: bool = False
    hospitals: List[HospitalResult] = Field(default_factory=list)
    hospital_inputs: List[HospitalInput] = Field(default_factory=list)
    created_at: datetime = Field(default_factory=_utcnow)
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    @property
    def processing_time_seconds(self) -> float:
        if self.started_at is None:
            return 0.0
        end = self.completed_at or datetime.now(timezone.utc)
        return round((end - self.started_at).total_seconds(), 3)

    def to_result(self) -> BatchProcessingResult:
        return BatchProcessingResult(
            batch_id=self.batch_id,
            total_hospitals=self.total_hospitals,
            processed_hospitals=self.processed_hospitals,
            failed_hospitals=self.failed_hospitals,
            processing_time_seconds=self.processing_time_seconds,
            batch_activated=self.batch_activated,
            status=self.status,
            hospitals=self.hospitals,
            created_at=self.created_at,
        )

    model_config = ConfigDict(arbitrary_types_allowed=True)
