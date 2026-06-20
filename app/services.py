"""
Business logic: CSV parsing/validation, HTTP integration with the Hospital
Directory API, and concurrent batch processing.
"""
from __future__ import annotations

import asyncio
import csv
import io
import logging
from datetime import datetime, timezone
from typing import Callable, List, Optional, Tuple

import aiohttp

from app.models import (
    BatchProgressUpdate,
    BatchStatus,
    CSVValidationResult,
    HospitalInput,
    HospitalResult,
    InternalBatchState,
)
from app.storage import broadcast_progress, save_batch

logger = logging.getLogger(__name__)

HOSPITAL_API_BASE = "https://hospital-directory.onrender.com"
MAX_HOSPITALS = 20
CONCURRENCY_LIMIT = 5  # max simultaneous requests to the upstream API

# --------------------------------------------------------------------------- #
# CSV helpers                                                                   #
# --------------------------------------------------------------------------- #

REQUIRED_COLUMNS = {"name", "address"}
OPTIONAL_COLUMNS = {"phone"}
ALL_COLUMNS = REQUIRED_COLUMNS | OPTIONAL_COLUMNS


def parse_csv(content: bytes) -> Tuple[List[HospitalInput], List[str]]:
    """
    Parse raw CSV bytes into HospitalInput objects.
    Returns (hospitals, errors).  Errors are non-fatal row-level issues.
    """
    text = content.decode("utf-8-sig").strip()
    reader = csv.DictReader(io.StringIO(text))

    if not reader.fieldnames:
        raise ValueError("CSV file is empty or missing a header row.")

    headers = {h.strip().lower() for h in reader.fieldnames}
    missing = REQUIRED_COLUMNS - headers
    if missing:
        raise ValueError(f"CSV is missing required columns: {', '.join(sorted(missing))}")

    hospitals: List[HospitalInput] = []
    errors: List[str] = []

    for row_num, raw_row in enumerate(reader, start=1):
        row = {k.strip().lower(): (v or "").strip() for k, v in raw_row.items()}

        name = row.get("name", "")
        address = row.get("address", "")
        phone = row.get("phone") or None

        row_errors = []
        if not name:
            row_errors.append(f"Row {row_num}: 'name' is empty.")
        if not address:
            row_errors.append(f"Row {row_num}: 'address' is empty.")

        if row_errors:
            errors.extend(row_errors)
            continue

        hospitals.append(HospitalInput(row=row_num, name=name, address=address, phone=phone))

    return hospitals, errors


def validate_csv(content: bytes) -> CSVValidationResult:
    """
    Full CSV validation without any API calls.  Used by the /validate endpoint.
    """
    errors: List[str] = []
    warnings: List[str] = []
    preview: list = []
    total_rows = 0

    try:
        hospitals, parse_errors = parse_csv(content)
        errors.extend(parse_errors)
        total_rows = len(hospitals) + len(parse_errors)

        if total_rows > MAX_HOSPITALS:
            errors.append(
                f"CSV contains {total_rows} data rows; maximum allowed is {MAX_HOSPITALS}."
            )

        text = content.decode("utf-8-sig").strip()
        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames:
            extra = {h.strip().lower() for h in reader.fieldnames} - ALL_COLUMNS
            if extra:
                warnings.append(
                    f"Unknown columns will be ignored: {', '.join(sorted(extra))}"
                )

        preview = [
            {"row": h.row, "name": h.name, "address": h.address, "phone": h.phone}
            for h in hospitals[:5]
        ]

    except ValueError as exc:
        errors.append(str(exc))

    return CSVValidationResult(
        valid=len(errors) == 0,
        total_rows=total_rows,
        errors=errors,
        warnings=warnings,
        preview=preview,
    )


# --------------------------------------------------------------------------- #
# HTTP helpers                                                                  #
# --------------------------------------------------------------------------- #

async def _create_hospital(
    session: aiohttp.ClientSession,
    hospital: HospitalInput,
    batch_id: str,
) -> HospitalResult:
    """POST a single hospital to the upstream API."""
    payload: dict = {
        "name": hospital.name,
        "address": hospital.address,
        "creation_batch_id": batch_id,
    }
    if hospital.phone:
        payload["phone"] = hospital.phone

    try:
        async with session.post(
            f"{HOSPITAL_API_BASE}/hospitals/",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            if resp.status in (200, 201):
                data = await resp.json()
                return HospitalResult(
                    row=hospital.row,
                    hospital_id=data.get("id"),
                    name=hospital.name,
                    status="created_and_activated",
                )
            body = await resp.text()
            return HospitalResult(
                row=hospital.row,
                name=hospital.name,
                status="failed",
                error=f"HTTP {resp.status}: {body[:200]}",
            )
    except asyncio.TimeoutError:
        return HospitalResult(
            row=hospital.row,
            name=hospital.name,
            status="failed",
            error="Request timed out.",
        )
    except aiohttp.ClientError as exc:
        return HospitalResult(
            row=hospital.row,
            name=hospital.name,
            status="failed",
            error=str(exc),
        )


async def _activate_batch(session: aiohttp.ClientSession, batch_id: str) -> bool:
    """PATCH the upstream API to activate all hospitals in the batch."""
    try:
        async with session.patch(
            f"{HOSPITAL_API_BASE}/hospitals/batch/{batch_id}/activate",
            timeout=aiohttp.ClientTimeout(total=30),
        ) as resp:
            return resp.status == 200
    except Exception as exc:
        logger.error("Failed to activate batch %s: %s", batch_id, exc)
        return False


# --------------------------------------------------------------------------- #
# Core batch processor                                                          #
# --------------------------------------------------------------------------- #

async def process_batch(
    batch: InternalBatchState,
    hospitals_to_process: Optional[List[HospitalInput]] = None,
    on_progress: Optional[Callable[[BatchProgressUpdate], None]] = None,
) -> InternalBatchState:
    """
    Concurrently create hospitals against the upstream API then activate the
    batch.  Updates `batch` in-place and persists to storage after every
    hospital so the polling endpoint always shows fresh data.

    `hospitals_to_process` defaults to `batch.hospital_inputs` but can be
    overridden for the resume path (failed rows only).
    """
    targets = hospitals_to_process if hospitals_to_process is not None else batch.hospital_inputs
    semaphore = asyncio.Semaphore(CONCURRENCY_LIMIT)

    batch.status = BatchStatus.PROCESSING
    batch.started_at = batch.started_at or datetime.now(timezone.utc)
    save_batch(batch)

    async def _bounded_create(hospital: HospitalInput, session: aiohttp.ClientSession) -> HospitalResult:
        async with semaphore:
            result = await _create_hospital(session, hospital, batch.batch_id)

            # Update counters and persist state atomically
            if result.status == "failed":
                batch.failed_hospitals += 1
            else:
                batch.processed_hospitals += 1

            # Upsert result for this row (replace any previous attempt)
            batch.hospitals = [
                r for r in batch.hospitals if r.row != result.row
            ]
            batch.hospitals.append(result)
            batch.hospitals.sort(key=lambda r: r.row)
            save_batch(batch)

            # Notify WebSocket listeners
            update = BatchProgressUpdate(
                batch_id=batch.batch_id,
                status=batch.status,
                total=batch.total_hospitals,
                processed=batch.processed_hospitals,
                failed=batch.failed_hospitals,
                percent_complete=round(
                    (batch.processed_hospitals + batch.failed_hospitals)
                    / max(batch.total_hospitals, 1)
                    * 100,
                    1,
                ),
                current_hospital=hospital.name,
            )
            await broadcast_progress(batch.batch_id, update)

            if on_progress:
                on_progress(update)

            return result

    async with aiohttp.ClientSession() as session:
        await asyncio.gather(*[_bounded_create(h, session) for h in targets])

        # Activate batch only when every hospital was created successfully
        all_ok = batch.failed_hospitals == 0
        if all_ok:
            batch.status = BatchStatus.ACTIVATING
            save_batch(batch)
            activated = await _activate_batch(session, batch.batch_id)
            batch.batch_activated = activated
        else:
            activated = False

    batch.completed_at = datetime.now(timezone.utc)
    if batch.failed_hospitals == 0:
        batch.status = BatchStatus.COMPLETED
        # Mark all results as activated
        batch.hospitals = [
            HospitalResult(**{**r.model_dump(), "status": "created_and_activated"})
            for r in batch.hospitals
        ]
    elif batch.processed_hospitals == 0:
        batch.status = BatchStatus.FAILED
    else:
        batch.status = BatchStatus.PARTIAL

    save_batch(batch)

    # Final WebSocket broadcast
    await broadcast_progress(
        batch.batch_id,
        BatchProgressUpdate(
            batch_id=batch.batch_id,
            status=batch.status,
            total=batch.total_hospitals,
            processed=batch.processed_hospitals,
            failed=batch.failed_hospitals,
            percent_complete=100.0,
        ),
    )

    return batch
