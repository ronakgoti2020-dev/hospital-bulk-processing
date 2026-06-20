"""
Hospital Bulk Processing API
----------------------------
Integrates with the Hospital Directory API to handle CSV bulk uploads.

Endpoints
---------
POST   /hospitals/bulk                        – Upload CSV, process & return results
GET    /hospitals/bulk/{batch_id}             – Poll batch status
POST   /hospitals/bulk/validate               – Validate CSV without processing
POST   /hospitals/bulk/{batch_id}/resume      – Retry failed hospitals in a batch
WS     /hospitals/bulk/{batch_id}/ws          – Real-time progress via WebSocket
GET    /hospitals/bulk                        – List all processed batches
GET    /                                      – Health check
"""
from __future__ import annotations

import logging
import uuid
from datetime import datetime, timezone
from typing import List

from fastapi import (
    BackgroundTasks,
    FastAPI,
    File,
    HTTPException,
    UploadFile,
    WebSocket,
    WebSocketDisconnect,
)
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from app.models import (
    BatchProcessingResult,
    BatchStatus,
    CSVValidationResult,
    InternalBatchState,
)
from app.services import MAX_HOSPITALS, parse_csv, process_batch, validate_csv
from app.storage import (
    broadcast_progress,
    get_batch,
    list_batches,
    register_ws,
    save_batch,
    unregister_ws,
)

# --------------------------------------------------------------------------- #
# App setup                                                                     #
# --------------------------------------------------------------------------- #

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s – %(message)s",
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="Hospital Bulk Processing API",
    description=(
        "Accepts CSV uploads of hospital records, processes them against the "
        "Hospital Directory API concurrently, and returns comprehensive batch results."
    ),
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# --------------------------------------------------------------------------- #
# Health                                                                        #
# --------------------------------------------------------------------------- #


@app.get("/", tags=["health"])
async def health_check():
    return {"status": "ok", "service": "Hospital Bulk Processing API"}


# --------------------------------------------------------------------------- #
# Primary bulk endpoint                                                         #
# --------------------------------------------------------------------------- #


@app.post(
    "/hospitals/bulk",
    response_model=BatchProcessingResult,
    status_code=202,
    summary="Bulk create hospitals from a CSV file",
    tags=["bulk"],
)
async def bulk_create_hospitals(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
):
    """
    Upload a CSV file with columns **name**, **address**, **phone** (optional).

    Returns **202 Accepted** immediately with a `batch_id` and `status: pending`.
    Processing runs in the background — track progress via:

    - **Poll:** `GET /hospitals/bulk/{batch_id}`
    - **Real-time:** `WS /hospitals/bulk/{batch_id}/ws`

    Processing steps (background):
    1. Validate the CSV (max 20 rows, required columns present, no blank required fields).
    2. Concurrently POST each hospital to the Hospital Directory API (up to 5 in parallel).
    3. If all hospitals were created successfully, activate the batch.
    """
    # ----- content-type guard -----
    if file.content_type not in ("text/csv", "text/plain", "application/csv", "application/octet-stream"):
        logger.warning("Received file with content_type=%s", file.content_type)

    content = await file.read()

    if not content.strip():
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")

    # ----- parse & validate -----
    try:
        hospitals, parse_errors = parse_csv(content)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))

    if parse_errors:
        raise HTTPException(
            status_code=422,
            detail={"message": "CSV contains invalid rows.", "errors": parse_errors},
        )

    if not hospitals:
        raise HTTPException(status_code=400, detail="No valid hospital rows found in CSV.")

    if len(hospitals) > MAX_HOSPITALS:
        raise HTTPException(
            status_code=400,
            detail=f"CSV contains {len(hospitals)} rows; maximum allowed is {MAX_HOSPITALS}.",
        )

    # ----- build batch state & return immediately -----
    batch_id = str(uuid.uuid4())
    batch = InternalBatchState(
        batch_id=batch_id,
        total_hospitals=len(hospitals),
        hospital_inputs=hospitals,
        created_at=datetime.now(timezone.utc),
    )
    save_batch(batch)
    logger.info("Batch %s accepted with %d hospitals — starting background processing.", batch_id, len(hospitals))

    # ----- kick off processing in the background -----
    background_tasks.add_task(_run_batch_background, batch)

    return batch.to_result()


async def _run_batch_background(batch: InternalBatchState) -> None:
    """Background task wrapper — logs completion/failure."""
    try:
        await process_batch(batch)
        logger.info(
            "Batch %s completed: %d ok, %d failed, activated=%s.",
            batch.batch_id,
            batch.processed_hospitals,
            batch.failed_hospitals,
            batch.batch_activated,
        )
    except Exception as exc:
        logger.exception("Batch %s crashed: %s", batch.batch_id, exc)


# --------------------------------------------------------------------------- #
# Status / polling                                                              #
# --------------------------------------------------------------------------- #


@app.get(
    "/hospitals/bulk",
    response_model=List[BatchProcessingResult],
    summary="List all batch operations",
    tags=["bulk"],
)
async def list_all_batches():
    """Return a summary of every batch processed since the server started."""
    return [b.to_result() for b in list_batches()]


@app.get(
    "/hospitals/bulk/{batch_id}",
    response_model=BatchProcessingResult,
    summary="Get status of a specific batch",
    tags=["bulk"],
)
async def get_batch_status(batch_id: str):
    """
    Poll this endpoint to check the progress or results of a batch.
    Returns live state while processing is in progress.
    """
    batch = get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail=f"Batch '{batch_id}' not found.")
    return batch.to_result()


# --------------------------------------------------------------------------- #
# CSV validation (bonus)                                                        #
# --------------------------------------------------------------------------- #


@app.post(
    "/hospitals/bulk/validate",
    response_model=CSVValidationResult,
    summary="Validate a CSV file without processing",
    tags=["bulk"],
)
async def validate_csv_file(file: UploadFile = File(...)):
    """
    Dry-run validation: checks header, required fields, row count limit, and
    returns a preview of the first 5 rows.  No data is sent to the upstream API.
    """
    content = await file.read()
    if not content.strip():
        raise HTTPException(status_code=400, detail="Uploaded file is empty.")
    return validate_csv(content)


# --------------------------------------------------------------------------- #
# Resume (bonus)                                                                #
# --------------------------------------------------------------------------- #


@app.post(
    "/hospitals/bulk/{batch_id}/resume",
    response_model=BatchProcessingResult,
    summary="Resume a partial or failed batch",
    tags=["bulk"],
)
async def resume_batch(batch_id: str):
    """
    Retry only the hospitals that failed in a previous batch run.
    Uses the **same batch ID** so all records are grouped together for activation.

    Only batches with status `partial` or `failed` can be resumed.
    """
    batch = get_batch(batch_id)
    if not batch:
        raise HTTPException(status_code=404, detail=f"Batch '{batch_id}' not found.")

    if batch.status not in (BatchStatus.PARTIAL, BatchStatus.FAILED):
        raise HTTPException(
            status_code=409,
            detail=(
                f"Batch '{batch_id}' has status '{batch.status}' and cannot be resumed. "
                "Only 'partial' or 'failed' batches are resumable."
            ),
        )

    # Identify rows that failed or were never attempted
    succeeded_rows = {r.row for r in batch.hospitals if r.status != "failed"}
    failed_inputs = [h for h in batch.hospital_inputs if h.row not in succeeded_rows]

    if not failed_inputs:
        raise HTTPException(
            status_code=409,
            detail="No failed hospitals found to retry.",
        )

    # Reset counters for the new attempt (keep existing successes)
    batch.failed_hospitals = 0
    batch.completed_at = None
    batch.status = BatchStatus.PENDING
    save_batch(batch)

    logger.info("Resuming batch %s: retrying %d failed hospitals.", batch_id, len(failed_inputs))

    await process_batch(batch, hospitals_to_process=failed_inputs)

    logger.info(
        "Resume of batch %s done: %d ok, %d failed, activated=%s.",
        batch_id,
        batch.processed_hospitals,
        batch.failed_hospitals,
        batch.batch_activated,
    )
    return batch.to_result()


# --------------------------------------------------------------------------- #
# WebSocket progress (bonus)                                                    #
# --------------------------------------------------------------------------- #


@app.websocket("/hospitals/bulk/{batch_id}/ws")
async def websocket_progress(websocket: WebSocket, batch_id: str):
    """
    Connect to receive real-time JSON progress updates while a batch is running.

    Each message is a `BatchProgressUpdate` JSON object:
    ```json
    {
      "batch_id": "...",
      "status": "processing",
      "total": 10,
      "processed": 4,
      "failed": 0,
      "percent_complete": 40.0,
      "current_hospital": "General Hospital"
    }
    ```
    The connection closes automatically once the batch reaches a terminal state.
    """
    batch = get_batch(batch_id)
    if not batch:
        await websocket.close(code=4004, reason=f"Batch '{batch_id}' not found.")
        return

    await websocket.accept()
    register_ws(batch_id, websocket)

    # If batch already finished, send the final state and close.
    if batch.status in (BatchStatus.COMPLETED, BatchStatus.PARTIAL, BatchStatus.FAILED):
        from app.models import BatchProgressUpdate

        await websocket.send_json(
            BatchProgressUpdate(
                batch_id=batch_id,
                status=batch.status,
                total=batch.total_hospitals,
                processed=batch.processed_hospitals,
                failed=batch.failed_hospitals,
                percent_complete=100.0,
            ).model_dump()
        )
        unregister_ws(batch_id, websocket)
        await websocket.close()
        return

    try:
        # Keep alive until client disconnects or batch finishes.
        while True:
            current = get_batch(batch_id)
            if current and current.status in (
                BatchStatus.COMPLETED,
                BatchStatus.PARTIAL,
                BatchStatus.FAILED,
            ):
                break
            # Wait for a push from broadcast_progress; we just keep reading to
            # detect disconnection.
            try:
                await websocket.receive_text()
            except WebSocketDisconnect:
                break
    finally:
        unregister_ws(batch_id, websocket)
