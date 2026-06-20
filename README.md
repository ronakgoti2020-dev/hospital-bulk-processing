# Hospital Bulk Processing API

A FastAPI service that accepts CSV uploads of hospital records and processes
them against the [Hospital Directory API](https://hospital-directory.onrender.com/docs)
in a concurrent, fault-tolerant pipeline.

---

## Architecture

```
Client
  │  POST /hospitals/bulk (multipart CSV)
  ▼
┌─────────────────────────────────────────────────────┐
│  Hospital Bulk Processing API  (this service)        │
│                                                     │
│  1. Validate CSV (columns, row count ≤ 20)           │
│  2. Generate UUID batch_id                           │
│  3. Concurrent POST /hospitals/ × N  ──────────────►│  Hospital Directory API
│     (up to 5 in-flight via asyncio semaphore)        │  (https://hospital-directory.onrender.com)
│  4. PATCH /hospitals/batch/{id}/activate ──────────►│
│  5. Return BatchProcessingResult                     │
└─────────────────────────────────────────────────────┘
```

### Key Design Decisions

| Concern | Approach |
|---|---|
| **Concurrency** | `asyncio` + `aiohttp` with a semaphore of 5, giving ≤5× speedup over serial |
| **Storage** | In-memory dict (no external DB required) |
| **Resume** | Failed row inputs are kept in state; `/resume` retries only those rows |
| **Real-time progress** | WebSocket broadcast after each hospital completes |
| **Fault isolation** | Per-hospital try/except; one failure doesn't abort others |

---

## Endpoints

| Method | Path | Description |
|---|---|---|
| `POST` | `/hospitals/bulk` | Upload CSV, process & return full results |
| `GET` | `/hospitals/bulk` | List all batches |
| `GET` | `/hospitals/bulk/{batch_id}` | Poll status of a single batch |
| `POST` | `/hospitals/bulk/validate` | Dry-run CSV validation (no API calls) |
| `POST` | `/hospitals/bulk/{batch_id}/resume` | Retry failed hospitals |
| `WS` | `/hospitals/bulk/{batch_id}/ws` | Real-time progress stream |
| `GET` | `/` | Health check |

---

## CSV Format

```csv
name,address,phone
General Hospital,123 Main St,555-0001
City Clinic,456 Oak Ave,
```

- **name** – required, non-empty
- **address** – required, non-empty
- **phone** – optional
- Maximum **20 rows** per upload
- UTF-8 encoding (BOM-safe)

---

## Response Schema

```json
{
  "batch_id": "550e8400-e29b-41d4-a716-446655440000",
  "total_hospitals": 5,
  "processed_hospitals": 5,
  "failed_hospitals": 0,
  "processing_time_seconds": 3.14,
  "batch_activated": true,
  "status": "completed",
  "created_at": "2025-09-19T10:30:00Z",
  "hospitals": [
    {
      "row": 1,
      "hospital_id": 101,
      "name": "General Hospital",
      "status": "created_and_activated",
      "error": null
    }
  ]
}
```

Possible `status` values: `pending` · `processing` · `activating` · `completed` · `partial` · `failed`

---

## Local Development

```bash
# 1. Clone and install dependencies
git clone <repo-url>
cd paribus
pip install -r requirements.txt

# 2. Start the server
uvicorn app.main:app --reload

# 3. Open interactive docs
open http://localhost:8000/docs
```

### Quick test with curl

```bash
curl -X POST http://localhost:8000/hospitals/bulk \
  -F "file=@sample.csv"
```

### Validate before uploading

```bash
curl -X POST http://localhost:8000/hospitals/bulk/validate \
  -F "file=@sample.csv"
```

---

## Running Tests

```bash
pytest tests/ -v
```

The test suite mocks the upstream Hospital Directory API so no real network
calls are made.

---

## Deployment (Render)

1. Push the repository to GitHub / GitLab.
2. Create a new **Web Service** on [Render](https://render.com).
3. Point it to the repo — Render will detect `render.yaml` automatically.
4. Deploy. The `render.yaml` sets:
   - Build: `pip install -r requirements.txt`
   - Start: `uvicorn app.main:app --host 0.0.0.0 --port $PORT`

---

## Performance Notes

- With `CONCURRENCY_LIMIT = 5` and 20 hospitals, total wall-clock time is
  roughly **4 × single-request latency** instead of 20×.
- Processing time is included in every response for benchmarking.
- The semaphore value is easily tunable in `app/services.py`.

---

## Bonus Features

| Feature | Endpoint |
|---|---|
| CSV Validation | `POST /hospitals/bulk/validate` |
| Real-time WebSocket progress | `WS /hospitals/bulk/{batch_id}/ws` |
| Resume failed batch | `POST /hospitals/bulk/{batch_id}/resume` |
| Batch polling | `GET /hospitals/bulk/{batch_id}` |
