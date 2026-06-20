"""
Real-time progress tracker for the Hospital Bulk Processing API.

Usage
-----
# 1. WebSocket live stream (recommended):
python track_progress.py ws sample.csv

# 2. Poll every second until done:
python track_progress.py poll sample.csv

# 3. If you already have a batch_id:
python track_progress.py ws  --batch-id <uuid>
python track_progress.py poll --batch-id <uuid>

Set BASE_URL to point at your deployed service.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import sys
import time
import urllib.request

BASE_URL = "https://hospital-bulk-processing-uidh.onrender.com"   # ← change to your Render URL when deployed
# BASE_URL = "https://hospital-bulk-processing.onrender.com"


# --------------------------------------------------------------------------- #
# Helpers                                                                       #
# --------------------------------------------------------------------------- #

def _bar(pct: float, width: int = 30) -> str:
    filled = int(width * pct / 100)
    return "[" + "█" * filled + "░" * (width - filled) + f"] {pct:.0f}%"


def _submit_csv(csv_path: str) -> str:
    """POST the CSV and return the batch_id."""
    import mimetypes
    import uuid as _uuid

    boundary = _uuid.uuid4().hex
    with open(csv_path, "rb") as f:
        body = (
            f"--{boundary}\r\n"
            f'Content-Disposition: form-data; name="file"; filename="{csv_path}"\r\n'
            f"Content-Type: text/csv\r\n\r\n"
        ).encode() + f.read() + f"\r\n--{boundary}--\r\n".encode()

    req = urllib.request.Request(
        f"{BASE_URL}/hospitals/bulk",
        data=body,
        headers={"Content-Type": f"multipart/form-data; boundary={boundary}"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        data = json.loads(resp.read())

    batch_id = data["batch_id"]
    total = data["total_hospitals"]
    print(f"\n✔ Accepted  batch_id = {batch_id}  |  total hospitals = {total}\n")
    return batch_id


def _get_status(batch_id: str) -> dict:
    url = f"{BASE_URL}/hospitals/bulk/{batch_id}"
    with urllib.request.urlopen(url) as resp:
        return json.loads(resp.read())


# --------------------------------------------------------------------------- #
# Mode 1: polling                                                               #
# --------------------------------------------------------------------------- #

def poll(batch_id: str, interval: float = 1.0) -> None:
    terminal = {"completed", "partial", "failed"}
    print("Polling every 1 s … (Ctrl-C to stop)\n")

    while True:
        data = _get_status(batch_id)
        status   = data["status"]
        total    = data["total_hospitals"]
        done     = data["processed_hospitals"] + data["failed_hospitals"]
        pct      = done / max(total, 1) * 100

        line = (
            f"\r  {_bar(pct)}  "
            f"{done}/{total}  "
            f"ok={data['processed_hospitals']}  "
            f"fail={data['failed_hospitals']}  "
            f"status={status}   "
        )
        print(line, end="", flush=True)

        if status in terminal:
            print(f"\n\nDone in {data['processing_time_seconds']} s  |  activated={data['batch_activated']}")
            _print_results(data)
            break

        time.sleep(interval)


# --------------------------------------------------------------------------- #
# Mode 2: WebSocket                                                             #
# --------------------------------------------------------------------------- #

async def websocket_track(batch_id: str) -> None:
    try:
        import websockets  # type: ignore
    except ImportError:
        print("Install websockets:  pip install websockets")
        sys.exit(1)

    ws_url = BASE_URL.replace("http", "ws") + f"/hospitals/bulk/{batch_id}/ws"
    print(f"Connecting to WebSocket: {ws_url}\n")
    terminal = {"completed", "partial", "failed"}

    async with websockets.connect(ws_url) as ws:
        async for raw in ws:
            msg = json.loads(raw)
            status = msg["status"]
            pct    = msg["percent_complete"]
            done   = msg["processed"] + msg["failed"]
            total  = msg["total"]
            name   = msg.get("current_hospital") or ""

            line = (
                f"\r  {_bar(pct)}  "
                f"{done}/{total}  "
                f"ok={msg['processed']}  "
                f"fail={msg['failed']}  "
                f"{('→ ' + name) if name else ''}  "
                f"status={status}   "
            )
            print(line, end="", flush=True)

            if status in terminal:
                print("\n")
                break

    # Fetch full result for summary
    data = _get_status(batch_id)
    print(f"Done in {data['processing_time_seconds']} s  |  activated={data['batch_activated']}")
    _print_results(data)


# --------------------------------------------------------------------------- #
# Result summary                                                                #
# --------------------------------------------------------------------------- #

def _print_results(data: dict) -> None:
    print("\n  Row  │ ID     │ Name                          │ Status")
    print("  ─────┼────────┼───────────────────────────────┼──────────────────────")
    for h in data.get("hospitals", []):
        row_id  = str(h.get("hospital_id") or "–").ljust(6)
        name    = (h["name"] or "")[:30].ljust(30)
        status  = h["status"]
        err     = f"  ✗ {h['error']}" if h.get("error") else ""
        print(f"  {h['row']:<4} │ {row_id} │ {name} │ {status}{err}")
    print()


# --------------------------------------------------------------------------- #
# CLI                                                                           #
# --------------------------------------------------------------------------- #

def main() -> None:
    parser = argparse.ArgumentParser(description="Hospital Bulk Processing progress tracker")
    parser.add_argument("mode", choices=["ws", "poll"], help="Tracking mode")
    parser.add_argument("csv", nargs="?", help="Path to CSV file (omit if using --batch-id)")
    parser.add_argument("--batch-id", help="Use existing batch ID instead of uploading a CSV")
    parser.add_argument("--base-url", default=BASE_URL, help=f"API base URL (default: {BASE_URL})")
    args = parser.parse_args()

    global BASE_URL
    BASE_URL = args.base_url.rstrip("/")

    if args.batch_id:
        batch_id = args.batch_id
    elif args.csv:
        batch_id = _submit_csv(args.csv)
    else:
        parser.error("Provide either a CSV file or --batch-id")

    if args.mode == "poll":
        poll(batch_id)
    else:
        asyncio.run(websocket_track(batch_id))


if __name__ == "__main__":
    main()
