"""
Thread-safe in-memory storage for batch state and WebSocket connections.
"""
from __future__ import annotations

import asyncio
from typing import Dict, List, Optional

from app.models import BatchProgressUpdate, InternalBatchState


# --------------------------------------------------------------------------- #
# Batch store                                                                   #
# --------------------------------------------------------------------------- #

_batch_store: Dict[str, InternalBatchState] = {}


def get_batch(batch_id: str) -> Optional[InternalBatchState]:
    return _batch_store.get(batch_id)


def save_batch(batch: InternalBatchState) -> None:
    _batch_store[batch.batch_id] = batch


def list_batches() -> List[InternalBatchState]:
    return list(_batch_store.values())


# --------------------------------------------------------------------------- #
# WebSocket connection registry                                                 #
# --------------------------------------------------------------------------- #

_ws_connections: Dict[str, List] = {}


def register_ws(batch_id: str, websocket) -> None:
    _ws_connections.setdefault(batch_id, []).append(websocket)


def unregister_ws(batch_id: str, websocket) -> None:
    conns = _ws_connections.get(batch_id, [])
    try:
        conns.remove(websocket)
    except ValueError:
        pass


async def broadcast_progress(batch_id: str, update: BatchProgressUpdate) -> None:
    """Send a progress update to all WebSocket clients watching this batch."""
    conns = list(_ws_connections.get(batch_id, []))
    dead: list = []
    payload = update.model_dump()
    for ws in conns:
        try:
            await ws.send_json(payload)
        except Exception:
            dead.append(ws)
    for ws in dead:
        unregister_ws(batch_id, ws)
