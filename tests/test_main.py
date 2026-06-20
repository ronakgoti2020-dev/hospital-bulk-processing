"""
Integration tests for the FastAPI app.

The upstream Hospital Directory API is mocked via pytest monkeypatching so
tests run without network access and remain deterministic.
"""
from __future__ import annotations

import json
import uuid
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient
from httpx import AsyncClient

from app.main import app
from app.models import BatchStatus, InternalBatchState
from app import storage as _storage


# --------------------------------------------------------------------------- #
# Fixtures                                                                      #
# --------------------------------------------------------------------------- #


@pytest.fixture(autouse=True)
def clear_storage():
    """Reset in-memory storage between tests."""
    _storage._batch_store.clear()
    _storage._ws_connections.clear()
    yield
    _storage._batch_store.clear()
    _storage._ws_connections.clear()


@pytest.fixture
def client():
    return TestClient(app)


def _make_csv(*rows: tuple[str, str, str | None]) -> bytes:
    lines = ["name,address,phone"]
    for name, address, phone in rows:
        lines.append(f"{name},{address},{phone or ''}")
    return "\n".join(lines).encode()


def _mock_aiohttp_session(created_ids: list[int], activate_ok: bool = True):
    """
    Build a context-manager-compatible aiohttp.ClientSession mock.
    POST /hospitals/ returns a created hospital; PATCH /activate returns 200.
    """
    _id_iter = iter(created_ids)

    class _MockResp:
        def __init__(self, status, data):
            self.status = status
            self._data = data

        async def json(self):
            return self._data

        async def text(self):
            return json.dumps(self._data)

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

    class _MockSession:
        def post(self, url, **kwargs):
            hospital_id = next(_id_iter)
            body = kwargs.get("json", {})
            return _MockResp(
                200,
                {
                    "id": hospital_id,
                    "name": body.get("name", ""),
                    "address": body.get("address", ""),
                    "phone": body.get("phone"),
                    "creation_batch_id": body.get("creation_batch_id"),
                    "active": False,
                    "created_at": "2025-01-01T00:00:00Z",
                },
            )

        def patch(self, url, **kwargs):
            return _MockResp(200 if activate_ok else 500, {})

        async def __aenter__(self):
            return self

        async def __aexit__(self, *_):
            pass

    return _MockSession()


# --------------------------------------------------------------------------- #
# Health                                                                        #
# --------------------------------------------------------------------------- #


def test_health_check(client):
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json()["status"] == "ok"


# --------------------------------------------------------------------------- #
# POST /hospitals/bulk                                                          #
# --------------------------------------------------------------------------- #


class TestBulkCreate:
    def test_successful_bulk_create(self, client):
        """
        POST returns 202 immediately with status=pending.
        Background processing completes synchronously inside TestClient,
        so we then poll GET to confirm the final completed state.
        """
        csv_data = _make_csv(
            ("General Hospital", "1 Main St", "555-0001"),
            ("City Clinic", "2 Park Ave", "555-0002"),
            ("Metro Health", "3 Oak Rd", None),
        )
        mock_session = _mock_aiohttp_session([101, 102, 103])

        with patch("app.services.aiohttp.ClientSession", return_value=mock_session):
            resp = client.post(
                "/hospitals/bulk",
                files={"file": ("hospitals.csv", csv_data, "text/csv")},
            )

        assert resp.status_code == 202
        data = resp.json()
        assert data["batch_id"] is not None
        assert data["total_hospitals"] == 3

        # Poll for final result (background task runs synchronously in TestClient)
        poll = client.get(f"/hospitals/bulk/{data['batch_id']}")
        assert poll.status_code == 200
        result = poll.json()
        assert result["processed_hospitals"] == 3
        assert result["failed_hospitals"] == 0
        assert result["batch_activated"] is True
        assert result["status"] == BatchStatus.COMPLETED
        assert len(result["hospitals"]) == 3
        assert result["hospitals"][0]["status"] == "created_and_activated"
        assert result["processing_time_seconds"] >= 0

    def test_empty_file_returns_400(self, client):
        resp = client.post(
            "/hospitals/bulk",
            files={"file": ("empty.csv", b"", "text/csv")},
        )
        assert resp.status_code == 400

    def test_missing_required_column_returns_400(self, client):
        csv_data = b"name,phone\nHospital X,555"
        resp = client.post(
            "/hospitals/bulk",
            files={"file": ("bad.csv", csv_data, "text/csv")},
        )
        assert resp.status_code == 400

    def test_exceeds_max_rows_returns_400(self, client):
        rows = [("H" + str(i), "Addr " + str(i), None) for i in range(21)]
        csv_data = _make_csv(*rows)
        resp = client.post(
            "/hospitals/bulk",
            files={"file": ("big.csv", csv_data, "text/csv")},
        )
        assert resp.status_code == 400
        assert "maximum" in resp.json()["detail"].lower()

    def test_invalid_rows_return_422(self, client):
        csv_data = b"name,address\n,Missing Name\n"
        resp = client.post(
            "/hospitals/bulk",
            files={"file": ("bad.csv", csv_data, "text/csv")},
        )
        assert resp.status_code == 422

    def test_partial_failure_reflected_in_response(self, client):
        csv_data = _make_csv(
            ("Good Hospital", "1 Main St", None),
            ("Bad Hospital", "2 Main St", None),
        )

        call_count = 0

        class _PartialSession:
            def post(self, url, **kwargs):
                nonlocal call_count
                call_count += 1
                if call_count == 1:
                    return _mock_aiohttp_session([201]).post(url, **kwargs)

                class _FailResp:
                    status = 500

                    async def text(self):
                        return "Internal Server Error"

                    async def __aenter__(self):
                        return self

                    async def __aexit__(self, *_):
                        pass

                return _FailResp()

            def patch(self, url, **kwargs):
                return _mock_aiohttp_session([], activate_ok=False).patch(url, **kwargs)

            async def __aenter__(self):
                return self

            async def __aexit__(self, *_):
                pass

        with patch("app.services.aiohttp.ClientSession", return_value=_PartialSession()):
            resp = client.post(
                "/hospitals/bulk",
                files={"file": ("mixed.csv", csv_data, "text/csv")},
            )

        assert resp.status_code == 202
        batch_id = resp.json()["batch_id"]

        poll = client.get(f"/hospitals/bulk/{batch_id}")
        data = poll.json()
        assert data["failed_hospitals"] == 1
        assert data["processed_hospitals"] == 1
        assert data["status"] == BatchStatus.PARTIAL
        assert data["batch_activated"] is False


# --------------------------------------------------------------------------- #
# GET /hospitals/bulk/{batch_id}                                                #
# --------------------------------------------------------------------------- #


class TestBatchStatus:
    def test_get_existing_batch(self, client):
        batch = InternalBatchState(
            batch_id=str(uuid.uuid4()),
            total_hospitals=2,
            processed_hospitals=2,
            status=BatchStatus.COMPLETED,
        )
        _storage.save_batch(batch)

        resp = client.get(f"/hospitals/bulk/{batch.batch_id}")
        assert resp.status_code == 200
        assert resp.json()["batch_id"] == batch.batch_id

    def test_get_nonexistent_batch_returns_404(self, client):
        resp = client.get(f"/hospitals/bulk/{uuid.uuid4()}")
        assert resp.status_code == 404

    def test_list_all_batches(self, client):
        for _ in range(3):
            b = InternalBatchState(
                batch_id=str(uuid.uuid4()),
                status=BatchStatus.COMPLETED,
            )
            _storage.save_batch(b)
        resp = client.get("/hospitals/bulk")
        assert resp.status_code == 200
        assert len(resp.json()) == 3


# --------------------------------------------------------------------------- #
# POST /hospitals/bulk/validate                                                 #
# --------------------------------------------------------------------------- #


class TestValidateEndpoint:
    def test_valid_csv(self, client):
        csv_data = _make_csv(("Hospital A", "Street 1", None))
        resp = client.post(
            "/hospitals/bulk/validate",
            files={"file": ("test.csv", csv_data, "text/csv")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is True
        assert data["errors"] == []

    def test_invalid_csv_flagged(self, client):
        resp = client.post(
            "/hospitals/bulk/validate",
            files={"file": ("test.csv", b"just,junk\n1,2", "text/csv")},
        )
        assert resp.status_code == 200
        data = resp.json()
        assert data["valid"] is False

    def test_empty_file_returns_400(self, client):
        resp = client.post(
            "/hospitals/bulk/validate",
            files={"file": ("empty.csv", b"", "text/csv")},
        )
        assert resp.status_code == 400


# --------------------------------------------------------------------------- #
# POST /hospitals/bulk/{batch_id}/resume                                        #
# --------------------------------------------------------------------------- #


class TestResumeEndpoint:
    def test_resume_partial_batch(self, client):
        from app.models import HospitalInput, HospitalResult

        batch_id = str(uuid.uuid4())
        batch = InternalBatchState(
            batch_id=batch_id,
            total_hospitals=2,
            processed_hospitals=1,
            failed_hospitals=1,
            status=BatchStatus.PARTIAL,
            hospital_inputs=[
                HospitalInput(row=1, name="Good H", address="1 St"),
                HospitalInput(row=2, name="Bad H", address="2 St"),
            ],
            hospitals=[
                HospitalResult(row=1, hospital_id=10, name="Good H", status="created_and_activated"),
                HospitalResult(row=2, name="Bad H", status="failed", error="HTTP 500"),
            ],
        )
        _storage.save_batch(batch)

        mock_session = _mock_aiohttp_session([20])

        with patch("app.services.aiohttp.ClientSession", return_value=mock_session):
            resp = client.post(f"/hospitals/bulk/{batch_id}/resume")

        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == BatchStatus.COMPLETED
        assert data["failed_hospitals"] == 0

    def test_resume_completed_batch_returns_409(self, client):
        batch = InternalBatchState(
            batch_id=str(uuid.uuid4()),
            status=BatchStatus.COMPLETED,
        )
        _storage.save_batch(batch)
        resp = client.post(f"/hospitals/bulk/{batch.batch_id}/resume")
        assert resp.status_code == 409

    def test_resume_nonexistent_batch_returns_404(self, client):
        resp = client.post(f"/hospitals/bulk/{uuid.uuid4()}/resume")
        assert resp.status_code == 404
