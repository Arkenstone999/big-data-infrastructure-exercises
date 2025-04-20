import pytest
from fastapi import status
from fastapi.testclient import TestClient
from pytest import approx

from bdi_api.app import app
import bdi_api.s8.exercise as s8mod

class DummyCursor:
    def __init__(self, rows_all=None, rows_one=None):
        # rows_all → list of tuples for fetchall()
        # rows_one → list for successive fetchone() calls
        self.rows_all = rows_all or []
        self.rows_one = list(rows_one or [])
        self.executed = []

    def execute(self, sql, params):
        self.executed.append((sql, params))

    def fetchall(self):
        return self.rows_all

    def fetchone(self):
        # pop from front if available, else None
        if self.rows_one:
            return self.rows_one.pop(0)
        return None

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        pass


class DummyConn:
    def __init__(self, cursor: DummyCursor):
        self._cursor = cursor
        self.closed = False

    def cursor(self):
        return self._cursor

    def close(self):
        self.closed = True


@pytest.fixture
def client():
    return TestClient(app)


def test_list_aircraft_success(monkeypatch, client):
    # Prepare two fake rows
    fake_rows = [
        ("ICAO1", "REG1", "TYPE1", "MFR1", "MODEL1"),
        ("ICAO2", "REG2", "TYPE2", "MFR2", "MODEL2"),
    ]
    dummy_cursor = DummyCursor(rows_all=fake_rows)
    monkeypatch.setattr(
        s8mod, "get_conn", lambda: DummyConn(dummy_cursor)
    )

    resp = client.get("/api/s8/aircraft/?num_results=2&page=1")
    assert resp.status_code == status.HTTP_200_OK
    body = resp.json()
    assert isinstance(body, list) and len(body) == 2

    # verify first item
    assert body[0] == {
        "icao": "ICAO1",
        "registration": "REG1",
        "type": "TYPE1",
        "manufacturer": "MFR1",
        "model": "MODEL1",
        # owner always None
        "owner": None,
    }
    # verify that LIMIT/OFFSET params were passed correctly
    sql, params = dummy_cursor.executed[0]
    assert params == (2, 2)  # num_results=2, offset=page*2=2


def test_list_aircraft_db_error(monkeypatch, client):
    # Make get_conn raise
    monkeypatch.setattr(s8mod, "get_conn", lambda: (_ for _ in ()).throw(Exception("boom")))
    resp = client.get("/api/s8/aircraft/")
    assert resp.status_code == status.HTTP_500_INTERNAL_SERVER_ERROR
    assert "DB error" in resp.json()["detail"]


@pytest.mark.parametrize(
    "query_day, status_code, detail_substr",
    [
        ("not-a-date", status.HTTP_422_UNPROCESSABLE_ENTITY, "`day` must be YYYY‑MM‑DD"),
        ("2025-02-30", status.HTTP_422_UNPROCESSABLE_ENTITY, "`day` must be YYYY‑MM‑DD"),
    ],
)
def test_get_co2_invalid_day(query_day, status_code, detail_substr, client):
    resp = client.get(f"/api/s8/aircraft/ABCD/co2?day={query_day}")
    assert resp.status_code == status_code
    assert detail_substr in resp.json()["detail"]


def test_get_co2_no_data(monkeypatch, client):
    # count=0, no galph row
    dummy_cursor = DummyCursor(rows_one=[(0,), None])
    monkeypatch.setattr(
        s8mod, "get_conn", lambda: DummyConn(dummy_cursor)
    )

    resp = client.get("/api/s8/aircraft/ABCD/co2?day=2025-04-15")
    assert resp.status_code == status.HTTP_200_OK
    data = resp.json()
    assert data["icao"] == "ABCD"
    assert data["hours_flown"] == 0.0
    assert data["co2"] is None


def test_get_co2_with_samples_but_no_rate(monkeypatch, client):
    # 72 samples → hours = (72*5)/3600 = 0.1, but no galph → co2 None
    dummy_cursor = DummyCursor(rows_one=[(72,), None])
    monkeypatch.setattr(
        s8mod, "get_conn", lambda: DummyConn(dummy_cursor)
    )

    resp = client.get("/api/s8/aircraft/WXYZ/co2?day=2025-04-15")
    assert resp.status_code == status.HTTP_200_OK
    data = resp.json()
    assert data["icao"] == "WXYZ"
    assert data["hours_flown"] == approx((72 * 5) / 3600.0)
    assert data["co2"] is None


def test_get_co2_full_path(monkeypatch, client):
    # 720 samples → hours = 1.0, galph=100 → fuel_gal=100, fuel_kg=304, co2≈(304*3.15)/907.185
    dummy_cursor = DummyCursor(rows_one=[(720,), (100,)])
    monkeypatch.setattr(
        s8mod, "get_conn", lambda: DummyConn(dummy_cursor)
    )

    resp = client.get("/api/s8/aircraft/ZZZZ/co2?day=2025-04-15")
    assert resp.status_code == status.HTTP_200_OK
    data = resp.json()
    assert data["icao"] == "ZZZZ"
    # hours
    assert data["hours_flown"] == approx(1.0)
    # expected co2
    expected_co2 = (100 * 1 * 3.04 * 3.15) / 907.185
    assert data["co2"] == approx(expected_co2, rel=1e-3)
