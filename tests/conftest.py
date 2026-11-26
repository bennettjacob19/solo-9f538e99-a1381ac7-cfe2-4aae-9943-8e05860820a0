"""Pytest bootstrap.

If DATABASE_URL points at a live PostgreSQL we use it; otherwise we boot an
embedded PostgreSQL via the pgserver wheel (real postgres binaries) so the
whole suite exercises real PREPARE/EXECUTE and pg_prepared_statements.
"""
from __future__ import annotations

import os
import tempfile

import pytest

if not os.environ.get("DATABASE_URL"):
    import pgserver

    _PG_DIR = tempfile.mkdtemp(prefix="pg_two_conn_pytest_")
    _server = pgserver.get_server(_PG_DIR)
    os.environ["DATABASE_URL"] = _server.get_uri()
    os.environ.setdefault("DB_CONNECT_TIMEOUT", "30")
    print(f"\n[conftest] embedded PostgreSQL at {_server.get_uri()}")

from fastapi.testclient import TestClient  # noqa: E402

from app import state  # noqa: E402
from app.bindings import BindingStore  # noqa: E402
from app.main import app  # noqa: E402


@pytest.fixture(scope="session")
def client():
    with TestClient(app) as c:
        yield c


@pytest.fixture(autouse=True)
def _clean_state(client):
    # Fresh binding namespace per test.
    state.bindings = BindingStore()
    # Empty batch history.
    with state.runtime.store._lock:
        state.runtime.store._items.clear()
    # Empty both physical prepared-statement caches for cache-isolation tests.
    for slot in state.runtime.slots:
        with slot.lock:
            try:
                slot.ensure_conn_locked()
                if slot.conn is not None:
                    slot.conn.execute("DEALLOCATE ALL")
            except Exception:
                try:
                    slot.reconcile()
                except Exception:
                    pass
            slot.statements.clear()
    # Defensive: a test must never leave the global batch lock held.
    if state.runtime.batch_lock.locked():
        try:
            state.runtime.batch_lock.release()
        except RuntimeError:
            pass
    yield
