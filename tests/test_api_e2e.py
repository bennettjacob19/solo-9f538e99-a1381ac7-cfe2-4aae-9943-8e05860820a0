"""End-to-end HTTP tests against a real PostgreSQL (embedded pgserver or
DATABASE_URL). Covers same-name rebinding/session isolation, prepare limits,
cross-batch cache, PIDs, results, error stop/reconcile and the batch lock."""
from __future__ import annotations

import uuid

import pytest


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def put_binding(client, session, name, *, template=None, sql=None, description=None):
    body = {}
    if template:
        body["template"] = template
    if sql:
        body["sql"] = sql
    if description:
        body["description"] = description
    return client.put(f"/api/sessions/{session}/bindings/{name}", json=body)


def qitem(session, name, params, *, arrival=0, duration=1, deadline=10, **extra):
    item = {
        "session": session,
        "name": name,
        "params": params,
        "arrival_tick": arrival,
        "duration_ticks": duration,
        "deadline_tick": deadline,
    }
    item.update(extra)
    return item


def batch_body(items, max_prepares=8, **kw):
    body = {"queries": items, "max_prepares": max_prepares}
    body.update(kw)
    return body


def cache_names(client):
    state = client.get("/api/cache").json()
    return [
        sorted(s["statement_name"] for s in conn["statements"])
        for conn in state["connections"]
    ]


# ---------------------------------------------------------------------------
# meta / seed
# ---------------------------------------------------------------------------
def test_docs_openapi_and_health(client):
    assert client.get("/docs").status_code == 200
    spec = client.get("/openapi.json")
    assert spec.status_code == 200
    assert "/api/batches" in spec.json()["paths"]
    health = client.get("/health").json()
    assert health["status"] == "ok"
    assert len(health["connections"]) == 2
    assert all(c["backend_pid"] > 0 for c in health["connections"])


def test_seed_deployment_records_filterable(client):
    all_resp = client.get("/api/deployments?limit=500").json()
    assert all_resp["count"] >= 30

    svc = client.get("/api/deployments", params={"service": "api-gateway"}).json()
    assert svc["count"] >= 1
    assert all(r["service"] == "api-gateway" for r in svc["records"])

    ver = client.get("/api/deployments", params={"version": "v2.0.1"}).json()
    assert ver["count"] >= 1
    assert all(r["version"] == "v2.0.1" for r in ver["records"])

    day = client.get("/api/deployments", params={"date": "2026-11-10"}).json()
    assert day["count"] == 4
    assert all(r["deployed_at"].startswith("2026-11-10") for r in day["records"])


def test_templates_catalog(client):
    data = client.get("/api/templates").json()
    ids = {t["id"] for t in data["templates"]}
    assert {
        "deployments_by_service",
        "deployments_by_version",
        "deployments_by_date",
    } <= ids


# ---------------------------------------------------------------------------
# bindings: isolation + rebind
# ---------------------------------------------------------------------------
def test_same_name_is_independent_across_sessions(client):
    r1 = put_binding(client, "alice", "rpt", template="deployments_by_service")
    assert r1.status_code == 200 and r1.json()["rebound"] is False
    r2 = put_binding(client, "bob", "rpt", template="deployments_by_version")
    assert r2.status_code == 200

    a = client.get("/api/sessions/alice/bindings/rpt").json()
    b = client.get("/api/sessions/bob/bindings/rpt").json()
    assert "service = $1" in a["sql"]
    assert "version = $1" in b["sql"]
    assert a["key"] != b["key"]

    sessions = {s["session"]: s for s in client.get("/api/sessions").json()["sessions"]}
    assert "rpt" in sessions["alice"]["query_names"]
    assert "rpt" in sessions["bob"]["query_names"]


def test_rebind_increments_version_and_reports_previous_sql(client):
    put_binding(client, "alice", "rpt", template="deployments_by_service")
    again = put_binding(client, "alice", "rpt", template="deployments_by_date").json()
    assert again["rebound"] is True
    assert again["previous_version"] == 1
    assert "service = $1" in again["previous_sql"]
    current = client.get("/api/sessions/alice/bindings/rpt").json()
    assert current["version"] == 2
    assert "deployed_at::date = $1" in current["sql"]


def test_named_and_positional_custom_sql_styles(client):
    put_binding(
        client,
        "carol",
        "cnt",
        sql="SELECT count(*) AS n FROM deployments WHERE service = :service AND status = :status",
    )
    resp = client.post(
        "/api/batches",
        json=batch_body([qitem("carol", "cnt", {"service": "api-gateway", "status": "success"})], 1),
    )
    assert resp.status_code == 200, resp.text
    row = resp.json()["queries"][0]["records"][0]
    assert row["n"] >= 1

    put_binding(
        client,
        "dave",
        "lst",
        sql="SELECT id FROM deployments WHERE version = $1 ORDER BY id LIMIT 2",
    )
    resp = client.post(
        "/api/batches",
        json=batch_body([qitem("dave", "lst", ["v2.0.1"])], 1),
    )
    assert resp.status_code == 200, resp.text
    q0 = resp.json()["queries"][0]
    assert q0["row_count"] == 2


def test_missing_binding_returns_404(client):
    resp = client.post(
        "/api/batches",
        json=batch_body([qitem("nobody", "ghost", [])], 1),
    )
    assert resp.status_code == 404
    assert resp.json()["detail"]["code"] == "binding_not_found"


# ---------------------------------------------------------------------------
# real fetch + PIDs + ticks
# ---------------------------------------------------------------------------
def test_batch_executes_for_real_with_pid_and_ticks(client):
    put_binding(client, "alice", "svc", template="deployments_by_service")
    resp = client.post(
        "/api/batches",
        json=batch_body(
            [qitem("alice", "svc", {"service": "api-gateway"}, arrival=0, duration=2, deadline=2)],
            1,
        ),
    )
    assert resp.status_code == 200, resp.text
    rec = resp.json()
    assert rec["status"] == "completed"
    assert rec["actual_prepares"] == 1
    q0 = rec["queries"][0]
    assert q0["status"] == "success"
    assert isinstance(q0["backend_pid"], int) and q0["backend_pid"] > 0
    assert q0["start_tick"] == 0 and q0["end_tick"] == 2
    assert q0["completion_tick"] == 2 and q0["last_busy_tick"] == 1
    assert q0["prepare_count"] == 1 and q0["cache_hit"] is False
    assert q0["row_count"] >= 1
    assert all(r["service"] == "api-gateway" for r in q0["records"])
    # PID must match the connection slot's live backend.
    conn_pids = {c["backend_pid"] for c in client.get("/api/connections").json()["connections"]}
    assert q0["backend_pid"] in conn_pids


def test_two_tight_parallel_queries_use_two_connections_and_pids(client):
    put_binding(client, "alice", "svc", template="deployments_by_service")
    put_binding(client, "bob", "ver", template="deployments_by_version")
    resp = client.post(
        "/api/batches",
        json=batch_body(
            [
                qitem("alice", "svc", {"service": "api-gateway"}, duration=2, deadline=2),
                qitem("bob", "ver", {"version": "v2.0.1"}, duration=2, deadline=2),
            ],
            2,
        ),
    )
    assert resp.status_code == 200, resp.text
    qs = resp.json()["queries"]
    assert {q["connection"] for q in qs} == {0, 1}
    assert qs[0]["backend_pid"] != qs[1]["backend_pid"]


# ---------------------------------------------------------------------------
# same-name rebind + acceptance snapshot
# ---------------------------------------------------------------------------
def test_batch_uses_binding_snapshot_at_acceptance(client):
    put_binding(client, "alice", "rpt", template="deployments_by_service")
    b1 = client.post(
        "/api/batches",
        json=batch_body([qitem("alice", "rpt", {"service": "api-gateway"})], 1),
    )
    assert b1.status_code == 200
    id1 = b1.json()["batch_id"]
    assert b1.json()["snapshot"][0]["binding_version_at_acceptance"] == 1
    assert "service = $1" in b1.json()["snapshot"][0]["sql"]

    # Rebind the same session/name to the date template.
    put_binding(client, "alice", "rpt", template="deployments_by_date")

    b2 = client.post(
        "/api/batches",
        json=batch_body([qitem("alice", "rpt", {"date": "2026-11-10"})], 1),
    )
    assert b2.status_code == 200
    rec2 = b2.json()
    assert rec2["snapshot"][0]["binding_version_at_acceptance"] == 2
    assert "deployed_at::date = $1" in rec2["snapshot"][0]["sql"]
    assert rec2["queries"][0]["row_count"] == 4

    # The stored first batch keeps its old snapshot and results.
    old = client.get(f"/api/batches/{id1}").json()
    assert old["snapshot"][0]["binding_version_at_acceptance"] == 1
    assert "service = $1" in old["snapshot"][0]["sql"]


def test_same_name_sessions_in_one_batch_are_independent(client):
    put_binding(client, "alice", "rpt", template="deployments_by_service")
    put_binding(client, "bob", "rpt", template="deployments_by_version")
    resp = client.post(
        "/api/batches",
        json=batch_body(
            [
                qitem("alice", "rpt", {"service": "report-worker"}, duration=2, deadline=4),
                qitem("bob", "rpt", {"version": "v1.8.3"}, duration=2, deadline=4),
            ],
            2,
        ),
    )
    assert resp.status_code == 200, resp.text
    snap = resp.json()["snapshot"]
    assert "service = $1" in snap[0]["sql"]
    assert "version = $1" in snap[1]["sql"]
    assert all(q["row_count"] >= 1 for q in resp.json()["queries"])


# ---------------------------------------------------------------------------
# prepare limit + cache preservation
# ---------------------------------------------------------------------------
def _unique_sql(tag):
    token = uuid.uuid4().hex[:10]
    return (
        "SELECT id, service FROM deployments "
        f"WHERE service = $1 /* {tag}-{token} */"
    )


def test_limit_rejection_leaves_original_cache_untouched(client):
    # Three brand-new SQL keys, cold caches, deadline 2 => minimum 3 PREPARE.
    for i in range(3):
        put_binding(client, "lim", f"q{i}", sql=_unique_sql(f"lim{i}"))

    before = cache_names(client)
    assert before == [[], []]

    rejected = client.post(
        "/api/batches",
        json=batch_body(
            [qitem("lim", f"q{i}", ["api-gateway"], duration=1, deadline=2) for i in range(3)],
            2,
        ),
    )
    assert rejected.status_code == 422
    body = rejected.json()
    assert body["status"] == "rejected"
    assert body["accepted"] is False
    assert body["rejection"]["reason"] == "prepare_budget_exceeded"
    assert body["rejection"]["minimum_prepares_among_deadline_feasible"] == 3

    # No PREPARE may have been issued: cache exactly as before.
    assert cache_names(client) == before


def test_accepted_limit_batch_then_zero_budget_reuses_everything(client):
    for i in range(3):
        put_binding(client, "lim", f"q{i}", sql=_unique_sql(f"warm{i}"))
    items = [qitem("lim", f"q{i}", ["api-gateway"], duration=1, deadline=2) for i in range(3)]

    ok = client.post("/api/batches", json=batch_body(items, 3))
    assert ok.status_code == 200, ok.text
    assert ok.json()["actual_prepares"] == 3

    cache = client.get("/api/cache").json()["connections"]
    assert all(c["prepared_count"] <= 2 for c in cache)
    assert sum(c["prepared_count"] for c in cache) == 3

    # Same batch again with PREPARE budget 0: every statement is warm.
    reuse = client.post("/api/batches", json=batch_body(items, 0))
    assert reuse.status_code == 200, reuse.text
    rec = reuse.json()
    assert rec["actual_prepares"] == 0
    assert all(q["cache_hit"] for q in rec["queries"])


def test_connection_cache_cap_two_lru_across_batches(client):
    # Single-query batches deterministically land on connection slot 0.
    keys = []
    for i in range(3):
        sql = _unique_sql(f"cap{i}")
        put_binding(client, "cap", f"q{i}", sql=sql)
        keys.append(client.get("/api/sessions/cap/bindings/q%i" % i).json()["statement_name"])

    for i in range(2):
        r = client.post("/api/batches", json=batch_body([qitem("cap", f"q{i}", ["api-gateway"])], 1))
        assert r.status_code == 200

    state = client.get("/api/cache").json()["connections"]
    c0 = next(c for c in state if c["connection"] == 0)
    assert c0["prepared_count"] == 2
    assert {s["statement_name"] for s in c0["statements"]} == set(keys[:2])

    # Third distinct statement evicts the LRU (keys[0]).
    client.post("/api/batches", json=batch_body([qitem("cap", "q2", ["api-gateway"])], 1))
    state = client.get("/api/cache").json()["connections"]
    c0 = next(c for c in state if c["connection"] == 0)
    assert c0["prepared_count"] == 2
    names = {s["statement_name"] for s in c0["statements"]}
    assert names == {keys[1], keys[2]}

    # q0 was evicted: running it must really PREPARE again (count 1).
    again = client.post("/api/batches", json=batch_body([qitem("cap", "q0", ["api-gateway"])], 1))
    q0 = again.json()["queries"][0]
    assert q0["prepare_count"] == 1 and q0["cache_hit"] is False


# ---------------------------------------------------------------------------
# execution error: stop batch + reconciliation
# ---------------------------------------------------------------------------
def test_execution_error_stops_batch_and_reconciles(client):
    put_binding(client, "bad", "broken", sql="SELECT * FROM no_such_report_table WHERE x = $1")
    put_binding(client, "good", "g1", template="deployments_by_service")
    put_binding(client, "good", "g2", template="deployments_by_version")

    items = [
        qitem("bad", "broken", [1], duration=1, deadline=5),
        qitem("good", "g1", {"service": "api-gateway"}, duration=1, deadline=5),
        qitem("good", "g2", {"version": "v2.0.1"}, duration=1, deadline=5),
    ]
    resp = client.post("/api/batches", json=batch_body(items, 8))
    assert resp.status_code == 500
    rec = resp.json()
    assert rec["status"] == "error"
    assert rec["error"]["sqlstate"] == "42P01"
    assert rec["error"]["query_name"] == "broken"

    # Exactly one failed query; anything scheduled after it is skipped.
    by_index = {q["query_index"]: q for q in rec["queries"]}
    failed = [i for i, q in by_index.items() if q["status"] == "error"]
    assert failed == [0]

    order = [s["query_index"] for s in rec["plan"]["steps"]]
    fail_pos = order.index(0)
    for later_index in order[fail_pos + 1 :]:
        assert by_index[later_index]["status"] == "skipped"

    # Cache reporting still obeys the <=2 cap and reconciliation endpoint works.
    rec_cache = client.post("/api/cache/reconcile").json()
    assert all(c["prepared_count"] <= 2 for c in rec_cache["connections"])

    # Subsequent good batch runs normally: service survived the error.
    follow = client.post(
        "/api/batches",
        json=batch_body([qitem("good", "g1", {"service": "api-gateway"})], 1),
    )
    assert follow.status_code == 200, follow.text
    assert follow.json()["status"] == "completed"


# ---------------------------------------------------------------------------
# validation / concurrency / aliases / dry-run
# ---------------------------------------------------------------------------
def test_more_than_eight_queries_rejected(client):
    items = [qitem("s", "n", [], duration=1, deadline=20) for _ in range(9)]
    resp = client.post("/api/batches", json=batch_body(items, 9))
    assert resp.status_code == 422


def test_batch_lock_returns_409_when_batch_running(client):
    from app import state

    put_binding(client, "z", "q", template="deployments_by_service")
    assert state.runtime.batch_lock.acquire(blocking=False)
    try:
        resp = client.post(
            "/api/batches",
            json=batch_body([qitem("z", "q", {"service": "api-gateway"})], 1),
        )
        assert resp.status_code == 409
        assert resp.json()["detail"]["code"] == "batch_busy"
    finally:
        state.runtime.batch_lock.release()


def test_dry_run_plan_does_not_touch_cache(client):
    put_binding(client, "p", "q", template="deployments_by_service")
    before = cache_names(client)
    resp = client.post(
        "/api/batches/plan",
        json=batch_body([qitem("p", "q", {"service": "api-gateway"})], 0),
    )
    assert resp.status_code == 200
    rec = resp.json()
    assert rec["status"] == "rejected"  # cold cache + budget 0
    assert cache_names(client) == before

    ok = client.post(
        "/api/batches/plan",
        json=batch_body([qitem("p", "q", {"service": "api-gateway"})], 1),
    )
    assert ok.status_code == 200
    assert ok.json()["status"] == "preview_feasible"
    assert cache_names(client) == before  # preview never prepares


def test_envelope_and_root_aliases(client):
    put_binding(client, "e", "q", template="deployments_by_service")
    payload = batch_body([qitem("e", "q", {"service": "api-gateway"})], 1)

    env = client.post("/api/batches/submit", json=payload)
    assert env.status_code == 200
    body = env.json()
    assert body["ok"] is True and body["status"] == "completed"
    assert body["result"]["queries"][0]["backend_pid"] > 0

    root = client.post("/batches", json=payload)
    assert root.status_code == 200

    assert client.get("/batches").status_code == 200
    assert client.get("/api/batches").status_code == 200
