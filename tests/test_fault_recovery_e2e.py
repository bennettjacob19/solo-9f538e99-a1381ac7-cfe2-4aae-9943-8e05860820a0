"""End-to-end failure-recovery tests against a real PostgreSQL.

Covers the mid-batch disconnect contract:

* one confirmed reconnect, then exact resume planning of the un-executed tail;
* fetched results / backend PIDs / start-end ticks are never re-run;
* original arrival/deadline ticks, original planned-start lower bounds and the
  two slots' occupied windows keep holding;
* already-issued PREPAREs count against the original batch limit (no
  over-issuance on a reconnected cold backend);
* no-solution / reconnect-failed / second-disconnect stop the batch and list
  the un-executed queries while keeping the surviving cache;
* side endpoints borrow only idle healthy slots, never reconnect, and report
  busy / temporarily-unverifiable during the fault.

Deterministic scenario used throughout (two distinct built-in SQL keys)::

    q0 svc  duration 2 deadline 2  -> conn X, ticks [0, 2)
    q1 ver  duration 2 deadline 2  -> conn Y, ticks [0, 2)
    q2 ver  duration 1 deadline 3  -> conn Y, ticks [2, 3)
    q3 svc  duration 1 deadline 3  -> conn X, ticks [2, 3)
    q4 svc  duration 1 deadline 4  -> conn X, ticks [3, 4)   (fault injected here)

The fault therefore happens *after* q0..q3 have really completed (both SQL
keys cached) and only q4 is un-executed; a warm survivor can absorb it with
zero extra PREPARE while the cold reconnected slot would need one.
"""
from __future__ import annotations

import psycopg
import pytest

from app import state
from app.runtime import STATE_FAULTED, STATE_RECONNECTED


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------
def put_binding(client, session, name, *, template=None, sql=None):
    body = {}
    if template:
        body["template"] = template
    if sql:
        body["sql"] = sql
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


def find_step(plan, query_index):
    return next(s for s in plan["steps"] if s["query_index"] == query_index)


def find_query(rec, query_index):
    return next(q for q in rec["queries"] if q["query_index"] == query_index)


@pytest.fixture
def two_bindings(client):
    put_binding(client, "rec", "svc", template="deployments_by_service")
    put_binding(client, "rec", "ver", template="deployments_by_version")
    return "svc", "ver"


@pytest.fixture
def four_items(two_bindings):
    svc, ver = two_bindings
    return [
        qitem("rec", svc, {"service": "api-gateway"}, duration=2, deadline=2),  # 0
        qitem("rec", ver, {"version": "v2.0.1"}, duration=2, deadline=2),       # 1
        qitem("rec", ver, {"version": "v2.0.1"}, duration=1, deadline=3),       # 2
        qitem("rec", svc, {"service": "api-gateway"}, duration=1, deadline=3),  # 3
        qitem("rec", svc, {"service": "api-gateway"}, duration=1, deadline=4),  # 4 fault
    ]


# The fault is always injected at the final query of the standard scenario.
FAULT_INDEX = 4


def plan_of(client, items, budget):
    return client.post("/api/batches/plan", json=batch_body(items, budget)).json()["plan"]


# ---------------------------------------------------------------------------
# 1. happy-path recovery: reconnect + resume completes the batch
# ---------------------------------------------------------------------------
def test_disconnect_reconnects_once_and_resumes_tail(client, four_items):
    runtime = state.runtime
    plan = plan_of(client, four_items, 4)
    assert plan["feasible"]
    # First two land on different connections at tick 0.
    assert {find_step(plan, 0)["connection"], find_step(plan, 1)["connection"]} == {0, 1}
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    fault_slot = runtime.slots[fault_conn]
    old_pid = fault_slot.pid
    fault_slot.inject_disconnect_at = FAULT_INDEX

    resp = client.post("/api/batches", json=batch_body(four_items, 4))
    assert resp.status_code == 200, resp.text
    rec = resp.json()

    assert rec["status"] == "completed"
    recovery = rec["recovery"]
    assert recovery["reconnected"] is True
    assert recovery["faulted_connection"] == fault_conn
    assert recovery["reconnect_event"]["previous_backend_pid"] == old_pid
    new_pid = recovery["reconnect_event"]["new_backend_pid"]
    assert isinstance(new_pid, int) and new_pid != old_pid

    # Queries 0..3 completed before the fault: never re-run, keep PID/ticks.
    for i in range(FAULT_INDEX):
        qi = find_query(rec, i)
        assert qi["phase"] == "initial" and qi["status"] == "success"
        original = find_step(rec["plan"], i)
        assert qi["start_tick"] == original["start_tick"]
        assert qi["end_tick"] == original["end_tick"]
        assert isinstance(qi["backend_pid"], int)

    # Only the interrupted q4 runs in the resume phase.
    q4 = find_query(rec, FAULT_INDEX)
    assert q4["phase"] == "resumed" and q4["status"] == "success"
    original = find_step(rec["plan"], FAULT_INDEX)
    assert q4["start_tick"] >= original["start_tick"]
    assert q4["end_tick"] <= original["deadline_tick"]

    # Two distinct SQL keys => two real PREPAREs total; the resumed svc hit is
    # served warm by the survivor (or re-prepared once), never over-issued.
    assert rec["actual_prepares"] <= 3
    assert rec["actual_prepares"] >= 2

    # Faulted slot is now a usable reconnected backend with the fresh PID.
    assert fault_slot.state == STATE_RECONNECTED
    assert fault_slot.healthy is True
    cache = client.get("/api/cache").json()["connections"]
    fault_desc = next(c for c in cache if c["connection"] == fault_conn)
    assert fault_desc["fault_count"] == 1
    assert all(c["prepared_count"] <= 2 for c in cache)


def test_resume_can_reassign_tail_to_surviving_slot_with_zero_extra_prepare(
    client, four_items
):
    runtime = state.runtime
    # Pre-warm the svc statement on BOTH backends with a parallel warmup batch
    # (two identical svc queries => one PREPARE per connection).
    warm = [
        qitem("rec", "svc", {"service": "api-gateway"}, duration=1, deadline=1),
        qitem("rec", "svc", {"service": "api-gateway"}, duration=1, deadline=1),
    ]
    warmup = client.post("/api/batches", json=batch_body(warm, 2))
    assert warmup.status_code == 200
    assert all(
        c["prepared_count"] >= 1
        for c in client.get("/api/cache").json()["connections"]
    )

    plan = plan_of(client, four_items, 4)
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    survivor_conn = 1 - fault_conn
    runtime.slots[fault_conn].inject_disconnect_at = FAULT_INDEX

    rec = client.post("/api/batches", json=batch_body(four_items, 4)).json()
    assert rec["status"] == "completed"
    q4 = find_query(rec, FAULT_INDEX)
    assert q4["end_tick"] <= 4
    # The survivor still has the svc statement warm, so the optimal resume
    # reassigns the tail there and spends no additional PREPARE at all (the
    # cold reconnected slot would have cost one — the planner avoids it).
    assert q4["prepare_count"] == 0 and q4["cache_hit"] is True
    assert q4["connection"] == survivor_conn
    cache = rec["cache_after"]
    survivor = next(c for c in cache if c["connection"] == survivor_conn)
    assert survivor["healthy"] is True and survivor["backend_pid"]


# ---------------------------------------------------------------------------
# 2. no feasible resume: budget exhausted
# ---------------------------------------------------------------------------
def test_resume_budget_exhausted_lists_unexecuted_and_keeps_survivor_cache(
    client, two_bindings
):
    runtime = state.runtime
    svc, ver = two_bindings
    # A third, distinct SQL key that ends up cached only on one connection.
    put_binding(
        client,
        "rec",
        "uniq",
        sql="SELECT id FROM deployments WHERE service = $1 /* resume-budget-drill */",
    )
    items = [
        qitem("rec", svc, {"service": "api-gateway"}, duration=2, deadline=2),  # 0
        qitem("rec", ver, {"version": "v2.0.1"}, duration=2, deadline=2),       # 1
        qitem("rec", ver, {"version": "v2.0.1"}, duration=1, deadline=3),       # 2
        qitem("rec", svc, {"service": "api-gateway"}, duration=1, deadline=3),  # 3
        qitem("rec", "uniq", ["api-gateway"], duration=1, deadline=4),          # 4 fault
    ]
    plan = plan_of(client, items, 3)
    fault_conn = find_step(plan, 4)["connection"]
    survivor_conn = 1 - fault_conn
    survivor_slot = runtime.slots[survivor_conn]
    # Budget 3: all three distinct PREPAREs are spent before the fault at q4.
    runtime.slots[fault_conn].inject_disconnect_at = 4

    resp = client.post("/api/batches", json=batch_body(items, 3))
    assert resp.status_code == 500
    rec = resp.json()
    assert rec["status"] == "error"
    assert rec["error_reason"] == "resume_infeasible"
    assert rec["resume_plan"]["feasible"] is False
    assert rec["resume_plan"]["reason"] == "resume_prepare_budget_exceeded"

    # q0..q3 completed; only the faulting q4 is un-executed.
    for i in range(4):
        assert find_query(rec, i)["status"] == "success"
    unexecuted = rec["error"]["unexecuted_queries"]
    assert [u["query_index"] for u in unexecuted] == [4]

    # Surviving slot cache preserved with its real PID and real statements.
    cache = rec["cache_after"]
    survivor_desc = next(c for c in cache if c["connection"] == survivor_conn)
    assert survivor_desc["healthy"] is True
    assert survivor_desc["prepared_count"] >= 1
    assert survivor_desc["backend_pid"] == survivor_slot.pid


# ---------------------------------------------------------------------------
# 3. no feasible resume: reconnect fails and the single survivor cannot fit
#    the tail before its deadline. (Pure deadline-infeasibility after a
#    *successful* reconnect is covered at the scheduler level in
#    test_planner.py::test_resume_deadline_impossible_after_fault, since a
#    reconnected slot can always reproduce the original feasible assignment.)
# ---------------------------------------------------------------------------
def test_resume_deadline_infeasible_when_only_survivor_remains(
    client, monkeypatch, two_bindings
):
    runtime = state.runtime
    svc, ver = two_bindings
    # Two parallel duration-2 queries occupy both slots for ticks [0,2); the
    # faulting slot's query also has a follow-up that must finish by tick 3.
    items = [
        qitem("rec", svc, {"service": "api-gateway"}, duration=2, deadline=2),  # 0
        qitem("rec", ver, {"version": "v2.0.1"}, duration=2, deadline=2),       # 1
        qitem("rec", ver, {"version": "v2.0.1"}, duration=1, deadline=3),       # 2
        qitem("rec", svc, {"service": "api-gateway"}, duration=1, deadline=3),  # 3 fault
    ]
    plan = plan_of(client, items, 4)
    fault_conn = find_step(plan, 3)["connection"]
    survivor_conn = 1 - fault_conn
    survivor_slot = runtime.slots[survivor_conn]
    runtime.slots[fault_conn].inject_disconnect_at = 3

    def boom(self, previous_pid=None):
        raise psycopg.OperationalError("reconnect disabled during test")

    monkeypatch.setattr(
        "app.runtime.ConnectionSlot.attempt_recovery_reconnect_locked", boom
    )

    resp = client.post("/api/batches", json=batch_body(items, 4))
    rec = resp.json()
    assert resp.status_code == 500
    assert rec["status"] == "error"
    # The confirmed reconnect failed; the batch stops without re-running work.
    assert rec["error_reason"] == "reconnect_failed"
    assert rec["recovery"]["reconnected"] is False
    # The two tick-0 queries completed; the faulting step is listed.
    for i in (0, 1):
        assert find_query(rec, i)["status"] == "success"
    assert rec["error"]["unexecuted_count"] >= 1
    # The survivor's real backend and cache are retained untouched.
    cache = rec["cache_after"]
    survivor_desc = next(c for c in cache if c["connection"] == survivor_conn)
    assert survivor_desc["healthy"] is True
    assert survivor_desc["backend_pid"] == survivor_slot.pid
    fault_desc = next(c for c in cache if c["connection"] == fault_conn)
    assert fault_desc["healthy"] is False and fault_desc["note"]


# ---------------------------------------------------------------------------
# 4. the single confirmed reconnect itself fails
# ---------------------------------------------------------------------------
def test_reconnect_failure_aborts_and_keeps_surviving_cache(
    client, monkeypatch, four_items
):
    runtime = state.runtime
    plan = plan_of(client, four_items, 4)
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    survivor_conn = 1 - fault_conn
    survivor_slot = runtime.slots[survivor_conn]
    fault_slot = runtime.slots[fault_conn]
    fault_slot.inject_disconnect_at = FAULT_INDEX

    def boom(self, previous_pid=None):
        raise psycopg.OperationalError("could not connect to server (simulated)")

    monkeypatch.setattr(
        "app.runtime.ConnectionSlot.attempt_recovery_reconnect_locked", boom
    )

    resp = client.post("/api/batches", json=batch_body(four_items, 4))
    assert resp.status_code == 500
    rec = resp.json()
    assert rec["error_reason"] == "reconnect_failed"
    assert rec["recovery"]["attempted_reconnect"] is True
    assert rec["recovery"]["reconnected"] is False
    assert fault_slot.state == STATE_FAULTED
    assert [u["query_index"] for u in rec["error"]["unexecuted_queries"]] == [FAULT_INDEX]

    cache = rec["cache_after"]
    survivor_desc = next(c for c in cache if c["connection"] == survivor_conn)
    assert survivor_desc["healthy"] is True
    assert survivor_desc["prepared_count"] >= 1
    assert survivor_desc["backend_pid"] == survivor_slot.pid


# ---------------------------------------------------------------------------
# 5. a second disconnect during resume aborts immediately (no 2nd reconnect)
# ---------------------------------------------------------------------------
def test_second_disconnect_during_resume_aborts_batch(client, monkeypatch, four_items):
    runtime = state.runtime
    plan = plan_of(client, four_items, 4)
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    fault_slot = runtime.slots[fault_conn]
    fault_slot.inject_disconnect_at = FAULT_INDEX

    # After the single recovery reconnect, make the next run_step of *any*
    # slot fail with a disconnect: the resume plan is free to reassign the tail
    # to the survivor, and a second fault must still abort without reconnecting.
    import app.runtime as rt

    original_run = rt.ConnectionSlot.run_step
    armed = {"after_first_reconnect": False}
    original_reconnect = fault_slot.attempt_recovery_reconnect_locked

    def reconnect_then_arm(previous_pid=None):
        event = original_reconnect(previous_pid=previous_pid)
        armed["after_first_reconnect"] = True
        return event

    def run_then_maybe_fault(self, step, rq):
        if armed["after_first_reconnect"]:
            armed["after_first_reconnect"] = False
            raise psycopg.OperationalError(
                "server closed the connection unexpectedly (second fault)"
            )
        return original_run(self, step, rq)

    monkeypatch.setattr(fault_slot, "attempt_recovery_reconnect_locked", reconnect_then_arm)
    monkeypatch.setattr(rt.ConnectionSlot, "run_step", run_then_maybe_fault)

    resp = client.post("/api/batches", json=batch_body(four_items, 4))
    assert resp.status_code == 500
    rec = resp.json()
    assert rec["error_reason"] == "second_disconnect_aborts"
    assert rec["recovery"]["reconnected"] is True  # only one reconnect happened
    # One of the two slots now carries the second fault.
    assert sum(s.fault_count for s in runtime.slots) >= 2
    # Earlier successes retained; tail reported as not executed.
    for i in range(FAULT_INDEX):
        assert find_query(rec, i)["status"] == "success"
    assert rec["error"]["unexecuted_count"] >= 1


# ---------------------------------------------------------------------------
# 6. a REAL backend kill (pg_terminate_backend), not just an injected error
# ---------------------------------------------------------------------------
def test_real_postgres_backend_kill_is_detected_and_recovered(client, four_items):
    runtime = state.runtime
    plan = plan_of(client, four_items, 4)
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    fault_slot = runtime.slots[fault_conn]
    victim_pid = fault_slot.pid
    dsn = runtime.dsn
    original_run = type(fault_slot).run_step

    def killing_run(self, step, rq):
        if self is fault_slot and rq.index == FAULT_INDEX:
            with psycopg.connect(dsn, autocommit=True) as admin:
                admin.execute("SELECT pg_terminate_backend(%s)", [victim_pid])
        return original_run(self, step, rq)

    import app.runtime as rt

    rt.ConnectionSlot.run_step = killing_run  # type: ignore[assignment]
    try:
        resp = client.post("/api/batches", json=batch_body(four_items, 4))
    finally:
        rt.ConnectionSlot.run_step = original_run  # type: ignore[assignment]

    assert resp.status_code == 200, resp.text
    rec = resp.json()
    assert rec["status"] == "completed"
    assert rec["recovery"]["reconnect_event"]["previous_backend_pid"] == victim_pid
    assert fault_slot.pid != victim_pid
    assert all(q["status"] == "success" for q in rec["queries"])
    assert rec["actual_prepares"] <= 4


# ---------------------------------------------------------------------------
# 7. side endpoints during a fault: no reconnect, healthy-slot borrow
# ---------------------------------------------------------------------------
def test_fault_injection_switchboard_arms_and_clears_over_http(client):
    runtime = state.runtime
    resp = client.post("/api/testing/faults/disconnect?connection=0&query_index=3")
    assert resp.status_code == 200
    body = resp.json()
    assert body["armed"] is True and body["disconnect_before_query_index"] == 3
    assert runtime.slots[0].inject_disconnect_at == 3

    cleared = client.delete("/api/testing/faults").json()
    assert {"connection": 0, "query_index": 3} in cleared["cleared"]
    assert all(s.inject_disconnect_at is None for s in runtime.slots)


def test_fault_injection_endpoint_hidden_when_disabled(client, monkeypatch):
    # Simulate the production/Docker setting: the switchboard is unavailable.
    import dataclasses

    monkeypatch.setattr(
        state,
        "settings",
        dataclasses.replace(state.settings, enable_fault_injection=False),
    )
    resp = client.post("/api/testing/faults/disconnect?connection=0&query_index=1")
    assert resp.status_code == 404
    assert resp.json()["detail"]["code"] == "fault_injection_disabled"
    assert all(s.inject_disconnect_at is None for s in state.runtime.slots)


def test_deployments_and_reconcile_during_fault_do_not_reconnect(
    client, monkeypatch, four_items
):
    runtime = state.runtime
    plan = plan_of(client, four_items, 4)
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    survivor_conn = 1 - fault_conn
    fault_slot = runtime.slots[fault_conn]
    survivor_slot = runtime.slots[survivor_conn]
    fault_slot.inject_disconnect_at = FAULT_INDEX

    def boom(self, previous_pid=None):
        raise psycopg.OperationalError("reconnect disabled during test")

    monkeypatch.setattr(
        "app.runtime.ConnectionSlot.attempt_recovery_reconnect_locked", boom
    )

    resp = client.post("/api/batches", json=batch_body(four_items, 4))
    assert resp.status_code == 500
    assert fault_slot.state == STATE_FAULTED

    # /deployments still answers via the idle healthy survivor, no reconnect.
    dep = client.get("/api/deployments", params={"service": "api-gateway"})
    assert dep.status_code == 200
    assert dep.json()["count"] >= 1

    # reconcile: real statements on healthy slot; faulted slot unverifiable.
    rec = client.post("/api/cache/reconcile").json()
    assert rec["reconnect_attempted"] is False
    by_conn = {c["connection"]: c for c in rec["connections"]}
    assert by_conn[survivor_conn]["healthy"] is True
    assert by_conn[survivor_conn]["reconciled"] is True
    fault_desc = by_conn[fault_conn]
    assert fault_desc["healthy"] is False
    assert fault_desc["note"]
    assert fault_slot.state == STATE_FAULTED  # endpoint did not heal it
    assert fault_slot.slot_name in rec["unverifiable_slots"]
    assert by_conn[survivor_conn]["backend_pid"] == survivor_slot.pid


def test_deployments_returns_503_slots_busy_when_all_slots_held(client):
    runtime = state.runtime
    acquired = []
    for slot in runtime.slots:
        assert slot.lock.acquire(blocking=False)
        acquired.append(slot)
    try:
        resp = client.get("/api/deployments")
        assert resp.status_code == 503
        assert resp.json()["detail"]["code"] == "slots_busy"
    finally:
        for slot in acquired:
            slot.lock.release()

    assert client.get("/api/deployments?limit=1").status_code == 200


def test_deployments_503_when_no_healthy_slot_after_unrecovered_fault(
    client, monkeypatch, four_items
):
    runtime = state.runtime
    plan = plan_of(client, four_items, 4)
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    other = 1 - fault_conn
    runtime.slots[fault_conn].inject_disconnect_at = FAULT_INDEX

    def boom(self, previous_pid=None):
        raise psycopg.OperationalError("reconnect disabled during test")

    monkeypatch.setattr(
        "app.runtime.ConnectionSlot.attempt_recovery_reconnect_locked", boom
    )
    client.post("/api/batches", json=batch_body(four_items, 4))
    # One slot faulted; occupy the survivor too -> no idle healthy slot.
    survivor = runtime.slots[other]
    assert survivor.lock.acquire(blocking=False)
    try:
        resp = client.get("/api/deployments")
        assert resp.status_code == 503
        assert resp.json()["detail"]["code"] == "slots_busy"
    finally:
        survivor.lock.release()


# ---------------------------------------------------------------------------
# 8. service keeps serving batches after a successful recovery
# ---------------------------------------------------------------------------
def test_service_serves_new_batches_after_recovery(client, four_items, two_bindings):
    runtime = state.runtime
    plan = plan_of(client, four_items, 4)
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    runtime.slots[fault_conn].inject_disconnect_at = FAULT_INDEX

    first = client.post("/api/batches", json=batch_body(four_items, 4))
    assert first.status_code == 200 and first.json()["status"] == "completed"

    svc, _ = two_bindings
    follow_items = [qitem("rec", svc, {"service": "api-gateway"}, duration=1, deadline=2)]
    follow = client.post("/api/batches", json=batch_body(follow_items, 1))
    assert follow.status_code == 200, follow.text
    assert follow.json()["status"] == "completed"

    health = client.get("/health").json()
    assert health["status"] == "ok"
    assert all(c["healthy"] for c in health["connections"])


# ---------------------------------------------------------------------------
# 9. interleaved normal / fault batches keep state coherent
# ---------------------------------------------------------------------------
def test_interleaved_batches_mix_recovery_and_normal_operation(
    client, four_items, two_bindings
):
    runtime = state.runtime
    svc, _ = two_bindings
    single = [qitem("rec", svc, {"service": "api-gateway"}, duration=1, deadline=2)]

    r0 = client.post("/api/batches", json=batch_body(single, 1))
    assert r0.status_code == 200 and r0.json()["actual_prepares"] == 1

    plan = plan_of(client, four_items, 4)
    fault_conn = find_step(plan, FAULT_INDEX)["connection"]
    runtime.slots[fault_conn].inject_disconnect_at = FAULT_INDEX
    fault = client.post("/api/batches", json=batch_body(four_items, 4))
    assert fault.status_code == 200 and fault.json()["status"] == "completed"

    r1 = client.post("/api/batches", json=batch_body(single, 1))
    assert r1.status_code == 200 and r1.json()["status"] == "completed"

    # A second fault episode (the planner may pick either slot; both paths
    # must recover identically).
    plan2 = plan_of(client, four_items, 4)
    target_conn = find_step(plan2, FAULT_INDEX)["connection"]
    runtime.slots[target_conn].inject_disconnect_at = FAULT_INDEX
    fault2 = client.post("/api/batches", json=batch_body(four_items, 4))
    assert fault2.status_code == 200 and fault2.json()["status"] == "completed"

    r2 = client.post("/api/batches", json=batch_body(single, 1))
    assert r2.status_code == 200 and r2.json()["status"] == "completed"

    history = client.get("/api/batches?limit=20").json()["batches"]
    assert len(history) >= 5
    for bid in (r0.json()["batch_id"], fault.json()["batch_id"], r2.json()["batch_id"]):
        detail = client.get(f"/api/batches/{bid}").json()
        for q in detail["queries"]:
            if q.get("status") == "success":
                assert isinstance(q["backend_pid"], int)
                assert "start_tick" in q and "end_tick" in q
                assert q["prepare_count"] in (0, 1)

    cache = client.get("/api/cache").json()["connections"]
    assert all(c["prepared_count"] <= 2 for c in cache)
    total_faults = sum(c["fault_count"] for c in cache)
    assert total_faults >= 2
