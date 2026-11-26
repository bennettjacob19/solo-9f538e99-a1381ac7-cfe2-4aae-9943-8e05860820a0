#!/usr/bin/env python3
"""Live walkthrough of the two headline scenarios plus error recovery.

Targets a running server (default http://127.0.0.1:8000). Uses only the Python
standard library so it can run anywhere:

    python scripts/demo_http.py
    BASE_URL=http://localhost:8000 python scripts/demo_http.py
"""
from __future__ import annotations

import json
import os
import sys
import time
import urllib.error
import urllib.request
import uuid

BASE = os.environ.get("BASE_URL", "http://127.0.0.1:8000").rstrip("/")
FAILURES: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    mark = "PASS" if condition else "FAIL"
    print(f"  [{mark}] {label}" + (f" -- {detail}" if detail and not condition else ""))
    if not condition:
        FAILURES.append(label + (f" ({detail})" if detail else ""))


def call(method: str, path: str, body=None):
    data = None
    headers = {"Accept": "application/json"}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(BASE + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=60) as resp:
            return resp.status, json.loads(resp.read().decode("utf-8"))
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", errors="replace")
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, {"raw": raw}


def wait_ready() -> None:
    for _ in range(60):
        try:
            status, body = call("GET", "/health")
            if status == 200 and body.get("status") == "ok":
                print(f"server ready at {BASE}")
                return
        except Exception:
            pass
        time.sleep(1)
    print(f"server not ready at {BASE}; start it with: python scripts/serve_local.py")
    sys.exit(2)


def put_binding(session, name, **body):
    return call("PUT", f"/api/sessions/{session}/bindings/{name}", body)


def q(session, name, params, arrival=0, duration=1, deadline=10):
    return {
        "session": session,
        "name": name,
        "params": params,
        "arrival_tick": arrival,
        "duration_ticks": duration,
        "deadline_tick": deadline,
    }


def cache_signature():
    _, body = call("GET", "/api/cache")
    return sorted(
        (conn["connection"], sorted(s["statement_name"] for s in conn["statements"]))
        for conn in body["connections"]
    )


def print_query_rows(record):
    for item in record.get("queries", []):
        if item.get("status") != "success":
            print(
                f"    #{item['query_index']} {item['session']}/{item['query_name']} "
                f"STATUS={item.get('status')} {item.get('error', {}).get('message', '')[:80]}"
            )
            continue
        first = item["records"][0] if item.get("records") else None
        print(
            f"    #{item['query_index']} {item['session']}/{item['query_name']} "
            f"{item['connection_slot']} pid={item['backend_pid']} "
            f"ticks={item['start_tick']}->{item['end_tick']} "
            f"prepare={item['prepare_count']} hit={item['cache_hit']} "
            f"rows={item['row_count']}"
        )
        if first:
            print(f"       first={json.dumps(first, ensure_ascii=False, default=str)[:160]}")


def main() -> int:
    wait_ready()
    token = uuid.uuid4().hex[:8]

    print("\n=== 0. /docs and constraints ===")
    try:
        with urllib.request.urlopen(BASE + "/docs", timeout=10) as resp:
            check("Swagger /docs returns 200", resp.status == 200)
    except Exception as exc:  # noqa: BLE001
        check("Swagger /docs returns 200", False, str(exc))
    _, root = call("GET", "/")
    print("  constraints:", json.dumps(root["constraints"], ensure_ascii=False))

    print("\n=== 1. same query name, independent sessions ===")
    st, a = put_binding("alice", "rpt", template="deployments_by_service")
    check("PUT alice/rpt (service template)", st == 200 and a["rebound"] is False)
    st, b = put_binding("bob", "rpt", template="deployments_by_version")
    check("PUT bob/rpt (version template)", st == 200 and b["rebound"] is False)
    _, got_a = call("GET", "/api/sessions/alice/bindings/rpt")
    _, got_b = call("GET", "/api/sessions/bob/bindings/rpt")
    check(
        "same name maps to different SQL keys",
        got_a["key"] != got_b["key"] and "service = $1" in got_a["sql"],
    )

    print("\n=== 2. first batch really fetches from PostgreSQL ===")
    payload = {
        "max_prepares": 2,
        "queries": [
            q("alice", "rpt", {"service": "api-gateway"}, duration=2, deadline=2),
            q("bob", "rpt", {"version": "v2.0.1"}, duration=2, deadline=2),
        ],
    }
    st, b1 = call("POST", "/api/batches", payload)
    check("batch B1 accepted & completed (200)", st == 200, f"status={st} body={str(b1)[:300]}")
    if st == 200:
        print_query_rows(b1)
        check("two distinct backend PIDs", len({x["backend_pid"] for x in b1["queries"]}) == 2)
        check("B1 actually prepared twice", b1["actual_prepares"] == 2)
        b1_id = b1["batch_id"]
    else:
        b1_id = None

    print("\n=== 3. REBIND same session/name; old batch keeps its snapshot ===")
    st, reb = put_binding("alice", "rpt", template="deployments_by_date")
    check(
        "rebind alice/rpt -> date template bumps v1->v2",
        st == 200 and reb["rebound"] and reb["previous_version"] == 1
        and reb["binding"]["version"] == 2,
    )
    payload2 = {
        "max_prepares": 2,
        "queries": [
            q("alice", "rpt", {"date": "2026-11-10"}, duration=1, deadline=3),
            q("bob", "rpt", {"version": "v2.0.1"}, duration=1, deadline=3),
        ],
    }
    st, b2 = call("POST", "/api/batches", payload2)
    check("batch B2 completed", st == 200, str(b2)[:300])
    if st == 200:
        print_query_rows(b2)
        check("only 1 new PREPARE (bob's version statement reused)", b2["actual_prepares"] == 1)
        alice_q = next(x for x in b2["queries"] if x["session"] == "alice")
        check(
            "alice date rebind returns exactly 4 rows for 2026-11-10",
            alice_q["row_count"] == 4,
            f"rows={alice_q['row_count']}",
        )
        check("bob same-name binding still version SQL and was a cache hit",
              next(x for x in b2["queries"] if x["session"] == "bob")["cache_hit"] is True)
    if b1_id:
        _, old = call("GET", f"/api/batches/{b1_id}")
        check(
            "stored B1 snapshot still v1 service SQL",
            old["snapshot"][0]["binding_version_at_acceptance"] == 1
            and "service = $1" in old["snapshot"][0]["sql"],
        )

    print("\n=== 4. PREPARE-limit batch: reject leaves cache untouched ===")
    for i in range(3):
        st, _ = put_binding(
            "lim", f"q{i}",
            sql=f"SELECT id FROM deployments WHERE service = $1 /* lim-{token}-{i} */",
        )
        check(f"create lim/q{i}", st == 200)
    items = [q("lim", f"q{i}", ["api-gateway"], duration=1, deadline=2) for i in range(3)]
    before = cache_signature()
    st, rejected = call("POST", "/api/batches", {"max_prepares": 2, "queries": items})
    check("budget=2 -> HTTP 422 rejected", st == 422)
    check(
        "diagnostic says deadline-feasible needs 3 PREPARE",
        rejected.get("rejection", {}).get("minimum_prepares_among_deadline_feasible") == 3,
    )
    after_reject = cache_signature()
    check("cache exactly unchanged after no-solution", before == after_reject)
    print(f"    cache before/after rejection: {before}")

    st, accepted = call("POST", "/api/batches", {"max_prepares": 3, "queries": items})
    check("budget=3 -> accepted & completed", st == 200, str(accepted)[:300])
    if st == 200:
        check("actual PREPARE count == 3", accepted["actual_prepares"] == 3)
        print_query_rows(accepted)

    st, reused = call("POST", "/api/batches", {"max_prepares": 0, "queries": items})
    check("repeat same batch with budget=0 -> 200", st == 200)
    if st == 200:
        check("repeat batch needs 0 PREPARE and all hits",
              reused["actual_prepares"] == 0
              and all(x["cache_hit"] for x in reused["queries"]))
        print_query_rows(reused)

    print("\n=== 5. deadline vs cached-connection trade-off ===")
    trade_sql = (
        "SELECT count(*) AS n FROM deployments "
        f"WHERE service = $1 /* trade-{token} */"
    )
    put_binding("trade", "same", sql=trade_sql)
    st, warm = call(
        "POST", "/api/batches",
        {"max_prepares": 1, "queries": [q("trade", "same", ["api-gateway"])]},
    )
    check("warm one prepared statement", st == 200 and warm["actual_prepares"] == 1)
    trade_items = [q("trade", "same", ["api-gateway"], duration=2, deadline=4) for _ in range(3)]
    st, tight0 = call("POST", "/api/batches", {"max_prepares": 0, "queries": trade_items})
    check("budget=0: waiting for the single cached conn misses deadlines -> 422",
          st == 422 and tight0["rejection"]["reason"] == "prepare_budget_exceeded"
          and tight0["rejection"]["minimum_prepares_among_deadline_feasible"] == 1)
    st, tight1 = call("POST", "/api/batches", {"max_prepares": 1, "queries": trade_items})
    check("budget=1: fresh PREPARE on second conn makes deadlines -> 200", st == 200)
    if st == 200:
        print_query_rows(tight1)
        check("exactly one real PREPARE on the other connection",
              tight1["actual_prepares"] == 1
              and [x["prepare_count"] for x in tight1["queries"]].count(1) == 1)

    print("\n=== 6. execution error stops batch and cache reconciles ===")
    put_binding("err", "broken", sql="SELECT * FROM no_such_report_table WHERE x = $1")
    err_items = [
        q("err", "broken", [1], duration=1, deadline=5),
        q("alice", "rpt", {"date": "2026-11-10"}, duration=1, deadline=5),
    ]
    st, erred = call("POST", "/api/batches", {"max_prepares": 4, "queries": err_items})
    check("bad SQL -> HTTP 500 error report", st == 500 and erred["status"] == "error")
    check("PostgreSQL sqlstate 42P01 surfaced",
          erred.get("error", {}).get("sqlstate") == "42P01")
    by_index = {x["query_index"]: x for x in erred.get("queries", [])}
    failed_indices = [i for i, x in by_index.items() if x["status"] == "error"]
    order = [s["query_index"] for s in (erred.get("plan") or {}).get("steps", [])]
    stopped_ok = len(failed_indices) == 1
    if failed_indices and order:
        fail_pos = order.index(failed_indices[0])
        # Every step scheduled AFTER the failing one must be skipped.
        stopped_ok = stopped_ok and all(
            by_index[j]["status"] == "skipped" for j in order[fail_pos + 1 :]
        )
    check("batch stops: one error, later schedule steps skipped "
          "(earlier tick-0 work may already be done)", stopped_ok,
          str([x.get("status") for x in erred.get("queries", [])]))
    st, recon = call("POST", "/api/cache/reconcile")
    check("reconcile endpoint ok and cap respected",
          st == 200 and all(c["prepared_count"] <= 2 for c in recon["connections"]))
    st, follow = call(
        "POST", "/api/batches",
        {"max_prepares": 1, "queries": [q("alice", "rpt", {"date": "2026-11-10"})]},
    )
    check("service still takes a good batch after the error", st == 200)

    print("\n=== 7. batch history ===")
    st, listing = call("GET", "/api/batches?limit=10")
    check("GET /api/batches lists records", st == 200 and len(listing["batches"]) >= 5)
    print(f"  stored batches returned: {len(listing['batches'])}")

    print("\n" + "=" * 60)
    if FAILURES:
        print(f"DEMO FINISHED WITH {len(FAILURES)} FAILED CHECKS:")
        for f in FAILURES:
            print(" -", f)
        return 1
    print("ALL DEMO CHECKS PASSED")
    return 0


if __name__ == "__main__":
    sys.exit(main())
