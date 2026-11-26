"""Ad-hoc interleaving self-test (not part of pytest).

Mixes normal batches, disconnect-recovery batches, deployment reads, cache
reconciliation and connection inspection against a real PostgreSQL to surface
any state-machine / locking / PREPARE-accounting inconsistency. Run with:

    python -I scripts/selftest_interleave.py
"""
from __future__ import annotations

import os
import sys
import tempfile

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

if not os.environ.get("DATABASE_URL"):
    import pgserver

    _d = tempfile.mkdtemp(prefix="interleave_")
    os.environ["DATABASE_URL"] = pgserver.get_server(_d).get_uri()
os.environ.setdefault("ENABLE_FAULT_INJECTION", "1")

from fastapi.testclient import TestClient  # noqa: E402

from app.main import app  # noqa: E402
from app import state  # noqa: E402


def main() -> int:
    failures: list[str] = []

    def check(label: str, cond: bool, detail: str = "") -> None:
        print(f"  [{'PASS' if cond else 'FAIL'}] {label}" + (f" -- {detail}" if not cond else ""))
        if not cond:
            failures.append(label)

    with TestClient(app) as c:
        c.put("/api/sessions/it/bindings/s", json={"template": "deployments_by_service"})
        c.put("/api/sessions/it/bindings/v", json={"template": "deployments_by_version"})

        def item(s, n, p, dur, dl):
            return {"session": s, "name": n, "params": p,
                    "arrival_tick": 0, "duration_ticks": dur, "deadline_tick": dl}

        normal = [item("it", "s", {"service": "api-gateway"}, 1, 4)]
        five = [
            item("it", "s", {"service": "api-gateway"}, 2, 2),
            item("it", "v", {"version": "v2.0.1"}, 2, 2),
            item("it", "v", {"version": "v2.0.1"}, 1, 3),
            item("it", "s", {"service": "api-gateway"}, 1, 3),
            item("it", "s", {"service": "api-gateway"}, 1, 4),
        ]

        rounds = 6
        for r in range(rounds):
            print(f"-- round {r} --")
            # 1. a normal batch
            rn = c.post("/api/batches", json={"max_prepares": 4, "queries": normal})
            check(f"r{r} normal batch 200", rn.status_code == 200, rn.text[:120])

            # 2. interleave a deployment read + reconcile
            dep = c.get("/api/deployments", params={"service": "api-gateway"})
            check(f"r{r} deployments readable", dep.status_code == 200 and dep.json()["count"] >= 1)
            rec = c.post("/api/cache/reconcile").json()
            check(f"r{r} reconcile no reconnect", rec["reconnect_attempted"] is False)

            # 3. plan + arm a fault on the slot chosen for query #4
            plan = c.post("/api/batches/plan", json={"max_prepares": 4, "queries": five}).json()["plan"]
            fault_conn = next(s["connection"] for s in plan["steps"] if s["query_index"] == 4)
            arm = c.post(f"/api/testing/faults/disconnect?connection={fault_conn}&query_index=4")
            check(f"r{r} fault armed", arm.status_code == 200)

            # 4. read while armed (slot still healthy, should work)
            dep2 = c.get("/api/deployments?limit=1")
            check(f"r{r} read while armed ok", dep2.status_code == 200)

            # 5. run the faulting batch; must recover and complete
            fb = c.post("/api/batches", json={"max_prepares": 4, "queries": five})
            body = fb.json()
            check(f"r{r} fault batch completes", fb.status_code == 200 and body["status"] == "completed",
                  str(body.get("error"))[:160])
            if fb.status_code == 200:
                check(f"r{r} exactly one reconnect", body["recovery"]["reconnected"] is True)
                check(f"r{r} PREPARE within budget", body["actual_prepares"] <= 4)
                check(f"r{r} all success", all(q["status"] == "success" for q in body["queries"]))
                # PID / ticks populated on every success row
                check(f"r{r} PID+ticks present",
                      all(isinstance(q["backend_pid"], int) and q["start_tick"] <= q["end_tick"]
                          for q in body["queries"]))

            # 6. health + cache cap invariants
            health = c.get("/health").json()
            check(f"r{r} health ok after recovery", health["status"] == "ok"
                  and all(x["healthy"] for x in health["connections"]))
            cache = c.get("/api/cache").json()["connections"]
            check(f"r{r} cache cap 2 respected", all(x["prepared_count"] <= 2 for x in cache))
            check(f"r{r} no armed hook left", all(s.inject_disconnect_at is None
                                                  for s in state.runtime.slots))

        # Final: a zero-budget batch should fully hit the warm cache.
        zero = c.post("/api/batches", json={"max_prepares": 0, "queries": normal})
        check("final zero-budget batch all hits",
              zero.status_code == 200 and zero.json()["actual_prepares"] == 0
              and all(q["cache_hit"] for q in zero.json()["queries"]), zero.text[:160])

    print("\n" + ("ALL INTERLEAVE CHECKS PASSED" if not failures
                  else f"{len(failures)} FAILURES: {failures}"))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
