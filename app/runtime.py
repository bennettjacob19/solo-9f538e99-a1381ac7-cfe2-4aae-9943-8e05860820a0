"""Two physical PostgreSQL connections + real PREPARE/EXECUTE execution."""
from __future__ import annotations

import datetime as dt
import threading
import time
import uuid
from collections import OrderedDict
from dataclasses import dataclass
from decimal import Decimal
from typing import Any

import psycopg
from psycopg import sql as psql

from .planner import PlanResult, PlannerQuery, StepPlan, plan_batch
from .seed import seed_if_empty
from .store import BatchStore


class BatchBusy(RuntimeError):
    """Another batch is currently running (one batch at a time)."""


class BatchExecutionError(RuntimeError):
    def __init__(self, record: dict[str, Any]):
        super().__init__((record.get("error") or {}).get("message", "batch execution failed"))
        self.record = record


@dataclass
class ResolvedQuery:
    index: int
    session: str
    name: str
    binding_version: int
    template: str | None
    sql: str
    key: str
    statement_name: str
    param_names: list[str]
    positional_params: list[Any]
    arrival: int
    duration: int
    deadline: int
    deadline_inclusive: bool | None = None

    def snapshot_dict(self) -> dict[str, Any]:
        return {
            "query_index": self.index,
            "session": self.session,
            "query_name": self.name,
            "binding_version_at_acceptance": self.binding_version,
            "template": self.template,
            "sql": self.sql,
            "cache_key": self.key,
            "statement_name": self.statement_name,
            "param_names": list(self.param_names),
            "params": self.positional_params,
            "arrival_tick": self.arrival,
            "duration_ticks": self.duration,
            "deadline_tick": self.deadline,
        }


def _adapt_param(value: Any) -> Any:
    """Wrap JSON objects so psycopg can quote them as jsonb literals.

    Lists are left alone: psycopg adapts them to native PostgreSQL arrays.
    """
    if isinstance(value, dict):
        from psycopg.types.json import Jsonb

        return Jsonb(value)
    return value


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (bool, int, float, str)):
        return value
    if isinstance(value, (dt.datetime, dt.date, dt.time)):
        return value.isoformat()
    if isinstance(value, dt.timedelta):
        return str(value)
    if isinstance(value, Decimal):
        # Integral decimals stay integers; others become floats for easy JSON use.
        if value == value.to_integral_value():
            return int(value)
        return float(value)
    if isinstance(value, uuid.UUID):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return bytes(value).decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(v) for v in value]
    return str(value)


class ConnectionSlot:
    """One long-lived PG backend and its LRU of prepared statements (<= cap)."""

    def __init__(self, index: int, dsn: str, cap: int = 2) -> None:
        self.index = index
        self.dsn = dsn
        self.cap = cap
        self.conn: psycopg.Connection | None = None
        self.pid: int | None = None
        # key -> entry; iteration order is LRU (oldest first).
        self.statements: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self.lock = threading.RLock()

    # ---- connection management ---------------------------------------
    def connect_locked(self) -> None:
        conn = psycopg.connect(self.dsn, autocommit=True)
        # Stop psycopg from creating its own hidden prepared statements so
        # observed PREPARE counts are exactly the ones this service issues.
        try:
            conn.prepare_threshold = None
        except Exception:
            pass
        self.conn = conn
        self.pid = conn.info.backend_pid
        self.statements.clear()

    def ensure_conn_locked(self) -> None:
        dead = self.conn is None
        if self.conn is not None:
            try:
                dead = bool(getattr(self.conn, "closed", False))
            except Exception:
                dead = True
        if dead:
            try:
                if self.conn is not None:
                    self.conn.close()
            except Exception:
                pass
            self.connect_locked()

    def close(self) -> None:
        with self.lock:
            if self.conn is not None:
                try:
                    self.conn.close()
                except Exception:
                    pass
            self.conn = None

    # ---- cache introspection / reconciliation -------------------------
    def lru_keys(self) -> list[str]:
        with self.lock:
            return list(self.statements.keys())

    def reconcile(self) -> dict[str, Any]:
        """Make our metadata match pg_prepared_statements exactly."""
        with self.lock:
            try:
                self.ensure_conn_locked()
                assert self.conn is not None
                rows = self.conn.execute(
                    "SELECT name, statement FROM pg_prepared_statements "
                    "WHERE left(name, 3) = 'rq_'"
                ).fetchall()
            except Exception:
                # Backend unusable: a reconnect gives an empty prepared-statement
                # set, which is the ground truth for the new backend.
                try:
                    if self.conn is not None:
                        self.conn.close()
                except Exception:
                    pass
                self.conn = None
                self.pid = None
                self.statements.clear()
                try:
                    self.ensure_conn_locked()
                except Exception:
                    pass
                return self.describe_locked(reconciled=True, connection_reset=True)

            valid: dict[str, str | None] = {name: statement for name, statement in rows}

            for key in list(self.statements.keys()):
                entry = self.statements[key]
                if entry["statement_name"] not in valid:
                    self.statements.pop(key, None)

            known_names = {e["statement_name"] for e in self.statements.values()}
            for name, statement_text in valid.items():
                if name in known_names:
                    continue
                key = name[3:] if name.startswith("rq_") else name
                recovered_sql = None
                if statement_text:
                    marker = " AS "
                    pos = statement_text.upper().find(marker)
                    if pos != -1:
                        recovered_sql = statement_text[pos + len(marker) :]
                self.statements[key] = {
                    "key": key,
                    "statement_name": name,
                    "sql": recovered_sql,
                    "source": "postgresql_reconciled",
                    "session": None,
                    "query_name": None,
                }
                self.statements.move_to_end(key)

            return self.describe_locked(reconciled=True, connection_reset=False)

    def describe(self) -> dict[str, Any]:
        with self.lock:
            return self.describe_locked(reconciled=False, connection_reset=False)

    def describe_locked(self, *, reconciled: bool, connection_reset: bool) -> dict[str, Any]:
        stmts = []
        total = len(self.statements)
        for pos, (key, entry) in enumerate(self.statements.items()):
            stmts.append(
                {
                    "key": key,
                    "statement_name": entry["statement_name"],
                    "sql": entry.get("sql"),
                    "source": entry.get("source", "batch"),
                    "last_session": entry.get("session"),
                    "last_query_name": entry.get("query_name"),
                    "lru_position": pos,
                    "most_recently_used": pos == total - 1,
                }
            )
        connected = self.conn is not None and not bool(getattr(self.conn, "closed", False))
        return {
            "connection": self.index,
            "connection_slot": f"pg-conn-{self.index + 1}",
            "connected": connected,
            "backend_pid": self.pid if connected else None,
            "prepared_count": total,
            "max_prepared": self.cap,
            "reconciled": reconciled,
            "connection_reset": connection_reset,
            "statements": stmts,
        }

    # ---- real execution ----------------------------------------------
    def run_step(self, step: StepPlan, rq: ResolvedQuery) -> dict[str, Any]:
        with self.lock:
            self.ensure_conn_locked()
            assert self.conn is not None
            pid = self.conn.info.backend_pid
            self.pid = pid

            entry = self.statements.get(rq.key)
            if entry is not None:
                self.statements.move_to_end(rq.key)
                prepared_now = 0
            else:
                prepare_stmt = psql.SQL("PREPARE {} AS ").format(
                    psql.Identifier(rq.statement_name)
                ) + psql.SQL(rq.sql)
                self.conn.execute(prepare_stmt)
                prepared_now = 1
                self.statements[rq.key] = {
                    "key": rq.key,
                    "statement_name": rq.statement_name,
                    "sql": rq.sql,
                    "source": "batch",
                    "session": rq.session,
                    "query_name": rq.name,
                }
                if len(self.statements) > self.cap:
                    old_key, old_entry = self.statements.popitem(last=False)
                    try:
                        self.conn.execute(
                            psql.SQL("DEALLOCATE {}").format(
                                psql.Identifier(old_entry["statement_name"])
                            )
                        )
                    except Exception:
                        # Truth is reconciled after the batch anyway.
                        pass

            exec_stmt = psql.SQL("EXECUTE {}").format(
                psql.Identifier(rq.statement_name)
            )
            if rq.positional_params:
                literals = psql.SQL(", ").join(
                    psql.Literal(_adapt_param(value))
                    for value in rq.positional_params
                )
                exec_stmt = exec_stmt + psql.SQL("(") + literals + psql.SQL(")")

            cursor = self.conn.execute(exec_stmt)
            columns = [desc.name for desc in cursor.description] if cursor.description else []
            raw_rows = cursor.fetchall() if cursor.description else []
            rows = [[json_safe(value) for value in row] for row in raw_rows]
            records = [dict(zip(columns, row)) for row in rows]

            return {
                "status": "success",
                "backend_pid": pid,
                "cache_hit": prepared_now == 0,
                "prepare_count": prepared_now,
                "statement_name": rq.statement_name,
                "columns": columns,
                "rows": rows,
                "records": records,
                "row_count": len(rows),
            }


class BatchRuntime:
    def __init__(self, dsn: str, *, connection_count: int = 2, cap: int = 2) -> None:
        self.dsn = dsn
        self.cap = cap
        self.slots = [ConnectionSlot(i, dsn, cap=cap) for i in range(connection_count)]
        self.batch_lock = threading.Lock()
        self.active_batch_id: str | None = None
        self.store = BatchStore()
        self._lifecycle_lock = threading.RLock()

    # ---- lifecycle ----------------------------------------------------
    def wait_for_database(self, timeout_seconds: float = 60.0) -> None:
        deadline = time.time() + timeout_seconds
        last_error: Exception | None = None
        while time.time() < deadline:
            try:
                probe = psycopg.connect(self.dsn, autocommit=True, connect_timeout=3)
                probe.close()
                return
            except Exception as exc:  # noqa: BLE001 - retry until timeout
                last_error = exc
                time.sleep(1.0)
        raise RuntimeError(f"无法在 {timeout_seconds}s 内连接 PostgreSQL: {last_error}")

    def startup(self, *, seed: bool = True, timeout_seconds: float = 60.0) -> int:
        self.wait_for_database(timeout_seconds)
        inserted = 0
        # Seed through a short-lived connection; the two service slots then
        # open their own long-lived backends.
        with psycopg.connect(self.dsn, autocommit=True) as conn:
            if seed:
                inserted = seed_if_empty(conn)
        for slot in self.slots:
            slot.connect_locked()
        return inserted

    def shutdown(self) -> None:
        for slot in self.slots:
            slot.close()

    def cache_state(self) -> list[dict[str, Any]]:
        return [slot.describe() for slot in self.slots]

    def connections_state(self) -> list[dict[str, Any]]:
        return [slot.describe() for slot in self.slots]

    def reconcile_all(self) -> list[dict[str, Any]]:
        return [slot.reconcile() for slot in self.slots]

    # ---- batch submission --------------------------------------------
    def submit_batch(
        self,
        *,
        resolved: list[ResolvedQuery],
        max_prepares: int,
        inclusive_deadline: bool = False,
        request_echo: dict[str, Any] | None = None,
        dry_run: bool = False,
    ) -> dict[str, Any]:
        if not self.batch_lock.acquire(blocking=False):
            raise BatchBusy(
                "已有一批查询正在排程/取数；本演练一次只跑一批，请稍后再提交。"
            )

        batch_id = uuid.uuid4().hex[:12]
        self.active_batch_id = batch_id
        created_iso = dt.datetime.now().isoformat(timespec="seconds")

        record: dict[str, Any] = {
            "batch_id": batch_id,
            "status": "planning",
            "accepted": False,
            "tick_convention": (
                "inclusive: last_busy_tick <= deadline"
                if inclusive_deadline
                else "end=start+duration, require end<=deadline"
            ),
            "max_prepares": max_prepares,
            "inclusive_deadline": inclusive_deadline,
            "query_count": len(resolved),
            "dry_run": dry_run,
            "created_at": created_iso,
            "decided_at": None,
            "started_at": None,
            "finished_at": None,
            "planned_prepares": None,
            "actual_prepares": None,
            "makespan_ticks": None,
            "snapshot": [rq.snapshot_dict() for rq in resolved],
            "request": request_echo,
            "plan": None,
            "queries": [],
            "cache_before": None,
            "cache_after": None,
            "error": None,
        }
        results: list[dict[str, Any] | None] = [None] * len(resolved)

        try:
            cache_before = [slot.reconcile() for slot in self.slots]
            record["cache_before"] = cache_before
            lrus = [slot.lru_keys() for slot in self.slots]

            pqueries = [
                PlannerQuery(
                    index=rq.index,
                    session=rq.session,
                    name=rq.name,
                    key=rq.key,
                    arrival=rq.arrival,
                    duration=rq.duration,
                    deadline=rq.deadline,
                    deadline_inclusive=rq.deadline_inclusive,
                )
                for rq in resolved
            ]
            plan: PlanResult = plan_batch(
                pqueries,
                lrus,
                max_prepares,
                cap=self.slots[0].cap,
                inclusive_deadline=inclusive_deadline,
            )
            record["plan"] = plan.schedule_dict()
            record["decided_at"] = dt.datetime.now().isoformat(timespec="seconds")

            if not plan.feasible:
                record["status"] = "rejected"
                record["accepted"] = False
                record["queries"] = []
                record["cache_after"] = self.cache_state()
                record["rejection"] = {
                    "reason": plan.reason,
                    "message": plan.message,
                    "max_prepares": max_prepares,
                    "minimum_prepares_among_deadline_feasible": (
                        plan.minimum_prepares_among_deadline_feasible
                    ),
                    "best_deadline_feasible": plan.best_deadline_feasible,
                }
                record["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
                self.store.add(record)
                return record

            # Accepted: execute the chosen schedule against real PostgreSQL.
            record["accepted"] = True
            record["planned_prepares"] = plan.total_prepares
            record["makespan_ticks"] = plan.makespan_ticks

            if dry_run:
                record["status"] = "preview_feasible"
                record["dry_run"] = True
                record["message"] = (
                    "排程可行（试算模式）：未真正执行，连接缓存未改动。"
                )
                record["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
                record["cache_after"] = self.cache_state()
                return record

            record["status"] = "running"
            record["started_at"] = dt.datetime.now().isoformat(timespec="seconds")
            self.store.add(record)

            ordered_steps = sorted(
                plan.steps, key=lambda s: (s.start_tick, s.connection, s.index)
            )

            for step in ordered_steps:
                rq = resolved[step.index]
                base: dict[str, Any] = {
                    "query_index": rq.index,
                    "session": rq.session,
                    "query_name": rq.name,
                    "binding_version_at_acceptance": rq.binding_version,
                    "connection": step.connection,
                    "connection_slot": f"pg-conn-{step.connection + 1}",
                    "start_tick": step.start_tick,
                    "end_tick": step.end_tick,
                    "completion_tick": step.completion_tick,
                    "last_busy_tick": step.last_busy_tick,
                    "arrival_tick": step.arrival_tick,
                    "duration_ticks": step.duration_ticks,
                    "deadline_tick": step.deadline_tick,
                    "cache_hit": step.cache_hit,
                    "prepare_count": step.prepare_count,
                    "planned_evicted_key": step.evicted_key,
                    "params": rq.positional_params,
                }
                try:
                    executed = self.slots[step.connection].run_step(step, rq)
                except Exception as exc:  # noqa: BLE001 - stop batch, report
                    err = {
                        "type": type(exc).__name__,
                        "message": str(exc).strip(),
                        "sqlstate": getattr(exc, "sqlstate", None),
                        "failed_query_index": rq.index,
                        "session": rq.session,
                        "query_name": rq.name,
                        "statement_name": rq.statement_name,
                    }
                    base.update(
                        {
                            "status": "error",
                            "error": err,
                            "backend_pid": self.slots[step.connection].pid,
                        }
                    )
                    results[rq.index] = base
                    raise _BatchStepFailure(rq.index, err) from exc

                base.update(executed)
                results[rq.index] = base

            record["queries"] = results
            record["actual_prepares"] = sum(
                int((r or {}).get("prepare_count") or 0) for r in results
            )
            record["status"] = "completed"
            record["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
            record["cache_after"] = [slot.reconcile() for slot in self.slots]
            self.store.update(record)
            return record

        except _BatchStepFailure as failure:
            # Stop the whole batch; align results to the original query order
            # and mark everything that never ran as skipped.
            aligned: list[dict[str, Any]] = []
            for i, item in enumerate(results):
                if item is not None:
                    aligned.append(item)
                    continue
                rq = resolved[i]
                aligned.append(
                    {
                        "query_index": i,
                        "session": rq.session,
                        "query_name": rq.name,
                        "status": "skipped",
                        "error": (
                            f"本批在查询 #{failure.index} 执行出错后停止，该查询未执行。"
                        ),
                    }
                )
            record["queries"] = aligned
            record["actual_prepares"] = sum(
                int(r.get("prepare_count") or 0) for r in aligned
            )
            record["status"] = "error"
            record["accepted"] = True
            record["error"] = failure.error
            record["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
            record["cache_after"] = self.reconcile_all()
            self.store.update(record)
            raise BatchExecutionError(record) from failure.__cause__
        finally:
            self.active_batch_id = None
            try:
                self.batch_lock.release()
            except RuntimeError:
                pass


class _BatchStepFailure(Exception):
    def __init__(self, index: int, error: dict[str, Any]):
        self.index = index
        self.error = error
        super().__init__(error.get("message", "step failed"))
