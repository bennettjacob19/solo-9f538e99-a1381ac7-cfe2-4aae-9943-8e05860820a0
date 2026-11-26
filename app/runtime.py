"""Two physical PostgreSQL connections + real PREPARE/EXECUTE execution.

Failure model implemented here (see README §4.6):

* A mid-batch **connection disconnect** is detected explicitly instead of
  being silently healed by ``ensure_conn_locked``. The slot moves to the
  ``faulted`` state and the batch pauses at the faulting step.
* Exactly one **confirmed** recovery reconnect is attempted for the faulted
  slot. The new backend starts with an empty prepared-statement set — that is
  ground truth, so the stale cache metadata is dropped and the un-executed
  tail is re-scheduled with :func:`app.planner.plan_resume` against the real
  surviving caches, the remaining PREPARE budget and every original
  arrival/deadline/start-tick constraint.
* SQL errors (the statement was delivered and PostgreSQL answered) still stop
  the batch immediately — a reconnect must never re-run an unknown query.
* Read-only side endpoints (:func:`app.routes` deployments / reconcile) never
  trigger a reconnect; they borrow an idle healthy slot or report busy.
"""
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
from psycopg import errors as pg_errors
from psycopg import sql as psql

from .planner import PlanResult, PlannerQuery, StepPlan, plan_batch, plan_resume
from .seed import seed_if_empty
from .store import BatchStore

# Slot lifecycle states.
STATE_HEALTHY = "healthy"
STATE_FAULTED = "faulted"          # backend gone; not yet recovered
STATE_RECONNECTED = "reconnected"  # one confirmed recovery reconnect done

# Batch error reasons (surfaced in records and HTTP 500 bodies).
REASON_SQL_ERROR = "sql_error"
REASON_DISCONNECT = "connection_disconnect"
REASON_RECONNECT_FAILED = "reconnect_failed"
REASON_RESUME_INFEASIBLE = "resume_infeasible"
REASON_SECOND_DISCONNECT = "second_disconnect_aborts"


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


def is_disconnect_error(exc: BaseException) -> bool:
    """True only when the connection itself is unusable.

    A regular SQL error (undefined table, bad types ...) means PostgreSQL
    received the statement and answered it — the connection stays healthy and
    the query must never be retried on a new backend.
    """
    if isinstance(
        exc,
        (
            pg_errors.OperationalError,
            pg_errors.InterfaceError,
            ConnectionError,
            EOFError,
            BrokenPipeError,
        ),
    ):
        return True
    # psycopg marks a dead backend with .sqlstate 08000/08003/08006 ...
    sqlstate = getattr(exc, "sqlstate", None)
    if isinstance(sqlstate, str) and sqlstate.startswith("08"):
        return True
    message = str(exc).lower()
    needles = (
        "connection",
        "server closed",
        "terminat",
        "broken pipe",
        "eof",
        "socket",
        "not connected",
        "already closed",
        "could not receive",
        "could not send",
    )
    return any(needle in message for needle in needles)


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
        # Failure-handling state machine.
        self.state: str = STATE_HEALTHY
        self.fault_count: int = 0
        self.faults: list[dict[str, Any]] = []
        # Test/chaos hook: when set, the next run_step matching this query
        # index raises a synthetic disconnect before any SQL is sent.
        self.inject_disconnect_at: int | None = None

    @property
    def slot_name(self) -> str:
        return f"pg-conn-{self.index + 1}"

    @property
    def healthy(self) -> bool:
        """Healthy and currently usable. Never performs I/O / reconnects.

        A ``reconnected`` slot is fully usable: the single confirmed recovery
        reconnect succeeded and its (cold) backend is live. Only ``faulted``
        or closed slots are unhealthy.
        """
        if self.state == STATE_FAULTED:
            return False
        conn = self.conn
        if conn is None:
            return False
        try:
            return not bool(getattr(conn, "closed", False))
        except Exception:
            return False

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
        self.state = STATE_HEALTHY

    def ensure_conn_locked(self) -> None:
        """Connect (or reconnect) on demand.

        Only used by lifecycle/startup paths and explicit recovery; normal
        read endpoints must call :meth:`healthy` first and never reconnect.
        """
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
            self.pid = None

    # ---- failure handling ---------------------------------------------
    def mark_faulted_locked(self, *, reason: str, detail: str) -> dict[str, Any]:
        """Record that this backend is gone. Stale metadata is preserved until
        the recovery decision (reconnected backends start cold anyway)."""
        old_pid = self.pid
        event = {
            "at": dt.datetime.now().isoformat(timespec="seconds"),
            "connection": self.index,
            "connection_slot": self.slot_name,
            "previous_backend_pid": old_pid,
            "reason": reason,
            "detail": detail[:300],
        }
        try:
            if self.conn is not None:
                self.conn.close()
        except Exception:
            pass
        self.conn = None
        self.pid = None
        # The new backend (if any) will have an empty prepared set; dropping
        # the metadata now makes the cold-cache fact explicit everywhere.
        self.statements.clear()
        self.state = STATE_FAULTED
        self.fault_count += 1
        self.faults.append(event)
        return dict(event)

    def attempt_recovery_reconnect_locked(self, previous_pid: int | None = None) -> dict[str, Any]:
        """The single confirmed recovery reconnect for this fault episode."""
        event = {
            "at": dt.datetime.now().isoformat(timespec="seconds"),
            "connection": self.index,
            "connection_slot": self.slot_name,
            "previous_backend_pid": previous_pid if previous_pid is not None else self.pid,
        }
        self.connect_locked()  # sets state=healthy, pid, clears statements
        self.state = STATE_RECONNECTED
        event["new_backend_pid"] = self.pid
        event["outcome"] = "reconnected"
        event["prepared_statements_after"] = 0
        return event

    def clear_injection(self) -> None:
        with self.lock:
            self.inject_disconnect_at = None

    # ---- cache introspection / reconciliation -------------------------
    def lru_keys(self) -> list[str]:
        with self.lock:
            return list(self.statements.keys())

    def probe_prepared_locked(self) -> list[tuple[str, str | None]]:
        """Read pg_prepared_statements on a currently-healthy slot only."""
        if not self.healthy:
            raise RuntimeError("slot_not_healthy")
        assert self.conn is not None
        rows = self.conn.execute(
            "SELECT name, statement FROM pg_prepared_statements "
            "WHERE left(name, 3) = 'rq_'"
        ).fetchall()
        return [(name, statement) for name, statement in rows]

    def reconcile_locked(self, *, allow_reconnect: bool) -> dict[str, Any]:
        """Make metadata match pg_prepared_statements exactly.

        ``allow_reconnect=False`` (the public endpoint during a fault) never
        opens a new connection: a faulted slot is reported as temporarily
        unverifiable and its metadata is left for the batch recovery flow.
        """
        if not allow_reconnect and not self.healthy:
            return self.describe_locked(
                reconciled=True,
                connection_reset=False,
                note="故障槽暂不可核对：本接口不补连，等待批次恢复流程处理。",
            )

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

    def reconcile(self, *, allow_reconnect: bool = True) -> dict[str, Any]:
        with self.lock:
            return self.reconcile_locked(allow_reconnect=allow_reconnect)

    def describe(self) -> dict[str, Any]:
        with self.lock:
            return self.describe_locked(reconciled=False, connection_reset=False)

    def describe_locked(
        self,
        *,
        reconciled: bool,
        connection_reset: bool,
        note: str | None = None,
    ) -> dict[str, Any]:
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
        connected = self.healthy
        return {
            "connection": self.index,
            "connection_slot": self.slot_name,
            "state": self.state,
            "connected": connected,
            "healthy": connected,
            "backend_pid": self.pid if connected else None,
            "prepared_count": total,
            "max_prepared": self.cap,
            "reconciled": reconciled,
            "connection_reset": connection_reset,
            "fault_count": self.fault_count,
            "note": note,
            "statements": stmts,
        }

    # ---- real execution ----------------------------------------------
    def run_step(self, step: StepPlan, rq: ResolvedQuery) -> dict[str, Any]:
        with self.lock:
            # Chaos/test injection: simulate the backend dying immediately
            # before this step would have touched it.
            if self.inject_disconnect_at == rq.index:
                self.inject_disconnect_at = None
                raise pg_errors.OperationalError(
                    "server closed the connection unexpectedly "
                    "(injected disconnect for failure-recovery drill)"
                )

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


class _StepFailure(Exception):
    """Internal control-flow signal for a failed execution step."""

    def __init__(self, index: int, error: dict[str, Any], *, disconnect: bool):
        self.index = index
        self.error = error
        self.disconnect = disconnect
        super().__init__(error.get("message", "step failed"))


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
            with slot.lock:
                slot.connect_locked()
        return inserted

    def shutdown(self) -> None:
        for slot in self.slots:
            slot.close()

    def cache_state(self) -> list[dict[str, Any]]:
        return [slot.describe() for slot in self.slots]

    def connections_state(self) -> list[dict[str, Any]]:
        return [slot.describe() for slot in self.slots]

    def reconcile_all(self, *, allow_reconnect: bool = True) -> list[dict[str, Any]]:
        return [slot.reconcile(allow_reconnect=allow_reconnect) for slot in self.slots]

    # ---- read-only slot borrowing (never reconnects) ------------------
    def borrow_idle_healthy_slot(self) -> ConnectionSlot:
        """Pick an idle, healthy slot for a side read endpoint.

        Raises :class:`NoHealthySlot` when no slot can serve the read without
        waiting (faulted backend, or every healthy slot is mid-batch). This
        method never opens a connection.
        """
        for slot in self.slots:
            if not slot.healthy:
                continue
            if slot.lock.acquire(blocking=False):
                # Re-check under the lock: state may have changed meanwhile.
                if slot.healthy:
                    return slot
                slot.lock.release()
        raise NoHealthySlot("当前没有空闲且健康的连接槽可借用（故障或批次占用中）。")

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
            "resume_plan": None,
            "recovery": None,
            "queries": [],
            "cache_before": None,
            "cache_after": None,
            "error": None,
            "error_reason": None,
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

            try:
                self._execute_steps(
                    ordered_steps=ordered_steps,
                    resolved=resolved,
                    results=results,
                    record=record,
                    plan=plan,
                    max_prepares=max_prepares,
                    inclusive_deadline=inclusive_deadline,
                )
            except _StepFailure as failure:
                if not failure.disconnect:
                    self._finalize_sql_error(record, results, resolved, failure)
                    raise BatchExecutionError(record) from failure.__cause__
                # Disconnect: the recovery flow either finishes the tail or
                # finalizes the record as an error itself.
                self._handle_disconnect(
                    failure=failure,
                    record=record,
                    results=results,
                    resolved=resolved,
                    plan=plan,
                    max_prepares=max_prepares,
                    inclusive_deadline=inclusive_deadline,
                )
                if record["status"] != "running":
                    # Recovery aborted (reconnect failure / no feasible resume
                    # / second disconnect): the record is already finalized.
                    raise BatchExecutionError(record)

            record["queries"] = results
            record["actual_prepares"] = sum(
                int((r or {}).get("prepare_count") or 0) for r in results
            )
            record["status"] = "completed"
            record["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
            record["cache_after"] = [slot.reconcile() for slot in self.slots]
            self.store.update(record)
            return record

        finally:
            self.active_batch_id = None
            try:
                self.batch_lock.release()
            except RuntimeError:
                pass

    # ---- execution helpers --------------------------------------------
    def _step_base(self, step: StepPlan, rq: ResolvedQuery) -> dict[str, Any]:
        return {
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
            "duration_tick": step.duration_ticks,
            "deadline_tick": step.deadline_tick,
            "cache_hit": step.cache_hit,
            "prepare_count": step.prepare_count,
            "planned_evicted_key": step.evicted_key,
            "params": rq.positional_params,
        }

    def _execute_steps(
        self,
        *,
        ordered_steps: list[StepPlan],
        resolved: list[ResolvedQuery],
        results: list[dict[str, Any] | None],
        record: dict[str, Any],
        plan: PlanResult,
        max_prepares: int,
        inclusive_deadline: bool,
        phase: str = "initial",
    ) -> None:
        """Run steps in global tick order; raise _StepFailure on any error."""
        for step in ordered_steps:
            rq = resolved[step.index]
            base = self._step_base(step, rq)
            base["phase"] = phase
            try:
                executed = self.slots[step.connection].run_step(step, rq)
            except Exception as exc:  # noqa: BLE001 - classify and stop/resume
                disconnect = is_disconnect_error(exc)
                slot = self.slots[step.connection]
                err = {
                    "type": type(exc).__name__,
                    "message": str(exc).strip(),
                    "sqlstate": getattr(exc, "sqlstate", None),
                    "failed_query_index": rq.index,
                    "session": rq.session,
                    "query_name": rq.name,
                    "statement_name": rq.statement_name,
                    "connection": step.connection,
                    "connection_slot": slot.slot_name,
                    "disconnect": disconnect,
                }
                if disconnect:
                    with slot.lock:
                        fault_event = slot.mark_faulted_locked(
                            reason=REASON_DISCONNECT,
                            detail=err["message"] or type(exc).__name__,
                        )
                    err["fault_event"] = fault_event
                    base.update(
                        {
                            "status": "interrupted",
                            "error": err,
                            "backend_pid": fault_event["previous_backend_pid"],
                        }
                    )
                    results[rq.index] = base
                else:
                    base.update(
                        {
                            "status": "error",
                            "error": err,
                            "backend_pid": slot.pid,
                        }
                    )
                    results[rq.index] = base
                raise _StepFailure(rq.index, err, disconnect=disconnect) from exc

            base.update(executed)
            results[rq.index] = base

    def _finalize_sql_error(
        self,
        record: dict[str, Any],
        results: list[dict[str, Any] | None],
        resolved: list[ResolvedQuery],
        failure: _StepFailure,
    ) -> None:
        """SQL errors stop the batch immediately; no reconnect, no re-run."""
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
        record["error_reason"] = REASON_SQL_ERROR
        record["error"] = failure.error
        record["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
        record["cache_after"] = self.reconcile_all()
        self.store.update(record)

    def _handle_disconnect(
        self,
        *,
        failure: _StepFailure,
        record: dict[str, Any],
        results: list[dict[str, Any] | None],
        resolved: list[ResolvedQuery],
        plan: PlanResult,
        max_prepares: int,
        inclusive_deadline: bool,
    ) -> None:
        """One confirmed reconnect, then exact re-planning of the tail."""
        faulted_index = failure.index
        faulted_slot = self.slots[failure.error["connection"]]

        recovery: dict[str, Any] = {
            "faulted_query_index": faulted_index,
            "faulted_connection": faulted_slot.index,
            "faulted_connection_slot": faulted_slot.slot_name,
            "attempted_reconnect": False,
            "reconnected": False,
            "reconnect_event": None,
            "resume": None,
        }
        record["recovery"] = recovery

        # Already-fetched results, backend PIDs and start/end ticks stay as
        # photographed; those queries are never re-run. Only the interrupted
        # step and everything still pending get re-scheduled.
        done_indices = {
            i
            for i, item in enumerate(results)
            if item is not None and item.get("status") == "success"
        }

        # Single confirmed recovery reconnect for the faulted slot.
        recovery["attempted_reconnect"] = True
        previous_pid = failure.error.get("fault_event", {}).get("previous_backend_pid")
        try:
            with faulted_slot.lock:
                reconnect_event = faulted_slot.attempt_recovery_reconnect_locked(
                    previous_pid=previous_pid
                )
        except Exception as exc:  # noqa: BLE001 - reconnect itself failed
            recovery["reconnected"] = False
            recovery["reconnect_error"] = str(exc).strip()
            self._abort_resume(
                record=record,
                results=results,
                resolved=resolved,
                reason=REASON_RECONNECT_FAILED,
                message=(
                    f"连接 {faulted_slot.slot_name} 断线且一次确认重连失败：{exc}。"
                    "本批停止，未执行项保留，幸存槽缓存保留。"
                ),
                failure=failure,
            )
            return
        recovery["reconnected"] = True
        recovery["reconnect_event"] = reconnect_event

        # Ground the surviving slots' metadata in their real backends before
        # planning, so cache-hit assumptions can never over-issue PREPARE.
        for slot in self.slots:
            with slot.lock:
                if slot.healthy:
                    slot.reconcile_locked(allow_reconnect=False)

        pending = [
            rq for rq in resolved if rq.index not in done_indices
        ]
        # Original planned start ticks are hard lower bounds for the tail.
        original_starts = {
            step.index: step.start_tick for step in plan.steps
        }
        # Ticks already occupied on each connection keep blocking it.
        free_at = [0, 0]
        for step in plan.steps:
            if step.index in done_indices:
                free_at[step.connection] = max(free_at[step.connection], step.end_tick)

        spent_prepares = sum(
            int((item or {}).get("prepare_count") or 0) for item in results
        )
        remaining_prepares = max_prepares - spent_prepares
        recovery["spent_prepares_before_fault"] = spent_prepares
        recovery["remaining_prepare_budget"] = remaining_prepares
        recovery["occupied_until_tick"] = list(free_at)

        current_lrus = [slot.lru_keys() for slot in self.slots]
        pending_pqueries = [
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
            for rq in pending
        ]
        resume = plan_resume(
            pending_pqueries,
            current_lrus,
            free_at,
            original_starts,
            remaining_prepares,
            cap=self.cap,
            inclusive_deadline=inclusive_deadline,
        )
        record["resume_plan"] = resume.schedule_dict()
        recovery["resume"] = {
            "feasible": resume.feasible,
            "reason": resume.reason,
            "message": resume.message,
            "tail_prepares_needed": resume.total_prepares,
            "pending_query_indices": [rq.index for rq in pending],
        }

        if not resume.feasible:
            self._abort_resume(
                record=record,
                results=results,
                resolved=resolved,
                reason=REASON_RESUME_INFEASIBLE,
                message=(
                    "断线已确认重连，但未执行尾部无可行续排："
                    f"{resume.message}"
                ),
                failure=failure,
                resume=resume,
            )
            return

        # A plan may still nominally touch a slot whose reconnect failed; that
        # path cannot happen here because the faulted slot did reconnect, but
        # guard the invariant explicitly.
        for step in resume.steps:
            if not self.slots[step.connection].healthy:
                self._abort_resume(
                    record=record,
                    results=results,
                    resolved=resolved,
                    reason=REASON_RECONNECT_FAILED,
                    message=(
                        f"续排需要连接槽 {step.connection}，但该槽当前不可用；"
                        "本批停止，未执行项保留，幸存槽缓存保留。"
                    ),
                    failure=failure,
                    resume=resume,
                )
                return

        resume_steps = sorted(
            resume.steps, key=lambda s: (s.start_tick, s.connection, s.index)
        )
        try:
            self._execute_steps(
                ordered_steps=resume_steps,
                resolved=resolved,
                results=results,
                record=record,
                plan=resume,
                max_prepares=max_prepares,
                inclusive_deadline=inclusive_deadline,
                phase="resumed",
            )
        except _StepFailure as second:
            if second.disconnect:
                # A second disconnect (on either slot) ends the drill batch:
                # no further reconnects are promised by the contract.
                # _execute_steps already marked that slot faulted; make sure
                # any still-healthy slot involved here is recorded too.
                slot = self.slots[second.error["connection"]]
                with slot.lock:
                    if slot.state == STATE_HEALTHY:
                        slot.mark_faulted_locked(
                            reason=REASON_SECOND_DISCONNECT,
                            detail=second.error.get("message", ""),
                        )
                    # A reconnected slot that died again is already faulted by
                    # _execute_steps; nothing else to change here.
                self._abort_resume(
                    record=record,
                    results=results,
                    resolved=resolved,
                    reason=REASON_SECOND_DISCONNECT,
                    message=(
                        "续跑期间再次发生连接断线；按约定每批最多一次确认重连，"
                        "本批立即停止，未执行项保留，幸存槽缓存保留。"
                    ),
                    failure=second,
                    resume=resume,
                )
                return
            self._finalize_sql_error(record, results, resolved, second)
            return

        # Tail finished: merge recovery bookkeeping into the completed record.
        tail_prepares = sum(
            int((results[rq.index] or {}).get("prepare_count") or 0)
            for rq in pending
        )
        recovery["tail_prepares_actual"] = tail_prepares
        recovery["completed"] = True
        record["status"] = "running"  # outer flow flips to "completed"

    def _abort_resume(
        self,
        *,
        record: dict[str, Any],
        results: list[dict[str, Any] | None],
        resolved: list[ResolvedQuery],
        reason: str,
        message: str,
        failure: _StepFailure,
        resume: PlanResult | None = None,
    ) -> None:
        """Finish the record as an error, listing every not-executed item."""
        unexecuted: list[dict[str, Any]] = []
        aligned: list[dict[str, Any]] = []
        for i, item in enumerate(results):
            if item is not None:
                aligned.append(item)
                if item.get("status") != "success":
                    unexecuted.append(
                        {
                            "query_index": i,
                            "session": resolved[i].session,
                            "query_name": resolved[i].name,
                            "arrival_tick": resolved[i].arrival,
                            "deadline_tick": resolved[i].deadline,
                            "status": item.get("status"),
                        }
                    )
                continue
            rq = resolved[i]
            skipped = {
                "query_index": i,
                "session": rq.session,
                "query_name": rq.name,
                "status": "not_executed",
                "error": "断线恢复流程未能安排该查询；本批停止，未重跑任何已完成查询。",
            }
            aligned.append(skipped)
            unexecuted.append(
                {
                    "query_index": i,
                    "session": rq.session,
                    "query_name": rq.name,
                    "arrival_tick": rq.arrival,
                    "deadline_tick": rq.deadline,
                    "status": "not_executed",
                }
            )

        record["queries"] = aligned
        record["actual_prepares"] = sum(
            int(r.get("prepare_count") or 0) for r in aligned
        )
        record["status"] = "error"
        record["accepted"] = True
        record["error_reason"] = reason
        record["error"] = {
            "reason": reason,
            "message": message,
            "failed_query_index": failure.index,
            "sqlstate": failure.error.get("sqlstate"),
            "unexecuted_count": len(unexecuted),
            "unexecuted_queries": unexecuted,
        }
        record["finished_at"] = dt.datetime.now().isoformat(timespec="seconds")
        # Reconcile only surviving healthy slots; faulted slots stay marked.
        record["cache_after"] = self.reconcile_all(allow_reconnect=False)
        self.store.update(record)


class NoHealthySlot(RuntimeError):
    """No idle healthy connection is available for a side read."""
