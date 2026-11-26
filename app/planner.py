"""Exact two-connection batch scheduler with a two-slot prepared-statement cache.

Model (logical "ticks", no wall-clock sleeping):

* query ``i`` arrives at ``arrival[i]``, occupies one connection for
  ``duration[i]`` ticks; earliest completion is ``start + duration``;
* standard convention accepts when ``start + duration <= deadline``
  (``inclusive_deadline`` changes this to ``last_busy_tick = end - 1 <= deadline``);
* a query runs on exactly one of two connections and queries on the same
  connection never overlap;
* each connection keeps at most ``cap`` (=2) prepared SQL texts using LRU
  eviction; a cache hit costs 0 PREPARE, a miss costs exactly 1 PREPARE;
* cache state carries over from previous batches.

With at most 8 queries we enumerate *every* partition into two connections
plus every per-connection ordering:
``sum_k C(n,k) k! (n-k)! = (n+1) n! <= 362_880`` schedules for n=8 — small
enough for an exact yes/no answer and a deterministic best plan.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from itertools import permutations
from typing import Any, Sequence


@dataclass(frozen=True)
class PlannerQuery:
    index: int
    session: str
    name: str
    key: str
    arrival: int
    duration: int
    deadline: int
    deadline_inclusive: bool | None = None


@dataclass
class StepPlan:
    index: int
    connection: int
    order_on_connection: int
    arrival_tick: int
    duration_ticks: int
    deadline_tick: int
    start_tick: int
    end_tick: int                 # exclusive completion boundary: start+duration
    last_busy_tick: int
    completion_tick: int          # alias of end_tick (scheduling completion C)
    cache_hit: bool
    prepare_count: int            # 0 on hit, 1 on miss
    evicted_key: str | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "query_index": self.index,
            "connection": self.connection,
            "connection_slot": f"pg-conn-{self.connection + 1}",
            "order_on_connection": self.order_on_connection,
            "arrival_tick": self.arrival_tick,
            "duration_ticks": self.duration_ticks,
            "deadline_tick": self.deadline_tick,
            "start_tick": self.start_tick,
            "end_tick": self.end_tick,
            "completion_tick": self.completion_tick,
            "last_busy_tick": self.last_busy_tick,
            "cache_hit": self.cache_hit,
            "prepare_count": self.prepare_count,
            "evicted_key": self.evicted_key,
        }


# Lightweight internal step:
# (query_index, order_idx, start, end, cache_hit, prepare_count, evicted_key)
LightStep = tuple[int, int, int, int, bool, int, "str | None"]


@dataclass
class _ConnResult:
    light: list[LightStep]
    prepares: int
    free_at: int
    sum_end: int
    final_lru: list[str]


@dataclass
class PlanResult:
    feasible: bool
    steps: list[StepPlan] = field(default_factory=list)
    total_prepares: int | None = None
    planned_prepares: int | None = None
    makespan_ticks: int | None = None
    max_prepares: int | None = None
    reason: str | None = None
    message: str | None = None
    minimum_prepares_among_deadline_feasible: int | None = None
    best_deadline_feasible: dict[str, Any] | None = None
    cache_before: list[list[str]] | None = None
    cache_after: list[list[str]] | None = None
    tick_convention: str = (
        "default end=start+duration require end<=deadline; "
        "inclusive_deadline=true uses last_busy_tick=end-1<=deadline"
    )

    def schedule_dict(self) -> dict[str, Any]:
        ordered = sorted(self.steps, key=lambda s: (s.start_tick, s.connection, s.index))
        return {
            "feasible": self.feasible,
            "reason": self.reason,
            "message": self.message,
            "connection_count": 2,
            "prepared_statement_cap_per_connection": 2,
            "tick_convention": self.tick_convention,
            "total_prepares": self.total_prepares,
            "planned_prepares": self.planned_prepares,
            "makespan_ticks": self.makespan_ticks,
            "max_prepares": self.max_prepares,
            "minimum_prepares_among_deadline_feasible": (
                self.minimum_prepares_among_deadline_feasible
            ),
            "cache_before_lru": self.cache_before,
            "cache_after_lru": self.cache_after,
            "steps": [s.to_dict() for s in ordered],
            "best_deadline_feasible": self.best_deadline_feasible,
        }


def _simulate_connection(
    order: tuple[int, ...],
    queries: Sequence[PlannerQuery],
    initial_lru: Sequence[str],
    cap: int,
) -> _ConnResult | None:
    lru: list[str] = list(initial_lru)  # least-recently-used .. most-recent
    free_at = 0
    light: list[LightStep] = []
    prepares = 0
    sum_end = 0

    for order_idx, qi in enumerate(order):
        q = queries[qi]
        start = q.arrival if q.arrival > free_at else free_at
        end = start + q.duration
        inclusive = bool(q.deadline_inclusive)
        allowed_end = q.deadline + (1 if inclusive else 0)
        if end > allowed_end:
            return None

        hit = q.key in lru
        evicted: str | None = None
        if hit:
            lru.remove(q.key)
            lru.append(q.key)
        else:
            prepares += 1
            lru.append(q.key)
            if len(lru) > cap:
                evicted = lru.pop(0)

        light.append((qi, order_idx, start, end, hit, 0 if hit else 1, evicted))
        free_at = end
        sum_end += end

    return _ConnResult(
        light=light,
        prepares=prepares,
        free_at=free_at,
        sum_end=sum_end,
        final_lru=lru,
    )


def _rank(c0: _ConnResult, c1: _ConnResult, mask: int) -> tuple:
    # Fewer PREPAREs first (the constrained shared resource), then earliest
    # makespan, then smallest sum of completion ticks, then deterministic
    # assignment/order tie-breakers.
    return (
        c0.prepares + c1.prepares,
        max(c0.free_at, c1.free_at),
        c0.sum_end + c1.sum_end,
        mask,
        tuple(step[0] for step in c0.light),
        tuple(step[0] for step in c1.light),
    )


def _materialize(
    c0: _ConnResult, c1: _ConnResult, queries: Sequence[PlannerQuery]
) -> list[StepPlan]:
    steps: list[StepPlan] = []
    for conn_idx, result in ((0, c0), (1, c1)):
        for qi, order_idx, start, end, hit, prep, evicted in result.light:
            q = queries[qi]
            steps.append(
                StepPlan(
                    index=qi,
                    connection=conn_idx,
                    order_on_connection=order_idx,
                    arrival_tick=q.arrival,
                    duration_ticks=q.duration,
                    deadline_tick=q.deadline,
                    start_tick=start,
                    end_tick=end,
                    last_busy_tick=end - 1,
                    completion_tick=end,
                    cache_hit=hit,
                    prepare_count=prep,
                    evicted_key=evicted,
                )
            )
    return steps


def plan_batch(
    queries: Sequence[PlannerQuery],
    cache_lrus: Sequence[Sequence[str]],
    max_prepares: int,
    *,
    cap: int = 2,
    inclusive_deadline: bool = False,
) -> PlanResult:
    n = len(queries)
    if not 1 <= n <= 8:
        raise ValueError("每批必须包含 1 到 8 个查询。")
    if max_prepares < 0:
        raise ValueError("max_prepares 不能为负。")

    # Resolve the batch convention for queries that did not set their own.
    resolved_qs: list[PlannerQuery] = []
    for q in queries:
        flag = (
            q.deadline_inclusive
            if q.deadline_inclusive is not None
            else inclusive_deadline
        )
        resolved_qs.append(
            PlannerQuery(
                index=q.index,
                session=q.session,
                name=q.name,
                key=q.key,
                arrival=q.arrival,
                duration=q.duration,
                deadline=q.deadline,
                deadline_inclusive=bool(flag),
            )
        )
    queries = resolved_qs

    lru0 = list(cache_lrus[0]) if len(cache_lrus) > 0 else []
    lru1 = list(cache_lrus[1]) if len(cache_lrus) > 1 else []

    best_rank: tuple | None = None
    best_c0: _ConnResult | None = None
    best_c1: _ConnResult | None = None

    dl_rank: tuple | None = None  # deadline-feasible ignoring budget
    dl_c0: _ConnResult | None = None
    dl_c1: _ConnResult | None = None

    for mask in range(1 << n):
        group0 = tuple(i for i in range(n) if ((mask >> i) & 1) == 0)
        group1 = tuple(i for i in range(n) if ((mask >> i) & 1) == 1)

        for perm0 in permutations(group0):
            r0 = _simulate_connection(perm0, queries, lru0, cap)
            if r0 is None:
                continue

            for perm1 in permutations(group1):
                r1 = _simulate_connection(perm1, queries, lru1, cap)
                if r1 is None:
                    continue

                rank = _rank(r0, r1, mask)

                if dl_rank is None or rank < dl_rank:
                    dl_rank, dl_c0, dl_c1 = rank, r0, r1

                if rank[0] <= max_prepares and (
                    best_rank is None or rank < best_rank
                ):
                    best_rank, best_c0, best_c1 = rank, r0, r1

    cache_before = [lru0, lru1]

    if best_c0 is not None and best_c1 is not None and best_rank is not None:
        steps = _materialize(best_c0, best_c1, queries)
        return PlanResult(
            feasible=True,
            steps=steps,
            total_prepares=best_rank[0],
            planned_prepares=best_rank[0],
            makespan_ticks=best_rank[1],
            max_prepares=max_prepares,
            cache_before=cache_before,
            cache_after=[best_c0.final_lru, best_c1.final_lru],
        )

    result = PlanResult(
        feasible=False,
        max_prepares=max_prepares,
        cache_before=cache_before,
    )

    if dl_c0 is not None and dl_c1 is not None and dl_rank is not None:
        min_prepares = dl_rank[0]
        result.minimum_prepares_among_deadline_feasible = min_prepares
        diag_steps = sorted(
            _materialize(dl_c0, dl_c1, queries),
            key=lambda s: (s.start_tick, s.connection, s.index),
        )
        result.best_deadline_feasible = {
            "total_prepares": min_prepares,
            "makespan_ticks": dl_rank[1],
            "within_prepare_limit": min_prepares <= max_prepares,
            "steps": [s.to_dict() for s in diag_steps],
            "cache_after_lru": [dl_c0.final_lru, dl_c1.final_lru],
        }
        result.reason = "prepare_budget_exceeded"
        result.message = (
            f"存在满足时限的排程，但整批最少需要 {min_prepares} 次 PREPARE，"
            f"超过本批上限 {max_prepares}；不予受理，连接缓存保持不变。"
        )
    else:
        result.reason = "deadline_infeasible"
        result.message = (
            "即使允许任意 PREPARE，也不存在让所有查询在最迟完成拍内结束的双连接排程；"
            "不予受理，连接缓存保持不变。"
        )

    return result
