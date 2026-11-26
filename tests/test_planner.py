"""Exact scheduler tests (no HTTP / DB needed)."""
from __future__ import annotations

import time

import pytest

from app.planner import PlannerQuery, _simulate_connection, plan_batch


def q(i, key=None, arrival=0, duration=1, deadline=10, inclusive=None):
    return PlannerQuery(
        index=i,
        session="s",
        name=f"n{i}",
        key=key or f"k{i}",
        arrival=arrival,
        duration=duration,
        deadline=deadline,
        deadline_inclusive=inclusive,
    )


def test_cold_cache_zero_budget_rejected():
    qs = [q(0), q(1)]
    r = plan_batch(qs, [[], []], 0)
    assert r.feasible is False
    assert r.reason == "prepare_budget_exceeded"
    assert r.minimum_prepares_among_deadline_feasible == 2


def test_distinct_sql_min_prepare_count():
    qs = [q(i, deadline=2) for i in range(3)]
    r = plan_batch(qs, [[], []], 2)
    assert r.feasible is False
    assert r.minimum_prepares_among_deadline_feasible == 3
    r3 = plan_batch(qs, [[], []], 3)
    assert r3.feasible is True
    assert r3.total_prepares == 3


def test_warm_distribution_all_hits_with_zero_budget():
    qs = [q(i, deadline=2) for i in range(3)]
    cold = plan_batch(qs, [[], []], 3)
    assert cold.feasible
    warm = plan_batch(qs, cold.cache_after, 0)
    assert warm.feasible
    assert warm.total_prepares == 0
    assert all(s.cache_hit for s in warm.steps)


def test_deadline_physically_impossible():
    # 3 jobs of length 2 on 2 conns need makespan 4; deadline 2 impossible.
    qs = [q(i, key="A", duration=2, deadline=2) for i in range(3)]
    r = plan_batch(qs, [[], []], 8)
    assert r.feasible is False
    assert r.reason == "deadline_infeasible"
    assert r.best_deadline_feasible is None


def test_prepare_enables_parallelism_to_meet_deadline():
    # 3 x same SQL cached only on conn0. Zero budget => serial on conn0 ends
    # at 6 > deadline 4. One fresh PREPARE on conn1 gives 2+1 packing => 4.
    qs = [q(i, key="A", duration=2, deadline=4) for i in range(3)]
    no_budget = plan_batch(qs, [["A"], []], 0)
    assert no_budget.feasible is False
    assert no_budget.reason == "prepare_budget_exceeded"
    assert no_budget.minimum_prepares_among_deadline_feasible == 1

    ok = plan_batch(qs, [["A"], []], 1)
    assert ok.feasible
    assert ok.total_prepares == 1
    misses = [s for s in ok.steps if not s.cache_hit]
    assert len(misses) == 1
    assert misses[0].connection == 1


def test_arrival_time_respected():
    # arrival 5, duration 2 -> earliest end 7; deadline 6 impossible, 7 ok.
    impossible = plan_batch([q(0, arrival=5, duration=2, deadline=6)], [[], []], 1)
    assert impossible.feasible is False
    ok = plan_batch([q(0, arrival=5, duration=2, deadline=7)], [[], []], 1)
    assert ok.feasible
    step = ok.steps[0]
    assert step.start_tick == 5
    assert step.end_tick == 7


def test_inclusive_deadline_boundary():
    # duration 1 at arrival 0: end=1, last_busy=0.
    strict = plan_batch([q(0, duration=1, deadline=0)], [[], []], 1)
    assert strict.feasible is False
    inclusive = plan_batch(
        [q(0, duration=1, deadline=0, inclusive=True)], [[], []], 1
    )
    assert inclusive.feasible


def test_lru_eviction_causes_reprepare_on_same_connection():
    # A B C A on one connection, cap 2:
    # A prepare, B prepare (cache A,B), C prepare evicts A (B,C),
    # A missing again -> prepare evicts B. Total 4 PREPARE.
    qs = tuple(q(i, key=k) for i, k in enumerate(["A", "B", "C", "A"]))
    res = _simulate_connection(tuple(range(4)), qs, [], cap=2)
    assert res is not None
    assert res.prepares == 4
    assert res.final_lru == ["C", "A"]
    evictions = [step[6] for step in res.light]
    assert evictions == [None, None, "A", "B"]


def test_cross_batch_lru_state_is_used():
    # conn0 already has A (LRU) and B (MRU); query A should hit and become MRU.
    qs = [q(0, key="A")]
    r = plan_batch(qs, [["B", "A"], []], 0)
    assert r.feasible
    assert r.steps[0].cache_hit
    assert r.cache_after[0][-1] == "A"


def test_eight_queries_enumerates_quickly():
    qs = [q(i, deadline=100) for i in range(8)]
    t0 = time.time()
    r = plan_batch(qs, [[], []], 8)
    elapsed = time.time() - t0
    assert r.feasible
    assert r.total_prepares == 8
    assert elapsed < 10.0  # generous upper bound even on slow CI


# ---------------------------------------------------------------------------
# Resume planning after a confirmed mid-batch disconnect
# ---------------------------------------------------------------------------
from app.planner import plan_resume  # noqa: E402


def test_resume_keeps_original_start_as_lower_bound():
    # Two pending queries originally scheduled at ticks 3 and 3; even with an
    # idle connection and arrival 0 they must not start earlier than 3.
    qs = [q(0, key="A", arrival=0, duration=1, deadline=5),
          q(1, key="B", arrival=0, duration=1, deadline=6)]
    r = plan_resume(
        qs,
        current_lrus=[[], []],
        free_at=[0, 0],
        original_starts={0: 3, 1: 3},
        remaining_prepares=2,
    )
    assert r.feasible
    for step in r.steps:
        assert step.start_tick >= 3


def test_resume_respects_occupied_ticks_of_completed_work():
    # Connection 0 is occupied until tick 4 by already-finished work.
    qs = [q(0, key="A", duration=2, deadline=8)]
    r = plan_resume(
        qs,
        current_lrus=[["A"], []],
        free_at=[4, 0],
        original_starts={0: 4},
        remaining_prepares=0,
    )
    assert r.feasible
    step = r.steps[0]
    # Cache hit only exists on conn 0, so it must wait there until tick 4.
    assert step.connection == 0
    assert step.start_tick == 4 and step.end_tick == 6
    assert step.cache_hit and step.prepare_count == 0


def test_resume_can_move_work_to_surviving_cold_slot():
    # Faulted slot 0 reconnects cold; slot 1 already caches A. Zero extra
    # PREPARE is possible only by moving the pending A query onto slot 1.
    qs = [q(0, key="A", duration=1, deadline=5)]
    r = plan_resume(
        qs,
        current_lrus=[[], ["A"]],
        free_at=[2, 2],
        original_starts={0: 2},
        remaining_prepares=0,
    )
    assert r.feasible
    assert r.steps[0].connection == 1
    assert r.steps[0].cache_hit
    assert r.total_prepares == 0


def test_resume_budget_exhausted_is_infeasible_with_diagnostic():
    # Cold caches need 1 PREPARE but the remaining budget is 0.
    qs = [q(0, key="A", duration=1, deadline=5)]
    r = plan_resume(
        qs,
        current_lrus=[[], []],
        free_at=[0, 0],
        original_starts={0: 0},
        remaining_prepares=0,
    )
    assert r.feasible is False
    assert r.reason == "resume_prepare_budget_exceeded"
    assert r.minimum_prepares_among_deadline_feasible == 1


def test_resume_deadline_impossible_after_fault():
    # Original start lower bound 4 plus duration 2 ends at 6 > deadline 5.
    qs = [q(0, key="A", duration=2, deadline=5)]
    r = plan_resume(
        qs,
        current_lrus=[["A"], []],
        free_at=[0, 0],
        original_starts={0: 4},
        remaining_prepares=0,
    )
    assert r.feasible is False
    assert r.reason == "resume_deadline_infeasible"
    assert r.best_deadline_feasible is None


def test_resume_arrival_tick_still_guarded():
    qs = [q(0, key="A", arrival=5, duration=1, deadline=6)]
    r = plan_resume(
        qs,
        current_lrus=[["A"], []],
        free_at=[0, 0],
        original_starts={0: 0},
        remaining_prepares=0,
    )
    assert r.feasible
    assert r.steps[0].start_tick == 5
