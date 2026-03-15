
from __future__ import annotations

import os
import re
import sqlite3
from contextlib import closing
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict

from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field, ValidationError

from ..contracts import StoryRequest, StoryResult
from ..utils import (
    build_chat_openai,
    extract_explicit_member_id,
    normalize_member_id,
    register_sqlite_alnum_normalizer,
)

PROJECT_PHASE_3 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_3 / "kb" / "DataScience" / "peloton_workouts.sqlite"

MIN_PEER_MEMBERS = 5
MIN_PEER_WORKOUTS = 30

PeerScope = Literal["all_members", "same_primary_type", "similar_activity_band"]
MetricName = Literal["weekly_workouts", "avg_session_length_min", "consistency_ratio"]
TimeframeLabel = Literal["last_4_weeks", "last_8_weeks", "last_12_weeks"]
SlotName = Literal["member_id", "peer_definition", "metrics", "timeframe"]


class PeerDefinition(BaseModel):
    scope: PeerScope
    rationale: str = ""
    primary_type: Optional[Literal["cycling", "tread", "rowing", "strength", "yoga"]] = None
    activity_band: Optional[Literal["low", "medium", "high"]] = None


class MetricSelection(BaseModel):
    selected: List[MetricName] = Field(min_length=1, max_length=3)
    inferred: List[MetricName] = Field(default_factory=list)


class PlanOutput(BaseModel):
    member_id: Optional[str] = None
    timeframe: TimeframeLabel = "last_8_weeks"
    peer_definition: PeerDefinition
    metrics: MetricSelection
    assumptions: List[str] = Field(default_factory=list, max_length=5)
    ambiguities: List[SlotName] = Field(default_factory=list, max_length=3)
    needs_clarification: bool = False
    requested_slot: Optional[SlotName] = None
    clarifying_question: Optional[str] = None
    planner_confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class PeerBenchmarkState(TypedDict, total=False):
    user_text: str
    fallback_member: Optional[str]
    prior_plan: Dict[str, Any]
    prior_pending_slot: Optional[str]
    plan: Dict[str, Any]
    start_date: str
    end_date: str
    timeframe_label: str
    member_metrics: Dict[str, Any]
    peer_benchmarks: Dict[str, Any]
    benchmark_availability: Dict[str, bool]
    comparisons: Dict[str, Dict[str, Optional[float]]]
    strengths: List[str]
    primary_gap_metric: Optional[str]
    suggestions: List[Dict[str, str]]
    response_text: str
    follow_up_question: Optional[str]
    planner_source: str
    uncertainty_score: float
    uncertainty_signals: Dict[str, Any]
    llm_plan_attempted: bool


METRIC_LABELS = {
    "weekly_workouts": "weekly workouts",
    "avg_session_length_min": "average session length",
    "consistency_ratio": "consistency",
}

METRIC_KEYWORDS = {
    "weekly_workouts": ["weekly workouts", "workouts per week", "frequency", "how often"],
    "avg_session_length_min": ["session length", "duration", "minutes", "longer sessions"],
    "consistency_ratio": ["consistency", "consistent", "active weeks", "regularity"],
}

DEFAULT_METRICS: List[MetricName] = ["weekly_workouts", "avg_session_length_min", "consistency_ratio"]
PRIORITY_METRICS: List[MetricName] = ["weekly_workouts", "consistency_ratio"]
HYBRID_UNCERTAINTY_THRESHOLD = 0.45
LLM_MIN_CONFIDENCE = 0.55


def _week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _canonical_workout_type(raw: Optional[str]) -> Optional[str]:
    value = (raw or "").strip().lower()
    if not value:
        return None
    mapping = {
        "bike": "cycling",
        "cycling": "cycling",
        "ride": "cycling",
        "tread": "tread",
        "run": "tread",
        "running": "tread",
        "treadmill": "tread",
        "row": "rowing",
        "rowing": "rowing",
        "strength": "strength",
        "yoga": "yoga",
    }
    return mapping.get(value, value)


def _contains_phrase(text: str, phrase: str) -> bool:
    return phrase.lower() in text.lower()


def _infer_timeframe_from_text(user_text: str) -> TimeframeLabel:
    tl = (user_text or "").lower()
    m = re.search(r"last\s+(\d+)\s+weeks?", tl)
    if m:
        n = int(m.group(1))
        if n <= 4:
            return "last_4_weeks"
        if n <= 8:
            return "last_8_weeks"
        return "last_12_weeks"
    if "last month" in tl:
        return "last_4_weeks"
    if "recently" in tl or "recent" in tl:
        return "last_4_weeks"
    if "last 3 months" in tl or "past 3 months" in tl or "quarter" in tl:
        return "last_12_weeks"
    return "last_8_weeks"


def _timeframe_dates(label: TimeframeLabel) -> Tuple[str, str]:
    weeks_by_label = {
        "last_4_weeks": 4,
        "last_8_weeks": 8,
        "last_12_weeks": 12,
    }
    weeks = weeks_by_label[label]
    end = date.today()
    start = end - timedelta(days=weeks * 7)
    return start.isoformat(), end.isoformat()


def _parse_metrics_from_text(user_text: str) -> Tuple[List[MetricName], List[MetricName], bool]:
    tl = (user_text or "").lower()
    selected: List[MetricName] = []

    for metric, terms in METRIC_KEYWORDS.items():
        if any(_contains_phrase(tl, term) for term in terms):
            selected.append(metric)  # type: ignore[arg-type]

    selected = sorted(set(selected), key=selected.index)
    mentions_generic_metrics = any(
        _contains_phrase(tl, term)
        for term in [
            "metric",
            "metrics",
            "performance metrics",
            "right metrics",
            "what matters most",
            "focus on what matters",
            "key metrics",
        ]
    )
    hedged_metric_request = any(
        _contains_phrase(tl, term)
        for term in [
            "or something",
            "maybe by",
            "maybe",
            "not sure which metrics",
        ]
    )
    ambiguous = (mentions_generic_metrics or hedged_metric_request) and not selected

    if not selected and not ambiguous:
        return list(DEFAULT_METRICS), list(DEFAULT_METRICS), False
    if not selected and ambiguous:
        return list(DEFAULT_METRICS), list(DEFAULT_METRICS), True

    return selected[:3], selected[:3], False


def _parse_peer_definition_from_text(user_text: str) -> Tuple[PeerDefinition, bool]:
    tl = (user_text or "").lower()
    workout_types = ["cycling", "tread", "rowing", "strength", "yoga"]

    explicit_type = None
    for t in workout_types:
        if re.search(rf"\b{re.escape(t)}\b", tl):
            explicit_type = t
            break
    if not explicit_type:
        if any(k in tl for k in ["bike", "ride"]):
            explicit_type = "cycling"
        elif any(k in tl for k in ["run", "running", "treadmill"]):
            explicit_type = "tread"

    not_everyone = any(k in tl for k in ["not everyone", "not all members", "not all peers"])
    if not_everyone:
        return (
            PeerDefinition(
                scope="similar_activity_band",
                rationale="User explicitly excluded broad all-members cohort.",
            ),
            False,
        )

    if any(k in tl for k in ["all members", "everyone", "overall peers", "all peers"]):
        return PeerDefinition(scope="all_members", rationale="User asked for broad peer baseline."), False

    if any(k in tl for k in ["same type", "same workout type", "same class type"]):
        return (
            PeerDefinition(
                scope="same_primary_type",
                primary_type=explicit_type,  # type: ignore[arg-type]
                rationale="User requested same workout-type peers.",
            ),
            False,
        )

    if any(k in tl for k in ["similar activity", "similar frequency", "peers like me", "similar peers"]):
        return (
            PeerDefinition(
                scope="similar_activity_band",
                rationale="User requested peers with similar activity level.",
            ),
            False,
        )

    if any(k in tl for k in ["similar users", "people like me", "like me", "fair comparison", "fair peer"]):
        return (
            PeerDefinition(
                scope="similar_activity_band",
                rationale="User requested a fair comparison to similar users.",
            ),
            False,
        )

    if "cohort" in tl:
        return (
            PeerDefinition(
                scope="similar_activity_band",
                rationale="User referenced cohort but did not define cohort type.",
            ),
            True,
        )

    if "peer" in tl or "peers" in tl:
        return (
            PeerDefinition(scope="all_members", rationale="Peer scope not specified; using temporary default."),
            True,
        )

    return PeerDefinition(scope="all_members", rationale="Defaulted peer scope to all members."), True

def _deterministic_plan(user_text: str, fallback_member: Optional[str]) -> PlanOutput:
    member_id = normalize_member_id(extract_explicit_member_id(user_text) or fallback_member)
    timeframe = _infer_timeframe_from_text(user_text)
    peer_definition, peer_ambiguous = _parse_peer_definition_from_text(user_text)
    selected_metrics, inferred_metrics, metrics_ambiguous = _parse_metrics_from_text(user_text)

    assumptions: List[str] = []
    ambiguities: List[SlotName] = []

    if not extract_explicit_member_id(user_text) and fallback_member:
        assumptions.append(f"Used member_id from conversation context: {fallback_member}.")
    if timeframe == "last_8_weeks" and "last" not in user_text.lower():
        assumptions.append("Used default timeframe: last 8 weeks.")
    if peer_definition.scope == "all_members":
        assumptions.append("Used default peer cohort: all members.")
    if inferred_metrics == DEFAULT_METRICS and not any(m in user_text.lower() for m in ["weekly", "duration", "consistency"]):
        assumptions.append("Used default metrics: weekly workouts, session length, consistency.")

    needs_clarification = False
    requested_slot: Optional[SlotName] = None
    clarifying_question: Optional[str] = None

    if member_id is None:
        needs_clarification = True
        requested_slot = "member_id"
        clarifying_question = "What is your member_id (for example, MB001)?"
        ambiguities.append("member_id")
    elif metrics_ambiguous:
        needs_clarification = True
        requested_slot = "metrics"
        clarifying_question = (
            "Which metrics should I compare: weekly workouts, session length, consistency, or all three?"
        )
        ambiguities.append("metrics")
    elif peer_ambiguous:
        needs_clarification = True
        requested_slot = "peer_definition"
        clarifying_question = (
            "How should I define peers: all members, same workout type, or similar activity level?"
        )
        ambiguities.append("peer_definition")

    return PlanOutput(
        member_id=member_id,
        timeframe=timeframe,
        peer_definition=peer_definition,
        metrics=MetricSelection(selected=selected_metrics, inferred=inferred_metrics),
        assumptions=assumptions[:5],
        ambiguities=ambiguities,
        needs_clarification=needs_clarification,
        requested_slot=requested_slot,
        clarifying_question=clarifying_question,
        planner_confidence=0.9 if not needs_clarification else 0.6,
    )


def _maybe_llm_plan(user_text: str, fallback_member: Optional[str]) -> Optional[PlanOutput]:
    if os.getenv("PROTOTYPE_DS3_USE_LLM_PLAN", "0").strip() not in {"1", "true", "TRUE"}:
        return None

    system = (
        "You are planning a peer-benchmark workout comparison request. "
        "Return structured output that conforms to PlanOutput. "
        "If the request is ambiguous, set needs_clarification=true and ask exactly one concise question."
    )
    user = (
        f"USER_QUERY: {user_text}\n"
        f"FALLBACK_MEMBER_ID: {fallback_member}\n"
        "Valid metrics: weekly_workouts, avg_session_length_min, consistency_ratio\n"
        "Valid timeframe labels: last_4_weeks, last_8_weeks, last_12_weeks\n"
        "Valid peer scopes: all_members, same_primary_type, similar_activity_band"
    )

    try:
        llm = build_chat_openai(model=os.getenv("PROTOTYPE_DS3_PLAN_MODEL", "gpt-4o-mini"), temperature=0)
        structured = llm.with_structured_output(PlanOutput)
        out = structured.invoke([("system", system), ("user", user)])
        return out
    except Exception:
        return None


def _estimate_uncertainty(plan: PlanOutput, user_text: str, pending_slot: Optional[str] = None) -> Tuple[float, Dict[str, Any]]:
    tl = (user_text or "").lower()
    score = 0.0

    signals: Dict[str, Any] = {
        "member_id_missing": plan.member_id is None,
        "metrics_ambiguous": "metrics" in plan.ambiguities,
        "peer_definition_ambiguous": "peer_definition" in plan.ambiguities,
        "timeframe_ambiguous": "timeframe" in plan.ambiguities,
        "pending_slot_active": bool(pending_slot),
        "low_phrase_coverage": False,
    }

    if signals["member_id_missing"]:
        score += 0.45
    if signals["metrics_ambiguous"]:
        score += 0.30
    if signals["peer_definition_ambiguous"]:
        score += 0.25
    if signals["timeframe_ambiguous"]:
        score += 0.20
    if signals["pending_slot_active"]:
        score += 0.15

    phrase_hits = 0
    metric_terms = [term for terms in METRIC_KEYWORDS.values() for term in terms]
    peer_terms = [
        "all members",
        "same workout type",
        "same type",
        "similar activity",
        "similar frequency",
        "last 4 weeks",
        "last 8 weeks",
        "last 12 weeks",
        "last month",
        "last 3 months",
    ]
    for term in metric_terms + peer_terms:
        if _contains_phrase(tl, term):
            phrase_hits += 1

    if phrase_hits == 0:
        score += 0.20
        signals["low_phrase_coverage"] = True
    elif phrase_hits == 1:
        score += 0.10
        signals["low_phrase_coverage"] = True

    signals["phrase_hits"] = phrase_hits
    score = max(0.0, min(1.0, round(score, 3)))
    return score, signals


def _llm_plan_contradictory(plan: PlanOutput) -> bool:
    if plan.needs_clarification and not plan.requested_slot:
        return True
    if not plan.needs_clarification and plan.requested_slot is not None:
        return True
    if plan.member_id is None and not plan.needs_clarification:
        return True
    if plan.requested_slot and plan.requested_slot not in plan.ambiguities:
        return True
    return False


def _slot_question(slot: SlotName) -> str:
    if slot == "member_id":
        return "What is your member_id (for example, MB001)?"
    if slot == "metrics":
        return "Which metrics should I compare: weekly workouts, session length, consistency, or all three?"
    if slot == "peer_definition":
        return "How should I define peers: all members, same workout type, or similar activity level?"
    return "What timeframe should I use: last 4, 8, or 12 weeks?"


def _choose_requested_slot(ambiguities: List[SlotName]) -> Optional[SlotName]:
    priority: List[SlotName] = ["member_id", "metrics", "peer_definition", "timeframe"]
    for slot in priority:
        if slot in ambiguities:
            return slot
    return None


def _merge_pending_slot_answer(plan: PlanOutput, pending_slot: Optional[str], answer_text: str) -> PlanOutput:
    if pending_slot not in {"member_id", "metrics", "peer_definition", "timeframe"}:
        return plan

    updated = plan.model_copy(deep=True)
    answer = answer_text or ""

    if pending_slot == "member_id":
        member = normalize_member_id(extract_explicit_member_id(answer) or answer)
        if member:
            updated.member_id = member
            updated.assumptions = [*updated.assumptions, "Used member_id from clarification."][:5]
            updated.needs_clarification = False
            updated.requested_slot = None
            updated.clarifying_question = None
            updated.ambiguities = [a for a in updated.ambiguities if a != "member_id"]
        return updated

    if pending_slot == "metrics":
        selected, inferred, ambiguous = _parse_metrics_from_text(answer)
        if not ambiguous:
            updated.metrics = MetricSelection(selected=selected, inferred=inferred)
            updated.assumptions = [
                a for a in updated.assumptions if not a.startswith("Used default metrics:")
            ]
            updated.assumptions = [*updated.assumptions, "Applied metrics from user clarification."][:5]
            updated.needs_clarification = False
            updated.requested_slot = None
            updated.clarifying_question = None
            updated.ambiguities = [a for a in updated.ambiguities if a != "metrics"]
        return updated

    if pending_slot == "peer_definition":
        peer_def, _ = _parse_peer_definition_from_text(answer)
        updated.peer_definition = peer_def
        updated.assumptions = [*updated.assumptions, "Applied peer definition from user clarification."][:5]
        updated.needs_clarification = False
        updated.requested_slot = None
        updated.clarifying_question = None
        updated.ambiguities = [a for a in updated.ambiguities if a != "peer_definition"]
        return updated

    if pending_slot == "timeframe":
        updated.timeframe = _infer_timeframe_from_text(answer)
        updated.assumptions = [*updated.assumptions, "Applied timeframe from user clarification."][:5]
        updated.needs_clarification = False
        updated.requested_slot = None
        updated.clarifying_question = None
        updated.ambiguities = [a for a in updated.ambiguities if a != "timeframe"]
        return updated

    return updated


def _to_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    try:
        return datetime.fromisoformat(str(v)).date()
    except ValueError:
        return None


def _activity_band(weekly_workouts: float) -> Literal["low", "medium", "high"]:
    if weekly_workouts < 2.0:
        return "low"
    if weekly_workouts < 4.0:
        return "medium"
    return "high"


def _metric_bundle_from_rows(rows: List[Dict[str, Any]], start_date: str, end_date: str) -> Dict[str, float]:
    start = datetime.fromisoformat(start_date).date()
    end = datetime.fromisoformat(end_date).date()
    total_days = max((end - start).days + 1, 1)
    total_weeks = max(round(total_days / 7.0, 2), 1.0)

    workout_count = len(rows)
    durations = [float(r.get("duration_min")) for r in rows if r.get("duration_min") is not None]
    avg_session_length = (sum(durations) / len(durations)) if durations else 0.0

    active_weeks = set()
    for r in rows:
        d = _to_date(r.get("date"))
        if not d:
            continue
        active_weeks.add(_week_start(d).isoformat())

    consistency_ratio = (len(active_weeks) / total_weeks) if total_weeks else 0.0
    return {
        "workout_count": workout_count,
        "total_weeks": total_weeks,
        "weekly_workouts": round(workout_count / total_weeks, 3),
        "avg_session_length_min": round(avg_session_length, 3),
        "consistency_ratio": round(consistency_ratio, 3),
    }

def _read_member_rows(member_id: str, start_date: str, end_date: str) -> List[Dict[str, Any]]:
    sql = """
    SELECT member_id, date, type, duration_min
    FROM workouts
    WHERE NORM_ALNUM(member_id) = NORM_ALNUM(?)
      AND date >= ?
      AND date <= ?
    """
    with closing(sqlite3.connect(DB_PATH)) as conn:
        register_sqlite_alnum_normalizer(conn)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, [member_id, start_date, end_date]).fetchall()
    return [dict(r) for r in rows]


def _read_peer_rows(start_date: str, end_date: str, exclude_member_id: str) -> List[Dict[str, Any]]:
    sql = """
    SELECT member_id, date, type, duration_min
    FROM workouts
    WHERE NORM_ALNUM(member_id) <> NORM_ALNUM(?)
      AND date >= ?
      AND date <= ?
    """
    with closing(sqlite3.connect(DB_PATH)) as conn:
        register_sqlite_alnum_normalizer(conn)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, [exclude_member_id, start_date, end_date]).fetchall()
    return [dict(r) for r in rows]


def read_member_metrics(member_id: str, start_date: str, end_date: str) -> Dict[str, Any]:
    rows = _read_member_rows(member_id=member_id, start_date=start_date, end_date=end_date)
    metrics = _metric_bundle_from_rows(rows, start_date=start_date, end_date=end_date)

    type_counts: Dict[str, int] = {}
    for row in rows:
        typ = _canonical_workout_type(str(row.get("type") or ""))
        if not typ:
            continue
        type_counts[typ] = type_counts.get(typ, 0) + 1

    primary_type = max(type_counts.items(), key=lambda kv: kv[1])[0] if type_counts else None

    return {
        "member_id": member_id,
        "start_date": start_date,
        "end_date": end_date,
        "has_data": bool(rows),
        "primary_type": primary_type,
        **metrics,
    }


def _filter_rows_by_peer_definition(
    rows: List[Dict[str, Any]],
    peer_definition: PeerDefinition,
    member_primary_type: Optional[str],
    member_activity_band: Optional[str],
    start_date: str,
    end_date: str,
) -> List[Dict[str, Any]]:
    scope = peer_definition.scope
    if scope == "all_members":
        return rows

    if scope == "same_primary_type":
        target = _canonical_workout_type(peer_definition.primary_type or member_primary_type)
        if not target:
            return rows
        return [r for r in rows if _canonical_workout_type(str(r.get("type") or "")) == str(target).lower()]

    # similar_activity_band
    if scope == "similar_activity_band" and member_activity_band:
        grouped: Dict[str, List[Dict[str, Any]]] = {}
        for r in rows:
            mid = str(r.get("member_id") or "").strip()
            if not mid:
                continue
            grouped.setdefault(mid, []).append(r)

        keep_member_ids: set[str] = set()
        for mid, mrows in grouped.items():
            metrics = _metric_bundle_from_rows(mrows, start_date=start_date, end_date=end_date)
            band = _activity_band(float(metrics.get("weekly_workouts", 0.0)))
            if band == member_activity_band:
                keep_member_ids.add(mid)
        return [r for r in rows if str(r.get("member_id") or "") in keep_member_ids]

    return rows


def _read_peer_aggregate_metrics(
    member_id: str,
    start_date: str,
    end_date: str,
    peer_definition: PeerDefinition,
    selected_metrics: List[MetricName],
    member_primary_type: Optional[str],
    member_activity_band: Optional[str],
) -> Dict[str, Any]:
    raw_rows = _read_peer_rows(start_date=start_date, end_date=end_date, exclude_member_id=member_id)
    scoped_rows = _filter_rows_by_peer_definition(
        rows=raw_rows,
        peer_definition=peer_definition,
        member_primary_type=member_primary_type,
        member_activity_band=member_activity_band,
        start_date=start_date,
        end_date=end_date,
    )

    by_member: Dict[str, List[Dict[str, Any]]] = {}
    for row in scoped_rows:
        mid = str(row.get("member_id") or "").strip()
        if not mid:
            continue
        by_member.setdefault(mid, []).append(row)

    peer_member_count = len(by_member)
    peer_workout_count = len(scoped_rows)

    availability = {k: False for k in ["weekly_workouts", "avg_session_length_min", "consistency_ratio"]}
    if peer_member_count < MIN_PEER_MEMBERS or peer_workout_count < MIN_PEER_WORKOUTS:
        return {
            "peer_member_count": peer_member_count,
            "peer_workout_count": peer_workout_count,
            "availability": availability,
            "benchmarks": {},
            "limitation": "Insufficient aggregated peer benchmark data for this cohort/timeframe.",
        }

    per_member_metrics = [
        _metric_bundle_from_rows(member_rows, start_date=start_date, end_date=end_date)
        for member_rows in by_member.values()
    ]

    benchmarks: Dict[str, float] = {}
    for metric in selected_metrics:
        vals = [float(m.get(metric, 0.0)) for m in per_member_metrics]
        benchmarks[metric] = round(sum(vals) / len(vals), 3) if vals else 0.0
        availability[metric] = True

    return {
        "peer_member_count": peer_member_count,
        "peer_workout_count": peer_workout_count,
        "availability": availability,
        "benchmarks": benchmarks,
    }


def compare_to_peers(
    member_data: Dict[str, Any],
    peer_data: Dict[str, Any],
    selected_metrics: List[MetricName],
) -> Dict[str, Any]:
    comparisons: Dict[str, Dict[str, Optional[float]]] = {}
    strengths: List[str] = []
    primary_gap_metric: Optional[str] = None

    max_positive_score: Optional[float] = None
    min_negative_score: Optional[float] = None

    peer_benchmarks = peer_data.get("benchmarks", {})
    availability = peer_data.get("availability", {})

    for metric in selected_metrics:
        if not availability.get(metric):
            comparisons[metric] = {
                "member_value": float(member_data.get(metric, 0.0)),
                "peer_value": None,
                "delta": None,
                "pct_delta": None,
            }
            continue

        member_value = float(member_data.get(metric, 0.0))
        peer_value = float(peer_benchmarks.get(metric, 0.0))
        delta = round(member_value - peer_value, 3)
        pct_delta = round((delta / peer_value) * 100.0, 2) if peer_value > 0 else None

        comparisons[metric] = {
            "member_value": member_value,
            "peer_value": peer_value,
            "delta": delta,
            "pct_delta": pct_delta,
        }

        score = pct_delta if isinstance(pct_delta, (int, float)) else delta
        if isinstance(score, (int, float)) and score > 0 and (
            max_positive_score is None or score > max_positive_score
        ):
            max_positive_score = score
            strengths = [metric]
        elif isinstance(score, (int, float)) and score > 0 and max_positive_score is not None and score == max_positive_score:
            strengths.append(metric)

        if isinstance(score, (int, float)) and score < 0 and (
            min_negative_score is None or score < min_negative_score
        ):
            min_negative_score = score
            primary_gap_metric = metric

    return {
        "comparisons": comparisons,
        "strengths": strengths,
        "primary_gap_metric": primary_gap_metric,
    }


def generate_improvement_suggestions(gaps: Dict[str, Any]) -> List[Dict[str, str]]:
    suggestions: List[Dict[str, str]] = []
    comparisons = gaps.get("comparisons", {})

    weekly_delta = (comparisons.get("weekly_workouts") or {}).get("delta")
    if isinstance(weekly_delta, (int, float)) and weekly_delta < 0:
        suggestions.append(
            {
                "metric": "weekly_workouts",
                "suggestion": "Try adding one short workout block each week to build consistency gradually.",
            }
        )

    length_delta = (comparisons.get("avg_session_length_min") or {}).get("delta")
    if isinstance(length_delta, (int, float)) and length_delta < 0:
        suggestions.append(
            {
                "metric": "avg_session_length_min",
                "suggestion": "Consider extending a few sessions by 5-10 minutes when it feels manageable.",
            }
        )

    consistency_delta = (comparisons.get("consistency_ratio") or {}).get("delta")
    if isinstance(consistency_delta, (int, float)) and consistency_delta < 0:
        suggestions.append(
            {
                "metric": "consistency_ratio",
                "suggestion": "A repeatable weekly schedule can help you keep active weeks more consistent.",
            }
        )

    if suggestions:
        return suggestions

    # If no gaps are below benchmark, provide a concrete optimization target
    # for the narrowest positive margin instead of a generic "keep routine" message.
    smallest_positive_metric: Optional[str] = None
    smallest_positive_score: Optional[float] = None
    for metric, row in comparisons.items():
        delta = row.get("delta")
        pct_delta = row.get("pct_delta")
        if not isinstance(delta, (int, float)):
            continue
        if delta < 0:
            continue
        score = pct_delta if isinstance(pct_delta, (int, float)) else delta
        if not isinstance(score, (int, float)):
            continue
        if smallest_positive_score is None or score < smallest_positive_score:
            smallest_positive_score = score
            smallest_positive_metric = metric

    if smallest_positive_metric == "weekly_workouts":
        suggestions.append(
            {
                "metric": "weekly_workouts",
                "suggestion": "Your workout frequency is only slightly above peers; protect that edge with one pre-scheduled session each week.",
            }
        )
    elif smallest_positive_metric == "avg_session_length_min":
        suggestions.append(
            {
                "metric": "avg_session_length_min",
                "suggestion": "Session length is close to peers; try extending one workout by 5-10 minutes each week.",
            }
        )
    elif smallest_positive_metric == "consistency_ratio":
        suggestions.append(
            {
                "metric": "consistency_ratio",
                "suggestion": "Consistency is your narrowest margin; anchor 2-3 fixed workout days to sustain active weeks.",
            }
        )

    return suggestions

def _format_metric_line(metric: str, comparison: Dict[str, Optional[float]]) -> str:
    member_value = comparison.get("member_value")
    peer_value = comparison.get("peer_value")
    delta = comparison.get("delta")
    if peer_value is None or delta is None:
        return f"- {METRIC_LABELS.get(metric, metric)}: benchmark unavailable for this cohort/timeframe."

    direction = "above" if delta > 0 else "below" if delta < 0 else "aligned with"
    return (
        f"- {METRIC_LABELS.get(metric, metric)}: you={member_value}, peers={peer_value} "
        f"({abs(delta)} {direction} benchmark)."
    )


def _plan_request_node(state: PeerBenchmarkState) -> PeerBenchmarkState:
    user_text = state.get("user_text", "")
    fallback_member = state.get("fallback_member")
    pending_slot = state.get("prior_pending_slot")

    deterministic = _deterministic_plan(user_text=user_text, fallback_member=fallback_member)
    uncertainty_score, uncertainty_signals = _estimate_uncertainty(
        plan=deterministic,
        user_text=user_text,
        pending_slot=pending_slot,
    )
    deterministic.planner_confidence = round(1.0 - uncertainty_score, 2)

    planner_source = "deterministic_fallback"
    llm_plan_attempted = False
    plan_model = deterministic

    llm_enabled = os.getenv("PROTOTYPE_DS3_USE_LLM_PLAN", "0").strip() in {"1", "true", "TRUE"}
    llm_min_conf = float(os.getenv("PROTOTYPE_DS3_LLM_MIN_CONF", str(LLM_MIN_CONFIDENCE)))

    # Run LLM planning whenever enabled on first-pass turns.
    # Keep deterministic planning during clarification-resume turns.
    if llm_enabled and pending_slot is None:
        llm_plan_attempted = True
        llm_plan = _maybe_llm_plan(user_text=user_text, fallback_member=fallback_member)
        if llm_plan is None:
            planner_source = "deterministic_fallback_llm_unavailable"
        elif _llm_plan_contradictory(llm_plan):
            planner_source = "deterministic_fallback_llm_contradictory"
        elif float(llm_plan.planner_confidence) < llm_min_conf:
            planner_source = "deterministic_fallback_llm_low_confidence"
        else:
            planner_source = "llm"
            plan_model = llm_plan

    return {
        "plan": plan_model.model_dump(),
        "planner_source": planner_source,
        "uncertainty_score": uncertainty_score,
        "uncertainty_signals": uncertainty_signals,
        "llm_plan_attempted": llm_plan_attempted,
    }


def _merge_user_answer_node(state: PeerBenchmarkState) -> PeerBenchmarkState:
    plan_dict = state.get("plan") or {}
    prior_plan_dict = state.get("prior_plan") or {}
    user_text = state.get("user_text", "")
    pending_slot = state.get("prior_pending_slot")

    # On clarification-resume turns, merge into the previously saved plan.
    if pending_slot in {"member_id", "metrics", "peer_definition", "timeframe"} and isinstance(prior_plan_dict, dict) and prior_plan_dict:
        try:
            plan = PlanOutput.model_validate(prior_plan_dict)
        except ValidationError:
            try:
                plan = PlanOutput.model_validate(plan_dict)
            except ValidationError:
                return {}
    else:
        try:
            plan = PlanOutput.model_validate(plan_dict)
        except ValidationError:
            return {}

    if pending_slot in {"member_id", "metrics", "peer_definition", "timeframe"}:
        plan = _merge_pending_slot_answer(plan=plan, pending_slot=pending_slot, answer_text=user_text)
    return {"plan": plan.model_dump()}


def _validate_plan_node(state: PeerBenchmarkState) -> PeerBenchmarkState:
    plan_dict = state.get("plan") or {}
    user_text = state.get("user_text", "")
    tl = user_text.lower()
    try:
        plan = PlanOutput.model_validate(plan_dict)
    except ValidationError:
        fallback = _deterministic_plan(user_text=user_text, fallback_member=state.get("fallback_member"))
        plan = fallback

    deterministic_metrics_selected, _, deterministic_metrics_ambiguous = _parse_metrics_from_text(user_text)
    deterministic_peer, deterministic_peer_ambiguous = _parse_peer_definition_from_text(user_text)

    if deterministic_metrics_ambiguous and "metrics" not in plan.ambiguities:
        plan.ambiguities.append("metrics")

    explicit_peer_scope_phrase = any(
        k in tl
        for k in [
            "all members",
            "everyone",
            "all peers",
            "overall peers",
            "same type",
            "same workout type",
            "same class type",
            "similar activity",
            "similar frequency",
            "similar users",
            "people like me",
            "peers like me",
            "similar peers",
            "fair comparison",
            "fair peer",
            "not everyone",
            "not all members",
            "not all peers",
        ]
    )

    if "cohort" in tl and not explicit_peer_scope_phrase and "peer_definition" not in plan.ambiguities:
        plan.ambiguities.append("peer_definition")

    # Hard negation guardrail: do not allow broad cohort when user excludes everyone/all-members.
    if any(k in tl for k in ["not everyone", "not all members", "not all peers"]) and plan.peer_definition.scope == "all_members":
        plan.peer_definition = deterministic_peer

    # For requests that ask to focus on key metrics, narrow to two metrics by policy.
    if any(k in tl for k in ["what matters most", "focus on what matters", "most important", "key metrics"]):
        explicit_all_three = any(k in tl for k in ["all three", "all metrics"])
        if not explicit_all_three and len(plan.metrics.selected) >= 3:
            plan.metrics = MetricSelection(selected=list(PRIORITY_METRICS), inferred=deterministic_metrics_selected)
            plan.assumptions = [
                a for a in plan.assumptions if not a.startswith("Used default metrics:")
            ]
            plan.assumptions = [*plan.assumptions, "Focused on priority metrics: weekly workouts and consistency."][:5]

    if deterministic_peer_ambiguous and "peer_definition" not in plan.ambiguities and "cohort" in tl:
        plan.ambiguities.append("peer_definition")

    if plan.member_id is None and "member_id" not in plan.ambiguities:
        plan.ambiguities.append("member_id")
    if plan.member_id is None:
        plan.needs_clarification = True
        plan.requested_slot = "member_id"
        plan.clarifying_question = _slot_question("member_id")

    # Ensure ambiguity flags and clarification state are consistent.
    if plan.ambiguities and not plan.needs_clarification:
        plan.needs_clarification = True
    if plan.needs_clarification:
        plan.requested_slot = _choose_requested_slot(plan.ambiguities) or plan.requested_slot
        if plan.requested_slot and plan.requested_slot not in plan.ambiguities:
            plan.ambiguities.append(plan.requested_slot)
        if not plan.clarifying_question and plan.requested_slot:
            plan.clarifying_question = _slot_question(plan.requested_slot)
    else:
        plan.requested_slot = None
        plan.clarifying_question = None

    start_date, end_date = _timeframe_dates(plan.timeframe)
    uncertainty_score, uncertainty_signals = _estimate_uncertainty(
        plan=plan,
        user_text=user_text,
        pending_slot=state.get("prior_pending_slot"),
    )
    return {
        "plan": plan.model_dump(),
        "start_date": start_date,
        "end_date": end_date,
        "timeframe_label": plan.timeframe,
        "uncertainty_score": uncertainty_score,
        "uncertainty_signals": uncertainty_signals,
    }


def _ask_clarification_node(state: PeerBenchmarkState) -> PeerBenchmarkState:
    plan_dict = state.get("plan") or {}
    plan = PlanOutput.model_validate(plan_dict)
    ask = plan.clarifying_question or "Could you clarify your preference so I can continue?"
    return {"response_text": ask, "follow_up_question": ask}


def _retrieve_node(state: PeerBenchmarkState) -> PeerBenchmarkState:
    plan = PlanOutput.model_validate(state["plan"])
    member_id = plan.member_id
    if not member_id:
        return {}

    start_date = state["start_date"]
    end_date = state["end_date"]
    member_metrics = read_member_metrics(member_id=member_id, start_date=start_date, end_date=end_date)

    if not member_metrics.get("has_data"):
        msg = f"I could not find workouts for {member_id} in this timeframe."
        return {"member_metrics": member_metrics, "response_text": msg, "follow_up_question": None}

    member_band = _activity_band(float(member_metrics.get("weekly_workouts", 0.0)))
    peer_data = _read_peer_aggregate_metrics(
        member_id=member_id,
        start_date=start_date,
        end_date=end_date,
        peer_definition=plan.peer_definition,
        selected_metrics=plan.metrics.selected,
        member_primary_type=member_metrics.get("primary_type"),
        member_activity_band=member_band,
    )

    return {
        "member_metrics": member_metrics,
        "peer_benchmarks": peer_data,
        "benchmark_availability": peer_data.get("availability", {}),
    }


def _compare_node(state: PeerBenchmarkState) -> PeerBenchmarkState:
    if state.get("response_text"):
        return {}
    plan = PlanOutput.model_validate(state["plan"])
    compared = compare_to_peers(
        member_data=state.get("member_metrics", {}),
        peer_data=state.get("peer_benchmarks", {}),
        selected_metrics=plan.metrics.selected,
    )
    return {
        "comparisons": compared.get("comparisons", {}),
        "strengths": compared.get("strengths", []),
        "primary_gap_metric": compared.get("primary_gap_metric"),
    }


def _suggest_node(state: PeerBenchmarkState) -> PeerBenchmarkState:
    if state.get("response_text"):
        return {}
    suggestions = generate_improvement_suggestions(
        {
            "comparisons": state.get("comparisons", {}),
            "primary_gap_metric": state.get("primary_gap_metric"),
        }
    )
    return {"suggestions": suggestions}


def _respond_node(state: PeerBenchmarkState) -> PeerBenchmarkState:
    if state.get("response_text"):
        return {}

    plan = PlanOutput.model_validate(state["plan"])
    member_id = plan.member_id
    if not member_id:
        msg = plan.clarifying_question or "What is your member_id?"
        return {"response_text": msg, "follow_up_question": msg}

    comparisons = state.get("comparisons", {})
    strengths = state.get("strengths", [])
    primary_gap = state.get("primary_gap_metric")
    suggestions = state.get("suggestions", [])
    peer_data = state.get("peer_benchmarks", {})

    lines = [
        f"Here is your benchmark summary for {member_id} ({state.get('timeframe_label')}):",
        "Peer values are aggregated benchmarks, not individual peer data.",
        f"Peer cohort: {plan.peer_definition.scope}.",
        "",
        "Metric comparison:",
    ]

    for metric in plan.metrics.selected:
        lines.append(_format_metric_line(metric, comparisons.get(metric, {})))

    lines.append("")
    if strengths:
        labels = ", ".join(METRIC_LABELS.get(s, s) for s in strengths)
        lines.append(f"Strength: you are currently above peers on {labels}.")
    else:
        lines.append("Strength: no single selected metric is clearly above peers in this window.")

    if primary_gap:
        lines.append(f"Primary gap: {METRIC_LABELS.get(primary_gap, primary_gap)} is your main opportunity area.")
    else:
        lines.append("Primary gap: no clear gap identified from available benchmarks.")

    lines.append("")
    lines.append("Suggested improvements (general fitness guidance):")
    if suggestions:
        for item in suggestions:
            lines.append(f"- {item['suggestion']}")
    else:
        lines.append("- Keep your current routine and reassess after another few weeks of data.")

    if plan.assumptions:
        lines.append("")
        lines.append("Assumptions used:")
        for a in plan.assumptions:
            lines.append(f"- {a}")

    if peer_data.get("limitation"):
        lines.append("")
        lines.append(f"Limitation: {peer_data['limitation']}")

    return {"response_text": "\n".join(lines), "follow_up_question": None}


def _route_after_validate(state: PeerBenchmarkState) -> str:
    plan = PlanOutput.model_validate(state["plan"])
    return "ask" if plan.needs_clarification else "go"


def _route_after_retrieve(state: PeerBenchmarkState) -> str:
    return "done" if state.get("response_text") else "go"


def _build_story_graph():
    g = StateGraph(PeerBenchmarkState)
    g.add_node("plan_request", _plan_request_node)
    g.add_node("merge_user_answer", _merge_user_answer_node)
    g.add_node("validate_plan", _validate_plan_node)
    g.add_node("ask_clarification", _ask_clarification_node)
    g.add_node("retrieve", _retrieve_node)
    g.add_node("compare", _compare_node)
    g.add_node("suggest", _suggest_node)
    g.add_node("respond", _respond_node)

    g.set_entry_point("plan_request")
    g.add_edge("plan_request", "merge_user_answer")
    g.add_edge("merge_user_answer", "validate_plan")
    g.add_conditional_edges("validate_plan", _route_after_validate, {"ask": "ask_clarification", "go": "retrieve"})
    g.add_edge("ask_clarification", END)
    g.add_conditional_edges("retrieve", _route_after_retrieve, {"done": END, "go": "compare"})
    g.add_edge("compare", "suggest")
    g.add_edge("suggest", "respond")
    g.add_edge("respond", END)
    return g.compile()


PEER_BENCHMARK_GRAPH = None


def _get_peer_benchmark_graph():
    global PEER_BENCHMARK_GRAPH
    if PEER_BENCHMARK_GRAPH is None:
        PEER_BENCHMARK_GRAPH = _build_story_graph()
    return PEER_BENCHMARK_GRAPH


def get_data_science_story3_mermaid() -> str:
    return _get_peer_benchmark_graph().get_graph().draw_mermaid()


def run_data_science_story3(req: StoryRequest) -> StoryResult:
    ds_state = req.domain_context.get("ds_story_3_state", {}) if isinstance(req.domain_context, dict) else {}
    prior_plan = ds_state.get("last_plan") if isinstance(ds_state, dict) else {}
    prior_pending_slot = ds_state.get("pending_slot") if isinstance(ds_state, dict) else None

    state_out = _get_peer_benchmark_graph().invoke(
        {
            "user_text": req.user_query,
            "fallback_member": req.member.member_id,
            "prior_plan": prior_plan if isinstance(prior_plan, dict) else {},
            "prior_pending_slot": prior_pending_slot if isinstance(prior_pending_slot, str) else None,
        }
    )

    plan_dict = state_out.get("plan", {})
    plan = PlanOutput.model_validate(plan_dict)

    if state_out.get("follow_up_question") and plan.needs_clarification:
        ask = state_out["follow_up_question"]
        return StoryResult(
            story_id=req.story_id,
            response_text=ask,
            follow_up_question=ask,
            story_output={
                "needs_clarification": True,
                "requested_slot": plan.requested_slot,
                "missing_slots": [plan.requested_slot] if plan.requested_slot else [],
                "plan_snapshot": plan.model_dump(),
                "planner_source": state_out.get("planner_source"),
                "uncertainty_score": state_out.get("uncertainty_score"),
                "uncertainty_signals": state_out.get("uncertainty_signals", {}),
                "llm_plan_attempted": state_out.get("llm_plan_attempted", False),
            },
            state_updates_domain={
                "ds_story_3_state": {
                    "pending_slot": plan.requested_slot,
                    "last_plan": plan.model_dump(),
                    "last_user_turn": req.user_query,
                }
            },
        )

    member_metrics = state_out.get("member_metrics", {})
    peer_data = state_out.get("peer_benchmarks", {})
    comparisons = state_out.get("comparisons", {})
    suggestions = state_out.get("suggestions", [])
    response_text = state_out.get("response_text") or "I can compare your workout metrics to peer benchmarks."

    story_output = {
        "member_id": plan.member_id,
        "start_date": state_out.get("start_date"),
        "end_date": state_out.get("end_date"),
        "timeframe_label": state_out.get("timeframe_label"),
        "plan_snapshot": plan.model_dump(),
        "planner_source": state_out.get("planner_source"),
        "uncertainty_score": state_out.get("uncertainty_score"),
        "uncertainty_signals": state_out.get("uncertainty_signals", {}),
        "llm_plan_attempted": state_out.get("llm_plan_attempted", False),
        "member_metrics": member_metrics,
        "peer_benchmarks": peer_data.get("benchmarks", {}),
        "peer_member_count": peer_data.get("peer_member_count", 0),
        "peer_workout_count": peer_data.get("peer_workout_count", 0),
        "benchmark_availability": state_out.get("benchmark_availability", {}),
        "comparisons": comparisons,
        "strengths": state_out.get("strengths", []),
        "primary_gap_metric": state_out.get("primary_gap_metric"),
        "suggestions": suggestions,
        "guardrails": {
            "aggregated_peer_data_only": True,
            "medical_advice_enabled": False,
            "supportive_tone_required": True,
        },
        "generated_on": date.today().isoformat(),
    }

    if peer_data.get("limitation"):
        story_output["benchmark_limitation"] = peer_data.get("limitation")

    return StoryResult(
        story_id=req.story_id,
        response_text=response_text,
        follow_up_question=state_out.get("follow_up_question"),
        story_output=story_output,
        state_updates_global={"member": {"member_id": plan.member_id}} if plan.member_id else {},
        state_updates_domain={
            "member_id": plan.member_id,
            "last_peer_benchmark_timeframe": state_out.get("timeframe_label"),
            "last_story_summary": "Compared member metrics against aggregated peer benchmarks.",
            "ds_story_3_state": {
                "pending_slot": None,
                "last_plan": plan.model_dump(),
                "last_user_turn": req.user_query,
            },
        },
    )
