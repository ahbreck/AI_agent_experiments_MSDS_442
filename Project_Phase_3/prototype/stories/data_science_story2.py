from __future__ import annotations

import json
import os
import re
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Tuple, TypedDict, Union

from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field, ValidationError
from ..contracts import StoryRequest, StoryResult
from ..utils import extract_explicit_member_id, parse_date_range_from_text

PROJECT_PHASE_2 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_2 / "kb" / "DataScience" / "peloton_workouts.sqlite"

SAFE_DEFAULT_METRICS = ["duration_min", "strive_score", "calories"]
ALL_METRICS = [
    "duration_min",
    "calories",
    "strive_score",
    "output_kj",
    "miles",
    "average_speed_mph",
    "avg_hr_bpm",
]
SUPPORTED_TYPES = ["cycling", "tread", "rowing", "strength", "yoga"]
TOOL_ORDER = ["summarize_time_series", "zone_distribution", "segment_by", "detect_anomalies"]


class DataScienceStoryState(TypedDict, total=False):
    user_text: str
    fallback_member: Optional[str]
    member_id: Optional[str]
    start_date: str
    end_date: str
    types: Optional[List[str]]
    plan: Dict[str, Any]
    rows: List[Dict[str, Any]]
    tool_results: Dict[str, Any]
    response_text: str
    follow_up_question: Optional[str]
    row_count: int


class SummarizeArgs(BaseModel):
    metrics: List[str] = Field(min_length=1)
    freq: str = "W"


class ZoneArgs(BaseModel):
    pass


class SegmentArgs(BaseModel):
    by: List[str] = Field(min_length=1)
    metrics: List[str] = Field(min_length=1)


class AnomalyArgs(BaseModel):
    metric: str


class SummarizeStep(BaseModel):
    tool: Literal["summarize_time_series"]
    args: SummarizeArgs


class ZoneStep(BaseModel):
    tool: Literal["zone_distribution"]
    args: ZoneArgs = Field(default_factory=ZoneArgs)


class SegmentStep(BaseModel):
    tool: Literal["segment_by"]
    args: SegmentArgs


class AnomalyStep(BaseModel):
    tool: Literal["detect_anomalies"]
    args: AnomalyArgs


PlanStep = Union[SummarizeStep, ZoneStep, SegmentStep, AnomalyStep]


class AnalysisPlan(BaseModel):
    member_id: str
    start_date: str
    end_date: str
    types: Optional[List[str]] = None
    steps: List[PlanStep] = Field(min_length=1, max_length=5)

PLAN_SYSTEM = """
You are a workout analytics planner.

Your job is to produce an ANALYSIS PLAN (not the final answer).

The plan must use the available analysis tools to answer the user's question.

AVAILABLE TOOLS:

1) summarize_time_series(rows, metrics, freq)
   - Use this when the user asks about improvement, trends, or change over time.
   - Typically use weekly frequency: freq="W".
   - Metrics must be explicitly provided.

2) zone_distribution(rows)
   - Use this when the user asks about intensity, heart rate zones, or effort distribution.

3) segment_by(rows, by, metrics)
   - Use this to determine what is driving performance.
   - Common segment dimensions:
       - "type"
       - "weekday"
       - "time_bucket"
   - Metrics must be explicitly provided.

4) detect_anomalies(rows, metric)
   - Use this to identify unusual spikes or drops in a metric.


IMPORTANT RULES:

- The plan must contain 1-5 steps.
- Each step must include the required arguments for that tool.
- If the user does not specify a date range, use the provided default start_date and end_date.
- Do NOT include the rows argument (it will be injected automatically).
- Do NOT write the final narrative response.
- Do NOT reference peers or infer individual peer data.
- This is a planning step only.


METRIC GUIDANCE:

Always safe to use:
  - "duration_min"
  - "calories"
  - "strive_score"

For bike or row workouts:
  - "output_kj"

For tread workouts:
  - "miles"
  - "average_speed_mph"

When unsure, default to:
  metrics = ["duration_min", "strive_score", "calories"]


COMMON PATTERNS:

If the user asks:
- "Am I improving?" -> use summarize_time_series.
- "What is driving intensity?" -> use zone_distribution and/or segment_by.
- "Any unusual drops or spikes?" -> use detect_anomalies.
- "What factors influence performance?" -> use segment_by.


The output must strictly conform to the AnalysisPlan schema.
Return only the structured plan object.
"""


def _safe_json_extract(text: str) -> Optional[Dict[str, Any]]:
    try:
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        pass
    m = re.search(r"\{[\s\S]*\}", text)
    if not m:
        return None
    try:
        parsed = json.loads(m.group(0))
        return parsed if isinstance(parsed, dict) else None
    except json.JSONDecodeError:
        return None


def _phrase_in_text(text: str, phrase: str) -> bool:
    p = phrase.strip().lower()
    t = text.lower()
    if " " in p:
        return p in t
    return re.search(rf"\b{re.escape(p)}s?\b", t) is not None


def _clean_member_token(member_id: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", str(member_id or "").upper())


def _extract_date_range(user_text: str) -> Optional[Tuple[str, str]]:
    text = user_text.strip()
    iso_dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", text)
    if len(iso_dates) >= 2:
        a, b = iso_dates[0], iso_dates[1]
        return (a, b) if a <= b else (b, a)
    if len(iso_dates) == 1 and re.search(r"\bsince\b", text, flags=re.I):
        return iso_dates[0], date.today().isoformat()
    return None


def _infer_types(user_text: str) -> Optional[List[str]]:
    alias_to_type = {
        "bike": "cycling",
        "cycling": "cycling",
        "ride": "cycling",
        "rides": "cycling",
        "run": "tread",
        "running": "tread",
        "tread": "tread",
        "treadmill": "tread",
        "row": "rowing",
        "rowing": "rowing",
        "strength": "strength",
        "weights": "strength",
        "lift": "strength",
        "lifting": "strength",
        "yoga": "yoga",
        "stretch": "yoga",
        "mobility": "yoga",
    }
    hits = []
    for alias, canonical in alias_to_type.items():
        if re.search(rf"\b{re.escape(alias)}\b", user_text, flags=re.I):
            hits.append(canonical)
    dedup = sorted(set(hits))
    return dedup or None


def _parse_request(user_text: str, fallback_member: Optional[str]) -> Tuple[Optional[str], str, str, Optional[List[str]]]:
    member = extract_explicit_member_id(user_text) or fallback_member
    explicit = _extract_date_range(user_text)
    if explicit:
        start, end = explicit
    else:
        start, end = parse_date_range_from_text(user_text, default_weeks=8)
    return member, start, end, _infer_types(user_text)


def _read_workouts(member_id: str, start_date: str, end_date: str, types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    q = """
    SELECT *
    FROM workouts
    WHERE REPLACE(REPLACE(UPPER(member_id), '-', ''), '_', '') = ?
      AND date >= ?
      AND date <= ?
    """
    params: List[Any] = [_clean_member_token(member_id), start_date, end_date]
    if types:
        q += f" AND LOWER(type) IN ({','.join(['?'] * len(types))})"
        params.extend([t.lower() for t in types])

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(q, params).fetchall()]


def _to_date(v: Any) -> Optional[date]:
    if v is None:
        return None
    try:
        return datetime.fromisoformat(str(v)).date()
    except ValueError:
        return None


def _to_float(v: Any) -> Optional[float]:
    if v is None:
        return None
    try:
        return float(v)
    except (TypeError, ValueError):
        return None


def _week_start(d: date) -> date:
    return d - timedelta(days=d.weekday())


def _period_start(d: date, freq: str) -> date:
    f = (freq or "W").upper()
    if f == "D":
        return d
    if f == "M":
        return date(d.year, d.month, 1)
    return _week_start(d)


def _linear_slope(values: List[float]) -> Optional[float]:
    if len(values) < 3:
        return None
    x = list(range(len(values)))
    x_mean = sum(x) / len(x)
    y_mean = sum(values) / len(values)
    denom = sum((xi - x_mean) ** 2 for xi in x)
    if denom == 0:
        return None
    return sum((xi - x_mean) * (yi - y_mean) for xi, yi in zip(x, values)) / denom


def summarize_time_series(rows: List[Dict[str, Any]], metrics: List[str], freq: str = "W") -> Dict[str, Any]:
    if not rows:
        return {"ok": True, "note": "No workouts in timeframe.", "by_period_mean": [], "trends": {}}

    metrics_present = [m for m in metrics if any(m in r for r in rows)]
    if not metrics_present:
        return {"ok": True, "note": "None of the requested metrics exist in the data.", "by_period_mean": [], "trends": {}}

    bucket: Dict[str, Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))
    for r in rows:
        d = _to_date(r.get("date"))
        if not d:
            continue
        period = _period_start(d, freq).isoformat()
        for m in metrics_present:
            v = _to_float(r.get(m))
            if v is not None:
                bucket[period][m].append(v)

    periods_sorted = sorted(bucket.keys())
    by_period_mean: List[Dict[str, Any]] = []
    trends: Dict[str, Any] = {}
    series_by_metric: Dict[str, List[float]] = defaultdict(list)

    for p in periods_sorted:
        row: Dict[str, Any] = {"period": p}
        for m in metrics_present:
            vals = bucket[p].get(m, [])
            if vals:
                m_mean = sum(vals) / len(vals)
                row[m] = round(m_mean, 3)
                series_by_metric[m].append(m_mean)
        by_period_mean.append(row)

    for m in metrics_present:
        slope = _linear_slope(series_by_metric.get(m, []))
        trends[m] = {"slope_per_period": None if slope is None else round(slope, 6)}
        if slope is None:
            trends[m]["note"] = "Not enough points for trend."

    return {
        "ok": True,
        "metrics_used": metrics_present,
        "by_period_mean": by_period_mean,
        "trends": trends,
        "period_count": len(by_period_mean),
    }


def zone_distribution(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"ok": True, "note": "No workouts.", "overall_zone_pct": {}, "by_type_zone_pct": {}}

    zone_cols = ["zone1_minutes", "zone2_minutes", "zone3_minutes", "zone4_minutes", "zone5_minutes"]

    def pct_for(subset: List[Dict[str, Any]]) -> Dict[str, Any]:
        totals = {z: 0.0 for z in zone_cols}
        denom = 0.0
        for r in subset:
            zsum = 0.0
            for z in zone_cols:
                v = _to_float(r.get(z))
                if v is not None:
                    totals[z] += v
                    zsum += v
            duration = _to_float(r.get("duration_min")) or 0.0
            denom += zsum if zsum > 0 else duration
        if denom <= 0:
            return {"note": "Missing zone minutes and duration for normalization."}
        return {z: round(totals[z] / denom, 4) for z in zone_cols}

    overall = pct_for(rows)
    by_type_rows: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_type_rows[str(r.get("type") or "UNKNOWN")].append(r)

    by_type_zone_pct = {t: pct_for(grp) for t, grp in sorted(by_type_rows.items())}
    return {"ok": True, "overall_zone_pct": overall, "by_type_zone_pct": by_type_zone_pct}


def _time_bucket(value: Any) -> str:
    if not isinstance(value, str) or not re.match(r"^\d{2}:\d{2}$", value):
        return "unknown"
    hh = int(value.split(":")[0])
    if 5 <= hh < 11:
        return "morning"
    if 11 <= hh < 17:
        return "midday"
    if 17 <= hh < 22:
        return "evening"
    return "late_night"


def segment_by(rows: List[Dict[str, Any]], by: List[str], metrics: List[str]) -> Dict[str, Any]:
    if not rows:
        return {"ok": True, "note": "No workouts.", "segments": []}

    metrics_present = [m for m in metrics if any(m in r for r in rows)]
    if not metrics_present:
        return {"ok": True, "note": "None of the requested metrics exist in the data.", "segments": []}

    groups: Dict[Tuple[Any, ...], Dict[str, List[float]]] = defaultdict(lambda: defaultdict(list))

    for r in rows:
        d = _to_date(r.get("date"))
        labels: List[Any] = []
        for key in by:
            if key == "weekday":
                labels.append(d.strftime("%A") if d else "unknown")
            elif key == "time_bucket":
                labels.append(_time_bucket(r.get("start_time_local")))
            else:
                labels.append(r.get(key))
        gk = tuple(labels)
        for m in metrics_present:
            v = _to_float(r.get(m))
            if v is not None:
                groups[gk][m].append(v)

    segments: List[Dict[str, Any]] = []
    for gk, mvals in groups.items():
        row: Dict[str, Any] = {}
        for idx, key in enumerate(by):
            row[key] = gk[idx]
        for m in metrics_present:
            vals = mvals.get(m, [])
            row[m] = round(sum(vals) / len(vals), 3) if vals else None
        segments.append(row)

    segments.sort(key=lambda s: tuple(str(s.get(k)) for k in by))
    return {"ok": True, "metrics_used": metrics_present, "segments": segments}


def detect_anomalies(rows: List[Dict[str, Any]], metric: str) -> Dict[str, Any]:
    vals: List[Tuple[Dict[str, Any], float]] = []
    for r in rows:
        v = _to_float(r.get(metric))
        if v is not None:
            vals.append((r, v))
    if len(vals) < 8:
        return {"ok": True, "note": "Not enough points for anomaly detection.", "outliers": []}

    sorted_values = sorted(v for _, v in vals)
    q1 = sorted_values[len(sorted_values) // 4]
    q3 = sorted_values[(len(sorted_values) * 3) // 4]
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr

    outliers = []
    for r, v in vals:
        if v < lo or v > hi:
            outliers.append(
                {
                    "workout_id": r.get("workout_id"),
                    "date": r.get("date"),
                    "type": r.get("type"),
                    metric: round(v, 3),
                }
            )

    return {"ok": True, "bounds": {"low": round(lo, 3), "high": round(hi, 3)}, "outliers": outliers[:20]}


def _pick_metrics(user_text: str, types: Optional[List[str]]) -> List[str]:
    alias_map = {
        "duration_min": ["duration", "time", "minutes", "longer", "consistency"],
        "calories": ["calories", "burn", "energy"],
        "strive_score": ["strive", "effort", "intensity", "harder", "easier"],
        "average_speed_mph": ["speed", "pace", "mph"],
        "miles": ["distance", "miles", "mileage"],
        "output_kj": ["output", "power", "kj", "watts"],
        "avg_hr_bpm": ["heart rate", "hr", "bpm", "cardio"],
    }

    scores = {m: 0 for m in ALL_METRICS}
    text = user_text.lower()
    for metric, aliases in alias_map.items():
        for alias in aliases:
            if _phrase_in_text(text, alias):
                scores[metric] += 1

    if types:
        tset = set(types)
        if "cycling" in tset or "rowing" in tset:
            scores["output_kj"] += 1
        if "tread" in tset:
            scores["miles"] += 1
            scores["average_speed_mph"] += 1

    picked = [m for m, s in scores.items() if s > 0]
    if not picked:
        return SAFE_DEFAULT_METRICS
    picked.sort(key=lambda m: scores[m], reverse=True)
    return picked[:4]


def _intent_scores(user_text: str) -> Dict[str, int]:
    intents = {
        "trend": [
            "improve",
            "improving",
            "trend",
            "over time",
            "progress",
            "change",
            "trajectory",
            "last",
        ],
        "intensity": ["intensity", "zone", "heart rate", "hr", "effort"],
        "drivers": ["driving", "driver", "factor", "influence", "why", "cause"],
        "segment": ["segment", "breakdown", "compare", "weekday", "time of day", "type"],
        "anomaly": ["anomaly", "outlier", "spike", "drop", "unusual"],
    }
    tl = user_text.lower()
    out = {}
    for k, phrases in intents.items():
        out[k] = sum(1 for p in phrases if _phrase_in_text(tl, p))
    return out


def _coerce_plan_from_model(plan: AnalysisPlan, member_id: str, start_date: str, end_date: str, types: Optional[List[str]]) -> Optional[Dict[str, Any]]:
    normalized_steps: List[Dict[str, Any]] = []
    for step in plan.steps[:5]:
        if isinstance(step, SummarizeStep):
            metrics = [m for m in step.args.metrics if m in ALL_METRICS]
            if not metrics:
                continue
            freq = str(step.args.freq or "W").upper()
            normalized_steps.append(
                {"tool": "summarize_time_series", "args": {"metrics": metrics, "freq": freq if freq in {"D", "W", "M"} else "W"}}
            )
            continue
        if isinstance(step, ZoneStep):
            normalized_steps.append({"tool": "zone_distribution", "args": {}})
            continue
        if isinstance(step, SegmentStep):
            by = [x for x in step.args.by if x in {"type", "weekday", "time_bucket"}]
            metrics = [m for m in step.args.metrics if m in ALL_METRICS]
            if not by or not metrics:
                continue
            normalized_steps.append({"tool": "segment_by", "args": {"by": by, "metrics": metrics}})
            continue
        if isinstance(step, AnomalyStep):
            if step.args.metric not in ALL_METRICS:
                continue
            normalized_steps.append({"tool": "detect_anomalies", "args": {"metric": step.args.metric}})

    if not normalized_steps:
        return None

    normalized_steps.sort(key=lambda s: TOOL_ORDER.index(s["tool"]))
    return {
        "member_id": member_id,
        "start_date": start_date,
        "end_date": end_date,
        "types": [t for t in (types or []) if t in SUPPORTED_TYPES] or None,
        "steps": normalized_steps[:5],
        "plan_system": PLAN_SYSTEM.strip(),
    }


def _maybe_llm_plan(user_text: str, member_id: str, start_date: str, end_date: str, types: Optional[List[str]], metrics: List[str]) -> Optional[Dict[str, Any]]:
    if os.getenv("PROTOTYPE_DS_USE_LLM_PLAN", "0").strip() not in {"1", "true", "TRUE"}:
        return None
    try:
        from langchain_openai import ChatOpenAI
    except Exception:
        return None

    llm = ChatOpenAI(model=os.getenv("PROTOTYPE_DS_PLAN_MODEL", "gpt-4o-mini"), temperature=0)
    prompt = (
        f"{PLAN_SYSTEM.strip()}\n\n"
        "Return JSON with this shape exactly:\n"
        "{"
        "\"member_id\":\"...\","
        "\"start_date\":\"YYYY-MM-DD\","
        "\"end_date\":\"YYYY-MM-DD\","
        "\"types\":[\"cycling\"],"
        "\"steps\":[{\"tool\":\"summarize_time_series\",\"args\":{\"metrics\":[\"duration_min\"],\"freq\":\"W\"}}]"
        "}\n\n"
        f"User request: {user_text}\n"
        f"Defaults: member_id={member_id}, start_date={start_date}, end_date={end_date}, types={types}, suggested_metrics={metrics}\n"
    )
    try:
        resp = llm.invoke(prompt)
        text = getattr(resp, "content", str(resp))
        parsed = _safe_json_extract(text)
        if not parsed:
            return None
        candidate = {
            "member_id": member_id,
            "start_date": start_date,
            "end_date": end_date,
            "types": types,
            "steps": parsed.get("steps", []),
        }
        plan_model = AnalysisPlan.model_validate(candidate)
        return _coerce_plan_from_model(
            plan_model,
            member_id=member_id,
            start_date=start_date,
            end_date=end_date,
            types=types,
        )
    except ValidationError:
        return None
    except Exception:
        return None


def _plan_from_query(user_text: str, member_id: str, start_date: str, end_date: str, types: Optional[List[str]]) -> Dict[str, Any]:
    tl = user_text.lower()
    metrics = _pick_metrics(user_text, types=types)
    scores = _intent_scores(user_text)
    llm_plan = _maybe_llm_plan(
        user_text=user_text,
        member_id=member_id,
        start_date=start_date,
        end_date=end_date,
        types=types,
        metrics=metrics,
    )
    if llm_plan:
        llm_plan["planner_mode"] = "llm"
        llm_plan["intent_scores"] = scores
        llm_plan["metrics_selected"] = metrics
        return llm_plan

    steps: List[Dict[str, Any]] = []
    explicit_tool_terms = {
        "summarize_time_series": ["trend", "improve", "over time", "progress", "time series"],
        "zone_distribution": ["zone", "intensity", "heart rate", "hr"],
        "segment_by": ["segment", "breakdown", "compare", "driver", "factor", "weekday", "time of day", "type"],
        "detect_anomalies": ["anomaly", "outlier", "spike", "drop", "unusual"],
    }

    def requested(tool_name: str) -> bool:
        terms = explicit_tool_terms[tool_name]
        return any(_phrase_in_text(tl, t) for t in terms)

    if requested("summarize_time_series") or max(scores.values()) == 0:
        steps.append({"tool": "summarize_time_series", "args": {"metrics": metrics, "freq": "W"}})
    if requested("zone_distribution") or scores["intensity"] > 0:
        steps.append({"tool": "zone_distribution", "args": {}})
    if requested("segment_by") or scores["drivers"] > 0 or scores["segment"] > 0:
        dims = ["type"]
        if re.search(r"\bweekday|day of week|day\b", tl):
            dims.append("weekday")
        if re.search(r"\btime of day|morning|evening|afternoon|time\b", tl):
            dims.append("time_bucket")
        steps.append({"tool": "segment_by", "args": {"by": dims, "metrics": metrics}})
    if requested("detect_anomalies") or scores["anomaly"] > 0:
        anomaly_metric = metrics[0] if metrics else "duration_min"
        steps.append({"tool": "detect_anomalies", "args": {"metric": anomaly_metric}})

    deduped: List[Dict[str, Any]] = []
    seen = set()
    for step in steps:
        tool = step["tool"]
        if tool not in seen:
            deduped.append(step)
            seen.add(tool)

    if not deduped:
        deduped = [
            {"tool": "summarize_time_series", "args": {"metrics": metrics, "freq": "W"}},
            {"tool": "zone_distribution", "args": {}},
            {"tool": "segment_by", "args": {"by": ["type"], "metrics": metrics}},
        ]

    deduped.sort(key=lambda s: TOOL_ORDER.index(s["tool"]))
    try:
        plan_model = AnalysisPlan.model_validate(
            {
                "member_id": member_id,
                "start_date": start_date,
                "end_date": end_date,
                "types": types,
                "steps": deduped[:5],
            }
        )
        out = _coerce_plan_from_model(
            plan_model,
            member_id=member_id,
            start_date=start_date,
            end_date=end_date,
            types=types,
        )
        if not out:
            raise ValueError("Coerced plan had no valid steps.")
    except (ValidationError, ValueError):
        fallback_model = AnalysisPlan.model_validate(
            {
                "member_id": member_id,
                "start_date": start_date,
                "end_date": end_date,
                "types": types,
                "steps": [{"tool": "summarize_time_series", "args": {"metrics": SAFE_DEFAULT_METRICS, "freq": "W"}}],
            }
        )
        out = _coerce_plan_from_model(
            fallback_model,
            member_id=member_id,
            start_date=start_date,
            end_date=end_date,
            types=types,
        )

    out["planner_mode"] = "heuristic"
    out["intent_scores"] = scores
    out["metrics_selected"] = metrics
    return out


def _execute_plan(plan: Dict[str, Any], rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    results: Dict[str, Any] = {
        "read_workouts": {
            "ok": True,
            "row_count": len(rows),
            "member_id": plan["member_id"],
            "start_date": plan["start_date"],
            "end_date": plan["end_date"],
            "types": plan.get("types"),
        }
    }
    for i, step in enumerate(plan.get("steps", []), start=1):
        tool_name = step["tool"]
        args = dict(step.get("args", {}))
        if tool_name == "summarize_time_series":
            out = summarize_time_series(rows=rows, metrics=args.get("metrics", SAFE_DEFAULT_METRICS), freq=args.get("freq", "W"))
        elif tool_name == "zone_distribution":
            out = zone_distribution(rows=rows)
        elif tool_name == "segment_by":
            out = segment_by(rows=rows, by=args.get("by", ["type"]), metrics=args.get("metrics", SAFE_DEFAULT_METRICS))
        elif tool_name == "detect_anomalies":
            out = detect_anomalies(rows=rows, metric=args.get("metric", "duration_min"))
        else:
            out = {"ok": False, "error": f"Unexpected tool: {tool_name}"}
        results[f"step_{i}_{tool_name}"] = out
    return results


def _slope_direction(slope: Optional[float]) -> str:
    if slope is None:
        return "insufficient data"
    if slope > 0.02:
        return "upward"
    if slope < -0.02:
        return "downward"
    return "mostly flat"


def _interpret_results(user_text: str, plan: Dict[str, Any], tool_results: Dict[str, Any]) -> str:
    lines = [
        f"Workout analysis for {plan['member_id']} ({plan['start_date']} to {plan['end_date']})",
        "",
        "1) Key takeaways",
    ]

    trend_keys = [k for k in tool_results.keys() if "summarize_time_series" in k]
    if trend_keys:
        trend_data = tool_results[trend_keys[0]]
        trends = trend_data.get("trends", {})
        if trends:
            for metric, payload in trends.items():
                lines.append(f"- {metric}: {_slope_direction(payload.get('slope_per_period'))} trend")
        else:
            lines.append("- Trend analysis was limited by available metric points.")
    else:
        lines.append("- Collected workout history and ran requested analytic steps.")

    zone_keys = [k for k in tool_results.keys() if "zone_distribution" in k]
    if zone_keys:
        zone = tool_results[zone_keys[0]].get("overall_zone_pct", {})
        if isinstance(zone, dict) and zone and "note" not in zone:
            top_zone = max(zone.items(), key=lambda kv: kv[1])
            lines.append(f"- Highest time share appears in {top_zone[0]} ({round(top_zone[1] * 100, 1)}%).")

    lines.extend(["", "2) Trends"])
    if trend_keys:
        by_period = tool_results[trend_keys[0]].get("by_period_mean", [])
        lines.append(f"Reviewed {len(by_period)} aggregated period(s) for trend direction across requested metrics.")
    else:
        lines.append("No explicit time-series step was requested in this run.")

    lines.extend(["", "3) What might be driving it"])
    seg_keys = [k for k in tool_results.keys() if "segment_by" in k]
    if seg_keys:
        segments = tool_results[seg_keys[0]].get("segments", [])
        for s in segments[:3]:
            desc = ", ".join([f"{k}={v}" for k, v in s.items()])
            lines.append(f"- {desc}")
    else:
        lines.append("- Segment-level drivers were not strongly indicated by your prompt.")

    lines.extend(["", "4) Suggested experiments"])
    lines.append("- Keep workout type consistent for 2 weeks, then re-check trend slopes.")
    lines.append("- Compare morning vs evening sessions to test time-of-day effects.")
    lines.append("- If intensity is a goal, track zone share shifts week over week.")

    lines.extend(["", "5) Data limitations"])
    row_count = tool_results.get("read_workouts", {}).get("row_count", 0)
    if row_count < 8:
        lines.append("- Small sample size; trend and anomaly confidence is limited.")
    else:
        lines.append("- Analysis reflects logged workouts only; missing or sparse HR fields can affect zone metrics.")

    anomaly_keys = [k for k in tool_results.keys() if "detect_anomalies" in k]
    if anomaly_keys:
        outliers = tool_results[anomaly_keys[0]].get("outliers", [])
        lines.append(f"- Detected {len(outliers)} potential outlier workout(s) using IQR bounds.")

    return "\n".join(lines)


def _plan_node(state: DataScienceStoryState) -> DataScienceStoryState:
    user_text = state.get("user_text", "")
    fallback_member = state.get("fallback_member")
    member_id, start_date, end_date, types = _parse_request(user_text, fallback_member=fallback_member)
    if not member_id:
        ask = "What is your member_id (e.g., MB001)? If dates are missing I will analyze the last 8 weeks."
        return {"response_text": ask, "follow_up_question": ask}

    plan = _plan_from_query(
        user_text=user_text,
        member_id=member_id,
        start_date=start_date,
        end_date=end_date,
        types=types,
    )
    return {
        "member_id": member_id,
        "start_date": start_date,
        "end_date": end_date,
        "types": types,
        "plan": plan,
    }


def _run_tools_node(state: DataScienceStoryState) -> DataScienceStoryState:
    member_id = state.get("member_id")
    if not member_id:
        return {}
    start_date = state["start_date"]
    end_date = state["end_date"]
    types = state.get("types")
    plan = state["plan"]

    rows = _read_workouts(member_id=member_id, start_date=start_date, end_date=end_date, types=types)
    rows_sorted = sorted(rows, key=lambda r: datetime.fromisoformat(str(r["date"])))

    if not rows_sorted:
        msg = f"I did not find workouts for {member_id} between {start_date} and {end_date}."
        return {"rows": [], "row_count": 0, "tool_results": {}, "response_text": msg, "follow_up_question": None}

    tool_results = _execute_plan(plan=plan, rows=rows_sorted)
    return {"rows": rows_sorted, "row_count": len(rows_sorted), "tool_results": tool_results}


def _interpret_node(state: DataScienceStoryState) -> DataScienceStoryState:
    if state.get("response_text"):
        return {}
    member_id = state.get("member_id")
    if not member_id:
        return {}
    rows = state.get("rows", [])
    if not rows:
        return {}
    response_text = _interpret_results(state.get("user_text", ""), state["plan"], state.get("tool_results", {}))
    return {"response_text": response_text, "follow_up_question": None}


def _route_after_plan(state: DataScienceStoryState) -> str:
    return "ask" if not state.get("member_id") else "go"


def _route_after_tools(state: DataScienceStoryState) -> str:
    return "done" if state.get("response_text") else "interpret"


def _build_story_graph():
    g = StateGraph(DataScienceStoryState)
    g.add_node("plan", _plan_node)
    g.add_node("run_tools", _run_tools_node)
    g.add_node("interpret", _interpret_node)
    g.set_entry_point("plan")
    g.add_conditional_edges("plan", _route_after_plan, {"ask": END, "go": "run_tools"})
    g.add_conditional_edges("run_tools", _route_after_tools, {"done": END, "interpret": "interpret"})
    g.add_edge("interpret", END)
    return g.compile()


DATA_SCIENCE_GRAPH = None


def _get_data_science_graph():
    global DATA_SCIENCE_GRAPH
    if DATA_SCIENCE_GRAPH is None:
        DATA_SCIENCE_GRAPH = _build_story_graph()
    return DATA_SCIENCE_GRAPH


def get_data_science_story2_mermaid() -> str:
    return _get_data_science_graph().get_graph().draw_mermaid()


def run_data_science_story2(req: StoryRequest) -> StoryResult:
    state_out = _get_data_science_graph().invoke(
        {
            "user_text": req.user_query,
            "fallback_member": req.member.member_id,
        }
    )

    if state_out.get("follow_up_question") and not state_out.get("member_id"):
        ask = state_out["follow_up_question"]
        return StoryResult(
            story_id=req.story_id,
            response_text=ask,
            follow_up_question=ask,
            story_output={"needs_member_id": True},
        )

    member_id = state_out.get("member_id")
    start_date = state_out.get("start_date")
    end_date = state_out.get("end_date")
    types = state_out.get("types")
    plan = state_out.get("plan", {})
    rows_sorted = state_out.get("rows", [])
    row_count = int(state_out.get("row_count", len(rows_sorted)))
    tool_results = state_out.get("tool_results", {})
    response_text = state_out.get("response_text") or "I can analyze your workout trends."

    return StoryResult(
        story_id=req.story_id,
        response_text=response_text,
        story_output={
            "member_id": member_id,
            "start_date": start_date,
            "end_date": end_date,
            "types": types,
            "row_count": len(rows_sorted),
            "plan": plan,
            "tool_results": tool_results,
            "generated_on": date.today().isoformat(),
        },
        state_updates_global={"member": {"member_id": member_id}} if member_id else {},
        state_updates_domain={"last_story_summary": f"Planned {len(plan.get('steps', []))} step(s); analyzed {row_count} workouts."},
    )
