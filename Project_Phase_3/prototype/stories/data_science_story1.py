from __future__ import annotations

import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from ..contracts import StoryRequest, StoryResult
from ..utils import (
    extract_explicit_member_id,
    normalize_member_id,
    parse_date_range_from_text,
    register_sqlite_alnum_normalizer,
)

PROJECT_PHASE_3 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_3 / "kb" / "DataScience" / "peloton_workouts.sqlite"

METRIC_ALIASES: Dict[str, List[str]] = {
    "duration_min": ["duration", "minutes", "session length", "duration_min"],
    "calories": ["calories", "calorie"],
    "strive_score": ["strive", "strive score"],
    "avg_hr_bpm": ["heart rate", "hr", "bpm", "avg_hr_bpm"],
    "output_kj": ["output", "kilojoules", "kj", "output_kj"],
    "miles": ["miles", "distance"],
    "average_speed_mph": ["speed", "mph", "average_speed_mph"],
    "cadence_rpm": ["cadence", "rpm", "cadence_rpm"],
    "resistance_percent": ["resistance", "resistance_percent"],
    "incline_percent": ["incline", "incline_percent"],
}
SUPPORTED_TRACE_TYPES = {"scatter", "bar", "histogram", "box"}
CHART_TYPE_HINTS: Dict[str, List[str]] = {
    "line": ["line", "trend", "over time", "timeline", "time series", "weekly"],
    "bar": ["bar", "compare", "comparison", "by type", "by weekday", "breakdown"],
    "scatter": ["scatter", "relationship", "correlation", "versus", "vs"],
    "histogram": ["histogram", "distribution", "spread"],
    "box": ["box", "boxplot", "box plot", "outlier", "quartile"],
}
GENERIC_VIZ_HINTS = ["chart", "graph", "plot", "visualize", "visualization", "show me"]


def _contains_any(text: str, terms: List[str]) -> bool:
    tl = text.lower()
    return any(term in tl for term in terms)


def _pick_metric(user_text: str, fallback: str = "duration_min") -> str:
    tl = user_text.lower()
    for metric, aliases in METRIC_ALIASES.items():
        if _contains_any(tl, aliases):
            return metric
    return fallback


def _metric_has_values(rows: List[Dict[str, Any]], metric: str) -> bool:
    for row in rows:
        if row.get(metric) is not None:
            return True
    return False


def _count_metric_values(rows: List[Dict[str, Any]], metric: str) -> int:
    return sum(1 for r in rows if r.get(metric) is not None)


def _pick_chart_type(user_text: str) -> str:
    tl = user_text.lower()
    if _contains_any(tl, ["scatter", "relationship", "correlation"]):
        return "scatter"
    if _contains_any(tl, ["histogram", "distribution"]):
        return "histogram"
    if _contains_any(tl, ["boxplot", "box plot", "box"]):
        return "box"
    if _contains_any(tl, ["trend", "over time", "timeline", "weekly", "time series"]):
        return "line"
    return "bar"


def _pick_dimension(user_text: str, chart_type: str) -> str:
    tl = user_text.lower()
    if _contains_any(tl, ["weekday", "day of week", "day"]):
        return "weekday"
    if _contains_any(tl, ["time of day", "time bucket", "morning", "afternoon", "evening"]):
        return "time_bucket"
    if _contains_any(tl, ["type", "class type", "modality", "workout type"]):
        return "type"
    if chart_type == "line":
        return "week_start"
    return "type"


def _extract_metrics_from_text(user_text: str) -> List[str]:
    tl = user_text.lower()
    matched: List[str] = []
    for metric, aliases in METRIC_ALIASES.items():
        if _contains_any(tl, aliases):
            matched.append(metric)
    return matched


def _is_explicit_chart_request(user_text: str) -> bool:
    tl = user_text.lower()
    for hints in CHART_TYPE_HINTS.values():
        if _contains_any(tl, hints):
            return True
    return False


def _is_underspecified_viz_request(user_text: str, metrics: List[str]) -> bool:
    tl = user_text.lower()
    if not _contains_any(tl, GENERIC_VIZ_HINTS):
        return False
    if not _is_explicit_chart_request(tl):
        return True
    return len(metrics) == 0


def _is_member_scoped_request(user_text: str) -> bool:
    tl = user_text.lower()
    return any(token in tl for token in [" my ", " me ", " i ", " mine ", " myself "]) or tl.startswith("my ")


def _parse_iso_date(raw: str) -> datetime:
    return datetime.strptime(raw, "%Y-%m-%d")


def _week_start(raw_date: str) -> str:
    d = _parse_iso_date(raw_date).date()
    ws = d.fromordinal(d.toordinal() - d.weekday())
    return ws.isoformat()


def _weekday(raw_date: str) -> str:
    return _parse_iso_date(raw_date).strftime("%a")


def _time_bucket(raw_time: Optional[str]) -> str:
    if not raw_time:
        return "unknown"
    hhmm = str(raw_time).strip()[:5]
    try:
        hour = int(hhmm.split(":")[0])
    except (ValueError, IndexError):
        return "unknown"
    if hour < 11:
        return "morning"
    if hour < 17:
        return "afternoon"
    return "evening"


def _dimension_value(row: Dict[str, Any], dim: str) -> str:
    if dim == "type":
        return str(row.get("type") or "unknown")
    if dim == "weekday":
        return _weekday(str(row.get("date") or "1970-01-01"))
    if dim == "time_bucket":
        return _time_bucket(row.get("start_time_local"))
    if dim == "week_start":
        return _week_start(str(row.get("date") or "1970-01-01"))
    return "unknown"


def _read_rows(member_id: Optional[str], start_date: str, end_date: str) -> List[Dict[str, Any]]:
    sql = """
    SELECT
      workout_id,
      member_id,
      date,
      start_time_local,
      type,
      duration_min,
      calories,
      strive_score,
      avg_hr_bpm,
      output_kj,
      miles,
      average_speed_mph,
      cadence_rpm,
      resistance_percent,
      incline_percent
    FROM workouts
    WHERE date BETWEEN ? AND ?
    """
    params: List[Any] = [start_date, end_date]
    if member_id:
        sql += " AND NORM_ALNUM(member_id) = NORM_ALNUM(?)"
        params.append(member_id)
    sql += " ORDER BY date ASC"

    with sqlite3.connect(DB_PATH) as conn:
        register_sqlite_alnum_normalizer(conn)
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql, params).fetchall()
    return [dict(r) for r in rows]


def _aggregate(rows: List[Dict[str, Any]], dimension: str, metric: str, agg: str) -> List[Dict[str, Any]]:
    grouped: Dict[str, Dict[str, float]] = defaultdict(lambda: {"sum": 0.0, "count": 0.0})
    for row in rows:
        key = _dimension_value(row, dimension)
        grouped[key]["count"] += 1.0
        if metric in row and row.get(metric) is not None:
            grouped[key]["sum"] += float(row.get(metric) or 0.0)

    out: List[Dict[str, Any]] = []
    for key, stats in grouped.items():
        count = int(stats["count"])
        if agg == "count":
            value = float(count)
        elif agg == "sum":
            value = round(stats["sum"], 3)
        else:
            value = round(stats["sum"] / max(count, 1), 3)
        out.append({"x": key, "y": value, "n": count})

    return sorted(out, key=lambda r: str(r["x"]))


def _scatter_points(rows: List[Dict[str, Any]], x_metric: str, y_metric: str, limit: int = 200) -> List[Dict[str, Any]]:
    pts: List[Dict[str, Any]] = []
    for row in rows:
        xv = row.get(x_metric)
        yv = row.get(y_metric)
        if xv is None or yv is None:
            continue
        pts.append(
            {
                "x": round(float(xv), 3),
                "y": round(float(yv), 3),
                "date": row.get("date"),
                "type": row.get("type"),
            }
        )
    return pts[:limit]


def _hist_values(rows: List[Dict[str, Any]], metric: str, limit: int = 1000) -> List[float]:
    vals = [round(float(r.get(metric) or 0.0), 3) for r in rows if r.get(metric) is not None]
    return vals[:limit]


def _box_values(rows: List[Dict[str, Any]], metric: str, dimension: str = "type") -> List[Dict[str, Any]]:
    grouped: Dict[str, List[float]] = defaultdict(list)
    for row in rows:
        v = row.get(metric)
        if v is None:
            continue
        grouped[_dimension_value(row, dimension)].append(round(float(v), 3))
    out: List[Dict[str, Any]] = []
    for key, vals in grouped.items():
        if vals:
            out.append({"group": key, "values": vals})
    return sorted(out, key=lambda r: r["group"])


def _build_plotly_spec(
    chart_type: str,
    metric: str,
    points: Any,
    dimension: Optional[str],
    title: str,
) -> Dict[str, Any]:
    if chart_type == "line":
        data = [
            {
                "type": "scatter",
                "mode": "lines+markers",
                "x": [p["x"] for p in points],
                "y": [p["y"] for p in points],
                "text": [f"n={p.get('n', 0)}" for p in points],
            }
        ]
    elif chart_type == "bar":
        data = [
            {
                "type": "bar",
                "x": [p["x"] for p in points],
                "y": [p["y"] for p in points],
                "text": [f"n={p.get('n', 0)}" for p in points],
            }
        ]
    elif chart_type == "scatter":
        data = [{"type": "scatter", "mode": "markers", "x": [p["x"] for p in points], "y": [p["y"] for p in points]}]
    elif chart_type == "histogram":
        data = [{"type": "histogram", "x": points}]
    else:
        data = [
            {
                "type": "box",
                "name": grp["group"],
                "y": grp["values"],
                "boxpoints": "outliers",
            }
            for grp in points
        ]

    return {
        "library": "plotly",
        "data": data,
        "layout": {
            "title": title,
            "xaxis": {"title": dimension or metric},
            "yaxis": {"title": metric},
        },
    }


def _is_valid_plotly_spec(chart_spec: Dict[str, Any]) -> bool:
    if not isinstance(chart_spec, dict):
        return False
    if chart_spec.get("library") != "plotly":
        return False
    data = chart_spec.get("data")
    if not isinstance(data, list) or not data:
        return False
    for trace in data:
        if not isinstance(trace, dict):
            return False
        if trace.get("type") not in SUPPORTED_TRACE_TYPES:
            return False
    return True


def _build_candidate_plans(user_text: str, primary_metric: str, metrics_in_text: List[str]) -> List[Dict[str, Any]]:
    metric_x = primary_metric
    metric_y = metrics_in_text[1] if len(metrics_in_text) > 1 else ("calories" if primary_metric != "calories" else "duration_min")
    base_dimension = _pick_dimension(user_text, chart_type="bar")
    return [
        {
            "chart_type": "line",
            "metric": metric_x,
            "secondary_metric": None,
            "dimension": "week_start",
            "aggregation": "avg",
            "reason": "Trend-oriented default for workout progress over time.",
        },
        {
            "chart_type": "bar",
            "metric": metric_x,
            "secondary_metric": None,
            "dimension": base_dimension,
            "aggregation": "avg",
            "reason": "Category comparison for workout segments.",
        },
        {
            "chart_type": "scatter",
            "metric": metric_x,
            "secondary_metric": metric_y,
            "dimension": None,
            "aggregation": None,
            "reason": "Relationship view between two metrics.",
        },
        {
            "chart_type": "histogram",
            "metric": metric_x,
            "secondary_metric": None,
            "dimension": None,
            "aggregation": None,
            "reason": "Distribution view for a single metric.",
        },
    ]


def _score_candidate_plan(plan: Dict[str, Any], rows: List[Dict[str, Any]], user_text: str) -> Tuple[float, Dict[str, Any]]:
    chart_type = str(plan.get("chart_type") or "bar")
    metric = str(plan.get("metric") or "duration_min")
    secondary = str(plan.get("secondary_metric") or "")
    dimension = str(plan.get("dimension") or "type")
    tl = user_text.lower()

    request_fit = 0.2
    if chart_type in CHART_TYPE_HINTS and _contains_any(tl, CHART_TYPE_HINTS[chart_type]):
        request_fit = 0.45
    elif _contains_any(tl, GENERIC_VIZ_HINTS):
        request_fit = 0.3

    sufficiency = 0.1
    detail: Dict[str, Any] = {}
    if chart_type in {"line", "bar"}:
        pts = _aggregate(rows=rows, dimension=dimension, metric=metric, agg=str(plan.get("aggregation") or "avg"))
        sufficiency = min(0.45, len(pts) / 20.0 + 0.1)
        detail = {"point_count": len(pts)}
    elif chart_type == "scatter":
        pts = _scatter_points(rows=rows, x_metric=metric, y_metric=secondary or "calories", limit=250)
        sufficiency = min(0.45, len(pts) / 180.0 + 0.08)
        detail = {"point_count": len(pts), "secondary_metric": secondary}
    elif chart_type == "histogram":
        vals = _hist_values(rows=rows, metric=metric, limit=2000)
        sufficiency = min(0.45, len(vals) / 350.0 + 0.08)
        detail = {"value_count": len(vals)}
    elif chart_type == "box":
        groups = _box_values(rows=rows, metric=metric, dimension=dimension)
        n_obs = sum(len(g.get("values", [])) for g in groups)
        sufficiency = min(0.45, (len(groups) / 8.0) + (n_obs / 450.0))
        detail = {"group_count": len(groups), "value_count": n_obs}

    metric_coverage = min(0.1, _count_metric_values(rows, metric) / 800.0)
    score = round(min(0.99, request_fit + sufficiency + metric_coverage), 3)
    detail.update(
        {
            "request_fit": round(request_fit, 3),
            "sufficiency": round(sufficiency, 3),
            "metric_coverage": round(metric_coverage, 3),
        }
    )
    return score, detail


def run_data_science_story1(req: StoryRequest) -> StoryResult:
    user_text = req.user_query or ""
    fallback_member = req.member.member_id if req.member else None
    explicit_member = extract_explicit_member_id(user_text)
    member_id = normalize_member_id(explicit_member or fallback_member)

    if _is_member_scoped_request(f" {user_text.strip()} ") and not member_id:
        ask = "I can build that chart. What is your member_id (for example, MB001)?"
        return StoryResult(
            story_id=req.story_id,
            response_text=ask,
            follow_up_question=ask,
            story_output={"needs_member_id": True, "requested_slot": "member_id", "missing_slots": ["member_id"]},
        )

    start_date, end_date = parse_date_range_from_text(user_text, default_weeks=8)
    metrics_in_text = _extract_metrics_from_text(user_text)
    chart_type = _pick_chart_type(user_text)
    primary_metric = metrics_in_text[0] if metrics_in_text else _pick_metric(user_text, fallback="duration_min")
    dimension = _pick_dimension(user_text, chart_type=chart_type)
    agg = "avg"
    if _contains_any(user_text.lower(), ["count", "number of workouts", "how many"]):
        agg = "count"

    scatter_y_metric = metrics_in_text[1] if len(metrics_in_text) > 1 else ("calories" if primary_metric != "calories" else "duration_min")
    assumptions: List[str] = []
    warnings: List[str] = []
    request_underspecified = _is_underspecified_viz_request(user_text, metrics=metrics_in_text)
    planner_source = "deterministic_direct"
    planner_confidence = 1.0 if _is_explicit_chart_request(user_text) else 0.7
    plan_candidates: List[Dict[str, Any]] = []
    selected_plan: Dict[str, Any] = {
        "chart_type": chart_type,
        "metric": primary_metric,
        "secondary_metric": scatter_y_metric if chart_type == "scatter" else None,
        "dimension": dimension,
        "aggregation": agg if chart_type in {"line", "bar"} else None,
    }
    selection_rationale = "Used direct deterministic mapping from explicit chart intent."
    if not explicit_member and member_id and _is_member_scoped_request(f" {user_text.strip()} "):
        assumptions.append(f"Used member_id from conversation context: {member_id}.")
    if "last" not in user_text.lower() and "between" not in user_text.lower():
        assumptions.append("Used default date range: last 8 weeks.")

    rows = _read_rows(member_id=member_id, start_date=start_date, end_date=end_date)
    if not rows:
        scope = member_id or "the selected scope"
        msg = f"I could not find workout rows for {scope} between {start_date} and {end_date}."
        return StoryResult(
            story_id=req.story_id,
            response_text=msg,
            story_output={
                "row_count": 0,
                "chart_type": chart_type,
                "metric": primary_metric,
                "dimension": dimension,
                "date_range": {"start_date": start_date, "end_date": end_date},
            },
        )

    if request_underspecified:
        planner_source = "deterministic_candidate_planner"
        raw_candidates = _build_candidate_plans(user_text=user_text, primary_metric=primary_metric, metrics_in_text=metrics_in_text)
        scored: List[Dict[str, Any]] = []
        for candidate in raw_candidates:
            score, details = _score_candidate_plan(candidate, rows=rows, user_text=user_text)
            scored.append({**candidate, "score": score, "score_details": details})
        scored.sort(key=lambda c: float(c.get("score", 0.0)), reverse=True)
        plan_candidates = scored[:4]
        top = scored[0] if scored else selected_plan
        second = scored[1] if len(scored) > 1 else None
        planner_confidence = float(top.get("score", 0.0))
        selected_plan = {
            "chart_type": top.get("chart_type"),
            "metric": top.get("metric"),
            "secondary_metric": top.get("secondary_metric"),
            "dimension": top.get("dimension"),
            "aggregation": top.get("aggregation"),
            "score": planner_confidence,
        }
        selection_rationale = str(top.get("reason") or "Selected highest-scoring visualization candidate.")

        margin = planner_confidence - float(second.get("score", 0.0)) if isinstance(second, dict) else planner_confidence
        if planner_confidence < 0.5 or margin < 0.06:
            question = (
                "I can build this in a few ways. Which do you prefer: trend line over time, bar by category, "
                "scatter relationship, or histogram distribution?"
            )
            return StoryResult(
                story_id=req.story_id,
                response_text=question,
                follow_up_question=question,
                story_output={
                    "needs_clarification": True,
                    "requested_slot": "chart_type",
                    "missing_slots": ["chart_type"],
                    "member_id": member_id,
                    "date_range": {"start_date": start_date, "end_date": end_date},
                    "request_underspecified": True,
                    "planner_source": planner_source,
                    "planner_confidence": planner_confidence,
                    "plan_candidates": plan_candidates,
                    "selected_plan": selected_plan,
                    "selection_rationale": selection_rationale,
                },
            )

        chart_type = str(selected_plan.get("chart_type") or chart_type)
        primary_metric = str(selected_plan.get("metric") or primary_metric)
        if selected_plan.get("secondary_metric"):
            scatter_y_metric = str(selected_plan.get("secondary_metric"))
        if selected_plan.get("dimension"):
            dimension = str(selected_plan.get("dimension"))
        if selected_plan.get("aggregation") and chart_type in {"line", "bar"} and agg != "count":
            agg = str(selected_plan.get("aggregation"))

    if not _metric_has_values(rows, primary_metric):
        warnings.append(f"Requested metric '{primary_metric}' had no values in this date range; used 'duration_min' instead.")
        primary_metric = "duration_min"

    if chart_type == "scatter":
        if not _metric_has_values(rows, scatter_y_metric):
            scatter_y_metric = "duration_min" if primary_metric != "duration_min" else "calories"
        if not _metric_has_values(rows, scatter_y_metric):
            chart_type = "bar"
            dimension = "type"
            agg = "count"
            warnings.append("Could not build a scatter plot due to sparse metric values; returned workout counts by type.")

    points: Any
    interpretation: str
    if chart_type in {"line", "bar"}:
        points = _aggregate(rows=rows, dimension=dimension, metric=primary_metric, agg=agg)
        interpretation = (
            f"Built a {chart_type} chart of {primary_metric} by {dimension} using {agg} aggregation across {len(rows)} workouts."
        )
        title = f"{chart_type.title()} chart: {primary_metric} by {dimension}"
    elif chart_type == "scatter":
        points = _scatter_points(rows=rows, x_metric=primary_metric, y_metric=scatter_y_metric, limit=250)
        interpretation = (
            f"Built a scatter chart for {primary_metric} vs {scatter_y_metric} across {len(points)} plotted workouts."
        )
        title = f"Scatter: {primary_metric} vs {scatter_y_metric}"
    elif chart_type == "histogram":
        points = _hist_values(rows=rows, metric=primary_metric, limit=2000)
        interpretation = f"Built a histogram for {primary_metric} using {len(points)} observations."
        title = f"Distribution of {primary_metric}"
    else:
        points = _box_values(rows=rows, metric=primary_metric, dimension=dimension)
        observation_count = sum(len(g.get("values", [])) for g in points)
        interpretation = (
            f"Built a box chart for {primary_metric} across {len(points)} {dimension} groups using {observation_count} observations."
        )
        title = f"Box plot: {primary_metric} by {dimension}"

    if isinstance(points, list) and len(points) == 0:
        chart_type = "bar"
        dimension = "type"
        agg = "count"
        points = _aggregate(rows=rows, dimension=dimension, metric=primary_metric, agg=agg)
        title = "Fallback chart: workout counts by type"
        interpretation = (
            f"The requested chart had no plottable values, so I returned workout counts by {dimension} "
            f"across {len(rows)} workouts."
        )
        warnings.append("Applied fallback chart because requested visualization had no plottable points.")

    chart_spec = _build_plotly_spec(
        chart_type=chart_type,
        metric=primary_metric,
        points=points,
        dimension=dimension,
        title=title,
    )
    if not _is_valid_plotly_spec(chart_spec):
        # Guaranteed-safe fallback spec
        chart_spec = _build_plotly_spec(
            chart_type="bar",
            metric="workout_count",
            points=_aggregate(rows=rows, dimension="type", metric="duration_min", agg="count"),
            dimension="type",
            title="Fallback chart: workout counts by type",
        )
        warnings.append("Generated fallback chart_spec after validation check failed.")
        chart_type = "bar"
        primary_metric = "workout_count"
        dimension = "type"
        agg = "count"

    selected_plan = {
        "chart_type": chart_type,
        "metric": primary_metric,
        "secondary_metric": scatter_y_metric if chart_type == "scatter" else None,
        "dimension": dimension,
        "aggregation": agg if chart_type in {"line", "bar"} else None,
        "score": planner_confidence,
    }
    response = (
        f"{interpretation}\n"
        f"Date range: {start_date} to {end_date}. "
        f"{'Member scope: ' + member_id + '. ' if member_id else 'Scope: all members in Data Science workouts. '}"
        f"{'I selected this using a candidate-plan scorer for your underspecified request. ' if request_underspecified else ''}"
        f"I included a Plotly chart spec in `story_output.chart_spec`."
    )

    return StoryResult(
        story_id=req.story_id,
        response_text=response,
        story_output={
            "member_id": member_id,
            "chart_type": chart_type,
            "metric": primary_metric,
            "secondary_metric": scatter_y_metric if chart_type == "scatter" else None,
            "dimension": dimension,
            "aggregation": agg if chart_type in {"line", "bar"} else None,
            "date_range": {"start_date": start_date, "end_date": end_date},
            "row_count": len(rows),
            "point_count": len(points) if isinstance(points, list) else 0,
            "assumptions": assumptions,
            "warnings": warnings,
            "request_underspecified": request_underspecified,
            "planner_source": planner_source,
            "planner_confidence": planner_confidence,
            "plan_candidates": plan_candidates,
            "selected_plan": selected_plan,
            "selection_rationale": selection_rationale,
            "interpretation": interpretation,
            "chart_spec": chart_spec,
            "data_preview": points[:15] if isinstance(points, list) else [],
        },
        state_updates_global={"member": {"member_id": member_id}} if member_id else {},
        state_updates_domain={"last_story_summary": interpretation},
    )
