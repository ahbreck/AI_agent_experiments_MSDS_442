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


def _contains_any(text: str, terms: List[str]) -> bool:
    tl = text.lower()
    return any(term in tl for term in terms)


def _pick_metric(user_text: str, fallback: str = "duration_min") -> str:
    tl = user_text.lower()
    for metric, aliases in METRIC_ALIASES.items():
        if _contains_any(tl, aliases):
            return metric
    return fallback


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
    chart_type = _pick_chart_type(user_text)
    primary_metric = _pick_metric(user_text, fallback="duration_min")
    dimension = _pick_dimension(user_text, chart_type=chart_type)
    agg = "avg"
    if _contains_any(user_text.lower(), ["count", "number of workouts", "how many"]):
        agg = "count"

    scatter_y_metric = "calories" if primary_metric != "calories" else "duration_min"
    assumptions: List[str] = []
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

    chart_spec = _build_plotly_spec(
        chart_type=chart_type,
        metric=primary_metric,
        points=points,
        dimension=dimension,
        title=title,
    )
    response = (
        f"{interpretation}\n"
        f"Date range: {start_date} to {end_date}. "
        f"{'Member scope: ' + member_id + '. ' if member_id else 'Scope: all members in Data Science workouts. '}"
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
            "interpretation": interpretation,
            "chart_spec": chart_spec,
            "data_preview": points[:15] if isinstance(points, list) else [],
        },
        state_updates_global={"member": {"member_id": member_id}} if member_id else {},
        state_updates_domain={"last_story_summary": interpretation},
    )
