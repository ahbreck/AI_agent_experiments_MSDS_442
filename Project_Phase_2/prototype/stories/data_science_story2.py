from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from datetime import date, datetime
from pathlib import Path
from statistics import mean
from typing import Any, Dict, List, Optional, Tuple

from ..contracts import StoryRequest, StoryResult
from ..utils import normalize_member_id, parse_date_range_from_text

PROJECT_PHASE_2 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_2 / "kb" / "DataScience" / "peloton_workouts.sqlite"


def _read_workouts(member_id: str, start_date: str, end_date: str, types: Optional[List[str]] = None) -> List[Dict[str, Any]]:
    q = """
    SELECT *
    FROM workouts
    WHERE REPLACE(REPLACE(UPPER(member_id), '-', ''), '_', '') = ?
      AND date >= ?
      AND date <= ?
    """
    params: List[Any] = [member_id, start_date, end_date]
    if types:
        q += f" AND type IN ({','.join(['?'] * len(types))})"
        params.extend(types)

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        return [dict(r) for r in conn.execute(q, params).fetchall()]


def _parse_request(user_text: str, fallback_member: Optional[str]) -> Tuple[Optional[str], str, str]:
    member = normalize_member_id(user_text) or fallback_member
    start, end = parse_date_range_from_text(user_text, default_weeks=8)
    return member, start, end


def _trend(rows: List[Dict[str, Any]], metric: str) -> Dict[str, Any]:
    vals = [float(r.get(metric)) for r in rows if r.get(metric) is not None]
    if len(vals) < 2:
        return {"metric": metric, "note": "Not enough points"}
    delta = vals[-1] - vals[0]
    direction = "up" if delta > 0 else "down" if delta < 0 else "flat"
    return {"metric": metric, "start": round(vals[0], 2), "end": round(vals[-1], 2), "delta": round(delta, 2), "direction": direction}


def _zone_distribution(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    zone_cols = ["zone1_minutes", "zone2_minutes", "zone3_minutes", "zone4_minutes", "zone5_minutes"]
    totals = {c: 0.0 for c in zone_cols}
    denom = 0.0

    for r in rows:
        zsum = 0.0
        for c in zone_cols:
            v = r.get(c)
            if v is not None:
                vv = float(v)
                totals[c] += vv
                zsum += vv
        denom += zsum if zsum > 0 else float(r.get("duration_min") or 0.0)

    if denom <= 0:
        return {"note": "No zone or duration data"}

    return {k: round(v / denom, 4) for k, v in totals.items()}


def _segment_by_type(rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    by_type: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_type[str(r.get("type") or "UNKNOWN")].append(r)

    out = []
    for t, grp in sorted(by_type.items(), key=lambda kv: len(kv[1]), reverse=True):
        duration = [float(g.get("duration_min") or 0) for g in grp]
        calories = [float(g.get("calories") or 0) for g in grp if g.get("calories") is not None]
        out.append(
            {
                "type": t,
                "n": len(grp),
                "avg_duration_min": round(mean(duration), 2) if duration else None,
                "avg_calories": round(mean(calories), 2) if calories else None,
            }
        )
    return out


def _detect_outliers(rows: List[Dict[str, Any]], metric: str) -> List[Dict[str, Any]]:
    pairs = [(r, float(r.get(metric))) for r in rows if r.get(metric) is not None]
    if len(pairs) < 8:
        return []
    vals = sorted(v for _, v in pairs)
    q1 = vals[len(vals) // 4]
    q3 = vals[(len(vals) * 3) // 4]
    iqr = q3 - q1
    lo, hi = q1 - 1.5 * iqr, q3 + 1.5 * iqr
    out = []
    for r, v in pairs:
        if v < lo or v > hi:
            out.append({"workout_id": r.get("workout_id"), "date": r.get("date"), "type": r.get("type"), metric: v})
    return out[:10]


def run_data_science_story2(req: StoryRequest) -> StoryResult:
    fallback_member = req.member.member_id
    member_id, start_date, end_date = _parse_request(req.user_query, fallback_member=fallback_member)

    if not member_id:
        ask = "What is your member_id (e.g., MB001)? If dates are missing I will analyze the last 8 weeks."
        return StoryResult(
            story_id=req.story_id,
            response_text=ask,
            follow_up_question=ask,
            story_output={"needs_member_id": True},
        )

    rows = _read_workouts(member_id=member_id, start_date=start_date, end_date=end_date)
    if not rows:
        msg = f"I did not find workouts for {member_id} between {start_date} and {end_date}."
        return StoryResult(
            story_id=req.story_id,
            response_text=msg,
            story_output={"member_id": member_id, "start_date": start_date, "end_date": end_date, "row_count": 0},
            state_updates_global={"member": {"member_id": member_id}},
        )

    rows_sorted = sorted(rows, key=lambda r: datetime.fromisoformat(str(r["date"])))
    trends = [_trend(rows_sorted, m) for m in ["duration_min", "calories", "strive_score"] if m in rows_sorted[0]]
    zones = _zone_distribution(rows_sorted)
    by_type = _segment_by_type(rows_sorted)
    outliers = _detect_outliers(rows_sorted, metric="duration_min")

    lines = [
        f"Workout analysis for {member_id}",
        f"Date range: {start_date}..{end_date}",
        f"Workouts analyzed: {len(rows_sorted)}",
        "Key trends:",
    ]
    for t in trends:
        if "note" in t:
            lines.append(f"- {t['metric']}: {t['note']}")
        else:
            lines.append(f"- {t['metric']}: {t['direction']} (delta={t['delta']}, {t['start']} -> {t['end']})")

    lines.append("Zone distribution:")
    if "note" in zones:
        lines.append(f"- {zones['note']}")
    else:
        lines.append("- " + ", ".join([f"{k}={v}" for k, v in zones.items()]))

    lines.append("Top workout types:")
    for s in by_type[:3]:
        lines.append(f"- {s['type']}: n={s['n']}, avg_duration={s['avg_duration_min']}, avg_calories={s['avg_calories']}")

    if outliers:
        lines.append(f"Detected {len(outliers)} duration outlier(s).")

    return StoryResult(
        story_id=req.story_id,
        response_text="\n".join(lines),
        story_output={
            "member_id": member_id,
            "start_date": start_date,
            "end_date": end_date,
            "row_count": len(rows_sorted),
            "trends": trends,
            "zone_distribution": zones,
            "segments_by_type": by_type,
            "outliers_duration": outliers,
            "generated_on": date.today().isoformat(),
        },
        state_updates_global={"member": {"member_id": member_id}},
        state_updates_domain={"last_story_summary": f"Analyzed {len(rows_sorted)} workouts."},
    )
