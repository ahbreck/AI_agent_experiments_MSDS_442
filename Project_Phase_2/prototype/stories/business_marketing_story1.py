from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List

from ..contracts import StoryRequest, StoryResult
from ..utils import normalize_campaign_id, normalize_member_id, parse_last_n_weeks

PROJECT_PHASE_2 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_2 / "kb" / "BusinessMarketing" / "brand_feedback.db"


def _read_campaign_feedback(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    where, params = [], []

    campaign_ids = filters.get("campaign_ids")
    if campaign_ids:
        where.append(f"REPLACE(REPLACE(UPPER(campaign_id), '-', ''), '_', '') IN ({','.join(['?'] * len(campaign_ids))})")
        params.extend(campaign_ids)

    channels = filters.get("feedback_channels")
    if channels:
        where.append(f"feedback_channel IN ({','.join(['?'] * len(channels))})")
        params.extend([c.lower() for c in channels])

    member_id = filters.get("member_id")
    if member_id:
        where.append("REPLACE(REPLACE(UPPER(member_id), '-', ''), '_', '') = ?")
        params.append(member_id)

    if filters.get("start_date"):
        where.append("created_at >= ?")
        params.append(filters["start_date"])

    if filters.get("end_date"):
        where.append("created_at <= ?")
        params.append(filters["end_date"])

    where_sql = "WHERE " + " AND ".join(where) if where else ""
    sql = f"""
    SELECT feedback_id, campaign_id, feedback_channel, sentiment,
           primary_theme, comment_length_words, created_at
    FROM campaign_feedback
    {where_sql}
    """

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        rows = [dict(r) for r in conn.execute(sql, params).fetchall()]
    return rows


def _aggregate(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"total_n": 0, "themes": []}

    by_theme: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for r in rows:
        by_theme[(r.get("primary_theme") or "UNKNOWN_THEME").strip()].append(r)

    out: List[Dict[str, Any]] = []
    total_n = len(rows)
    overall_avg_len = sum(int(r.get("comment_length_words") or 0) for r in rows) / total_n

    for theme, grp in by_theme.items():
        n = len(grp)
        share = n / total_n
        avg_len = sum(int(g.get("comment_length_words") or 0) for g in grp) / n
        pos = sum(1 for g in grp if (g.get("sentiment") or "").lower() == "positive")
        neg = sum(1 for g in grp if (g.get("sentiment") or "").lower() == "negative")
        neu = n - pos - neg
        pos_share, neg_share, neu_share = pos / n, neg / n, neu / n
        salience = share * ((avg_len / overall_avg_len) if overall_avg_len else 1.0) * (1.0 + 0.5 * neg_share)
        out.append(
            {
                "theme": theme,
                "n": n,
                "share": round(share, 4),
                "avg_comment_len": round(avg_len, 2),
                "pos_share": round(pos_share, 4),
                "neu_share": round(neu_share, 4),
                "neg_share": round(neg_share, 4),
                "salience": round(salience, 6),
            }
        )

    out.sort(key=lambda x: x["salience"], reverse=True)
    return {"total_n": total_n, "themes": out}


def _parse_filters(user_text: str) -> Dict[str, Any]:
    campaign_ids_raw = re.findall(r"\bCAMP[_-]?\d+\b", user_text.upper())
    campaign_ids = [normalize_campaign_id(cid) for cid in campaign_ids_raw]
    campaign_ids = [cid for cid in campaign_ids if cid]
    member_id = normalize_member_id(user_text)
    channels = [c for c in ["email", "app", "social", "web"] if re.search(rf"\b{c}\b", user_text.lower())]
    start_date, end_date, timeframe_label = parse_last_n_weeks(user_text, default_weeks=4)
    return {
        "campaign_ids": campaign_ids or None,
        "member_id": member_id,
        "feedback_channels": channels or None,
        "start_date": start_date,
        "end_date": end_date,
        "timeframe_label": timeframe_label,
    }


def _build_adjustments(themes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    chosen = themes[:3]
    adjustments = []
    for t in chosen:
        theme = t["theme"]
        adjustments.append(
            {
                "title": f"Address {theme}",
                "change": f"Update campaign messaging/creative to directly reduce friction around '{theme}'.",
                "why_grounded": f"Theme salience={t['salience']}, share={t['share']}, neg_share={t['neg_share']}.",
                "receipts": [f"n={t['n']}", f"share={t['share']}", f"neg_share={t['neg_share']}"],
            }
        )
    return adjustments


def run_business_marketing_story1(req: StoryRequest) -> StoryResult:
    filters = _parse_filters(req.user_query)
    rows = _read_campaign_feedback(filters)
    agg = _aggregate(rows)

    if agg["total_n"] == 0:
        text = (
            "No feedback matched those filters. "
            "Try widening the date range, removing channel filters, or omitting campaign_ids."
        )
        return StoryResult(
            story_id=req.story_id,
            response_text=text,
            story_output={"filters": filters, "feedback_rows": [], "aggregation": agg},
            state_updates_domain={"last_story_summary": "No rows for applied filters."},
        )

    adjustments = _build_adjustments(agg["themes"])
    lines = [
        "Brand Manager Feedback Themes Summary",
        f"Date range: {filters['start_date']}..{filters['end_date']} ({filters['timeframe_label']})",
        f"Rows analyzed: {agg['total_n']}",
        "Top themes:",
    ]
    for t in agg["themes"][:5]:
        lines.append(
            f"- {t['theme']}: n={t['n']}, share={t['share']}, neg={t['neg_share']}, pos={t['pos_share']}, salience={t['salience']}"
        )

    lines.append("\n3 content adjustments:")
    for i, a in enumerate(adjustments, start=1):
        lines.append(f"{i}. {a['title']} | {a['change']} | {a['why_grounded']}")

    return StoryResult(
        story_id=req.story_id,
        response_text="\n".join(lines),
        story_output={
            "filters": filters,
            "feedback_rows": rows,
            "aggregation": agg,
            "focus_themes": [t["theme"] for t in agg["themes"][:4]],
            "adjustments": adjustments,
            "generated_on": date.today().isoformat(),
        },
        state_updates_domain={"last_story_summary": f"Analyzed {agg['total_n']} feedback rows."},
    )
