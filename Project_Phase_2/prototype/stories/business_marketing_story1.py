from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from ..contracts import StoryRequest, StoryResult
from ..utils import extract_explicit_member_id, normalize_campaign_id, parse_last_n_weeks

PROJECT_PHASE_2 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_2 / "kb" / "BusinessMarketing" / "brand_feedback.db"
LOW_DATA_THRESHOLD = 15


class FeedbackFilters(BaseModel):
    campaign_ids: Optional[List[str]] = None
    feedback_channels: Optional[List[str]] = None
    member_id: Optional[str] = None
    start_date: Optional[str] = None
    end_date: Optional[str] = None
    timeframe_label: str = "last_4_weeks"


class ThemeStats(BaseModel):
    theme: str
    n: int
    share: float
    avg_comment_len: float
    pos_share: float
    neu_share: float
    neg_share: float
    salience: float


class Adjustment(BaseModel):
    title: str
    change: str
    why_grounded: str
    receipts: List[str]


class AdjustmentsLLMOutput(BaseModel):
    adjustments: List[Adjustment] = Field(..., min_length=3, max_length=3)


class FeedbackGraphState(TypedDict, total=False):
    user_text: str
    filters: Dict[str, Any]
    feedback_rows: List[Dict[str, Any]]
    aggregation: Dict[str, Any]
    focus_themes: List[str]
    adjustments: List[Dict[str, Any]]
    route_mode: str
    data_quality: Dict[str, Any]
    response_text: str
    follow_up_question: Optional[str]


def _read_campaign_feedback(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    where = []
    params: List[Any] = []

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


def _aggregate_themes(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not rows:
        return {"total_n": 0, "overall_avg_len": 0.0, "themes": []}

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
    return {"total_n": total_n, "overall_avg_len": round(overall_avg_len, 2), "themes": out}


def _parse_filters(user_text: str) -> Dict[str, Any]:
    campaign_ids_raw = re.findall(r"\bCAMP[_-]?\d+\b", user_text.upper())
    campaign_ids = [normalize_campaign_id(cid) for cid in campaign_ids_raw]
    campaign_ids = [cid for cid in campaign_ids if cid]
    member_id = extract_explicit_member_id(user_text)
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


def _select_focus_themes(theme_stats: List[ThemeStats], k: int = 4) -> List[str]:
    if not theme_stats:
        return []
    by_sal = sorted(theme_stats, key=lambda t: t.salience, reverse=True)
    focus = [t.theme for t in by_sal[:k]]

    neg_sorted = sorted(theme_stats, key=lambda t: t.neg_share, reverse=True)
    if neg_sorted and neg_sorted[0].neg_share >= 0.5 and neg_sorted[0].theme not in focus:
        focus[-1] = neg_sorted[0].theme

    mixed = [t for t in theme_stats if t.pos_share > 0.2 and t.neg_share > 0.2]
    mixed_sorted = sorted(mixed, key=lambda t: t.salience, reverse=True)
    if mixed_sorted and mixed_sorted[0].theme not in focus:
        focus[-1] = mixed_sorted[0].theme

    out = []
    for x in focus:
        if x not in out:
            out.append(x)
    return out


def _deterministic_adjustments(themes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    chosen = themes[:3]
    adjustments: List[Dict[str, Any]] = []
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


def _parse_node(state: FeedbackGraphState) -> FeedbackGraphState:
    filters = _parse_filters(state.get("user_text", ""))
    return {"filters": FeedbackFilters(**filters).model_dump()}


def _retrieve_node(state: FeedbackGraphState) -> FeedbackGraphState:
    rows = _read_campaign_feedback(state["filters"])
    return {"feedback_rows": rows}


def _aggregate_node(state: FeedbackGraphState) -> FeedbackGraphState:
    agg = _aggregate_themes(state.get("feedback_rows", []))
    return {"aggregation": agg}


def _focus_node(state: FeedbackGraphState) -> FeedbackGraphState:
    stats = [ThemeStats(**t) for t in state.get("aggregation", {}).get("themes", [])]
    focus = _select_focus_themes(stats, k=4)
    return {"focus_themes": focus}


def _route_node(state: FeedbackGraphState) -> FeedbackGraphState:
    agg = state.get("aggregation", {})
    total_n = int(agg.get("total_n", 0))
    if total_n == 0:
        mode = "no_data"
    elif total_n < LOW_DATA_THRESHOLD:
        mode = "low_data"
    else:
        mode = "normal"
    return {
        "route_mode": mode,
        "data_quality": {
            "total_n": total_n,
            "low_data_threshold": LOW_DATA_THRESHOLD,
        },
    }


def _recommend_node(state: FeedbackGraphState, llm: ChatOpenAI) -> FeedbackGraphState:
    themes = state.get("aggregation", {}).get("themes", [])
    if not themes:
        return {"adjustments": []}

    focus = set(state.get("focus_themes", []))
    focus_rows = [t for t in themes if t["theme"] in focus] or themes[:4]
    system = (
        "You are a brand/content strategy assistant.\n"
        "Return exactly 3 content adjustments.\n"
        "Content-only changes: copy, creative, messaging structure, framing, CTA, sequencing.\n"
        "Ground strictly in provided theme stats. Do not introduce new themes. Do not claim causes.\n"
        "Each adjustment must include 2-3 receipts citing actual numbers.\n"
        "Output must match schema: adjustments[{title, change, why_grounded, receipts}]."
    )
    user = f"Theme stats (sorted by salience):\n{themes}\n\nFocus themes:\n{focus_rows}\n"

    try:
        structured = llm.with_structured_output(AdjustmentsLLMOutput)
        out = structured.invoke([("system", system), ("user", user)])
        return {"adjustments": [a.model_dump() for a in out.adjustments]}
    except Exception:
        return {"adjustments": _deterministic_adjustments(themes)}


def _format_node(state: FeedbackGraphState) -> FeedbackGraphState:
    filters = FeedbackFilters(**state["filters"])
    rows = state.get("feedback_rows", [])
    agg = state.get("aggregation", {})
    themes = agg.get("themes", [])
    adjustments = state.get("adjustments", [])
    route_mode = state.get("route_mode") or "normal"

    if route_mode == "no_data" or not themes:
        text = (
            "No feedback matched those filters. "
            "Try widening the date range, removing channel filters, or omitting campaign_ids."
        )
        return {"response_text": text, "follow_up_question": None}
    if route_mode == "low_data":
        lines = [
            "Brand Manager Feedback Themes Summary (Limited Sample)",
            f"Date range: {filters.start_date}..{filters.end_date} ({filters.timeframe_label})",
            f"Rows analyzed: {agg.get('total_n', len(rows))} (low confidence; target >= {LOW_DATA_THRESHOLD})",
            "Top themes (provisional):",
        ]
        for t in themes[:2]:
            lines.append(
                f"- {t['theme']}: n={t['n']}, share={t['share']}, neg={t['neg_share']}, pos={t['pos_share']}, salience={t['salience']}"
            )
        follow_up = (
            "Would you like me to widen the window to the last 8 weeks, "
            "or include all channels/campaigns to improve confidence?"
        )
        lines.append("")
        lines.append("I can provide stronger recommendations after expanding scope.")
        return {"response_text": "\n".join(lines), "follow_up_question": follow_up}

    lines = [
        "Brand Manager Feedback Themes Summary",
        f"Date range: {filters.start_date}..{filters.end_date} ({filters.timeframe_label})",
        f"Rows analyzed: {agg.get('total_n', len(rows))}",
        "Top themes:",
    ]
    for t in themes[:5]:
        lines.append(
            f"- {t['theme']}: n={t['n']}, share={t['share']}, neg={t['neg_share']}, pos={t['pos_share']}, salience={t['salience']}"
        )

    lines.append("\n3 content adjustments:")
    for i, a in enumerate(adjustments[:3], start=1):
        lines.append(f"{i}. {a['title']} | {a['change']} | {a['why_grounded']}")

    return {"response_text": "\n".join(lines), "follow_up_question": None}


def _build_story_graph():
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    g = StateGraph(FeedbackGraphState)

    def recommend_node(state: FeedbackGraphState) -> FeedbackGraphState:
        return _recommend_node(state, llm)

    g.add_node("parse", _parse_node)
    g.add_node("retrieve", _retrieve_node)
    g.add_node("aggregate", _aggregate_node)
    g.add_node("route", _route_node)
    g.add_node("focus", _focus_node)
    g.add_node("recommend", recommend_node)
    g.add_node("format", _format_node)

    def route_after_route(state: FeedbackGraphState) -> str:
        return state.get("route_mode") or "normal"

    g.set_entry_point("parse")
    g.add_edge("parse", "retrieve")
    g.add_edge("retrieve", "aggregate")
    g.add_edge("aggregate", "route")
    g.add_conditional_edges("route", route_after_route, {"no_data": "format", "low_data": "format", "normal": "focus"})
    g.add_edge("focus", "recommend")
    g.add_edge("recommend", "format")
    g.add_edge("format", END)
    return g.compile()


BUSINESS_MARKETING_GRAPH = None


def _get_business_marketing_graph():
    global BUSINESS_MARKETING_GRAPH
    if BUSINESS_MARKETING_GRAPH is None:
        BUSINESS_MARKETING_GRAPH = _build_story_graph()
    return BUSINESS_MARKETING_GRAPH


def get_business_marketing_story1_mermaid() -> str:
    return _get_business_marketing_graph().get_graph().draw_mermaid()


def run_business_marketing_story1(req: StoryRequest) -> StoryResult:
    state_out = _get_business_marketing_graph().invoke({"user_text": req.user_query})
    filters = state_out.get("filters", {})
    rows = state_out.get("feedback_rows", [])
    agg = state_out.get("aggregation", {"total_n": 0, "themes": []})
    focus = state_out.get("focus_themes", [])
    adjustments = state_out.get("adjustments", [])
    route_mode = state_out.get("route_mode", "normal")
    data_quality = state_out.get("data_quality", {})
    text = state_out.get("response_text", "I can summarize campaign feedback.")

    return StoryResult(
        story_id=req.story_id,
        response_text=text,
        follow_up_question=state_out.get("follow_up_question"),
        story_output={
            "filters": filters,
            "feedback_rows": rows,
            "aggregation": agg,
            "focus_themes": focus,
            "adjustments": adjustments,
            "route_mode": route_mode,
            "data_quality": data_quality,
            "generated_on": date.today().isoformat(),
        },
        state_updates_domain={"last_story_summary": f"Analyzed {agg.get('total_n', 0)} feedback rows."},
    )
