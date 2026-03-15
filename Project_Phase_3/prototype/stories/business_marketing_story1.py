from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Dict, List, Optional, TypedDict

from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from ..contracts import StoryRequest, StoryResult
from ..utils import (
    build_chat_openai,
    extract_explicit_member_id,
    normalize_campaign_id,
    parse_last_n_weeks,
    register_sqlite_alnum_normalizer,
)

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
    validation_warnings: List[str]
    route_mode: str
    data_quality: Dict[str, Any]
    rescope_attempt: int
    max_rescope_attempts: int
    rescope_actions: List[Dict[str, str]]
    rescope_exhausted: bool
    recommend_retry_count: int
    recommend_retry_requested: bool
    response_text: str
    follow_up_question: Optional[str]


def _read_campaign_feedback(filters: Dict[str, Any]) -> List[Dict[str, Any]]:
    where = []
    params: List[Any] = []

    campaign_ids = filters.get("campaign_ids")
    if campaign_ids:
        where.append(f"NORM_ALNUM(campaign_id) IN ({','.join(['?'] * len(campaign_ids))})")
        params.extend(campaign_ids)

    channels = filters.get("feedback_channels")
    if channels:
        where.append(f"feedback_channel IN ({','.join(['?'] * len(channels))})")
        params.extend([c.lower() for c in channels])

    member_id = filters.get("member_id")
    if member_id:
        where.append("NORM_ALNUM(member_id) = ?")
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

    with closing(sqlite3.connect(DB_PATH)) as conn:
        register_sqlite_alnum_normalizer(conn)
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
    campaign_ids_raw = re.findall(r"\bCAMP[\W_]*\d+\b", user_text.upper())
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


def _deterministic_adjustment_for_theme(theme: Dict[str, Any]) -> Dict[str, Any]:
    tname = str(theme.get("theme") or "UNKNOWN_THEME")
    n = theme.get("n", 0)
    share = theme.get("share", 0)
    neg_share = theme.get("neg_share", 0)
    salience = theme.get("salience", 0)
    return {
        "title": f"Address {tname}",
        "change": (
            f"Revise campaign copy and CTA sequencing to reduce friction around '{tname}' "
            "in primary message and first CTA step."
        ),
        "why_grounded": (
            "This theme is prioritized by observed salience and sentiment mix in the selected window."
        ),
        "receipts": [
            f"theme={tname}",
            f"n={n}",
            f"share={share}",
            f"neg_share={neg_share}",
            f"salience={salience}",
        ],
    }


def _parse_node(state: FeedbackGraphState) -> FeedbackGraphState:
    filters = _parse_filters(state.get("user_text", ""))
    return {
        "filters": FeedbackFilters(**filters).model_dump(),
        "rescope_attempt": 0,
        "max_rescope_attempts": 3,
        "rescope_actions": [],
        "rescope_exhausted": False,
        "recommend_retry_count": 0,
        "recommend_retry_requested": False,
    }


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
    attempts = int(state.get("rescope_attempt", 0))
    max_attempts = int(state.get("max_rescope_attempts", 3))
    if total_n == 0:
        mode = "no_data"
    elif total_n < LOW_DATA_THRESHOLD:
        mode = "low_data"
    else:
        mode = "normal"
    exhausted = False
    if mode in {"no_data", "low_data"}:
        exhausted = attempts >= max_attempts or _next_rescope_action(state) is None
    return {
        "route_mode": mode,
        "data_quality": {
            "total_n": total_n,
            "low_data_threshold": LOW_DATA_THRESHOLD,
        },
        "rescope_exhausted": exhausted,
    }


def _timeframe_weeks(filters: Dict[str, Any]) -> Optional[int]:
    start = filters.get("start_date")
    end = filters.get("end_date")
    if start and end:
        try:
            d_start = date.fromisoformat(str(start))
            d_end = date.fromisoformat(str(end))
            days = max((d_end - d_start).days, 0)
            return max(1, (days + 6) // 7)
        except ValueError:
            pass

    label = str(filters.get("timeframe_label") or "")
    m = re.search(r"(\d+)\s*_?\s*weeks?", label.lower())
    if m:
        try:
            return int(m.group(1))
        except ValueError:
            return None
    return None


def _set_timeframe(filters: Dict[str, Any], weeks: int) -> None:
    today = date.today()
    filters["end_date"] = today.isoformat()
    filters["start_date"] = (today - timedelta(weeks=weeks)).isoformat()
    filters["timeframe_label"] = f"last_{weeks}_weeks"


def _next_rescope_action(state: FeedbackGraphState) -> Optional[Dict[str, str]]:
    filters = dict(state.get("filters", {}))
    done = {str(a.get("action")) for a in state.get("rescope_actions", [])}
    weeks = _timeframe_weeks(filters)

    if "widen_to_8_weeks" not in done and (weeks is None or weeks < 8):
        return {"action": "widen_to_8_weeks", "reason": f"timeframe_weeks={weeks or 'unknown'} < 8"}
    if "widen_to_12_weeks" not in done and (weeks is None or weeks < 12):
        return {"action": "widen_to_12_weeks", "reason": f"timeframe_weeks={weeks or 'unknown'} < 12"}
    if "drop_campaign_ids" not in done and filters.get("campaign_ids"):
        return {"action": "drop_campaign_ids", "reason": "campaign_ids filter present"}
    if "drop_feedback_channels" not in done and filters.get("feedback_channels"):
        return {"action": "drop_feedback_channels", "reason": "feedback_channels filter present"}
    return None


def _rescope_node(state: FeedbackGraphState) -> FeedbackGraphState:
    filters = dict(state.get("filters", {}))
    actions = list(state.get("rescope_actions", []))
    attempts = int(state.get("rescope_attempt", 0))
    max_attempts = int(state.get("max_rescope_attempts", 3))
    if attempts >= max_attempts:
        return {"rescope_exhausted": True}

    step = _next_rescope_action(state)
    if not step:
        return {"rescope_exhausted": True}

    action = step["action"]
    if action == "widen_to_8_weeks":
        _set_timeframe(filters, 8)
    elif action == "widen_to_12_weeks":
        _set_timeframe(filters, 12)
    elif action == "drop_campaign_ids":
        filters["campaign_ids"] = None
    elif action == "drop_feedback_channels":
        filters["feedback_channels"] = None

    actions.append(step)
    return {
        "filters": FeedbackFilters(**filters).model_dump(),
        "rescope_attempt": attempts + 1,
        "rescope_actions": actions,
        "rescope_exhausted": False,
    }


def _recommend_node(state: FeedbackGraphState, llm: Any) -> FeedbackGraphState:
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
    if int(state.get("recommend_retry_count", 0)) > 0:
        system += (
            "\nStrict grounding mode: every adjustment must name exactly one known theme and include at least "
            "two exact numeric receipts that match that theme's stats."
        )
    user = f"Theme stats (sorted by salience):\n{themes}\n\nFocus themes:\n{focus_rows}\n"

    try:
        structured = llm.with_structured_output(AdjustmentsLLMOutput)
        out = structured.invoke([("system", system), ("user", user)])
        return {"adjustments": [a.model_dump() for a in out.adjustments], "recommend_retry_requested": False}
    except Exception:
        return {"adjustments": _deterministic_adjustments(themes), "recommend_retry_requested": False}


def _validate_recommendations(state: FeedbackGraphState) -> Dict[str, Any]:
    themes = state.get("aggregation", {}).get("themes", [])
    adjustments = state.get("adjustments", [])
    focus_themes = [str(t) for t in state.get("focus_themes", [])]
    allowed_themes = set(focus_themes) | {str(t.get("theme")) for t in themes}

    warnings: List[str] = []
    invalid_indices: List[int] = []
    if not adjustments:
        return {"warnings": warnings, "invalid_indices": invalid_indices}

    theme_names_sorted = sorted([t for t in allowed_themes if t], key=len, reverse=True)
    metric_keys = {"n", "share", "neg_share", "salience"}

    def _find_theme(text: str) -> Optional[str]:
        tl = text.lower()
        for name in theme_names_sorted:
            if name.lower() in tl:
                return name
        return None

    theme_metric_index: Dict[str, Dict[str, float]] = {}
    for t in themes:
        name = str(t.get("theme") or "")
        if not name:
            continue
        theme_metric_index[name] = {}
        for key in metric_keys:
            try:
                theme_metric_index[name][key] = float(t.get(key))
            except (TypeError, ValueError):
                continue

    for idx, adj in enumerate(adjustments, start=1):
        prior_warning_count = len(warnings)
        if not isinstance(adj, dict):
            warnings.append(f"adjustment_{idx}: not a dict")
            invalid_indices.append(idx - 1)
            continue

        title = str(adj.get("title") or "").strip()
        change = str(adj.get("change") or "").strip()
        why = str(adj.get("why_grounded") or "").strip()
        receipts = adj.get("receipts")

        if not title:
            warnings.append(f"adjustment_{idx}: missing title")
        if not change:
            warnings.append(f"adjustment_{idx}: missing change")
        if not why:
            warnings.append(f"adjustment_{idx}: missing why_grounded")
        if not isinstance(receipts, list):
            warnings.append(f"adjustment_{idx}: receipts missing or not a list")
            receipts = []
        elif len(receipts) < 2:
            warnings.append(f"adjustment_{idx}: fewer than 2 receipts")

        referenced_theme = _find_theme(" ".join([title, change, why]))
        if not referenced_theme:
            warnings.append(f"adjustment_{idx}: no recognized theme reference")

        metric_receipt_count = 0
        for receipt in receipts:
            rtxt = str(receipt or "").strip()
            m = re.search(r"\b(n|share|neg_share|salience)\s*=\s*([0-9]*\.?[0-9]+)\b", rtxt)
            if not m:
                continue
            metric_receipt_count += 1
            key = m.group(1)
            try:
                val = float(m.group(2))
            except ValueError:
                continue

            if referenced_theme and referenced_theme in theme_metric_index and key in theme_metric_index[referenced_theme]:
                expected = theme_metric_index[referenced_theme][key]
                if abs(expected - val) > 1e-6:
                    warnings.append(
                        f"adjustment_{idx}: receipt mismatch for {referenced_theme} {key} (got {val}, expected {expected})"
                    )

        if metric_receipt_count < 2:
            warnings.append(f"adjustment_{idx}: fewer than 2 numeric metric receipts")

        if len(warnings) > prior_warning_count:
            invalid_indices.append(idx - 1)

    invalid_dedup = sorted(set(i for i in invalid_indices if 0 <= i < len(adjustments)))
    return {"warnings": warnings, "invalid_indices": invalid_dedup}


def _validate_node(state: FeedbackGraphState) -> FeedbackGraphState:
    checked = _validate_recommendations(state)
    warnings: List[str] = list(checked.get("warnings", []))
    invalid_indices: List[int] = list(checked.get("invalid_indices", []))
    adjustments = list(state.get("adjustments", []))
    retry_count = int(state.get("recommend_retry_count", 0))

    if invalid_indices and retry_count < 1:
        warnings.append("validation: retrying recommendation generation in strict grounding mode")
        return {
            "validation_warnings": warnings,
            "recommend_retry_count": retry_count + 1,
            "recommend_retry_requested": True,
        }

    if not invalid_indices or not adjustments:
        return {"validation_warnings": warnings, "recommend_retry_requested": False}

    themes = state.get("aggregation", {}).get("themes", [])
    focus = [str(x) for x in state.get("focus_themes", [])]

    candidate_themes: List[Dict[str, Any]] = []
    for name in focus:
        hit = next((t for t in themes if str(t.get("theme")) == name), None)
        if hit:
            candidate_themes.append(hit)
    for t in themes:
        if t not in candidate_themes:
            candidate_themes.append(t)
    if not candidate_themes:
        return {"validation_warnings": warnings}

    for replacement_order, bad_idx in enumerate(invalid_indices):
        theme = candidate_themes[replacement_order % len(candidate_themes)]
        adjustments[bad_idx] = _deterministic_adjustment_for_theme(theme)
        warnings.append(
            f"adjustment_{bad_idx + 1}: replaced with deterministic grounded adjustment for theme={theme.get('theme')}"
        )

    return {"adjustments": adjustments, "validation_warnings": warnings, "recommend_retry_requested": False}


def _format_node(state: FeedbackGraphState) -> FeedbackGraphState:
    filters = FeedbackFilters(**state["filters"])
    rows = state.get("feedback_rows", [])
    agg = state.get("aggregation", {})
    themes = agg.get("themes", [])
    adjustments = state.get("adjustments", [])
    route_mode = state.get("route_mode") or "normal"
    rescope_actions = state.get("rescope_actions", [])
    rescope_attempt = int(state.get("rescope_attempt", 0))
    max_rescope_attempts = int(state.get("max_rescope_attempts", 3))

    if route_mode == "no_data" or not themes:
        text = "No feedback matched those filters."
        if rescope_actions:
            action_text = "; ".join(f"{a.get('action')} ({a.get('reason')})" for a in rescope_actions)
            text += f" Auto-rescope attempts: {rescope_attempt}/{max_rescope_attempts}. Tried: {action_text}."
        text += " Try widening the date range, removing channel filters, or omitting campaign_ids."
        return {"response_text": text, "follow_up_question": None}
    if route_mode == "low_data":
        lines = [
            "Brand Manager Feedback Themes Summary (Limited Sample)",
            f"Date range: {filters.start_date}..{filters.end_date} ({filters.timeframe_label})",
            f"Rows analyzed: {agg.get('total_n', len(rows))} (low confidence; target >= {LOW_DATA_THRESHOLD})",
            f"Auto-rescope attempts used: {rescope_attempt}/{max_rescope_attempts}",
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
        if rescope_actions:
            lines.append("Rescope actions tried:")
            for a in rescope_actions:
                lines.append(f"- {a.get('action')}: {a.get('reason')}")
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
    llm = build_chat_openai(model="gpt-4o-mini", temperature=0)
    g = StateGraph(FeedbackGraphState)

    def recommend_node(state: FeedbackGraphState) -> FeedbackGraphState:
        return _recommend_node(state, llm)

    g.add_node("parse", _parse_node)
    g.add_node("retrieve", _retrieve_node)
    g.add_node("aggregate", _aggregate_node)
    g.add_node("route", _route_node)
    g.add_node("rescope", _rescope_node)
    g.add_node("focus", _focus_node)
    g.add_node("recommend", recommend_node)
    g.add_node("validate", _validate_node)
    g.add_node("format", _format_node)

    def route_after_route(state: FeedbackGraphState) -> str:
        mode = state.get("route_mode") or "normal"
        if mode in {"no_data", "low_data"}:
            attempts = int(state.get("rescope_attempt", 0))
            max_attempts = int(state.get("max_rescope_attempts", 3))
            if attempts < max_attempts and _next_rescope_action(state):
                return "retry"
            return "format"
        return "normal"

    def route_after_validate(state: FeedbackGraphState) -> str:
        return "retry_recommend" if state.get("recommend_retry_requested") else "format"

    g.set_entry_point("parse")
    g.add_edge("parse", "retrieve")
    g.add_edge("retrieve", "aggregate")
    g.add_edge("aggregate", "route")
    g.add_conditional_edges("route", route_after_route, {"retry": "rescope", "format": "format", "normal": "focus"})
    g.add_edge("rescope", "retrieve")
    g.add_edge("focus", "recommend")
    g.add_edge("recommend", "validate")
    g.add_conditional_edges("validate", route_after_validate, {"retry_recommend": "recommend", "format": "format"})
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
    validation_warnings = state_out.get("validation_warnings", [])
    route_mode = state_out.get("route_mode", "normal")
    data_quality = state_out.get("data_quality", {})
    rescope_attempt = int(state_out.get("rescope_attempt", 0))
    max_rescope_attempts = int(state_out.get("max_rescope_attempts", 3))
    rescope_actions = state_out.get("rescope_actions", [])
    rescope_exhausted = bool(state_out.get("rescope_exhausted", False))
    recommend_retry_count = int(state_out.get("recommend_retry_count", 0))
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
            "validation_warnings": validation_warnings,
            "route_mode": route_mode,
            "data_quality": data_quality,
            "rescope_attempt": rescope_attempt,
            "max_rescope_attempts": max_rescope_attempts,
            "rescope_actions": rescope_actions,
            "rescope_exhausted": rescope_exhausted,
            "recommend_retry_count": recommend_retry_count,
            "generated_on": date.today().isoformat(),
        },
        state_updates_domain={"last_story_summary": f"Analyzed {agg.get('total_n', 0)} feedback rows."},
    )
