from __future__ import annotations

import os
import re
import sqlite3
from contextlib import closing
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, TypedDict

try:
    from langchain_core.runnables.graph import MermaidDrawMethod
except Exception:  # pragma: no cover - backward compatibility for older langchain-core
    from langchain_core.runnables.graph_mermaid import MermaidDrawMethod
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field
from ..contracts import StoryRequest, StoryResult
from ..utils import build_chat_openai, normalize_campaign_id, register_sqlite_alnum_normalizer

PROJECT_PHASE_3 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_3 / "kb" / "BusinessMarketing" / "brand_feedback.db"

AVAILABLE_GROUPINGS = ["campaign_id", "channel", "target_segment", "week_start"]
PLAUSIBLE_CTR_MIN = 0.0
PLAUSIBLE_CTR_MAX = 1.0
INTENT_TYPES = ["underperformers_only", "compare_metrics", "overview", "definitions"]
DEFAULT_THRESHOLD_ROWS = [
    ("click_through_rate", "min", 0.050, None, None, None, None, 0, "Global baseline"),
    ("customer_acquisition_cost", "max", 65.0, None, None, None, None, 0, "Global baseline"),
    ("return_on_ad_spend", "min", 3.0, None, None, None, None, 0, "Global baseline"),
    ("click_through_rate", "min", 0.055, "email", None, None, None, 10, "Email benchmark"),
    ("click_through_rate", "min", 0.048, "social", None, None, None, 10, "Social benchmark"),
    ("customer_acquisition_cost", "max", 70.0, None, "new_prospects", None, None, 10, "Acquisition-heavy segment"),
    ("return_on_ad_spend", "min", 3.1, None, "active_members", None, None, 10, "Retention-heavy segment"),
    ("return_on_ad_spend", "min", 3.15, None, None, "retention", None, 10, "Retention objective"),
    ("customer_acquisition_cost", "max", 72.0, None, None, "acquisition", None, 10, "Acquisition objective"),
]

METRIC_META = {
    "click_through_rate": {"label": "CTR", "value_key": "avg_ctr", "delta_key": "delta_ctr"},
    "customer_acquisition_cost": {"label": "CAC", "value_key": "avg_cac", "delta_key": "delta_cac"},
    "return_on_ad_spend": {"label": "ROAS", "value_key": "avg_roas", "delta_key": "delta_roas"},
    "spend": {"label": "Spend", "value_key": "total_spend", "delta_key": "delta_spend"},
}

BUSINESS_MARKETING_STORY2_GRAPH = None
STORY2_STATE_KEY = "bm_story_2_state"
MAX_REPLAN_ITERATIONS = 1


class IntentClassifierOutput(BaseModel):
    intent: Literal["underperformers_only", "compare_metrics", "overview", "definitions"]
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    rationale: str = Field(default="")
    concise: bool = Field(default=False)


class GroundedNarrativeOutput(BaseModel):
    narrative: str = Field(default="")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    used_fields: List[str] = Field(default_factory=list)


class ScopePlannerOutput(BaseModel):
    intent: Literal["underperformers_only", "compare_metrics", "overview", "definitions"]
    metrics_requested: List[str] = Field(default_factory=list)
    group_by: List[str] = Field(default_factory=list)
    ask_clarification: bool = Field(default=False)
    follow_up_question: str = Field(default="")
    concise: bool = Field(default=False)
    rationale: str = Field(default="")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)


class CriticDecisionOutput(BaseModel):
    action: Literal["continue", "replan_once", "ask_user"]
    follow_up_question: str = Field(default="")
    rationale: str = Field(default="")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _ensure_threshold_table(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        CREATE TABLE IF NOT EXISTS campaign_metric_thresholds (
            threshold_id INTEGER PRIMARY KEY AUTOINCREMENT,
            metric TEXT NOT NULL,
            threshold_type TEXT NOT NULL,
            threshold_value REAL NOT NULL,
            channel TEXT,
            target_segment TEXT,
            objective TEXT,
            campaign_id TEXT,
            priority INTEGER NOT NULL DEFAULT 0,
            note TEXT
        )
        """
    )
    # Keep defaults calibrated across runs while preserving any user-added custom rows.
    for seed in DEFAULT_THRESHOLD_ROWS:
        metric, threshold_type, threshold_value, channel, target_segment, objective, campaign_id, priority, note = seed
        existing = conn.execute(
            """
            SELECT threshold_id
            FROM campaign_metric_thresholds
            WHERE metric = ?
              AND threshold_type = ?
              AND IFNULL(channel, '') = IFNULL(?, '')
              AND IFNULL(target_segment, '') = IFNULL(?, '')
              AND IFNULL(objective, '') = IFNULL(?, '')
              AND IFNULL(campaign_id, '') = IFNULL(?, '')
              AND IFNULL(note, '') = IFNULL(?, '')
            ORDER BY threshold_id DESC
            LIMIT 1
            """,
            (metric, threshold_type, channel, target_segment, objective, campaign_id, note),
        ).fetchone()
        if existing:
            conn.execute(
                """
                UPDATE campaign_metric_thresholds
                SET threshold_value = ?, priority = ?
                WHERE threshold_id = ?
                """,
                (threshold_value, priority, int(existing[0])),
            )
        else:
            conn.execute(
                """
                INSERT INTO campaign_metric_thresholds (
                    metric, threshold_type, threshold_value, channel, target_segment, objective, campaign_id, priority, note
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    metric,
                    threshold_type,
                    threshold_value,
                    channel,
                    target_segment,
                    objective,
                    campaign_id,
                    priority,
                    note,
                ),
            )


def _get_available_weeks(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute("SELECT DISTINCT week_start FROM weekly_campaign_metrics ORDER BY week_start").fetchall()
    return [str(r[0]) for r in rows]


def convert_weekly(user_text: str, available_weeks: Sequence[str]) -> Dict[str, Any]:
    if not available_weeks:
        return {
            "start_week": None,
            "end_week": None,
            "label": "no_weeks_available",
            "assumption": None,
            "selected_weeks": [],
        }

    text = user_text.lower()
    weeks = [datetime.strptime(w, "%Y-%m-%d").date() for w in available_weeks]
    today = date.today()
    min_week = min(weeks)
    past_or_current_weeks = [w for w in weeks if w <= today]
    anchor_end = max(past_or_current_weeks) if past_or_current_weeks else max(weeks)

    def _calendar_window(days: int, label: str, assumption: Optional[str]) -> Dict[str, Any]:
        start = anchor_end - timedelta(days=days)
        notes: List[str] = []
        if start < min_week:
            start = min_week
            notes.append(f"Requested range extends before available data; clamped start to {min_week.isoformat()}.")
        if assumption:
            notes.insert(0, assumption)
        selected = [w for w in weeks if start <= w <= anchor_end]
        return {
            "start_week": start.isoformat(),
            "end_week": anchor_end.isoformat(),
            "label": label,
            "assumption": " ".join(notes) if notes else None,
            "selected_weeks": [d.isoformat() for d in selected],
        }

    iso_dates = re.findall(r"\b\d{4}-\d{2}-\d{2}\b", user_text)
    if len(iso_dates) >= 2:
        a = datetime.strptime(iso_dates[0], "%Y-%m-%d").date()
        b = datetime.strptime(iso_dates[1], "%Y-%m-%d").date()
        start, end = (a, b) if a <= b else (b, a)
        selected = [w for w in weeks if start <= w <= end]
        if not selected:
            return _calendar_window(
                7,
                "nearest_available_week",
                f"No exact weeks found for {start.isoformat()}..{end.isoformat()}; using latest available week window.",
            )
        return {
            "start_week": selected[0].isoformat(),
            "end_week": selected[-1].isoformat(),
            "label": "explicit_date_range",
            "assumption": None,
            "selected_weeks": [d.isoformat() for d in selected],
        }

    m = re.search(r"last\s+(\d+)\s+weeks?", text)
    if m:
        n = int(m.group(1))
        return _calendar_window(n * 7, f"last_{n}_weeks", None)

    if "last quarter" in text:
        return _calendar_window(90, "last_quarter", None)
    if "last month" in text:
        return _calendar_window(30, "last_month", None)
    if re.search(r"\b(this week|current week|latest week|most recent week|latest)\b", text):
        return _calendar_window(7, "latest_week", None)

    return _calendar_window(
        7,
        "latest_week_default",
        f"No timeframe was provided; defaulted to latest available week ending around {anchor_end.isoformat()}.",
    )


def _requested_metrics(user_text: str) -> List[str]:
    t = user_text.lower()
    out: List[str] = []
    if "ctr" in t or "click through" in t or "click-through" in t:
        out.append("click_through_rate")
    if "cac" in t or "customer acquisition cost" in t:
        out.append("customer_acquisition_cost")
    if "roas" in t or "return on ad spend" in t:
        out.append("return_on_ad_spend")
    if "spend" in t or "budget" in t:
        out.append("spend")
    dedup: List[str] = []
    for m in out:
        if m not in dedup:
            dedup.append(m)
    return dedup


def _detect_intent_deterministic(user_text: str) -> Dict[str, Any]:
    t = user_text.lower()
    has_definition = bool(re.search(r"\b(define|definition|what is|what are|mean(?:ing)?|glossary)\b", t))
    has_under = bool(re.search(r"\b(underperform\w*|below\s+threshold|flag\w*|failing|thresholds?)\b", t))
    has_compare = bool(re.search(r"\b(compare|versus|vs\.?|rank|top|bottom|best|worst)\b", t))
    has_summary = bool(re.search(r"\b(summary|overview|overall|snapshot)\b", t))
    concise = bool(re.search(r"\b(only|just|brief|concise)\b", t))

    if has_definition and not (has_under or has_compare or has_summary):
        return {"intent": "definitions", "confidence": 0.95, "source": "deterministic", "rationale": "Definition/glossary intent.", "concise": concise}
    if has_under and not has_compare:
        return {"intent": "underperformers_only", "confidence": 0.90, "source": "deterministic", "rationale": "Underperformance language detected.", "concise": concise}
    if has_compare:
        return {"intent": "compare_metrics", "confidence": 0.88, "source": "deterministic", "rationale": "Comparison language detected.", "concise": concise}
    if has_summary:
        return {"intent": "overview", "confidence": 0.82, "source": "deterministic", "rationale": "Summary/overview language detected.", "concise": concise}
    return {"intent": "overview", "confidence": 0.60, "source": "deterministic", "rationale": "Fallback overview intent.", "concise": concise}


def _classify_intent(user_text: str) -> Dict[str, Any]:
    base = _detect_intent_deterministic(user_text)
    if base["confidence"] >= 0.85:
        return base

    system = (
        "You are an intent classifier for weekly marketing KPI analysis.\n"
        "Choose exactly one intent from: underperformers_only, compare_metrics, overview, definitions.\n"
        "Use definitions only for glossary/meaning questions.\n"
        "Use underperformers_only when the user asks for threshold breaches or weak performers.\n"
        "Use compare_metrics when the user asks to compare or rank metrics/groups.\n"
        "Use overview for broad summaries.\n"
        "Output strictly in schema."
    )
    user = f"USER_QUERY: {user_text}"
    try:
        llm = build_chat_openai(model="gpt-4o-mini", temperature=0)
        structured = llm.with_structured_output(IntentClassifierOutput)
        out = structured.invoke([("system", system), ("user", user)])
        llm_choice = out.intent if out.intent in INTENT_TYPES else base["intent"]
        llm_conf = float(out.confidence)
        if llm_conf >= 0.55:
            return {
                "intent": llm_choice,
                "confidence": llm_conf,
                "source": "llm",
                "rationale": out.rationale or "LLM intent classification.",
                "concise": bool(out.concise),
            }
    except Exception:
        pass
    return base


def _default_metrics_for_intent(intent: str) -> List[str]:
    if intent == "underperformers_only":
        return ["click_through_rate", "customer_acquisition_cost", "return_on_ad_spend"]
    if intent == "compare_metrics":
        return ["return_on_ad_spend", "customer_acquisition_cost"]
    return ["click_through_rate", "customer_acquisition_cost", "return_on_ad_spend", "spend"]


def _requested_grouping(user_text: str) -> List[str]:
    t = user_text.lower()
    groups: List[str] = []
    if re.search(r"\b(by|group(?:ed)?\s+by)\s+[^.]*\bchannel\b", t) or "channel breakdown" in t:
        groups.append("channel")
    if re.search(r"\b(by|group(?:ed)?\s+by)\s+[^.]*\bcampaign(?:_id)?\b", t):
        groups.append("campaign_id")
    if re.search(r"\b(by|group(?:ed)?\s+by)\s+[^.]*\b(segment|target segment)\b", t):
        groups.append("target_segment")
    if re.search(r"\b(by|group(?:ed)?\s+by)\s+[^.]*\bweek\b", t):
        groups.append("week_start")
    if not groups and "segments" in t:
        groups.append("target_segment")

    dedup: List[str] = []
    for g in groups:
        if g not in dedup:
            dedup.append(g)
    return dedup


def _requested_unavailable_dimensions(user_text: str) -> List[str]:
    t = user_text.lower()
    unavailable = []
    for dim in ["geography", "geo", "region", "device", "country", "city", "state"]:
        if re.search(rf"\b{re.escape(dim)}\b", t):
            unavailable.append(dim)
    return sorted(set(unavailable))


def _extract_filters(user_text: str, grouped_by: Sequence[str]) -> Dict[str, Any]:
    t = user_text.lower()
    campaign_ids_raw = re.findall(r"\bCAMP[\W_]*\d+\b", user_text.upper())
    campaign_ids = [normalize_campaign_id(cid) for cid in campaign_ids_raw]
    campaign_ids = [cid for cid in campaign_ids if cid]

    channels = []
    for ch in ["email", "app", "social", "web"]:
        if re.search(rf"\b{ch}\b", t):
            channels.append(ch)
    channel_filters = None
    if channels:
        if "channel" not in grouped_by or re.search(r"\b(only|just)\b", t):
            channel_filters = sorted(set(channels))

    seg_alias = {
        "new_prospects": ["new prospects", "new members", "prospects"],
        "active_members": ["active members", "existing members", "retained members"],
        "general_audience": ["general audience", "broad audience", "general market"],
    }
    seg_filters: List[str] = []
    for canonical, aliases in seg_alias.items():
        if any(alias in t for alias in aliases):
            seg_filters.append(canonical)
    if "target_segment" in grouped_by and not re.search(r"\b(only|just)\b", t):
        seg_filters = []

    obj_filters = []
    for objective in ["acquisition", "retention", "engagement", "conversion"]:
        if re.search(rf"\b{objective}\b", t):
            obj_filters.append(objective)

    return {
        "campaign_ids": campaign_ids or None,
        "channels": channel_filters,
        "target_segments": sorted(set(seg_filters)) or None,
        "objectives": sorted(set(obj_filters)) or None,
    }


def read_campaign_metrics(
    conn: sqlite3.Connection,
    start_week: str,
    end_week: str,
    filters: Dict[str, Any],
) -> List[Dict[str, Any]]:
    where = ["wm.week_start >= ?", "wm.week_start <= ?"]
    params: List[Any] = [start_week, end_week]

    campaign_ids = filters.get("campaign_ids")
    if campaign_ids:
        where.append(f"NORM_ALNUM(wm.campaign_id) IN ({','.join(['?'] * len(campaign_ids))})")
        params.extend(campaign_ids)

    channels = filters.get("channels")
    if channels:
        where.append(f"wm.channel IN ({','.join(['?'] * len(channels))})")
        params.extend(channels)

    target_segments = filters.get("target_segments")
    if target_segments:
        where.append(f"wm.target_segment IN ({','.join(['?'] * len(target_segments))})")
        params.extend(target_segments)

    objectives = filters.get("objectives")
    if objectives:
        where.append(f"LOWER(c.objective) IN ({','.join(['?'] * len(objectives))})")
        params.extend([o.lower() for o in objectives])

    sql = f"""
    SELECT
        wm.campaign_id,
        wm.channel,
        wm.target_segment,
        wm.week_start,
        wm.week_end,
        wm.click_through_rate,
        wm.customer_acquisition_cost,
        wm.return_on_ad_spend,
        wm.spend,
        c.objective
    FROM weekly_campaign_metrics wm
    JOIN campaigns c ON c.campaign_id = wm.campaign_id
    WHERE {' AND '.join(where)}
    ORDER BY wm.week_start, wm.campaign_id, wm.channel, wm.target_segment
    """
    conn.row_factory = sqlite3.Row
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _group_key(row: Dict[str, Any], group_by: Sequence[str]) -> Tuple[Any, ...]:
    if not group_by:
        return ("ALL",)
    return tuple(row.get(g) for g in group_by)


def _aggregate_rows(rows: Sequence[Dict[str, Any]], group_by: Sequence[str]) -> Dict[Tuple[Any, ...], Dict[str, Any]]:
    grouped: Dict[Tuple[Any, ...], List[Dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[_group_key(row, group_by)].append(row)

    out: Dict[Tuple[Any, ...], Dict[str, Any]] = {}
    for key, grp in grouped.items():
        total_spend = sum(_to_float(r.get("spend")) or 0.0 for r in grp)
        weighted_den = sum((_to_float(r.get("spend")) or 0.0) for r in grp if (_to_float(r.get("spend")) or 0.0) > 0)

        def wavg(metric: str) -> Optional[float]:
            if weighted_den <= 0:
                return None
            num = 0.0
            for r in grp:
                spend = _to_float(r.get("spend")) or 0.0
                value = _to_float(r.get(metric))
                if spend > 0 and value is not None:
                    num += value * spend
            return num / weighted_den

        channels = sorted({str(r.get("channel")) for r in grp if r.get("channel")})
        segments = sorted({str(r.get("target_segment")) for r in grp if r.get("target_segment")})
        objectives = sorted({str(r.get("objective")) for r in grp if r.get("objective")})

        out[key] = {
            "row_count": len(grp),
            "campaign_count": len({r.get("campaign_id") for r in grp if r.get("campaign_id")}),
            "total_spend": round(total_spend, 2),
            "avg_ctr": None if wavg("click_through_rate") is None else round(wavg("click_through_rate") or 0.0, 4),
            "avg_cac": None if wavg("customer_acquisition_cost") is None else round(wavg("customer_acquisition_cost") or 0.0, 2),
            "avg_roas": None if wavg("return_on_ad_spend") is None else round(wavg("return_on_ad_spend") or 0.0, 2),
            "channels": channels,
            "target_segments": segments,
            "objectives": objectives,
        }
    return out


def aggregate_summarize(
    rows: Sequence[Dict[str, Any]],
    group_by: Sequence[str],
    prior_rows: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    current = _aggregate_rows(rows, group_by)
    prior = _aggregate_rows(prior_rows or [], group_by) if prior_rows else {}

    summary_rows: List[Dict[str, Any]] = []
    for key, stats in current.items():
        row = dict(stats)
        if group_by:
            for idx, g in enumerate(group_by):
                row[g] = key[idx]

        prior_stats = prior.get(key)
        if prior_stats:
            row["delta_ctr"] = None if row["avg_ctr"] is None else round((row["avg_ctr"] or 0.0) - (prior_stats["avg_ctr"] or 0.0), 4)
            row["delta_cac"] = None if row["avg_cac"] is None else round((row["avg_cac"] or 0.0) - (prior_stats["avg_cac"] or 0.0), 2)
            row["delta_roas"] = None if row["avg_roas"] is None else round((row["avg_roas"] or 0.0) - (prior_stats["avg_roas"] or 0.0), 2)
            row["delta_spend"] = round((row["total_spend"] or 0.0) - (prior_stats["total_spend"] or 0.0), 2)
        else:
            row["delta_ctr"] = None
            row["delta_cac"] = None
            row["delta_roas"] = None
            row["delta_spend"] = None

        summary_rows.append(row)

    summary_rows.sort(key=lambda r: (r.get("avg_roas") is None, -(r.get("avg_roas") or -1e9)))
    overall = _aggregate_rows(rows, [])[("ALL",)] if rows else {"row_count": 0, "total_spend": 0.0, "avg_ctr": None, "avg_cac": None, "avg_roas": None}
    return {"overall": overall, "rows": summary_rows}


def consult_thresholds(conn: sqlite3.Connection, context: Dict[str, Optional[str]]) -> Dict[str, Optional[float]]:
    def lookup(metric: str, threshold_type: str) -> Optional[float]:
        sql = """
        SELECT threshold_value
        FROM campaign_metric_thresholds
        WHERE metric = ?
          AND threshold_type = ?
          AND (campaign_id IS NULL OR campaign_id = ?)
          AND (objective IS NULL OR objective = ?)
          AND (target_segment IS NULL OR target_segment = ?)
          AND (channel IS NULL OR channel = ?)
        ORDER BY
          (CASE WHEN campaign_id IS NOT NULL THEN 8 ELSE 0 END +
           CASE WHEN objective IS NOT NULL THEN 4 ELSE 0 END +
           CASE WHEN target_segment IS NOT NULL THEN 2 ELSE 0 END +
           CASE WHEN channel IS NOT NULL THEN 1 ELSE 0 END) DESC,
          priority DESC,
          threshold_id DESC
        LIMIT 1
        """
        row = conn.execute(
            sql,
            (
                metric,
                threshold_type,
                context.get("campaign_id"),
                context.get("objective"),
                context.get("target_segment"),
                context.get("channel"),
            ),
        ).fetchone()
        return _to_float(row[0]) if row else None

    return {
        "ctr_min": lookup("click_through_rate", "min"),
        "cac_max": lookup("customer_acquisition_cost", "max"),
        "roas_min": lookup("return_on_ad_spend", "min"),
    }


def explain_metric_defs() -> Dict[str, str]:
    return {
        "CTR": (
            "Click-through rate (CTR) measures how often people click an ad after seeing it. "
            "In this dataset it is read from weekly_campaign_metrics.click_through_rate."
        ),
        "CAC": (
            "Customer acquisition cost (CAC) measures how much it costs to acquire one new customer, "
            "computed as total spend divided by the number of new customers. "
            "In this dataset it is read from weekly_campaign_metrics.customer_acquisition_cost."
        ),
        "ROAS": (
            "Return on ad spend (ROAS) measures revenue generated for every dollar spent on advertising. "
            "Revenue is in the numerator and spend is in the denominator of the equation. "
            "In this dataset it is read from weekly_campaign_metrics.return_on_ad_spend."
        ),
        "Spend": "Weekly campaign spend from weekly_campaign_metrics.spend.",
    }


def _requested_metric_labels(user_text: str) -> List[str]:
    metric_to_label = {
        "click_through_rate": "CTR",
        "customer_acquisition_cost": "CAC",
        "return_on_ad_spend": "ROAS",
        "spend": "Spend",
    }
    labels: List[str] = []
    for metric in _requested_metrics(user_text):
        label = metric_to_label.get(metric)
        if label and label not in labels:
            labels.append(label)
    return labels


def _select_metric_definitions(user_text: str) -> Dict[str, str]:
    all_defs = explain_metric_defs()
    labels = _requested_metric_labels(user_text)
    if not labels:
        return all_defs
    return {label: all_defs[label] for label in labels if label in all_defs}


def _evaluate_underperformance(
    conn: sqlite3.Connection,
    grouped_rows: Sequence[Dict[str, Any]],
    metrics_requested: Sequence[str],
) -> List[Dict[str, Any]]:
    allowed = set(metrics_requested)
    out: List[Dict[str, Any]] = []
    for row in grouped_rows:
        context = {
            "campaign_id": row.get("campaign_id") if isinstance(row.get("campaign_id"), str) else None,
            "channel": row.get("channel") if isinstance(row.get("channel"), str) else (row.get("channels") or [None])[0],
            "target_segment": row.get("target_segment")
            if isinstance(row.get("target_segment"), str)
            else (row.get("target_segments") or [None])[0],
            "objective": (row.get("objectives") or [None])[0] if isinstance(row.get("objectives"), list) else None,
        }
        th = consult_thresholds(conn, context)
        reasons = []

        avg_ctr = _to_float(row.get("avg_ctr"))
        avg_cac = _to_float(row.get("avg_cac"))
        avg_roas = _to_float(row.get("avg_roas"))

        if (
            "click_through_rate" in allowed
            and th["ctr_min"] is not None
            and avg_ctr is not None
            and avg_ctr < th["ctr_min"]
        ):
            reasons.append(
                {
                    "metric": "CTR",
                    "actual": round(avg_ctr, 4),
                    "threshold": round(th["ctr_min"] or 0.0, 4),
                    "distance": round(avg_ctr - (th["ctr_min"] or 0.0), 4),
                    "direction": "below_min",
                }
            )
        if (
            "customer_acquisition_cost" in allowed
            and th["cac_max"] is not None
            and avg_cac is not None
            and avg_cac > th["cac_max"]
        ):
            reasons.append(
                {
                    "metric": "CAC",
                    "actual": round(avg_cac, 2),
                    "threshold": round(th["cac_max"] or 0.0, 2),
                    "distance": round(avg_cac - (th["cac_max"] or 0.0), 2),
                    "direction": "above_max",
                }
            )
        if (
            "return_on_ad_spend" in allowed
            and th["roas_min"] is not None
            and avg_roas is not None
            and avg_roas < th["roas_min"]
        ):
            reasons.append(
                {
                    "metric": "ROAS",
                    "actual": round(avg_roas, 2),
                    "threshold": round(th["roas_min"] or 0.0, 2),
                    "distance": round(avg_roas - (th["roas_min"] or 0.0), 2),
                    "direction": "below_min",
                }
            )

        if reasons:
            rec = {
                "group": {k: row.get(k) for k in AVAILABLE_GROUPINGS if k in row},
                "reasons": reasons,
                "avg_ctr": row.get("avg_ctr"),
                "avg_cac": row.get("avg_cac"),
                "avg_roas": row.get("avg_roas"),
                "total_spend": row.get("total_spend"),
            }
            out.append(rec)
    return out


def _quality_flags(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    invalid_ctr = 0
    null_cac = 0
    null_roas = 0
    excluded_for_thresholding = 0

    valid_rows = []
    for row in rows:
        ctr = _to_float(row.get("click_through_rate"))
        cac = _to_float(row.get("customer_acquisition_cost"))
        roas = _to_float(row.get("return_on_ad_spend"))
        bad_ctr = ctr is None or ctr < PLAUSIBLE_CTR_MIN or ctr > PLAUSIBLE_CTR_MAX
        bad_cac = cac is None
        bad_roas = roas is None
        if bad_ctr:
            invalid_ctr += 1
        if bad_cac:
            null_cac += 1
        if bad_roas:
            null_roas += 1
        if bad_ctr or bad_cac or bad_roas:
            excluded_for_thresholding += 1
        else:
            valid_rows.append(row)

    return {
        "invalid_ctr_rows": invalid_ctr,
        "null_cac_rows": null_cac,
        "null_roas_rows": null_roas,
        "excluded_from_thresholding": excluded_for_thresholding,
        "valid_threshold_rows": len(valid_rows),
        "rows_for_thresholding": valid_rows,
    }


def _make_group_label(row: Dict[str, Any], group_by: Sequence[str]) -> str:
    if not group_by:
        return "overall"
    return ", ".join(f"{g}={row.get(g)}" for g in group_by)


def _format_metric_snapshot(row: Dict[str, Any], metrics_requested: Sequence[str], include_delta: bool) -> str:
    parts: List[str] = []
    for metric in metrics_requested:
        meta = METRIC_META.get(metric)
        if not meta:
            continue
        val = row.get(meta["value_key"])
        if include_delta:
            delta = row.get(meta["delta_key"])
            parts.append(f"{meta['label']}={val} (d={delta})")
        else:
            parts.append(f"{meta['label']}={val}")
    return ", ".join(parts) if parts else "No requested metrics available."


def _format_response(payload: Dict[str, Any]) -> str:
    if payload.get("glossary_only"):
        defs = payload["metric_defs"]
        lines = ["Metric definitions:"]
        lines.extend([f"- {k}: {v}" for k, v in defs.items()])
        return "\n".join(lines)

    if payload.get("row_count", 0) == 0:
        return (
            "No weekly campaign metrics matched those filters/time range. "
            "Try widening the week range or removing channel/campaign/segment filters."
        )

    timeframe = payload["timeframe"]
    assumptions = payload.get("assumptions", [])
    overall = payload["summary"]["overall"]
    grouped_rows = payload["summary"]["rows"]
    group_by = payload["group_by"]
    under = payload["underperformers"]
    quality = payload["quality"]
    unavailable_dims = payload.get("unavailable_dimensions", [])
    intent = payload.get("intent", "overview")
    metrics_requested = payload.get("metrics_requested", [])
    concise = bool(payload.get("concise", False))

    title = {
        "underperformers_only": "Underperformers vs Thresholds",
        "compare_metrics": "Campaign Metric Comparison",
        "overview": "Weekly Campaign KPI Summary",
        "definitions": "Metric definitions",
    }.get(intent, "Weekly Campaign KPI Summary")

    lines = [title, f"Week range: {timeframe['start_week']}..{timeframe['end_week']} ({timeframe['label']})", f"Rows analyzed: {payload['row_count']}"]

    for a in assumptions:
        lines.append(f"Assumption: {a}")

    if unavailable_dims:
        lines.append(
            "Requested dimension(s) not available in current table: "
            f"{', '.join(unavailable_dims)}. "
            "Available groupings: campaign_id, channel, target_segment, week_start."
        )

    lines.append(f"Top-line overall: {_format_metric_snapshot(overall, metrics_requested, include_delta=False)}")

    if intent in {"overview", "compare_metrics"}:
        if group_by:
            lines.append(f"Grouped breakdown (by {', '.join(group_by)}):")
            max_rows = 5 if concise else 8
            for r in grouped_rows[:max_rows]:
                lines.append(f"- {_make_group_label(r, group_by)} | {_format_metric_snapshot(r, metrics_requested, include_delta=True)}")
        else:
            lines.append("No grouping requested; showing overall summary only.")

    if intent in {"overview", "underperformers_only"}:
        if under:
            lines.append("Underperformers:")
            max_rows = 10 if intent == "underperformers_only" else (5 if concise else 8)
            for rec in under[:max_rows]:
                grp = rec.get("group") or {}
                label = ", ".join([f"{k}={v}" for k, v in grp.items() if v is not None]) or "overall"
                reasons = "; ".join(
                    [
                        f"{x['metric']} {x['actual']} vs {x['threshold']} ({x['direction']}, delta={x['distance']})"
                        for x in rec["reasons"]
                    ]
                )
                lines.append(f"- {label}: {reasons}")
        else:
            lines.append("Underperformers: none flagged against available thresholds for this slice.")

    if quality["excluded_from_thresholding"] > 0:
        lines.append(
            "Data quality note: "
            f"excluded {quality['excluded_from_thresholding']} row(s) from thresholding "
            f"(invalid CTR={quality['invalid_ctr_rows']}, null CAC={quality['null_cac_rows']}, null ROAS={quality['null_roas_rows']})."
        )

    return "\n".join(lines)


class Story2GraphState(TypedDict, total=False):
    user_query: str
    domain_context: Dict[str, Any]
    user_turn_number: int
    lower_text: str
    glossary_only: bool
    analysis_signals: bool
    intent: str
    concise: bool
    intent_info: Dict[str, Any]
    metric_defs: Dict[str, str]
    group_by: List[str]
    group_by_explicit: bool
    unavailable_dimensions: List[str]
    filters: Dict[str, Any]
    metrics_explicit: List[str]
    metrics_requested: List[str]
    planner_source: str
    planner_confidence: float
    planner_rationale: str
    memory_applied_fields: List[str]
    available_weeks: List[str]
    timeframe: Dict[str, Any]
    assumptions: List[str]
    rows: List[Dict[str, Any]]
    prior_rows: List[Dict[str, Any]]
    summary: Dict[str, Any]
    quality: Dict[str, Any]
    clean_summary: Dict[str, Any]
    underperformers: List[Dict[str, Any]]
    replan_count: int
    critic_action: str
    critic_reason: str
    critic_confidence: float
    follow_up_question: str
    requested_slot: str
    missing_slots: List[str]
    narrative_text: str
    narrative_source: str
    narrative_confidence: float
    response_text: str
    story_output: Dict[str, Any]
    state_summary: str


def _user_turn_number(messages: List[Dict[str, Any]]) -> int:
    turns = 0
    for m in messages or []:
        if str(m.get("role", "")).strip().lower() == "user":
            turns += 1
    return turns + 1


def _classify_intent_node(state: Story2GraphState) -> Story2GraphState:
    user_text = state.get("user_query", "")
    lower_text = user_text.lower()
    glossary_only = bool(re.search(r"\b(define|definition|what is|what are|mean(?:ing)?|glossary)\b", lower_text))
    analysis_signals = bool(
        re.search(
            r"\b(show|analy[sz]e|underperform|performance|compare|trend|last|week|month|quarter|campaign_id|group by|by channel|by segment)\b",
            lower_text,
        )
    )
    intent_info = _classify_intent(user_text)
    intent = str(intent_info.get("intent", "overview"))
    concise = bool(intent_info.get("concise", False))
    metric_defs = _select_metric_definitions(user_text) if (glossary_only or intent == "definitions") else {}
    return {
        "lower_text": lower_text,
        "glossary_only": glossary_only,
        "analysis_signals": analysis_signals,
        "intent_info": intent_info,
        "intent": intent,
        "concise": concise,
        "metric_defs": metric_defs,
    }


def _route_after_classify(state: Story2GraphState) -> str:
    if state.get("intent") == "definitions" or (state.get("glossary_only") and not state.get("analysis_signals")):
        return "format"
    return "go"


def _extract_scope_node(state: Story2GraphState) -> Story2GraphState:
    user_text = state.get("user_query", "")
    intent = str(state.get("intent", "overview"))
    explicit_group_by = _requested_grouping(user_text)
    group_by = list(explicit_group_by)
    if intent == "compare_metrics" and not explicit_group_by:
        group_by = ["campaign_id"]
    metrics_explicit = _requested_metrics(user_text)
    return {
        "group_by": group_by,
        "group_by_explicit": bool(explicit_group_by),
        "unavailable_dimensions": _requested_unavailable_dimensions(user_text),
        "filters": _extract_filters(user_text, grouped_by=group_by),
        "metrics_explicit": metrics_explicit,
        "metrics_requested": metrics_explicit or _default_metrics_for_intent(intent),
        "planner_source": "deterministic_scope",
        "planner_confidence": 1.0 if metrics_explicit or group_by else 0.7,
        "planner_rationale": "Deterministic scope extraction from user query.",
    }


def _is_follow_up_like(user_text: str, user_turn_number: int, prior_turn: int) -> bool:
    text = (user_text or "").strip().lower()
    if not text:
        return False
    tokens = [t for t in re.split(r"\s+", text) if t]
    deictic = bool(re.search(r"\b(same|that|those|it|also|instead|keep|use previous|as above)\b", text))
    short_turn = len(tokens) <= 14
    close_turn = prior_turn > 0 and (user_turn_number - prior_turn) <= 2
    return (short_turn and close_turn) or deictic


def _merge_with_prior_scope_node(state: Story2GraphState) -> Story2GraphState:
    domain_context = state.get("domain_context", {}) or {}
    prior_state = domain_context.get(STORY2_STATE_KEY)
    if not isinstance(prior_state, dict):
        return {"memory_applied_fields": []}

    prior_scope = prior_state.get("last_resolved_scope")
    if not isinstance(prior_scope, dict):
        return {"memory_applied_fields": []}

    user_turn = int(state.get("user_turn_number", 0) or 0)
    prior_turn = int(prior_state.get("last_user_turn_number", 0) or 0)
    user_text = str(state.get("user_query", "") or "")
    if not _is_follow_up_like(user_text, user_turn_number=user_turn, prior_turn=prior_turn):
        return {"memory_applied_fields": []}

    applied: List[str] = []
    updates: Dict[str, Any] = {"memory_applied_fields": applied}

    if not state.get("metrics_explicit") and isinstance(prior_scope.get("metrics_requested"), list):
        prior_metrics = _valid_metric_list(prior_scope.get("metrics_requested", []))
        if prior_metrics:
            updates["metrics_requested"] = prior_metrics
            applied.append("metrics_requested")

    if not bool(state.get("group_by_explicit")) and isinstance(prior_scope.get("group_by"), list):
        prior_group = _valid_group_list(prior_scope.get("group_by", []))
        if prior_group:
            updates["group_by"] = prior_group
            applied.append("group_by")

    current_filters = state.get("filters", {}) or {}
    has_explicit_filters = any(current_filters.get(k) for k in ["campaign_ids", "channels", "target_segments", "objectives"])
    prior_filters = prior_scope.get("filters")
    if (not has_explicit_filters) and isinstance(prior_filters, dict) and any(
        prior_filters.get(k) for k in ["campaign_ids", "channels", "target_segments", "objectives"]
    ):
        updates["filters"] = {
            "campaign_ids": prior_filters.get("campaign_ids"),
            "channels": prior_filters.get("channels"),
            "target_segments": prior_filters.get("target_segments"),
            "objectives": prior_filters.get("objectives"),
        }
        applied.append("filters")

    if float((state.get("intent_info") or {}).get("confidence", 0.0)) < 0.75 and str(prior_scope.get("intent", "")) in INTENT_TYPES:
        updates["intent"] = str(prior_scope.get("intent"))
        applied.append("intent")

    if applied:
        existing_assumptions = list(state.get("assumptions", []))
        existing_assumptions.append(f"Used prior Story 2 context for: {', '.join(applied)}.")
        updates["assumptions"] = existing_assumptions
    return updates


def _valid_metric_list(raw: Sequence[str]) -> List[str]:
    allowed = set(METRIC_META.keys())
    dedup: List[str] = []
    for m in raw:
        key = str(m).strip().lower()
        if key in allowed and key not in dedup:
            dedup.append(key)
    return dedup


def _valid_group_list(raw: Sequence[str]) -> List[str]:
    allowed = set(AVAILABLE_GROUPINGS)
    dedup: List[str] = []
    for g in raw:
        key = str(g).strip().lower()
        if key in allowed and key not in dedup:
            dedup.append(key)
    return dedup


def _should_try_llm_scope_plan(state: Story2GraphState) -> bool:
    if os.getenv("PROTOTYPE_BM2_USE_LLM_PLAN", "1").strip().lower() in {"0", "false", "no"}:
        return False
    intent_conf = float((state.get("intent_info") or {}).get("confidence", 0.0))
    if intent_conf < 0.85:
        return True
    if not state.get("metrics_explicit") or not state.get("group_by"):
        return True
    return False


def _maybe_llm_scope_plan(state: Story2GraphState) -> Optional[Dict[str, Any]]:
    if not _should_try_llm_scope_plan(state):
        return None
    user_text = state.get("user_query", "")
    deterministic_scope = {
        "intent": state.get("intent"),
        "metrics_requested": state.get("metrics_requested", []),
        "group_by": state.get("group_by", []),
        "filters": state.get("filters", {}),
    }
    try:
        llm = build_chat_openai(model=os.getenv("PROTOTYPE_BM2_PLAN_MODEL", "gpt-4o-mini"), temperature=0)
        structured = llm.with_structured_output(ScopePlannerOutput)
        system = (
            "You are a planning assistant for weekly marketing KPI analysis.\n"
            "Output only schema-compliant structured data.\n"
            "Only use allowed metrics: click_through_rate, customer_acquisition_cost, return_on_ad_spend, spend.\n"
            "Only use allowed group_by values: campaign_id, channel, target_segment, week_start.\n"
            "Set ask_clarification true only when the request is too ambiguous to proceed safely."
        )
        user = f"USER_QUERY: {user_text}\nDETERMINISTIC_SCOPE: {deterministic_scope}"
        out = structured.invoke([("system", system), ("user", user)])
        return out.model_dump()
    except Exception:
        return None


def _plan_with_llm_node(state: Story2GraphState) -> Story2GraphState:
    llm_plan = _maybe_llm_scope_plan(state)
    if not llm_plan:
        return {
            "planner_source": "deterministic_scope",
            "planner_confidence": float(state.get("planner_confidence", 0.7)),
            "planner_rationale": str(state.get("planner_rationale", "LLM planner unavailable or skipped.")),
        }

    llm_conf = float(llm_plan.get("confidence", 0.0))
    if llm_conf < 0.62:
        return {
            "planner_source": "deterministic_scope_low_llm_confidence",
            "planner_confidence": round(llm_conf, 2),
            "planner_rationale": str(llm_plan.get("rationale") or "LLM planner confidence below acceptance threshold."),
        }

    planned_metrics = _valid_metric_list(llm_plan.get("metrics_requested", []))
    planned_group = _valid_group_list(llm_plan.get("group_by", []))
    planned_intent = str(llm_plan.get("intent") or state.get("intent", "overview"))
    if planned_intent not in INTENT_TYPES:
        planned_intent = str(state.get("intent", "overview"))

    out: Dict[str, Any] = {
        "planner_source": "llm_scope_plan",
        "planner_confidence": round(llm_conf, 2),
        "planner_rationale": str(llm_plan.get("rationale") or "LLM planner accepted."),
        "intent": planned_intent,
        "concise": bool(llm_plan.get("concise", state.get("concise", False))),
    }
    if planned_metrics:
        out["metrics_requested"] = planned_metrics
    if planned_group:
        out["group_by"] = planned_group

    ask_clarification = bool(llm_plan.get("ask_clarification", False))
    question = str(llm_plan.get("follow_up_question", "")).strip()
    if ask_clarification and question:
        out["follow_up_question"] = question
        out["requested_slot"] = "analysis_scope"
        out["missing_slots"] = ["analysis_scope"]
    return out


def _maybe_llm_critic_decision(state: Story2GraphState) -> Optional[Dict[str, Any]]:
    try:
        llm = build_chat_openai(model=os.getenv("PROTOTYPE_BM2_CRITIC_MODEL", "gpt-4o-mini"), temperature=0)
        structured = llm.with_structured_output(CriticDecisionOutput)
        summary_rows = ((state.get("summary") or {}).get("rows") or [])[:3]
        quality = state.get("quality", {})
        user = {
            "user_query": state.get("user_query", ""),
            "row_count": len(state.get("rows", [])),
            "group_by": state.get("group_by", []),
            "metrics_requested": state.get("metrics_requested", []),
            "filters": state.get("filters", {}),
            "quality": {
                "invalid_ctr_rows": quality.get("invalid_ctr_rows", 0),
                "null_cac_rows": quality.get("null_cac_rows", 0),
                "null_roas_rows": quality.get("null_roas_rows", 0),
                "excluded_from_thresholding": quality.get("excluded_from_thresholding", 0),
                "valid_threshold_rows": quality.get("valid_threshold_rows", 0),
            },
            "summary_rows_sample": summary_rows,
            "replan_count": int(state.get("replan_count", 0)),
            "max_replans": MAX_REPLAN_ITERATIONS,
        }
        system = (
            "You are a critic for a marketing analysis plan.\n"
            "Choose action from: continue, replan_once, ask_user.\n"
            "Use replan_once only if one simple scope adjustment likely improves usefulness.\n"
            "Never choose replan_once when replan_count >= max_replans.\n"
            "Use ask_user when ambiguity is unresolved and cannot be safely inferred."
        )
        out = structured.invoke([("system", system), ("user", str(user))])
        return out.model_dump()
    except Exception:
        return None


def _critic_node(state: Story2GraphState) -> Story2GraphState:
    replan_count = int(state.get("replan_count", 0))
    row_count = len(state.get("rows", []))
    quality = state.get("quality", {})

    # Deterministic critic baseline.
    action = "continue"
    reason = "Data quality and coverage are sufficient for threshold evaluation."
    confidence = 0.75
    question = ""

    if row_count == 0:
        action = "ask_user"
        reason = "No rows matched the current filters/timeframe."
        confidence = 0.92
        question = "I found no matching weekly rows. Should I widen to the latest 8 weeks and remove filters?"
    elif (
        replan_count < MAX_REPLAN_ITERATIONS
        and row_count < 6
        and str(state.get("intent", "overview")) in {"overview", "compare_metrics"}
    ):
        action = "replan_once"
        reason = "Sample size is small; widening scope once may improve signal."
        confidence = 0.82
    elif (
        replan_count < MAX_REPLAN_ITERATIONS
        and int(quality.get("excluded_from_thresholding", 0)) == row_count
        and row_count > 0
    ):
        action = "replan_once"
        reason = "All rows were excluded from thresholding; trying one broader read."
        confidence = 0.86

    llm_out = _maybe_llm_critic_decision(state)
    if llm_out:
        llm_conf = float(llm_out.get("confidence", 0.0))
        llm_action = str(llm_out.get("action", "")).strip()
        if llm_conf >= 0.65 and llm_action in {"continue", "replan_once", "ask_user"}:
            if llm_action == "replan_once" and replan_count >= MAX_REPLAN_ITERATIONS:
                llm_action = "continue"
            action = llm_action
            reason = str(llm_out.get("rationale") or reason)
            confidence = llm_conf
            if llm_action == "ask_user":
                llm_q = str(llm_out.get("follow_up_question") or "").strip()
                if llm_q:
                    question = llm_q

    out: Dict[str, Any] = {
        "critic_action": action,
        "critic_reason": reason,
        "critic_confidence": round(confidence, 2),
    }
    if action == "ask_user":
        out["follow_up_question"] = question or "Can you clarify your preferred scope so I can continue?"
        out["requested_slot"] = "analysis_scope"
        out["missing_slots"] = ["analysis_scope"]
    return out


def _route_after_critic(state: Story2GraphState) -> str:
    action = str(state.get("critic_action", "continue"))
    replan_count = int(state.get("replan_count", 0))
    if action == "ask_user":
        return "ask"
    if action == "replan_once" and replan_count < MAX_REPLAN_ITERATIONS:
        return "replan"
    return "threshold"


def _replan_scope_node(state: Story2GraphState) -> Story2GraphState:
    replan_count = int(state.get("replan_count", 0)) + 1
    group_by = list(state.get("group_by", []))
    metrics = list(state.get("metrics_requested", []))
    filters = dict(state.get("filters", {}))
    assumptions = list(state.get("assumptions", []))

    if not group_by:
        group_by = ["campaign_id"]
        assumptions.append("Critic replan: added campaign_id grouping for clearer comparison.")
    elif "week_start" not in group_by and len(group_by) < 2:
        group_by = group_by + ["week_start"]
        assumptions.append("Critic replan: added week_start grouping to increase trend visibility.")

    if not metrics:
        metrics = ["click_through_rate", "customer_acquisition_cost", "return_on_ad_spend"]
        assumptions.append("Critic replan: restored core KPI metrics.")

    if len(state.get("rows", [])) == 0 and any(filters.get(k) for k in ["campaign_ids", "channels", "target_segments", "objectives"]):
        filters = {"campaign_ids": None, "channels": None, "target_segments": None, "objectives": None}
        assumptions.append("Critic replan: removed restrictive filters after zero-row read.")

    return {
        "replan_count": replan_count,
        "group_by": group_by,
        "metrics_requested": metrics,
        "filters": filters,
        "assumptions": assumptions,
        "planner_source": "critic_replan",
        "planner_rationale": "Applied one critic-guided scope adjustment.",
    }


def _route_after_extract_scope(state: Story2GraphState) -> str:
    if str(state.get("follow_up_question", "")).strip():
        return "ask"
    text = state.get("lower_text", "")
    has_timeframe = bool(re.search(r"\b(last|week|weeks|month|quarter|between|from|since|latest)\b", text)) or bool(
        re.search(r"\b\d{4}-\d{2}-\d{2}\b", text)
    )
    has_group = bool(state.get("group_by"))
    has_metrics = bool(state.get("metrics_explicit"))
    intent = str(state.get("intent", "overview"))
    conf = float((state.get("intent_info") or {}).get("confidence", 0.0))
    if intent == "overview" and conf < 0.7 and not has_timeframe and not has_group and not has_metrics:
        return "ask"
    return "go"


def _clarify_request_node(state: Story2GraphState) -> Story2GraphState:
    question = str(state.get("follow_up_question", "")).strip()
    if not question:
        question = (
            "I can run this, but I need one detail first: do you want an overview, a comparison, "
            "or only underperformers for a specific timeframe (for example, last 4 weeks)?"
        )
    return {
        "follow_up_question": question,
        "requested_slot": "analysis_scope",
        "missing_slots": ["analysis_scope"],
    }


def _read_metrics_node(state: Story2GraphState) -> Story2GraphState:
    user_text = state.get("user_query", "")
    filters = state.get("filters", {})
    with closing(sqlite3.connect(DB_PATH)) as conn:
        register_sqlite_alnum_normalizer(conn)
        _ensure_threshold_table(conn)
        available_weeks = _get_available_weeks(conn)
        timeframe = convert_weekly(user_text, available_weeks)
        assumptions = [timeframe["assumption"]] if timeframe.get("assumption") else []
        start_week = timeframe.get("start_week")
        end_week = timeframe.get("end_week")
        if not start_week or not end_week:
            return {
                "available_weeks": available_weeks,
                "timeframe": timeframe,
                "assumptions": assumptions,
                "rows": [],
                "prior_rows": [],
            }

        rows = read_campaign_metrics(conn, start_week=start_week, end_week=end_week, filters=filters)

        prior_rows: List[Dict[str, Any]] = []
        if rows:
            start_date = datetime.strptime(start_week, "%Y-%m-%d").date()
            end_date = datetime.strptime(end_week, "%Y-%m-%d").date()
            span_days = max(1, (end_date - start_date).days + 1)
            prior_end = start_date - timedelta(days=1)
            prior_start = prior_end - timedelta(days=span_days - 1)
            prior_rows = read_campaign_metrics(
                conn,
                start_week=prior_start.isoformat(),
                end_week=prior_end.isoformat(),
                filters=filters,
            )

    return {
        "available_weeks": available_weeks,
        "timeframe": timeframe,
        "assumptions": assumptions,
        "rows": rows,
        "prior_rows": prior_rows,
    }


def _route_after_read_metrics(state: Story2GraphState) -> str:
    timeframe = state.get("timeframe", {})
    if not timeframe.get("start_week") or not timeframe.get("end_week"):
        return "format"
    return "go"


def _aggregate_node(state: Story2GraphState) -> Story2GraphState:
    rows = state.get("rows", [])
    group_by = state.get("group_by", [])
    prior_rows = state.get("prior_rows", [])
    return {"summary": aggregate_summarize(rows, group_by=group_by, prior_rows=prior_rows)}


def _quality_checks_node(state: Story2GraphState) -> Story2GraphState:
    rows = state.get("rows", [])
    group_by = state.get("group_by", [])
    prior_rows = state.get("prior_rows", [])
    quality = _quality_flags(rows)
    clean_summary = aggregate_summarize(quality["rows_for_thresholding"], group_by=group_by, prior_rows=prior_rows)
    return {"quality": quality, "clean_summary": clean_summary}


def _threshold_eval_node(state: Story2GraphState) -> Story2GraphState:
    metrics = state.get("metrics_requested", [])
    threshold_metrics = [m for m in metrics if m in {"click_through_rate", "customer_acquisition_cost", "return_on_ad_spend"}]
    if not threshold_metrics:
        threshold_metrics = ["click_through_rate", "customer_acquisition_cost", "return_on_ad_spend"]
    with closing(sqlite3.connect(DB_PATH)) as conn:
        register_sqlite_alnum_normalizer(conn)
        _ensure_threshold_table(conn)
        underperformers = _evaluate_underperformance(conn, state.get("clean_summary", {}).get("rows", []), metrics_requested=threshold_metrics)
    return {"underperformers": underperformers}


def _maybe_generate_grounded_narrative(payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    try:
        llm = build_chat_openai(model="gpt-4o-mini", temperature=0)
        structured = llm.with_structured_output(GroundedNarrativeOutput)
        facts = {
            "intent": payload.get("intent"),
            "timeframe": payload.get("timeframe"),
            "row_count": payload.get("row_count"),
            "metrics_requested": payload.get("metrics_requested"),
            "group_by": payload.get("group_by"),
            "overall": (payload.get("summary") or {}).get("overall", {}),
            "group_rows_sample": ((payload.get("summary") or {}).get("rows") or [])[:3],
            "underperformers_sample": (payload.get("underperformers") or [])[:3],
            "quality": payload.get("quality"),
            "assumptions": payload.get("assumptions"),
            "unavailable_dimensions": payload.get("unavailable_dimensions"),
        }
        system = (
            "Write a concise, grounded analysis summary for marketing KPIs.\n"
            "Use only the provided FACTS. Do not invent fields or dimensions.\n"
            "If evidence is limited, state limitations directly.\n"
            "Return structured output only."
        )
        user = f"FACTS: {facts}"
        out = structured.invoke([("system", system), ("user", user)])
        conf = float(out.confidence)
        narrative = str(out.narrative or "").strip()
        if conf < 0.6 or not narrative:
            return None
        return {
            "narrative_text": narrative,
            "narrative_source": "llm_grounded",
            "narrative_confidence": round(conf, 2),
            "used_fields": list(out.used_fields or []),
        }
    except Exception:
        return None


def _narrative_node(state: Story2GraphState) -> Story2GraphState:
    intent = str(state.get("intent", "overview"))
    glossary_only = bool(state.get("glossary_only", False))
    analysis_signals = bool(state.get("analysis_signals", False))
    if intent == "definitions" or (glossary_only and not analysis_signals):
        return {}
    timeframe = state.get("timeframe", {})
    if not timeframe.get("start_week") or not timeframe.get("end_week"):
        return {}
    rows = state.get("rows", [])
    payload = {
        "intent": intent,
        "timeframe": timeframe,
        "assumptions": state.get("assumptions", []),
        "filters": state.get("filters", {}),
        "metrics_requested": state.get("metrics_requested", []),
        "group_by": state.get("group_by", []),
        "row_count": len(rows),
        "summary": state.get("summary", {}),
        "underperformers": state.get("underperformers", []),
        "quality": state.get("quality", {}),
        "unavailable_dimensions": state.get("unavailable_dimensions", []),
    }
    llm_out = _maybe_generate_grounded_narrative(payload)
    if not llm_out:
        return {"narrative_source": "deterministic_fallback", "narrative_confidence": 0.0}
    return {
        "narrative_text": llm_out["narrative_text"],
        "narrative_source": llm_out["narrative_source"],
        "narrative_confidence": llm_out["narrative_confidence"],
    }


def _response_self_check_node(state: Story2GraphState) -> Story2GraphState:
    response_text = str(state.get("response_text", "")).strip()
    if not response_text:
        return {}
    source = str((state.get("story_output") or {}).get("response_source", ""))
    if source != "llm_grounded":
        story_output = dict(state.get("story_output", {}))
        story_output["self_check"] = {"status": "skipped_non_llm_response"}
        return {"story_output": story_output}

    unsupported = list(state.get("unavailable_dimensions", []))
    lower = response_text.lower()
    unsupported_mentions = [d for d in unsupported if re.search(rf"\b{re.escape(d)}\b", lower)]
    if not unsupported_mentions:
        story_output = dict(state.get("story_output", {}))
        story_output["self_check"] = {"status": "passed", "unsupported_mentions": []}
        return {"story_output": story_output}

    payload = {
        "glossary_only": False,
        "intent": state.get("intent", "overview"),
        "concise": bool(state.get("concise", False)),
        "timeframe": state.get("timeframe", {}),
        "assumptions": state.get("assumptions", []),
        "filters": state.get("filters", {}),
        "metrics_requested": state.get("metrics_requested", []),
        "group_by": state.get("group_by", []),
        "row_count": len(state.get("rows", [])),
        "summary": state.get("summary", {}),
        "underperformers": state.get("underperformers", []),
        "quality": state.get("quality", {}),
        "unavailable_dimensions": state.get("unavailable_dimensions", []),
    }
    fallback = _format_response(payload)
    story_output = dict(state.get("story_output", {}))
    story_output["response_source"] = "deterministic_self_check_fallback"
    story_output["response_confidence"] = 0.0
    story_output["self_check"] = {
        "status": "fallback_applied",
        "reason": "unsupported_dimension_mention",
        "unsupported_mentions": unsupported_mentions,
    }
    return {"response_text": fallback, "story_output": story_output}


def _format_response_node(state: Story2GraphState) -> Story2GraphState:
    intent = str(state.get("intent", "overview"))
    glossary_only = bool(state.get("glossary_only", False))
    analysis_signals = bool(state.get("analysis_signals", False))
    intent_info = state.get("intent_info", {})
    metric_defs = state.get("metric_defs", {})
    follow_up_question = str(state.get("follow_up_question", "")).strip()

    if follow_up_question:
        return {
            "response_text": follow_up_question,
            "follow_up_question": follow_up_question,
            "story_output": {
                "needs_clarification": True,
                "requested_slot": state.get("requested_slot", "analysis_scope"),
                "missing_slots": state.get("missing_slots", ["analysis_scope"]),
                "intent": intent,
                "intent_source": intent_info.get("source"),
                "intent_confidence": intent_info.get("confidence"),
                "intent_rationale": intent_info.get("rationale"),
                "planner_source": state.get("planner_source", "deterministic_scope"),
                "planner_confidence": float(state.get("planner_confidence", 0.0)),
                "planner_rationale": state.get("planner_rationale", ""),
                "critic_action": state.get("critic_action", ""),
                "critic_reason": state.get("critic_reason", ""),
                "critic_confidence": float(state.get("critic_confidence", 0.0)),
                "replan_count": int(state.get("replan_count", 0)),
                "generated_on": date.today().isoformat(),
            },
            "state_summary": "Asked user for analysis scope clarification before data retrieval.",
        }

    if intent == "definitions" or (glossary_only and not analysis_signals):
        response_text = _format_response({"glossary_only": True, "metric_defs": metric_defs})
        return {
            "response_text": response_text,
            "story_output": {
                "metric_definitions": metric_defs,
                "intent": intent,
                "intent_source": intent_info.get("source"),
                "intent_confidence": intent_info.get("confidence"),
                "intent_rationale": intent_info.get("rationale"),
                "generated_on": date.today().isoformat(),
            },
            "state_summary": "Returned KPI glossary definitions.",
        }

    timeframe = state.get("timeframe", {})
    if not timeframe.get("start_week") or not timeframe.get("end_week"):
        return {
            "response_text": "No weekly data is available in the database yet.",
            "story_output": {"row_count": 0, "generated_on": date.today().isoformat()},
            "state_summary": "No weekly campaign metrics available.",
        }

    rows = state.get("rows", [])
    payload = {
        "glossary_only": False,
        "intent": intent,
        "concise": bool(state.get("concise", False)),
        "timeframe": timeframe,
        "assumptions": state.get("assumptions", []),
        "filters": state.get("filters", {}),
        "metrics_requested": state.get("metrics_requested", []),
        "group_by": state.get("group_by", []),
        "row_count": len(rows),
        "summary": state.get("summary", {}),
        "underperformers": state.get("underperformers", []),
        "quality": state.get("quality", {}),
        "unavailable_dimensions": state.get("unavailable_dimensions", []),
    }
    quality = state.get("quality", {})
    response_text = str(state.get("narrative_text", "")).strip() or _format_response(payload)
    return {
        "response_text": response_text,
        "story_output": {
            "intent": intent,
            "intent_source": intent_info.get("source"),
            "intent_confidence": intent_info.get("confidence"),
            "intent_rationale": intent_info.get("rationale"),
            "planner_source": state.get("planner_source", "deterministic_scope"),
            "planner_confidence": float(state.get("planner_confidence", 0.0)),
            "planner_rationale": state.get("planner_rationale", ""),
            "critic_action": state.get("critic_action", ""),
            "critic_reason": state.get("critic_reason", ""),
            "critic_confidence": float(state.get("critic_confidence", 0.0)),
            "replan_count": int(state.get("replan_count", 0)),
            "timeframe": timeframe,
            "filters": state.get("filters", {}),
            "group_by": state.get("group_by", []),
            "metrics_requested": state.get("metrics_requested", []),
            "row_count": len(rows),
            "summary": state.get("summary", {}),
            "underperformers": state.get("underperformers", []),
            "quality": {
                "invalid_ctr_rows": quality.get("invalid_ctr_rows", 0),
                "null_cac_rows": quality.get("null_cac_rows", 0),
                "null_roas_rows": quality.get("null_roas_rows", 0),
                "excluded_from_thresholding": quality.get("excluded_from_thresholding", 0),
                "valid_threshold_rows": quality.get("valid_threshold_rows", 0),
            },
            "unavailable_dimensions": state.get("unavailable_dimensions", []),
            "metric_definitions": metric_defs if glossary_only else {},
            "response_source": state.get("narrative_source", "deterministic_template"),
            "response_confidence": float(state.get("narrative_confidence", 0.0)),
            "generated_on": date.today().isoformat(),
        },
        "state_summary": (
            f"Analyzed {len(rows)} weekly metric rows "
            f"for {timeframe.get('start_week')}..{timeframe.get('end_week')}."
        ),
    }


def run_business_marketing_story2(req: StoryRequest) -> StoryResult:
    user_turn = _user_turn_number(req.messages)
    state_out = _get_business_marketing_story2_graph().invoke(
        {"user_query": req.user_query or "", "domain_context": req.domain_context or {}, "user_turn_number": user_turn}
    )
    follow_up = state_out.get("follow_up_question")
    if follow_up:
        return StoryResult(
            story_id=req.story_id,
            response_text=state_out.get("response_text") or str(follow_up),
            follow_up_question=follow_up,
            story_output=state_out.get("story_output", {}),
            state_updates_domain={
                "last_story_summary": state_out.get("state_summary", "Asked for clarification."),
                STORY2_STATE_KEY: {
                    "last_user_turn_number": user_turn,
                    "last_user_query": req.user_query,
                    "last_partial_scope": {
                        "intent": state_out.get("intent"),
                        "metrics_requested": state_out.get("metrics_requested", []),
                        "group_by": state_out.get("group_by", []),
                        "filters": state_out.get("filters", {}),
                    },
                    "pending_slot": state_out.get("requested_slot"),
                    "pending_question": follow_up,
                },
            },
        )
    return StoryResult(
        story_id=req.story_id,
        response_text=state_out.get("response_text") or "Unable to generate a business marketing analysis response.",
        follow_up_question=follow_up,
        story_output=state_out.get("story_output", {}),
        state_updates_domain={
            "last_story_summary": state_out.get("state_summary", "Completed business marketing story 2 workflow."),
            STORY2_STATE_KEY: {
                "last_user_turn_number": user_turn,
                "last_user_query": req.user_query,
                "last_resolved_scope": {
                    "intent": state_out.get("intent"),
                    "metrics_requested": state_out.get("metrics_requested", []),
                    "group_by": state_out.get("group_by", []),
                    "filters": state_out.get("filters", {}),
                },
                "memory_applied_fields": state_out.get("memory_applied_fields", []),
                "pending_slot": None,
                "pending_question": None,
            },
        },
    )


def get_business_marketing_story2_mermaid() -> str:
    return _get_business_marketing_story2_graph().get_graph().draw_mermaid()


def _get_business_marketing_story2_graph():
    global BUSINESS_MARKETING_STORY2_GRAPH
    if BUSINESS_MARKETING_STORY2_GRAPH is not None:
        return BUSINESS_MARKETING_STORY2_GRAPH

    g = StateGraph(Story2GraphState)
    g.add_node("classify_intent", _classify_intent_node)
    g.add_node("extract_scope", _extract_scope_node)
    g.add_node("plan_with_llm", _plan_with_llm_node)
    g.add_node("merge_prior_scope", _merge_with_prior_scope_node)
    g.add_node("clarify_request", _clarify_request_node)
    g.add_node("read_metrics", _read_metrics_node)
    g.add_node("aggregate", _aggregate_node)
    g.add_node("quality_checks", _quality_checks_node)
    g.add_node("critic", _critic_node)
    g.add_node("replan_scope", _replan_scope_node)
    g.add_node("threshold_eval", _threshold_eval_node)
    g.add_node("narrative", _narrative_node)
    g.add_node("format_response", _format_response_node)
    g.add_node("self_check", _response_self_check_node)

    g.set_entry_point("classify_intent")
    g.add_conditional_edges("classify_intent", _route_after_classify, {"go": "extract_scope", "format": "format_response"})
    g.add_edge("extract_scope", "plan_with_llm")
    g.add_edge("plan_with_llm", "merge_prior_scope")
    g.add_conditional_edges("merge_prior_scope", _route_after_extract_scope, {"go": "read_metrics", "ask": "clarify_request"})
    g.add_edge("clarify_request", "format_response")
    g.add_conditional_edges("read_metrics", _route_after_read_metrics, {"go": "aggregate", "format": "format_response"})
    g.add_edge("aggregate", "quality_checks")
    g.add_edge("quality_checks", "critic")
    g.add_conditional_edges("critic", _route_after_critic, {"threshold": "threshold_eval", "replan": "replan_scope", "ask": "clarify_request"})
    g.add_edge("replan_scope", "read_metrics")
    g.add_edge("threshold_eval", "narrative")
    g.add_edge("narrative", "format_response")
    g.add_edge("format_response", "self_check")
    g.add_edge("self_check", END)

    BUSINESS_MARKETING_STORY2_GRAPH = g.compile()
    return BUSINESS_MARKETING_STORY2_GRAPH


def get_business_marketing_story2_mermaid_png() -> bytes:
    graph = _get_business_marketing_story2_graph().get_graph()
    try:
        return graph.draw_mermaid_png(draw_method=MermaidDrawMethod.PYPPETEER)
    except Exception:
        return graph.draw_mermaid_png()
