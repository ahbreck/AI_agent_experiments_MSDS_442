from __future__ import annotations

import re
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, Sequence, Tuple, TypedDict

from langchain_openai import ChatOpenAI
from langchain_core.runnables.graph_mermaid import MermaidDrawMethod
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field
from ..contracts import StoryRequest, StoryResult
from ..utils import normalize_campaign_id, register_sqlite_alnum_normalizer

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


class IntentClassifierOutput(BaseModel):
    intent: Literal["underperformers_only", "compare_metrics", "overview", "definitions"]
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    rationale: str = Field(default="")
    concise: bool = Field(default=False)


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
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
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


def run_business_marketing_story2(req: StoryRequest) -> StoryResult:
    user_text = req.user_query or ""
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

    with sqlite3.connect(DB_PATH) as conn:
        register_sqlite_alnum_normalizer(conn)
        _ensure_threshold_table(conn)
        available_weeks = _get_available_weeks(conn)

        timeframe = convert_weekly(user_text, available_weeks)
        group_by = _requested_grouping(user_text)
        if intent == "compare_metrics" and not group_by:
            group_by = ["campaign_id"]
        unavailable_dimensions = _requested_unavailable_dimensions(user_text)
        filters = _extract_filters(user_text, grouped_by=group_by)
        metrics_explicit = _requested_metrics(user_text)
        metrics = metrics_explicit or _default_metrics_for_intent(intent)
        assumptions = [timeframe["assumption"]] if timeframe.get("assumption") else []

        if intent == "definitions" or (glossary_only and not analysis_signals):
            response_text = _format_response({"glossary_only": True, "metric_defs": metric_defs})
            return StoryResult(
                story_id=req.story_id,
                response_text=response_text,
                story_output={
                    "metric_definitions": metric_defs,
                    "intent": intent,
                    "intent_source": intent_info.get("source"),
                    "intent_confidence": intent_info.get("confidence"),
                    "intent_rationale": intent_info.get("rationale"),
                    "generated_on": date.today().isoformat(),
                },
                state_updates_domain={"last_story_summary": "Returned KPI glossary definitions."},
            )

        start_week = timeframe.get("start_week")
        end_week = timeframe.get("end_week")
        if not start_week or not end_week:
            response_text = "No weekly data is available in the database yet."
            return StoryResult(
                story_id=req.story_id,
                response_text=response_text,
                story_output={"row_count": 0, "generated_on": date.today().isoformat()},
                state_updates_domain={"last_story_summary": "No weekly campaign metrics available."},
            )

        rows = read_campaign_metrics(conn, start_week=start_week, end_week=end_week, filters=filters)

        # Prior period with matching calendar duration.
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

        summary = aggregate_summarize(rows, group_by=group_by, prior_rows=prior_rows)
        quality = _quality_flags(rows)
        clean_summary = aggregate_summarize(quality["rows_for_thresholding"], group_by=group_by, prior_rows=prior_rows)
        threshold_metrics = [m for m in metrics if m in {"click_through_rate", "customer_acquisition_cost", "return_on_ad_spend"}]
        if not threshold_metrics:
            threshold_metrics = ["click_through_rate", "customer_acquisition_cost", "return_on_ad_spend"]
        underperformers = _evaluate_underperformance(conn, clean_summary["rows"], metrics_requested=threshold_metrics)

        payload = {
            "glossary_only": False,
            "intent": intent,
            "concise": concise,
            "timeframe": timeframe,
            "assumptions": assumptions,
            "filters": filters,
            "metrics_requested": metrics,
            "group_by": group_by,
            "row_count": len(rows),
            "summary": summary,
            "underperformers": underperformers,
            "quality": quality,
            "unavailable_dimensions": unavailable_dimensions,
        }
        response_text = _format_response(payload)

    return StoryResult(
        story_id=req.story_id,
        response_text=response_text,
        story_output={
            "intent": intent,
            "intent_source": intent_info.get("source"),
            "intent_confidence": intent_info.get("confidence"),
            "intent_rationale": intent_info.get("rationale"),
            "timeframe": timeframe,
            "filters": filters,
            "group_by": group_by,
            "metrics_requested": metrics,
            "row_count": len(rows),
            "summary": summary,
            "underperformers": underperformers,
            "quality": {
                "invalid_ctr_rows": quality["invalid_ctr_rows"],
                "null_cac_rows": quality["null_cac_rows"],
                "null_roas_rows": quality["null_roas_rows"],
                "excluded_from_thresholding": quality["excluded_from_thresholding"],
                "valid_threshold_rows": quality["valid_threshold_rows"],
            },
            "unavailable_dimensions": unavailable_dimensions,
            "metric_definitions": metric_defs if glossary_only else {},
            "generated_on": date.today().isoformat(),
        },
        state_updates_domain={
            "last_story_summary": (
                f"Analyzed {len(rows)} weekly metric rows "
                f"for {timeframe.get('start_week')}..{timeframe.get('end_week')}."
            )
        },
    )


def get_business_marketing_story2_mermaid() -> str:
    return _get_business_marketing_story2_graph().get_graph().draw_mermaid()


class Story2GraphState(TypedDict, total=False):
    user_query: str


def _get_business_marketing_story2_graph():
    global BUSINESS_MARKETING_STORY2_GRAPH
    if BUSINESS_MARKETING_STORY2_GRAPH is not None:
        return BUSINESS_MARKETING_STORY2_GRAPH

    g = StateGraph(Story2GraphState)

    def passthrough(state: Story2GraphState) -> Story2GraphState:
        return state

    g.add_node("classify_intent", passthrough)
    g.add_node("extract_scope", passthrough)
    g.add_node("read_metrics", passthrough)
    g.add_node("aggregate", passthrough)
    g.add_node("quality_checks", passthrough)
    g.add_node("threshold_eval", passthrough)
    g.add_node("format_response", passthrough)

    g.set_entry_point("classify_intent")
    g.add_edge("classify_intent", "extract_scope")
    g.add_edge("extract_scope", "read_metrics")
    g.add_edge("read_metrics", "aggregate")
    g.add_edge("aggregate", "quality_checks")
    g.add_edge("quality_checks", "threshold_eval")
    g.add_edge("threshold_eval", "format_response")
    g.add_edge("format_response", END)

    BUSINESS_MARKETING_STORY2_GRAPH = g.compile()
    return BUSINESS_MARKETING_STORY2_GRAPH


def get_business_marketing_story2_mermaid_png() -> bytes:
    graph = _get_business_marketing_story2_graph().get_graph()
    try:
        return graph.draw_mermaid_png(draw_method=MermaidDrawMethod.PYPPETEER)
    except Exception:
        return graph.draw_mermaid_png()
