from __future__ import annotations

import re
import sqlite3
from contextlib import closing
from datetime import date
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple, TypedDict

from langchain_openai import ChatOpenAI
try:
    from langchain_core.runnables.graph import MermaidDrawMethod
except Exception:  # pragma: no cover - backward compatibility for older langchain-core
    from langchain_core.runnables.graph_mermaid import MermaidDrawMethod
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from ..contracts import StoryRequest, StoryResult

PROJECT_PHASE_3 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_3 / "kb" / "BusinessMarketing" / "brand_feedback.db"

VALID_LOOKBACKS = (7, 14, 30)
DEFAULT_LOOKBACK_DAYS = 7
DEFAULT_TOP_N = 10
DEFAULT_CHANNEL = "email"
DEFAULT_TONE = "consultative"
DEFAULT_RECENCY_BONUS_BASE = 10.0
VALID_CHANNELS = {"email", "call", "sms"}
VALID_TONES = {"friendly", "consultative", "urgent", "neutral"}
INFER_ACCEPT_CONFIDENCE = 0.90
INFER_CLARIFY_CONFIDENCE = 0.65
CARRY_FORWARD_MAX_USER_TURN_GAP = 3
STORY3_STATE_KEY = "bm_story_3_state"
BUSINESS_MARKETING_STORY3_GRAPH = None


class LeadPlanningOutput(BaseModel):
    lookback_days: Optional[int] = Field(default=None)
    top_n: Optional[int] = Field(default=None)
    channel: Optional[str] = Field(default=None)
    tone: Optional[str] = Field(default=None)
    primary_class_interest: List[str] = Field(default_factory=list)
    lookback_confidence: Optional[float] = Field(default=None)
    channel_confidence: Optional[float] = Field(default=None)
    interest_confidence: Optional[float] = Field(default=None)
    lookback_evidence: Optional[str] = Field(default=None)
    channel_evidence: Optional[str] = Field(default=None)
    interest_evidence: Optional[str] = Field(default=None)
    assumptions: List[str] = Field(default_factory=list)
    rationale: str = Field(default="")


def _closest_lookback(days: int) -> int:
    return min(VALID_LOOKBACKS, key=lambda x: abs(x - days))


def _parse_top_n(user_text: str) -> int:
    m = re.search(r"\btop\s+(\d{1,3})\b", user_text.lower())
    if m:
        return max(1, min(100, int(m.group(1))))
    return DEFAULT_TOP_N


def _parse_lookback_days(user_text: str) -> Tuple[int, Optional[str]]:
    text = user_text.lower()
    explicit = re.search(r"\blast\s+(\d{1,3})\s+days?\b", text)
    if explicit:
        raw = int(explicit.group(1))
        if raw in VALID_LOOKBACKS:
            return raw, None
        coerced = _closest_lookback(raw)
        return coerced, f"Requested last {raw} days; mapped to supported {coerced}-day lookback."
    if "this week" in text or "current week" in text or "latest week" in text:
        return 7, None
    if "last two weeks" in text:
        return 14, None
    if "last month" in text:
        return 30, None
    return DEFAULT_LOOKBACK_DAYS, "No timeframe provided; defaulted to last 7 days."


def _parse_channel(user_text: str) -> Tuple[str, Optional[str]]:
    text = user_text.lower()
    if re.search(r"\b(email|e-mail)\b", text):
        return "email", None
    if re.search(r"\b(call|phone)\b", text):
        return "call", None
    if re.search(r"\bsms|text\b", text):
        return "sms", None
    return DEFAULT_CHANNEL, "No outreach channel provided; defaulted to email."


def _parse_tone(user_text: str) -> str:
    text = user_text.lower()
    for tone in ("friendly", "consultative", "urgent", "neutral"):
        if re.search(rf"\b{tone}\b", text):
            return tone
    return DEFAULT_TONE


def _parse_interest_filters(user_text: str) -> Optional[List[str]]:
    mapping = {
        "cycling": "Cycling",
        "yoga": "Yoga",
        "strength": "Strength",
        "running": "Running",
    }
    found: List[str] = []
    text = user_text.lower()
    for token, canonical in mapping.items():
        if re.search(rf"\b{token}\b", text):
            found.append(canonical)
    if not found:
        return None
    dedup: List[str] = []
    for item in found:
        if item not in dedup:
            dedup.append(item)
    return dedup


def _query_has_explicit_lookback(user_text: str) -> bool:
    text = user_text.lower()
    if re.search(r"\blast\s+(\d{1,3})\s+days?\b", text):
        return True
    return any(phrase in text for phrase in ("this week", "current week", "latest week", "last two weeks", "last month"))


def _query_has_explicit_channel(user_text: str) -> bool:
    text = user_text.lower()
    return bool(re.search(r"\b(email|e-mail|call|phone|sms|text)\b", text))


def _query_has_explicit_interest(user_text: str) -> bool:
    text = user_text.lower()
    return bool(re.search(r"\b(cycling|yoga|strength|running)\b", text))


def _query_requests_all_interests(user_text: str) -> bool:
    text = user_text.lower()
    if re.search(r"\b(all|any)\s+(interests?|classes?|categories?)\b", text):
        return True
    if re.search(r"\b(top\s+\d{1,3}\s+)?leads?\b", text) and not _query_has_explicit_interest(text):
        # "top N leads" without an explicit interest is treated as a broad-scope request.
        return True
    return False


def _query_has_explicit_top_n(user_text: str) -> bool:
    return re.search(r"\btop\s+(\d{1,3})\b", user_text.lower()) is not None


def _query_has_explicit_tone(user_text: str) -> bool:
    text = user_text.lower()
    return any(re.search(rf"\b{tone}\b", text) for tone in VALID_TONES)


def _user_turn_number(messages: Sequence[Dict[str, Any]]) -> int:
    return sum(1 for m in messages if str(m.get("role", "")).lower() == "user")


def _is_refinement_like_query(user_text: str) -> bool:
    text = user_text.lower()
    refinement_terms = {
        "change",
        "update",
        "adjust",
        "refine",
        "switch",
        "instead",
        "use",
        "set",
        "make",
        "now",
        "only",
        "keep",
        "same",
        "show top",
        "tone",
        "top ",
    }
    shift_terms = {
        "campaign",
        "ctr",
        "cac",
        "roas",
        "spend",
        "sentiment",
        "theme",
        "fraud",
        "security",
        "login",
        "workout",
        "heart rate",
        "zone",
        "cadence",
        "member_id",
    }
    has_refinement = any(term in text for term in refinement_terms)
    has_shift = any(term in text for term in shift_terms)
    return has_refinement and not has_shift


def _clear_clarification_for_field(plan: Dict[str, Any], field_name: str) -> None:
    unresolved = [f for f in list(plan.get("unresolved_fields", [])) if f != field_name]
    plan["unresolved_fields"] = unresolved
    plan["needs_clarification"] = bool(unresolved)

    parts: List[str] = []
    if "lookback_days" in unresolved:
        parts.append("timeframe (7, 14, or 30 days)")
    if "channel" in unresolved:
        parts.append("outreach channel (email, call, or sms)")
    if "primary_class_interest" in unresolved:
        parts.append("interest focus (Cycling, Yoga, Strength, Running, or all)")
    plan["clarification_question"] = ("Please confirm " + " and ".join(parts) + ".") if parts else None


def _merge_with_prior_plan(
    plan: Dict[str, Any],
    prior_story3_state: Optional[Dict[str, Any]],
    user_text: str,
    user_turn_number: int,
) -> Dict[str, Any]:
    if not prior_story3_state:
        return plan
    prior_plan = prior_story3_state.get("last_resolved_plan")
    if not isinstance(prior_plan, dict):
        return plan

    prior_turn = int(prior_story3_state.get("last_user_turn_number", 0) or 0)
    turn_gap = max(0, user_turn_number - prior_turn)
    if turn_gap > CARRY_FORWARD_MAX_USER_TURN_GAP:
        return plan
    if not _is_refinement_like_query(user_text):
        return plan

    out = dict(plan)
    out["assumptions"] = list(out.get("assumptions", []))
    out["field_resolution"] = dict(out.get("field_resolution", {}))

    explicit_lookback = _query_has_explicit_lookback(user_text)
    explicit_channel = _query_has_explicit_channel(user_text)
    explicit_interest = _query_has_explicit_interest(user_text)
    explicit_all_interests = _query_requests_all_interests(user_text)
    explicit_top_n = _query_has_explicit_top_n(user_text)
    explicit_tone = _query_has_explicit_tone(user_text)

    carried_fields: List[str] = []

    def _drop_assumptions(containing: Sequence[str]) -> None:
        out["assumptions"] = [
            a
            for a in out.get("assumptions", [])
            if not any(token in a.lower() for token in containing)
        ]

    def carry(field_name: str, value: Any) -> None:
        out[field_name] = value
        fr = dict(out.get("field_resolution", {}).get(field_name, {}))
        fr["source"] = "carried_forward"
        fr["confidence"] = round(float(fr.get("confidence", 0.0) or 0.0), 2)
        fr["evidence"] = (
            "Carried forward from prior bm_story_3 plan in this thread due to refinement-style query."
        )
        out["field_resolution"][field_name] = fr
        carried_fields.append(field_name)
        _clear_clarification_for_field(out, field_name)

    if not explicit_lookback and "lookback_days" in prior_plan:
        carry("lookback_days", int(prior_plan["lookback_days"]))
    if not explicit_channel and "channel" in prior_plan:
        carry("channel", str(prior_plan["channel"]))
    if explicit_all_interests:
        out["primary_class_interest"] = None
        fr = dict(out.get("field_resolution", {}).get("primary_class_interest", {}))
        fr["source"] = "explicit"
        fr["evidence"] = "Detected broad lead request; cleared prior interest filter to include all interests."
        out["field_resolution"]["primary_class_interest"] = fr
        _clear_clarification_for_field(out, "primary_class_interest")
        _drop_assumptions(("inferred primary_class_interest=",))
        out["assumptions"].append(
            "Interpreted request as all-interest scope; prior primary_class_interest filter was cleared."
        )
    elif not explicit_interest and "primary_class_interest" in prior_plan:
        out["primary_class_interest"] = prior_plan["primary_class_interest"]
        carry("primary_class_interest", prior_plan["primary_class_interest"])

    if not explicit_top_n and "top_n" in prior_plan:
        out["top_n"] = int(prior_plan["top_n"])
        out["assumptions"].append("Carried forward top_n from previous bm_story_3 turn.")
    if not explicit_tone and "tone" in prior_plan:
        out["tone"] = str(prior_plan["tone"])
        out["assumptions"].append("Carried forward tone from previous bm_story_3 turn.")

    # Ensure assumptions are consistent with carried-forward resolution.
    if "lookback_days" in carried_fields:
        _drop_assumptions(("no timeframe provided; defaulted", "inferred lookback="))
        out["assumptions"].append("Carried forward lookback_days from previous bm_story_3 turn.")
    if "channel" in carried_fields:
        _drop_assumptions(("no outreach channel provided; defaulted", "inferred channel='"))
        out["assumptions"].append("Carried forward channel from previous bm_story_3 turn.")
    if "primary_class_interest" in carried_fields:
        _drop_assumptions(("inferred primary_class_interest=",))
        out["assumptions"].append("Carried forward primary_class_interest from previous bm_story_3 turn.")

    if out.get("needs_clarification") is False:
        out["follow_up_question"] = None
    return out


def _deterministic_request_plan(user_text: str) -> Dict[str, Any]:
    top_n = _parse_top_n(user_text)
    lookback_days, lookback_assumption = _parse_lookback_days(user_text)
    channel, channel_assumption = _parse_channel(user_text)
    tone = _parse_tone(user_text)
    interests = _parse_interest_filters(user_text)
    assumptions = [x for x in [lookback_assumption, channel_assumption] if x]
    return {
        "top_n": top_n,
        "lookback_days": lookback_days,
        "channel": channel,
        "tone": tone,
        "primary_class_interest": interests,
        "assumptions": assumptions,
        "planning_source": "deterministic",
        "planning_rationale": "Deterministic parser outputs.",
    }


def _llm_request_plan(user_text: str) -> Optional[LeadPlanningOutput]:
    system = (
        "You plan lead-prioritization requests into constrained fields.\n"
        "Allowed lookback_days: 7, 14, 30.\n"
        "Allowed channel: email, call, sms.\n"
        "Allowed tone: friendly, consultative, urgent, neutral.\n"
        "Allowed interests: Cycling, Yoga, Strength, Running.\n"
        "For lookback/channel/interest include confidence scores from 0.0 to 1.0.\n"
        "Include short evidence strings for lookback/channel/interest.\n"
        "If the user is ambiguous, set sensible defaults and include assumptions.\n"
        "Return only structured output."
    )
    user = f"USER_QUERY: {user_text}"
    try:
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        structured = llm.with_structured_output(LeadPlanningOutput)
        return structured.invoke([("system", system), ("user", user)])
    except Exception:
        return None


def _normalize_interest_values(values: Sequence[str]) -> Optional[List[str]]:
    mapping = {
        "cycling": "Cycling",
        "yoga": "Yoga",
        "strength": "Strength",
        "running": "Running",
    }
    out: List[str] = []
    for raw in values:
        key = str(raw or "").strip().lower()
        val = mapping.get(key)
        if val and val not in out:
            out.append(val)
    return out or None


def _resolve_request_plan(user_text: str) -> Dict[str, Any]:
    deterministic = _deterministic_request_plan(user_text)
    llm_plan = _llm_request_plan(user_text)
    if llm_plan is None:
        deterministic["planning_source"] = "deterministic_fallback_llm_unavailable"
        deterministic["planning_rationale"] = "LLM planner unavailable; used deterministic parser."
        return deterministic

    # Keep assumptions deterministic/system-generated only.
    # LLM free-form assumption text can introduce pseudo-factual phrasing.
    assumptions = list(deterministic["assumptions"])

    explicit_lookback = _query_has_explicit_lookback(user_text)
    explicit_channel = _query_has_explicit_channel(user_text)
    explicit_interest = _query_has_explicit_interest(user_text)

    def _safe_conf(v: Optional[float]) -> float:
        if v is None:
            return 0.0
        try:
            return max(0.0, min(1.0, float(v)))
        except Exception:
            return 0.0

    raw_top_n = llm_plan.top_n if llm_plan.top_n is not None else deterministic["top_n"]
    top_n = max(1, min(100, int(raw_top_n)))

    raw_lookback = llm_plan.lookback_days if llm_plan.lookback_days is not None else deterministic["lookback_days"]
    lookback_conf = _safe_conf(llm_plan.lookback_confidence)
    if explicit_lookback and int(raw_lookback) in VALID_LOOKBACKS:
        lookback_days = int(raw_lookback)
        lookback_source = "explicit"
    elif explicit_lookback:
        lookback_days = _closest_lookback(int(raw_lookback))
        lookback_source = "explicit_coerced"
        assumptions.append(
            f"Requested/parsed lookback {int(raw_lookback)} is unsupported; mapped to {lookback_days} days."
        )
    elif llm_plan.lookback_days is not None and lookback_conf >= INFER_ACCEPT_CONFIDENCE:
        candidate = int(raw_lookback)
        lookback_days = candidate if candidate in VALID_LOOKBACKS else _closest_lookback(candidate)
        lookback_source = "inferred"
        assumptions.append(
            f"Inferred lookback={lookback_days} days from query ambiguity (confidence={lookback_conf:.2f})."
        )
        if candidate not in VALID_LOOKBACKS:
            assumptions.append(
                f"Inferred lookback {candidate} is unsupported; mapped to {lookback_days} days."
            )
    elif llm_plan.lookback_days is not None and lookback_conf >= INFER_CLARIFY_CONFIDENCE:
        lookback_days = int(deterministic["lookback_days"])
        lookback_source = "default_pending_clarification"
    else:
        lookback_days = int(deterministic["lookback_days"])
        lookback_source = "default"

    raw_channel = str(llm_plan.channel or deterministic["channel"]).strip().lower()
    channel_conf = _safe_conf(llm_plan.channel_confidence)
    if explicit_channel:
        channel = raw_channel if raw_channel in VALID_CHANNELS else deterministic["channel"]
        channel_source = "explicit"
        if raw_channel not in VALID_CHANNELS:
            channel_source = "explicit_coerced"
            assumptions.append(f"Unsupported channel '{raw_channel}' detected; defaulted to {channel}.")
    elif llm_plan.channel and raw_channel in VALID_CHANNELS and channel_conf >= INFER_ACCEPT_CONFIDENCE:
        channel = raw_channel
        channel_source = "inferred"
        assumptions.append(f"Inferred channel='{channel}' from query ambiguity (confidence={channel_conf:.2f}).")
    elif llm_plan.channel and channel_conf >= INFER_CLARIFY_CONFIDENCE:
        channel = str(deterministic["channel"])
        channel_source = "default_pending_clarification"
    else:
        channel = str(deterministic["channel"])
        channel_source = "default"

    raw_tone = str(llm_plan.tone or deterministic["tone"]).strip().lower()
    tone = raw_tone if raw_tone in VALID_TONES else deterministic["tone"]
    if raw_tone not in VALID_TONES:
        assumptions.append(f"Unsupported tone '{raw_tone}' detected; defaulted to {tone}.")

    interest_conf = _safe_conf(llm_plan.interest_confidence)
    interests = _normalize_interest_values(llm_plan.primary_class_interest)
    if explicit_interest:
        interest_source = "explicit"
        if interests is None:
            interests = deterministic["primary_class_interest"]
            interest_source = "explicit_unresolved"
    elif interests and interest_conf >= INFER_ACCEPT_CONFIDENCE:
        interest_source = "inferred"
        assumptions.append(
            f"Inferred primary_class_interest={interests} from query ambiguity (confidence={interest_conf:.2f})."
        )
    elif interests and interest_conf >= INFER_CLARIFY_CONFIDENCE:
        interests = deterministic["primary_class_interest"]
        interest_source = "default_pending_clarification"
    else:
        interests = deterministic["primary_class_interest"]
        interest_source = "default"

    unresolved_fields: List[str] = []
    if lookback_source == "default_pending_clarification":
        unresolved_fields.append("lookback_days")
    if channel_source == "default_pending_clarification":
        unresolved_fields.append("channel")
    if interest_source == "default_pending_clarification":
        unresolved_fields.append("primary_class_interest")

    clarification_parts: List[str] = []
    if "lookback_days" in unresolved_fields:
        clarification_parts.append("timeframe (7, 14, or 30 days)")
    if "channel" in unresolved_fields:
        clarification_parts.append("outreach channel (email, call, or sms)")
    if "primary_class_interest" in unresolved_fields:
        clarification_parts.append("interest focus (Cycling, Yoga, Strength, Running, or all)")

    clarification_question = None
    if clarification_parts:
        clarification_question = "Please confirm " + " and ".join(clarification_parts) + "."

    # Keep assumptions aligned with the selected resolution for each field.
    if lookback_source == "inferred":
        assumptions = [
            a for a in assumptions if "no timeframe provided; defaulted to last 7 days" not in a.lower()
        ]
    if channel_source == "inferred":
        assumptions = [
            a for a in assumptions if "no outreach channel provided; defaulted to email" not in a.lower()
        ]

    # Remove any remaining pseudo-factual assumption text from planner free-form language.
    assumptions = [
        a
        for a in assumptions
        if not any(
            term in a.lower()
            for term in (
                "based on recent engagement",
                "based on previous",
                "past interactions",
                "recent trends",
                "lead behavior",
                "most responsive",
                "most effective",
            )
        )
    ]

    def _resolution_evidence(field_name: str, source: str) -> str:
        if source in {"explicit", "explicit_coerced", "explicit_unresolved"}:
            return f"Detected explicit {field_name} intent in user query text."
        if source == "inferred":
            return (
                f"Inferred {field_name} from ambiguous user wording; "
                "this is a language-based inference, not a data-derived fact."
            )
        if source == "default_pending_clarification":
            return f"Signal for {field_name} was partially informative; waiting for user confirmation."
        return f"No reliable signal for {field_name}; deterministic default was applied."

    return {
        "top_n": top_n,
        "lookback_days": lookback_days,
        "channel": channel,
        "tone": tone,
        "primary_class_interest": interests,
        "assumptions": assumptions,
        "planning_source": "llm_validated",
        "planning_rationale": llm_plan.rationale or "LLM planner output accepted and normalized.",
        "field_resolution": {
            "lookback_days": {
                "source": lookback_source,
                "confidence": round(lookback_conf, 2),
                "evidence": _resolution_evidence("lookback_days", lookback_source),
            },
            "channel": {
                "source": channel_source,
                "confidence": round(channel_conf, 2),
                "evidence": _resolution_evidence("channel", channel_source),
            },
            "primary_class_interest": {
                "source": interest_source,
                "confidence": round(interest_conf, 2),
                "evidence": _resolution_evidence("primary_class_interest", interest_source),
            },
        },
        "needs_clarification": bool(unresolved_fields),
        "unresolved_fields": unresolved_fields,
        "clarification_question": clarification_question,
    }


def _read_as_of_date(conn: sqlite3.Connection) -> Optional[str]:
    row = conn.execute(
        """
        SELECT MAX(as_of_date)
        FROM lead_engagement_signals
        WHERE as_of_date <= DATE('now')
        """
    ).fetchone()
    if row and row[0]:
        return str(row[0])
    row = conn.execute("SELECT MAX(as_of_date) FROM lead_engagement_signals").fetchone()
    if row and row[0]:
        return str(row[0])
    return None


def read_lead_signals(
    conn: sqlite3.Connection,
    filters: Dict[str, Any],
    recency_window: int,
    as_of_date: str,
) -> List[Dict[str, Any]]:
    where = ["s.lookback_days = ?", "s.as_of_date = ?"]
    params: List[Any] = [recency_window, as_of_date]

    interests = filters.get("primary_class_interest")
    if interests:
        placeholders = ",".join(["?"] * len(interests))
        where.append(f"s.primary_class_interest IN ({placeholders})")
        params.extend(interests)

    sql = f"""
    SELECT
      s.lead_id,
      s.member_id,
      s.as_of_date,
      s.lookback_days,
      s.pages_viewed,
      s.primary_class_interest,
      s.cart_abandonments,
      s.trial_used,
      s.days_since_last_visit,
      s.email_opens,
      s.last_visit_at,
      l.first_name,
      l.company_name,
      l.email,
      l.phone
    FROM lead_engagement_signals s
    INNER JOIN leads l ON l.lead_id = s.lead_id
    WHERE {" AND ".join(where)}
    """
    conn.row_factory = sqlite3.Row
    return [dict(r) for r in conn.execute(sql, params).fetchall()]


def _read_scoring_rules(conn: sqlite3.Connection) -> Dict[str, float]:
    rows = conn.execute(
        """
        SELECT metric_name, weight
        FROM lead_scoring_config
        WHERE is_active = 1
        """
    ).fetchall()
    rules = {str(r[0]): float(r[1]) for r in rows}
    return {
        "pages_viewed": rules.get("pages_viewed", 2.0),
        "cart_abandonments": rules.get("cart_abandonments", 5.0),
        "trial_used": rules.get("trial_used", 8.0),
        "email_opens": rules.get("email_opens", 1.0),
        "recency_bonus_base": rules.get("recency_bonus_base", DEFAULT_RECENCY_BONUS_BASE),
    }


def _read_suppressed_leads(conn: sqlite3.Connection, channel: str) -> set[str]:
    rows = conn.execute(
        """
        SELECT lead_id
        FROM suppression_list
        WHERE is_suppressed = 1
          AND channel IN (?, 'all')
        """,
        (channel,),
    ).fetchall()
    return {str(r[0]) for r in rows}


def _tier_from_score(score: float) -> str:
    if score >= 35:
        return "High"
    if score >= 20:
        return "Med"
    return "Low"


def score_and_rank_leads(
    conn: sqlite3.Connection,
    data: Sequence[Dict[str, Any]],
    scoring_rules: Dict[str, float],
    channel: str,
    top_n: int,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    suppressed = _read_suppressed_leads(conn, channel=channel)
    ranked: List[Dict[str, Any]] = []
    excluded_count = 0

    for row in data:
        lead_id = str(row["lead_id"])
        if lead_id in suppressed:
            excluded_count += 1
            continue

        recency_bonus = max(
            0.0,
            float(scoring_rules["recency_bonus_base"]) - float(row.get("days_since_last_visit") or 0),
        )
        score = (
            float(scoring_rules["pages_viewed"]) * float(row.get("pages_viewed") or 0)
            + float(scoring_rules["cart_abandonments"]) * float(row.get("cart_abandonments") or 0)
            + float(scoring_rules["trial_used"]) * (1.0 if int(row.get("trial_used") or 0) == 1 else 0.0)
            + float(scoring_rules["email_opens"]) * float(row.get("email_opens") or 0)
            + recency_bonus
        )
        enriched = dict(row)
        enriched["lead_score"] = round(score, 2)
        enriched["priority_tier"] = _tier_from_score(score)
        ranked.append(enriched)

    ranked.sort(
        key=lambda x: (
            -float(x["lead_score"]),
            int(x.get("days_since_last_visit") or 9999),
            -int(x.get("pages_viewed") or 0),
        )
    )
    return ranked[:top_n], {"suppressed_excluded_count": excluded_count, "candidate_count": len(data)}


def _read_intent_priority(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        """
        SELECT intent
        FROM intent_priority_config
        ORDER BY priority_rank ASC
        """
    ).fetchall()
    ordered = [str(r[0]) for r in rows]
    if ordered:
        return ordered
    return ["purchase_ready", "trial_engaged", "considering", "browsing"]


def infer_intent_from_signals(conn: sqlite3.Connection, lead_signals: Dict[str, Any]) -> str:
    pages = int(lead_signals.get("pages_viewed") or 0)
    cart = int(lead_signals.get("cart_abandonments") or 0)
    trial = int(lead_signals.get("trial_used") or 0)
    opens = int(lead_signals.get("email_opens") or 0)

    rules = {
        "purchase_ready": cart > 0,
        "trial_engaged": trial == 1,
        "considering": pages >= 5 or opens >= 3,
        "browsing": True,
    }
    for intent in _read_intent_priority(conn):
        if rules.get(intent, False):
            return intent
    return "browsing"


def select_message_template(
    conn: sqlite3.Connection,
    intent: str,
    primary_class_interest: Optional[str],
    channel: str,
    tone: str,
) -> Optional[Dict[str, Any]]:
    params: List[Any] = [intent, channel, tone]
    where = ["intent = ?", "channel = ?", "tone = ?", "is_active = 1"]
    if primary_class_interest:
        where.append("(primary_class_interest = ? OR primary_class_interest IS NULL)")
        params.append(primary_class_interest)

    sql = f"""
    SELECT template_id, intent, primary_class_interest, channel, tone, subject_template, body_template, cta_template, priority
    FROM message_templates
    WHERE {" AND ".join(where)}
    ORDER BY
      CASE WHEN primary_class_interest = ? THEN 0 ELSE 1 END,
      priority ASC
    LIMIT 1
    """
    params_with_order = params + [primary_class_interest or ""]
    conn.row_factory = sqlite3.Row
    row = conn.execute(sql, params_with_order).fetchone()
    if row:
        return dict(row)

    # Tone fallback for deterministic resiliency.
    row = conn.execute(
        """
        SELECT template_id, intent, primary_class_interest, channel, tone, subject_template, body_template, cta_template, priority
        FROM message_templates
        WHERE intent = ?
          AND channel = ?
          AND is_active = 1
        ORDER BY priority ASC
        LIMIT 1
        """,
        (intent, channel),
    ).fetchone()
    return dict(row) if row else None


def _safe_render(template: Optional[str], values: Dict[str, str]) -> str:
    if not template:
        return ""

    def repl(match: re.Match[str]) -> str:
        key = match.group(1).strip()
        return values.get(key, "")

    return re.sub(r"\{([^{}]+)\}", repl, template)


def _template_fields_used(template: Optional[Dict[str, Any]]) -> set[str]:
    if not template:
        return set()
    used: set[str] = set()
    for key in ("subject_template", "body_template", "cta_template"):
        text = str(template.get(key) or "")
        for m in re.finditer(r"\{([^{}]+)\}", text):
            used.add(m.group(1).strip())
    return used


def draft_followup_message(lead: Dict[str, Any], template: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    if not template:
        return {
            "template_id": None,
            "subject": "",
            "body": "No active template matched this lead/channel/tone.",
            "cta": "",
            "missing_personalization_fields": ["template_id"],
        }

    missing: List[str] = []
    required_fields = _template_fields_used(template)
    first_name = str(lead.get("first_name") or "").strip()
    company_name = str(lead.get("company_name") or "").strip()
    interest = str(lead.get("primary_class_interest") or "").strip()
    if "first_name" in required_fields and not first_name:
        missing.append("first_name")
    if "company_name" in required_fields and not company_name:
        missing.append("company_name")
    if "primary_class_interest" in required_fields and not interest:
        missing.append("primary_class_interest")

    if not first_name:
        first_name = "there"
    if not company_name:
        company_name = "your team"
    if not interest:
        interest = "our classes"

    values = {
        "first_name": first_name,
        "company_name": company_name,
        "primary_class_interest": interest,
    }
    return {
        "template_id": template["template_id"],
        "subject": _safe_render(template.get("subject_template"), values),
        "body": _safe_render(template.get("body_template"), values),
        "cta": _safe_render(template.get("cta_template"), values),
        "missing_personalization_fields": missing,
    }


def _format_response(
    as_of_date: str,
    lookback_days: int,
    channel: str,
    top_n: int,
    assumptions: Sequence[str],
    ranked_leads: Sequence[Dict[str, Any]],
    suppression_stats: Dict[str, Any],
) -> str:
    lines = [
        "Lead Prioritization + Follow-up Drafts",
        f"As of {as_of_date} using last {lookback_days} days; channel={channel}; requested top {top_n}.",
    ]
    if assumptions:
        lines.append("Assumptions:")
        for a in assumptions:
            lines.append(f"- {a}")
    lines.append(
        f"Suppression filter removed {suppression_stats.get('suppressed_excluded_count', 0)} lead(s)."
    )

    if not ranked_leads:
        lines.append("No eligible leads found after filters.")
        return "\n".join(lines)

    lines.append("Prioritized leads:")
    for idx, lead in enumerate(ranked_leads, start=1):
        msg = lead["draft_message"]
        lines.append(
            f"{idx}. {lead['lead_id']} | score={lead['lead_score']} | tier={lead['priority_tier']} | intent={lead['intent']} | interest={lead.get('primary_class_interest') or 'Unknown'}"
        )
        if msg.get("subject"):
            lines.append(f"   Subject: {msg['subject']}")
        lines.append(f"   Draft: {msg['body']}")
        if msg.get("cta"):
            lines.append(f"   CTA: {msg['cta']}")
        if msg.get("missing_personalization_fields"):
            lines.append(
                "   Missing personalization: "
                + ", ".join(msg["missing_personalization_fields"])
            )
    return "\n".join(lines)


def _build_final_planning_rationale(plan: Dict[str, Any], field_resolution: Dict[str, Any]) -> str:
    lookback_days = int(plan.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    channel = str(plan.get("channel", DEFAULT_CHANNEL))
    tone = str(plan.get("tone", DEFAULT_TONE))
    top_n = int(plan.get("top_n", DEFAULT_TOP_N))
    interests = plan.get("primary_class_interest")
    interest_text = "all interests" if not interests else ", ".join([str(x) for x in interests])

    def src(field_name: str) -> str:
        return str((field_resolution.get(field_name, {}) or {}).get("source", "unknown"))

    parts = [
        f"Resolved request to top_n={top_n}, lookback_days={lookback_days}, channel={channel}, tone={tone}.",
        f"Interest scope: {interest_text}.",
        (
            "Resolution provenance: "
            f"lookback_days={src('lookback_days')}, "
            f"channel={src('channel')}, "
            f"primary_class_interest={src('primary_class_interest')}."
        ),
    ]
    return " ".join(parts)


class Story3GraphState(TypedDict, total=False):
    user_query: str
    domain_context: Dict[str, Any]
    user_turn_number: int
    plan: Dict[str, Any]
    as_of_date: Optional[str]
    candidate_rows: List[Dict[str, Any]]
    scoring_rules: Dict[str, float]
    suppression_stats: Dict[str, Any]
    ranked_leads: List[Dict[str, Any]]
    field_resolution: Dict[str, Any]
    unresolved_fields: List[str]
    clarification_question: Optional[str]
    response_text: str
    follow_up_question: Optional[str]


def _plan_node(state: Story3GraphState) -> Story3GraphState:
    user_text = str(state.get("user_query", "") or "").strip()
    if not user_text:
        ask = (
            "Please provide what you want prioritized (for example: "
            "'top 10 leads for last 14 days and draft email follow-ups')."
        )
        return {"response_text": ask, "follow_up_question": ask}
    plan = _resolve_request_plan(user_text)
    domain_context = state.get("domain_context", {}) or {}
    prior_story3_state = domain_context.get(STORY3_STATE_KEY)
    plan = _merge_with_prior_plan(
        plan=plan,
        prior_story3_state=prior_story3_state if isinstance(prior_story3_state, dict) else None,
        user_text=user_text,
        user_turn_number=int(state.get("user_turn_number", 0) or 0),
    )
    if plan.get("needs_clarification") and plan.get("clarification_question"):
        ask = str(plan["clarification_question"])
        return {
            "plan": plan,
            "field_resolution": dict(plan.get("field_resolution", {})),
            "unresolved_fields": list(plan.get("unresolved_fields", [])),
            "clarification_question": ask,
            "response_text": ask,
            "follow_up_question": ask,
        }
    return {"plan": plan, "follow_up_question": None}


def _validate_plan_node(state: Story3GraphState) -> Story3GraphState:
    raw = dict(state.get("plan", {}))
    top_n = max(1, min(100, int(raw.get("top_n", DEFAULT_TOP_N))))

    assumptions = list(raw.get("assumptions", []))
    lookback_days = int(raw.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    if lookback_days not in VALID_LOOKBACKS:
        lookback_days = _closest_lookback(lookback_days)
        assumptions.append(f"Unsupported lookback normalized to {lookback_days} days.")

    channel = str(raw.get("channel", DEFAULT_CHANNEL)).strip().lower()
    if channel not in VALID_CHANNELS:
        assumptions.append(f"Unsupported channel '{channel}' detected; defaulted to {DEFAULT_CHANNEL}.")
        channel = DEFAULT_CHANNEL

    tone = str(raw.get("tone", DEFAULT_TONE)).strip().lower()
    if tone not in VALID_TONES:
        assumptions.append(f"Unsupported tone '{tone}' detected; defaulted to {DEFAULT_TONE}.")
        tone = DEFAULT_TONE

    interests = raw.get("primary_class_interest")
    if isinstance(interests, list):
        interests = _normalize_interest_values(interests)
    else:
        interests = None

    raw["top_n"] = top_n
    raw["lookback_days"] = lookback_days
    raw["channel"] = channel
    raw["tone"] = tone
    raw["primary_class_interest"] = interests
    raw["assumptions"] = assumptions
    final_field_resolution = dict(raw.get("field_resolution", state.get("field_resolution", {})))
    raw["planning_rationale"] = _build_final_planning_rationale(raw, final_field_resolution)
    return {
        "plan": raw,
        "field_resolution": final_field_resolution,
        "unresolved_fields": list(raw.get("unresolved_fields", state.get("unresolved_fields", []))),
        "clarification_question": raw.get("clarification_question", state.get("clarification_question")),
    }


def _read_signals_node(state: Story3GraphState) -> Story3GraphState:
    plan = state["plan"]
    with closing(sqlite3.connect(DB_PATH)) as conn:
        as_of_date = _read_as_of_date(conn)
        if not as_of_date:
            msg = "No lead engagement signal data is available yet."
            return {
                "as_of_date": None,
                "candidate_rows": [],
                "response_text": msg,
                "suppression_stats": {"candidate_count": 0, "suppressed_excluded_count": 0},
                "scoring_rules": {},
                "ranked_leads": [],
            }

        filters = {"primary_class_interest": plan.get("primary_class_interest")}
        candidate_rows = read_lead_signals(
            conn,
            filters=filters,
            recency_window=int(plan["lookback_days"]),
            as_of_date=as_of_date,
        )
        scoring_rules = _read_scoring_rules(conn)
        return {
            "as_of_date": as_of_date,
            "candidate_rows": candidate_rows,
            "scoring_rules": scoring_rules,
        }


def _score_node(state: Story3GraphState) -> Story3GraphState:
    plan = state["plan"]
    candidate_rows = state.get("candidate_rows", [])
    if state.get("response_text"):
        return {}
    with closing(sqlite3.connect(DB_PATH)) as conn:
        ranked, suppression_stats = score_and_rank_leads(
            conn,
            data=candidate_rows,
            scoring_rules=state.get("scoring_rules", {}),
            channel=str(plan["channel"]),
            top_n=int(plan["top_n"]),
        )
    return {"ranked_leads": ranked, "suppression_stats": suppression_stats}


def _enrich_drafts_node(state: Story3GraphState) -> Story3GraphState:
    ranked = list(state.get("ranked_leads", []))
    if not ranked:
        return {}
    plan = state["plan"]
    with closing(sqlite3.connect(DB_PATH)) as conn:
        for lead in ranked:
            intent = infer_intent_from_signals(conn, lead)
            template = select_message_template(
                conn,
                intent=intent,
                primary_class_interest=lead.get("primary_class_interest"),
                channel=str(plan["channel"]),
                tone=str(plan["tone"]),
            )
            draft = draft_followup_message(lead, template)
            lead["intent"] = intent
            lead["template_id"] = draft.get("template_id")
            lead["draft_message"] = draft
    return {"ranked_leads": ranked}


def _format_response_node(state: Story3GraphState) -> Story3GraphState:
    if state.get("response_text"):
        return {}
    plan = state["plan"]
    as_of_date = state.get("as_of_date")
    if not as_of_date:
        msg = "No lead engagement signal data is available yet."
        return {"response_text": msg}
    response_text = _format_response(
        as_of_date=as_of_date,
        lookback_days=int(plan["lookback_days"]),
        channel=str(plan["channel"]),
        top_n=int(plan["top_n"]),
        assumptions=plan.get("assumptions", []),
        ranked_leads=state.get("ranked_leads", []),
        suppression_stats=state.get("suppression_stats", {}),
    )
    return {"response_text": response_text}


def _route_after_plan(state: Story3GraphState) -> str:
    return "ask" if state.get("follow_up_question") else "go"


def _route_after_read(state: Story3GraphState) -> str:
    return "done" if state.get("response_text") else "go"


def _route_after_score(state: Story3GraphState) -> str:
    return "format" if not state.get("ranked_leads") else "enrich"


def _build_story3_graph():
    g = StateGraph(Story3GraphState)
    g.add_node("plan", _plan_node)
    g.add_node("validate_plan", _validate_plan_node)
    g.add_node("read_lead_signals", _read_signals_node)
    g.add_node("score_rank", _score_node)
    g.add_node("enrich_drafts", _enrich_drafts_node)
    g.add_node("format_response", _format_response_node)
    g.set_entry_point("plan")
    g.add_conditional_edges("plan", _route_after_plan, {"ask": END, "go": "validate_plan"})
    g.add_edge("validate_plan", "read_lead_signals")
    g.add_conditional_edges("read_lead_signals", _route_after_read, {"done": END, "go": "score_rank"})
    g.add_conditional_edges("score_rank", _route_after_score, {"format": "format_response", "enrich": "enrich_drafts"})
    g.add_edge("enrich_drafts", "format_response")
    g.add_edge("format_response", END)
    return g.compile()


def _get_business_marketing_story3_graph():
    global BUSINESS_MARKETING_STORY3_GRAPH
    if BUSINESS_MARKETING_STORY3_GRAPH is None:
        BUSINESS_MARKETING_STORY3_GRAPH = _build_story3_graph()
    return BUSINESS_MARKETING_STORY3_GRAPH


def run_business_marketing_story3(req: StoryRequest) -> StoryResult:
    user_turn = _user_turn_number(req.messages)
    state_out = _get_business_marketing_story3_graph().invoke(
        {
            "user_query": req.user_query or "",
            "domain_context": req.domain_context or {},
            "user_turn_number": user_turn,
        }
    )

    follow_up = state_out.get("follow_up_question")
    if follow_up:
        return StoryResult(
            story_id=req.story_id,
            response_text=follow_up,
            story_output={
                "needs_request_details": True,
                "field_resolution": dict(state_out.get("field_resolution", {})),
                "unresolved_fields": list(state_out.get("unresolved_fields", [])),
                "clarification_question": state_out.get("clarification_question"),
            },
            state_updates_domain={
                STORY3_STATE_KEY: {
                    "last_user_turn_number": user_turn,
                    "last_user_query": req.user_query,
                    "last_partial_plan": dict(state_out.get("plan", {})),
                }
            },
        )

    plan = dict(state_out.get("plan", {}))
    ranked = list(state_out.get("ranked_leads", []))
    suppression_stats = dict(state_out.get("suppression_stats", {}))
    scoring_rules = dict(state_out.get("scoring_rules", {}))
    as_of_date = state_out.get("as_of_date")
    response_text = state_out.get("response_text") or "I can prioritize leads and draft outreach follow-ups."
    lookback_days = int(plan.get("lookback_days", DEFAULT_LOOKBACK_DAYS))
    channel = str(plan.get("channel", DEFAULT_CHANNEL))
    top_n = int(plan.get("top_n", DEFAULT_TOP_N))
    if top_n > 0 and len(ranked) > top_n:
        # Defensive invariant: keep payload aligned with requested top_n.
        ranked = ranked[:top_n]

    return StoryResult(
        story_id=req.story_id,
        response_text=response_text,
        story_output={
            "as_of_date": as_of_date,
            "lookback_days": lookback_days,
            "channel": channel,
            "tone": str(plan.get("tone", DEFAULT_TONE)),
            "top_n": top_n,
            "filters": {"primary_class_interest": plan.get("primary_class_interest")},
            "assumptions": list(plan.get("assumptions", [])),
            "planning_source": str(plan.get("planning_source", "deterministic")),
            "planning_rationale": str(plan.get("planning_rationale", "")),
            "field_resolution": dict(state_out.get("field_resolution", plan.get("field_resolution", {}))),
            "unresolved_fields": list(state_out.get("unresolved_fields", plan.get("unresolved_fields", []))),
            "scoring_rules": scoring_rules,
            "candidate_count": suppression_stats.get("candidate_count", 0),
            "suppressed_excluded_count": suppression_stats.get("suppressed_excluded_count", 0),
            "ranked_leads": ranked,
            "generated_on": date.today().isoformat(),
        },
        state_updates_domain={
            "last_story_summary": (
                f"Generated {len(ranked)} prioritized leads "
                f"for as_of={as_of_date}, lookback={lookback_days}, channel={channel}."
            ),
            STORY3_STATE_KEY: {
                "last_user_turn_number": user_turn,
                "last_user_query": req.user_query,
                "last_resolved_plan": {
                    "lookback_days": lookback_days,
                    "channel": channel,
                    "tone": str(plan.get("tone", DEFAULT_TONE)),
                    "top_n": int(plan.get("top_n", DEFAULT_TOP_N)),
                    "primary_class_interest": plan.get("primary_class_interest"),
                },
                "field_resolution": dict(state_out.get("field_resolution", plan.get("field_resolution", {}))),
            },
        },
    )


def get_business_marketing_story3_mermaid() -> str:
    return _get_business_marketing_story3_graph().get_graph().draw_mermaid()


def get_business_marketing_story3_mermaid_png() -> bytes:
    graph = _get_business_marketing_story3_graph().get_graph()
    try:
        return graph.draw_mermaid_png(draw_method=MermaidDrawMethod.PYPPETEER)
    except Exception:
        return graph.draw_mermaid_png()
