from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, TypedDict

from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from ..contracts import StoryRequest, StoryResult
from ..utils import extract_explicit_member_id, member_id_aliases, normalize_member_id, register_sqlite_alnum_normalizer

PROJECT_PHASE_3 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_3 / "kb" / "MembershipFraud" / "membership_fraud.db"

Timeframe = Literal["recent_monthly_avg", "last_3_months", "last_6_months"]

FEATURE_RANK = {
    "low": 1,
    "medium": 2,
    "high": 3,
}

LLM_ACCEPT_CONFIDENCE = 0.65
LLM_HYBRID_MIN_CONFIDENCE = 0.5


class LLMEvidencePlan(BaseModel):
    gather_more_evidence: bool = Field(default=False)
    target_timeframe: Timeframe = Field(default="recent_monthly_avg")
    run_volatility_check: bool = Field(default=False)
    run_feature_breakdown: bool = Field(default=False)
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    rationale: str = Field(default="")


class LLMRationaleOutput(BaseModel):
    summary: str = Field(default="")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    tone: Literal["neutral", "cautious"] = Field(default="neutral")
    guardrail_ok: bool = Field(default=True)
    guardrail_notes: str = Field(default="")


class TierFitState(TypedDict, total=False):
    user_text: str
    member_id: Optional[str]
    timeframe: Optional[Timeframe]
    plan: Dict[str, Any]
    member_usage: Dict[str, Any]
    analysis_usage: Dict[str, Any]
    secondary_usage: Dict[str, Any]
    tier_definitions: List[Dict[str, Any]]
    evidence_plan: Dict[str, Any]
    additional_evidence: Dict[str, Any]
    evaluation: Dict[str, Any]
    options: Dict[str, Any]
    llm_rationale: Dict[str, Any]
    response_text: str
    follow_up_question: Optional[str]


def _dedupe_preserve_order(values: List[str]) -> List[str]:
    seen: set[str] = set()
    out: List[str] = []
    for raw in values:
        v = str(raw or "").strip()
        if not v:
            continue
        key = v.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(v)
    return out


def _planner_contradicts_deterministic(det: Dict[str, Any], llm_plan: Dict[str, Any]) -> bool:
    if bool(det.get("run_volatility_check")) and not bool(llm_plan.get("run_volatility_check")):
        return True
    if bool(det.get("run_feature_breakdown")) and not bool(llm_plan.get("run_feature_breakdown")):
        return True
    det_target = str(det.get("target_timeframe") or "")
    llm_target = str(llm_plan.get("target_timeframe") or "")
    if det_target == "last_6_months" and llm_target and llm_target != "last_6_months":
        return True
    if bool(det.get("gather_more_evidence")) and not bool(llm_plan.get("gather_more_evidence")):
        return True
    return False


def _rationale_contradicts_decision(summary: str, decision: str) -> bool:
    text = (summary or "").lower()
    dec = (decision or "").lower()
    has_upgrade = "upgrade" in text
    has_downgrade = "downgrade" in text
    has_keep = any(k in text for k in ["keep", "no change", "stay"])
    if dec == "upgrade" and has_downgrade:
        return True
    if dec == "downgrade" and has_upgrade:
        return True
    if dec == "no_change" and (has_upgrade or has_downgrade) and not has_keep:
        return True
    return False


def _infer_timeframe(user_text: str) -> Timeframe:
    tl = (user_text or "").lower()
    if any(k in tl for k in ["last 6", "past 6", "6 month", "six month", "half year"]):
        return "last_6_months"
    if any(k in tl for k in ["last 3", "past 3", "3 month", "three month", "quarter"]):
        return "last_3_months"
    return "recent_monthly_avg"


def _read_member_tier_usage(member_id: str, timeframe: Timeframe) -> Dict[str, Any]:
    aliases = member_id_aliases(member_id)
    if not aliases:
        return {}
    placeholders = ",".join(["?"] * len(aliases))

    sql = f"""
    SELECT
      member_id,
      current_tier,
      avg_monthly_classes,
      tier_class_limit,
      feature_usage_level,
      tier_utilization_pct,
      recommended_tier,
      interpretation_summary,
      avg_monthly_classes_3mo,
      avg_monthly_classes_6mo,
      tier_utilization_pct_3mo,
      tier_utilization_pct_6mo,
      classes_stddev_6mo,
      months_observed,
      data_quality,
      volatility_index
    FROM membership_tier_optimization
    WHERE NORM_ALNUM(member_id) IN ({placeholders})
    LIMIT 1
    """

    with closing(sqlite3.connect(DB_PATH)) as conn:
        register_sqlite_alnum_normalizer(conn)
        conn.row_factory = sqlite3.Row
        row = conn.execute(sql, aliases).fetchone()
        if not row:
            return {}
        usage = dict(row)

    if timeframe == "last_3_months":
        usage["avg_monthly_classes_selected"] = float(
            usage.get("avg_monthly_classes_3mo") or usage.get("avg_monthly_classes") or 0.0
        )
        usage["tier_utilization_pct_selected"] = float(
            usage.get("tier_utilization_pct_3mo") or usage.get("tier_utilization_pct") or 0.0
        )
    elif timeframe == "last_6_months":
        usage["avg_monthly_classes_selected"] = float(
            usage.get("avg_monthly_classes_6mo") or usage.get("avg_monthly_classes") or 0.0
        )
        usage["tier_utilization_pct_selected"] = float(
            usage.get("tier_utilization_pct_6mo") or usage.get("tier_utilization_pct") or 0.0
        )
    else:
        usage["avg_monthly_classes_selected"] = float(usage.get("avg_monthly_classes") or 0.0)
        usage["tier_utilization_pct_selected"] = float(usage.get("tier_utilization_pct") or 0.0)
    return usage


def _read_tier_definitions() -> List[Dict[str, Any]]:
    sql = """
    SELECT tier, included_monthly_classes, feature_access, intended_user_profile
    FROM membership_tier_definitions
    ORDER BY included_monthly_classes ASC
    """
    with closing(sqlite3.connect(DB_PATH)) as conn:
        conn.row_factory = sqlite3.Row
        rows = conn.execute(sql).fetchall()
    return [dict(r) for r in rows]


def _feature_rank_from_access(feature_access: str) -> int:
    value = (feature_access or "").lower()
    if "all_access" in value or "all access" in value or "elite" in value:
        return 3
    if "premium" in value or "guest" in value or "plus" in value:
        return 2
    return 1


def _derive_feature_breakdown(feature_level: str) -> Dict[str, Any]:
    level = (feature_level or "medium").lower()
    if level == "high":
        return {
            "premium_content_usage": "high",
            "guest_pass_usage": "medium",
            "advanced_program_usage": "high",
            "notes": "Frequent use of advanced and premium member benefits.",
        }
    if level == "low":
        return {
            "premium_content_usage": "low",
            "guest_pass_usage": "low",
            "advanced_program_usage": "low",
            "notes": "Benefit usage is concentrated in core classes.",
        }
    return {
        "premium_content_usage": "medium",
        "guest_pass_usage": "low",
        "advanced_program_usage": "medium",
        "notes": "Mix of core and selective premium benefit usage.",
    }


def _evaluate_tier_fit(member_usage: Dict[str, Any], tier_defs: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not member_usage or not tier_defs:
        return {
            "recommended_tier": member_usage.get("current_tier"),
            "decision": "no_change",
            "confidence": 0.0,
            "uncertain": True,
            "reason": "Missing member usage or tier definition data.",
            "evidence": [],
        }

    current = str(member_usage.get("current_tier") or "")
    avg_classes = float(member_usage.get("avg_monthly_classes_selected") or 0.0)
    utilization = float(member_usage.get("tier_utilization_pct_selected") or 0.0)
    feature_level = str(member_usage.get("feature_usage_level") or "medium").lower()
    feature_need_rank = FEATURE_RANK.get(feature_level, 2)
    std_6mo = float(member_usage.get("classes_stddev_6mo") or 0.0)
    avg_6mo = float(member_usage.get("avg_monthly_classes_6mo") or avg_classes or 0.0)
    months_observed = int(member_usage.get("months_observed") or 0)
    data_quality = str(member_usage.get("data_quality") or "").lower()

    ordered = sorted(tier_defs, key=lambda r: int(r.get("included_monthly_classes") or 0))
    index = {str(t.get("tier")): i for i, t in enumerate(ordered)}
    if current not in index:
        return {
            "recommended_tier": current,
            "decision": "no_change",
            "confidence": 0.0,
            "uncertain": True,
            "reason": "Current tier is not present in tier definitions.",
            "evidence": [],
        }

    curr_i = index[current]
    curr_tier = ordered[curr_i]
    curr_limit = int(curr_tier.get("included_monthly_classes") or member_usage.get("tier_class_limit") or 0)
    curr_feature_rank = _feature_rank_from_access(str(curr_tier.get("feature_access") or ""))
    volatility_index = float(member_usage.get("volatility_index") or (std_6mo / max(avg_6mo, 1.0)))

    insufficient = months_observed < 3 or data_quality == "low"
    volatile = volatility_index >= 0.45 or std_6mo >= 6.0

    evidence = [
        f"Selected timeframe utilization={utilization:.1f}%",
        f"Selected timeframe avg_monthly_classes={avg_classes:.1f} vs class_limit={curr_limit}",
        f"Feature usage level={feature_level}",
        f"6-month volatility_index={volatility_index:.2f}",
    ]

    if insufficient or volatile:
        reason = "Usage evidence is insufficient or too volatile; defaulting to no change."
        return {
            "recommended_tier": current,
            "decision": "no_change",
            "confidence": 0.45 if insufficient else 0.5,
            "uncertain": True,
            "reason": reason,
            "evidence": evidence,
        }

    recommended_i = curr_i

    # Feature mismatch can force at least one tier up.
    if feature_need_rank > curr_feature_rank and curr_i < len(ordered) - 1:
        for j in range(curr_i + 1, len(ordered)):
            if _feature_rank_from_access(str(ordered[j].get("feature_access") or "")) >= feature_need_rank:
                recommended_i = j
                break

    # Class-limit pressure suggests upgrade.
    if utilization >= 90.0 and curr_i < len(ordered) - 1:
        recommended_i = max(recommended_i, curr_i + 1)

    # Sustained under-utilization can suggest downgrade.
    if utilization <= 55.0 and avg_classes <= 0.65 * curr_limit and curr_i > 0:
        candidate_i = curr_i - 1
        candidate_limit = int(ordered[candidate_i].get("included_monthly_classes") or 0)
        if avg_classes <= 0.9 * candidate_limit:
            recommended_i = min(recommended_i, candidate_i)

    recommended = str(ordered[recommended_i].get("tier"))
    decision = "no_change"
    confidence = 0.74
    if recommended_i > curr_i:
        decision = "upgrade"
        confidence = 0.82
    elif recommended_i < curr_i:
        decision = "downgrade"
        confidence = 0.8

    return {
        "recommended_tier": recommended,
        "decision": decision,
        "confidence": confidence,
        "uncertain": False,
        "reason": "Recommendation is grounded in utilization and feature fit.",
        "evidence": evidence,
    }


def _explain_tier_options(
    current: str,
    recommended: str,
    tier_defs: List[Dict[str, Any]],
    evaluation: Dict[str, Any],
) -> Dict[str, Any]:
    tier_map = {str(t.get("tier")): t for t in tier_defs}
    current_row = tier_map.get(current, {})
    rec_row = tier_map.get(recommended, {})
    decision = str(evaluation.get("decision") or "no_change")

    lines = []
    lines.append(f"Current tier: {current}")
    lines.append(
        f"Current fit: up to {current_row.get('included_monthly_classes', 'n/a')} classes/month, "
        f"feature access={current_row.get('feature_access', 'n/a')}."
    )

    if decision == "upgrade":
        lines.append(
            f"Recommended change: upgrade to {recommended} for additional class capacity and/or feature access "
            "based on your observed usage."
        )
    elif decision == "downgrade":
        lines.append(
            f"Recommended change: downgrade to {recommended} because your sustained usage appears below "
            "your current tier limits."
        )
    else:
        lines.append("Recommended change: keep your current tier for now.")

    lines.append(
        f"Comparison: {recommended} includes up to {rec_row.get('included_monthly_classes', 'n/a')} classes/month "
        f"with feature access={rec_row.get('feature_access', 'n/a')}."
    )
    lines.append("Guardrail: this guidance is usage-based and does not include pricing claims.")

    return {
        "summary": " ".join(lines),
        "current_tier_details": current_row,
        "recommended_tier_details": rec_row,
        "decision": decision,
    }


def _planner_node(state: TierFitState) -> TierFitState:
    user_text = state.get("user_text", "")
    existing_member_id = state.get("member_id")
    existing_timeframe = state.get("timeframe")
    member_id = normalize_member_id(extract_explicit_member_id(user_text) or existing_member_id)
    timeframe = _infer_timeframe(user_text) if user_text else (existing_timeframe or "recent_monthly_avg")
    ask_for_member = member_id is None

    return {
        "member_id": member_id,
        "timeframe": timeframe,
        "plan": {
            "member_id": member_id,
            "timeframe": timeframe,
            "ask_for_member_id": ask_for_member,
        },
        "follow_up_question": (
            "I can evaluate your tier fit, what is your member_id (e.g., MB001)?"
            if ask_for_member
            else None
        ),
    }


def _retrieve_member_usage_node(state: TierFitState) -> TierFitState:
    member_id = state.get("member_id")
    timeframe = state.get("timeframe") or "recent_monthly_avg"
    if not member_id:
        return {}
    usage = _read_member_tier_usage(member_id=member_id, timeframe=timeframe)
    return {"member_usage": usage}


def _retrieve_tier_defs_node(state: TierFitState) -> TierFitState:
    return {"tier_definitions": _read_tier_definitions()}


def _evidence_planner_node(state: TierFitState) -> TierFitState:
    usage = state.get("member_usage") or {}
    timeframe = state.get("timeframe") or "recent_monthly_avg"
    user_text = (state.get("user_text") or "").lower()
    member_id = state.get("member_id")

    if not member_id or not usage:
        return {
            "evidence_plan": {
                "gather_more_evidence": False,
                "target_timeframe": timeframe,
                "run_volatility_check": False,
                "run_feature_breakdown": False,
                "reasons": ["Member data not available for expanded evidence gathering."],
            }
        }

    util = float(usage.get("tier_utilization_pct_selected") or 0.0)
    std_6mo = float(usage.get("classes_stddev_6mo") or 0.0)
    volatility_index = float(usage.get("volatility_index") or 0.0)
    months_observed = int(usage.get("months_observed") or 0)
    feature_level = str(usage.get("feature_usage_level") or "medium").lower()

    near_boundary = (85.0 <= util <= 98.0) or (45.0 <= util <= 60.0)
    sparse_history = months_observed < 6
    volatile = volatility_index >= 0.35 or std_6mo >= 4.5
    asks_features = any(k in user_text for k in ["feature", "benefit", "perk", "access"])

    need_longer_timeframe = timeframe != "last_6_months" and (near_boundary or sparse_history or volatile)
    run_volatility_check = volatile or near_boundary
    run_feature_breakdown = asks_features or feature_level in {"medium", "high"}
    gather_more = need_longer_timeframe or run_volatility_check or run_feature_breakdown

    reasons: List[str] = []
    if near_boundary:
        reasons.append("Utilization is near an upgrade/downgrade decision boundary.")
    if sparse_history:
        reasons.append("Observed history is shorter than six months.")
    if volatile:
        reasons.append("Usage volatility is elevated.")
    if run_feature_breakdown:
        reasons.append("Feature-level evidence can improve recommendation quality.")
    if not reasons:
        reasons.append("Current evidence is sufficient for recommendation.")

    return {
        "evidence_plan": {
            "gather_more_evidence": gather_more,
            "target_timeframe": "last_6_months" if need_longer_timeframe else timeframe,
            "run_volatility_check": run_volatility_check,
            "run_feature_breakdown": run_feature_breakdown,
            "reasons": reasons,
        }
    }


def _evidence_planner_node_llm(state: TierFitState, llm: Optional[ChatOpenAI]) -> TierFitState:
    deterministic = _evidence_planner_node(state).get("evidence_plan", {})
    if llm is None:
        deterministic["planner_source"] = "deterministic_no_llm"
        return {"evidence_plan": deterministic}
    usage = state.get("member_usage") or {}
    user_text = state.get("user_text") or ""
    timeframe = state.get("timeframe") or "recent_monthly_avg"

    if not usage:
        deterministic["planner_source"] = "deterministic_no_usage"
        return {"evidence_plan": deterministic}

    system = (
        "You are an evidence planning assistant for membership tier recommendations.\n"
        "Choose whether to gather more evidence before a recommendation.\n"
        "Guardrails:\n"
        "- Keep recommendations customer-centric and neutral.\n"
        "- Prefer gathering more evidence when usage is volatile or near decision boundaries.\n"
        "- If confidence is low, fall back to deterministic planning.\n"
        "Return only structured output."
    )
    user = (
        f"USER_QUERY: {user_text}\n"
        f"CURRENT_TIMEFRAME: {timeframe}\n"
        f"CURRENT_USAGE: {usage}\n"
        f"DETERMINISTIC_PLAN: {deterministic}"
    )
    try:
        structured = llm.with_structured_output(LLMEvidencePlan)
        out = structured.invoke([("system", system), ("user", user)])
        llm_plan = out.model_dump()
        llm_conf = float(llm_plan.get("confidence", 0.0))
        if llm_conf < LLM_HYBRID_MIN_CONFIDENCE:
            deterministic["planner_source"] = "deterministic_low_llm_confidence"
            deterministic["planner_confidence"] = round(llm_conf, 2)
            return {"evidence_plan": deterministic}
        if _planner_contradicts_deterministic(deterministic, llm_plan):
            deterministic["planner_source"] = "deterministic_llm_contradiction"
            deterministic["planner_confidence"] = round(llm_conf, 2)
            return {"evidence_plan": deterministic}

        merged = {
            # Hybrid rule: LLM can add coverage but cannot weaken deterministic safety checks.
            "gather_more_evidence": bool(llm_plan.get("gather_more_evidence")) or bool(
                deterministic.get("gather_more_evidence")
            ),
            "target_timeframe": (
                "last_6_months"
                if str(deterministic.get("target_timeframe")) == "last_6_months"
                else (llm_plan.get("target_timeframe") or deterministic.get("target_timeframe"))
            ),
            "run_volatility_check": bool(llm_plan.get("run_volatility_check")) or bool(
                deterministic.get("run_volatility_check")
            ),
            "run_feature_breakdown": bool(
                llm_plan.get("run_feature_breakdown")
            )
            or bool(
                deterministic.get("run_feature_breakdown")
            ),
            "reasons": _dedupe_preserve_order(
                [str(llm_plan.get("rationale") or "").strip()] + list(deterministic.get("reasons") or [])
            ),
            "planner_source": "llm",
            "planner_confidence": round(llm_conf, 2),
        }
        return {"evidence_plan": merged}
    except Exception:
        deterministic["planner_source"] = "deterministic_llm_error"
        return {"evidence_plan": deterministic}


def _gather_evidence_node(state: TierFitState) -> TierFitState:
    plan = state.get("evidence_plan") or {}
    member_id = state.get("member_id")
    base_usage = dict(state.get("member_usage") or {})
    timeframe = state.get("timeframe") or "recent_monthly_avg"
    target_timeframe = str(plan.get("target_timeframe") or timeframe)

    analysis_usage = dict(base_usage)
    secondary_usage: Dict[str, Any] = {}
    additional_evidence: Dict[str, Any] = {"actions_taken": []}
    analysis_notes: List[str] = []

    if not member_id or not base_usage:
        return {"analysis_usage": analysis_usage, "additional_evidence": additional_evidence, "secondary_usage": {}}

    if target_timeframe != timeframe:
        secondary_usage = _read_member_tier_usage(member_id, target_timeframe)  # type: ignore[arg-type]
        if secondary_usage:
            analysis_usage["avg_monthly_classes_selected"] = float(
                secondary_usage.get("avg_monthly_classes_selected") or analysis_usage.get("avg_monthly_classes_selected") or 0.0
            )
            analysis_usage["tier_utilization_pct_selected"] = float(
                secondary_usage.get("tier_utilization_pct_selected")
                or analysis_usage.get("tier_utilization_pct_selected")
                or 0.0
            )
            analysis_notes.append(f"Used longer evidence window ({target_timeframe}) for recommendation.")
            additional_evidence["actions_taken"].append("extended_timeframe")

    if bool(plan.get("run_volatility_check")):
        util_now = float(base_usage.get("tier_utilization_pct_selected") or 0.0)
        util_long = float(secondary_usage.get("tier_utilization_pct_selected") or util_now)
        additional_evidence["volatility_check"] = {
            "utilization_delta_pct_points": round(util_now - util_long, 1),
            "classes_stddev_6mo": float(base_usage.get("classes_stddev_6mo") or 0.0),
            "volatility_index": float(base_usage.get("volatility_index") or 0.0),
        }
        analysis_notes.append("Performed volatility check across available windows.")
        additional_evidence["actions_taken"].append("volatility_check")

    if bool(plan.get("run_feature_breakdown")):
        breakdown = _derive_feature_breakdown(str(base_usage.get("feature_usage_level") or "medium"))
        additional_evidence["feature_breakdown"] = breakdown
        analysis_notes.append("Added feature-level benefit usage breakdown.")
        additional_evidence["actions_taken"].append("feature_breakdown")

    if analysis_notes:
        analysis_usage["analysis_notes"] = analysis_notes

    return {
        "analysis_usage": analysis_usage,
        "secondary_usage": secondary_usage,
        "additional_evidence": additional_evidence,
    }


def _evaluate_node(state: TierFitState) -> TierFitState:
    member_usage = state.get("analysis_usage") or state.get("member_usage") or {}
    tier_defs = state.get("tier_definitions") or []
    evaluation = _evaluate_tier_fit(member_usage, tier_defs)
    evidence_plan = state.get("evidence_plan") or {}
    additional_evidence = state.get("additional_evidence") or {}
    analysis_notes = member_usage.get("analysis_notes") if isinstance(member_usage, dict) else None
    if isinstance(analysis_notes, list) and analysis_notes:
        evaluation["evidence"] = (evaluation.get("evidence") or []) + [str(n) for n in analysis_notes]
    plan_reasons = evidence_plan.get("reasons")
    if isinstance(plan_reasons, list) and plan_reasons:
        evaluation["evidence"] = (evaluation.get("evidence") or []) + [f"Planner: {str(r)}" for r in plan_reasons]
    if additional_evidence.get("volatility_check"):
        vc = additional_evidence["volatility_check"]
        evaluation["evidence"] = (evaluation.get("evidence") or []) + [
            "Volatility detail: "
            f"delta={vc.get('utilization_delta_pct_points')}pp, "
            f"std_6mo={vc.get('classes_stddev_6mo')}, "
            f"vol_idx={vc.get('volatility_index')}"
        ]
    evaluation["evidence"] = _dedupe_preserve_order([str(e) for e in (evaluation.get("evidence") or [])])

    options = _explain_tier_options(
        current=str(member_usage.get("current_tier") or ""),
        recommended=str(evaluation.get("recommended_tier") or member_usage.get("current_tier") or ""),
        tier_defs=tier_defs,
        evaluation=evaluation,
    )
    return {"evaluation": evaluation, "options": options}


def _rationale_node_llm(state: TierFitState, llm: Optional[ChatOpenAI]) -> TierFitState:
    if llm is None:
        return {"llm_rationale": {"summary": "", "source": "skipped_no_llm"}}
    member_id = state.get("member_id")
    if not member_id:
        return {"llm_rationale": {"summary": "", "source": "skipped_no_member"}}

    usage = state.get("analysis_usage") or state.get("member_usage") or {}
    evaluation = state.get("evaluation") or {}
    options = state.get("options") or {}
    evidence_plan = state.get("evidence_plan") or {}
    additional_evidence = state.get("additional_evidence") or {}
    if not usage or not evaluation:
        return {"llm_rationale": {"summary": "", "source": "skipped_missing_inputs"}}

    rationale_context = {
        "selected_window": {
            "avg_monthly_classes": float(usage.get("avg_monthly_classes_selected") or 0.0),
            "tier_utilization_pct": float(usage.get("tier_utilization_pct_selected") or 0.0),
            "timeframe": str(evidence_plan.get("target_timeframe") or state.get("timeframe") or "recent_monthly_avg"),
        },
        "comparison_windows": {
            "recent_monthly_avg": {
                "avg_monthly_classes": float(usage.get("avg_monthly_classes") or 0.0),
                "tier_utilization_pct": float(usage.get("tier_utilization_pct") or 0.0),
            },
            "last_3_months": {
                "avg_monthly_classes": float(usage.get("avg_monthly_classes_3mo") or 0.0),
                "tier_utilization_pct": float(usage.get("tier_utilization_pct_3mo") or 0.0),
            },
            "last_6_months": {
                "avg_monthly_classes": float(usage.get("avg_monthly_classes_6mo") or 0.0),
                "tier_utilization_pct": float(usage.get("tier_utilization_pct_6mo") or 0.0),
            },
        },
        "uncertainty_signals": {
            "classes_stddev_6mo": float(usage.get("classes_stddev_6mo") or 0.0),
            "volatility_index": float(usage.get("volatility_index") or 0.0),
            "months_observed": int(usage.get("months_observed") or 0),
            "data_quality": str(usage.get("data_quality") or ""),
            "volatility_check": additional_evidence.get("volatility_check"),
        },
        "feature_signals": {
            "feature_usage_level": str(usage.get("feature_usage_level") or ""),
            "feature_breakdown": additional_evidence.get("feature_breakdown"),
        },
        "planner_summary": {
            "gather_more_evidence": bool(evidence_plan.get("gather_more_evidence")),
            "actions_taken": list(additional_evidence.get("actions_taken") or []),
            "reasons": list(evidence_plan.get("reasons") or []),
        },
        "decision_bundle": {
            "current_tier": str(usage.get("current_tier") or ""),
            "recommended_tier": str(evaluation.get("recommended_tier") or ""),
            "decision": str(evaluation.get("decision") or ""),
            "confidence": float(evaluation.get("confidence") or 0.0),
            "reason": str(evaluation.get("reason") or ""),
            "guardrails": {
                "no_pricing_claims": True,
                "no_aggressive_upsell": True,
                "state_uncertainty_when_needed": True,
            },
            "base_summary_template": str(options.get("summary") or ""),
        },
    }

    system = (
        "You are a customer-facing membership assistant.\n"
        "Write a short neutral explanation of the recommendation.\n"
        "Use selected_window values as the primary usage numbers.\n"
        "Use comparison_windows only as supporting context.\n"
        "Guardrails:\n"
        "- No pricing claims.\n"
        "- No aggressive upsell language.\n"
        "- If uncertainty exists, state it clearly and favor no-change framing.\n"
        "Return only structured output."
    )
    user = (
        f"MEMBER_ID: {member_id}\n"
        f"RATIONALE_CONTEXT: {rationale_context}"
    )
    try:
        structured = llm.with_structured_output(LLMRationaleOutput)
        out = structured.invoke([("system", system), ("user", user)])
        payload = out.model_dump()
        conf = float(payload.get("confidence", 0.0))
        if conf < LLM_HYBRID_MIN_CONFIDENCE:
            return {
                "llm_rationale": {
                    "summary": "",
                    "source": "skipped_low_llm_confidence",
                    "confidence": round(conf, 2),
                }
            }
        if not bool(payload.get("guardrail_ok", True)):
            return {
                "llm_rationale": {
                    "summary": "",
                    "source": "skipped_guardrail_not_ok",
                    "guardrail_notes": str(payload.get("guardrail_notes") or ""),
                }
            }
        summary = str(payload.get("summary") or "").strip()
        decision = str(evaluation.get("decision") or "")
        if _rationale_contradicts_decision(summary, decision):
            return {
                "llm_rationale": {
                    "summary": "",
                    "source": "skipped_llm_contradiction",
                    "confidence": round(conf, 2),
                }
            }
        return {
            "llm_rationale": {
                "summary": summary,
                "source": "llm",
                "confidence": round(conf, 2),
                "tone": str(payload.get("tone") or "neutral"),
                "guardrail_notes": str(payload.get("guardrail_notes") or ""),
            }
        }
    except Exception:
        return {"llm_rationale": {"summary": "", "source": "skipped_llm_error"}}


def _respond_node(state: TierFitState) -> TierFitState:
    member_id = state.get("member_id")
    follow_up = state.get("follow_up_question")
    if not member_id:
        msg = follow_up or "What is your member_id?"
        return {"response_text": msg, "follow_up_question": msg}

    usage = state.get("member_usage") or {}
    if not usage:
        msg = f"I could not find membership usage data for {member_id}, so I recommend no change for now."
        return {"response_text": msg, "follow_up_question": None}

    evaluation = state.get("evaluation") or {}
    options = state.get("options") or {}
    evidence_plan = state.get("evidence_plan") or {}
    llm_rationale = state.get("llm_rationale") or {}
    evidence = evaluation.get("evidence") or []
    evidence_text = "\n".join([f"- {e}" for e in evidence]) if evidence else "- No supporting evidence available."

    uncertainty_note = ""
    if evaluation.get("uncertain"):
        uncertainty_note = (
            "\n\nUncertainty note: usage signals were limited or volatile, so a no-change recommendation is safest."
        )

    explanation_text = str(llm_rationale.get("summary") or "").strip() or options.get("summary", "No explanation available.")

    msg = (
        f"Tier fit analysis for {member_id}:\n"
        f"- Current tier: {usage.get('current_tier')}\n"
        f"- Recommended tier: {evaluation.get('recommended_tier', usage.get('current_tier'))}\n"
        f"- Decision: {evaluation.get('decision', 'no_change')}\n"
        f"- Confidence: {float(evaluation.get('confidence', 0.0)):.2f}\n\n"
        f"Evidence:\n{evidence_text}\n\n"
        f"Evidence planner:\n- Gathered additional evidence: {'yes' if evidence_plan.get('gather_more_evidence') else 'no'}\n\n"
        f"Explanation:\n{explanation_text}"
        f"{uncertainty_note}"
    )
    return {"response_text": msg, "follow_up_question": None}


def _build_story_graph():
    llm: Optional[ChatOpenAI] = None
    try:
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    except Exception:
        llm = None
    builder = StateGraph(TierFitState)

    def route_after_plan(state: TierFitState) -> str:
        return "ask" if state.get("follow_up_question") and not state.get("member_id") else "go"

    def route_after_evidence_plan(state: TierFitState) -> str:
        plan = state.get("evidence_plan") or {}
        return "gather" if bool(plan.get("gather_more_evidence")) else "skip"

    builder.add_node("plan", _planner_node)
    builder.add_node("retrieve_member_usage", _retrieve_member_usage_node)
    builder.add_node("retrieve_tier_defs", _retrieve_tier_defs_node)
    builder.add_node("evidence_plan", lambda s: _evidence_planner_node_llm(s, llm))
    builder.add_node("gather_evidence", _gather_evidence_node)
    builder.add_node("evaluate", _evaluate_node)
    builder.add_node("rationale", lambda s: _rationale_node_llm(s, llm))
    builder.add_node("respond", _respond_node)
    builder.set_entry_point("plan")
    builder.add_conditional_edges("plan", route_after_plan, {"ask": "respond", "go": "retrieve_member_usage"})
    builder.add_edge("retrieve_member_usage", "retrieve_tier_defs")
    builder.add_edge("retrieve_tier_defs", "evidence_plan")
    builder.add_conditional_edges("evidence_plan", route_after_evidence_plan, {"gather": "gather_evidence", "skip": "evaluate"})
    builder.add_edge("gather_evidence", "evaluate")
    builder.add_edge("evaluate", "rationale")
    builder.add_edge("rationale", "respond")
    builder.add_edge("respond", END)
    return builder.compile()


TIER_FIT_GRAPH = None


def _get_tier_fit_graph():
    global TIER_FIT_GRAPH
    if TIER_FIT_GRAPH is None:
        TIER_FIT_GRAPH = _build_story_graph()
    return TIER_FIT_GRAPH


def get_membership_fraud_story3_mermaid() -> str:
    return _get_tier_fit_graph().get_graph().draw_mermaid()


def run_membership_fraud_story3(req: StoryRequest) -> StoryResult:
    member_from_query = extract_explicit_member_id(req.user_query)
    member_id = normalize_member_id(member_from_query or req.member.member_id or req.domain_context.get("member_id"))

    context_timeframe = req.domain_context.get("tier_timeframe")
    valid_timeframes = {"recent_monthly_avg", "last_3_months", "last_6_months"}
    timeframe = context_timeframe if context_timeframe in valid_timeframes else None

    state_in: TierFitState = {
        "user_text": req.user_query,
        "member_id": member_id,
        "timeframe": timeframe,
    }
    state_out = _get_tier_fit_graph().invoke(state_in)

    response_text = state_out.get("response_text") or "I can help evaluate whether your membership tier is a good fit."
    final_member = state_out.get("member_id") or member_id
    final_timeframe = state_out.get("timeframe") or _infer_timeframe(req.user_query)
    follow_up = state_out.get("follow_up_question")
    member_usage = state_out.get("member_usage") or {}
    analysis_usage = state_out.get("analysis_usage") or member_usage
    secondary_usage = state_out.get("secondary_usage") or {}
    evidence_plan = state_out.get("evidence_plan") or {}
    additional_evidence = state_out.get("additional_evidence") or {}
    tier_defs = state_out.get("tier_definitions") or []
    evaluation = state_out.get("evaluation") or {}
    options = state_out.get("options") or {}
    llm_rationale = state_out.get("llm_rationale") or {}
    analysis_timeframe_used = str(evidence_plan.get("target_timeframe") or final_timeframe)

    story_output = {
        "member_id": final_member,
        "timeframe": final_timeframe,
        "plan": state_out.get("plan", {}),
        "member_usage": member_usage,
        "analysis_usage": analysis_usage,
        "secondary_usage": secondary_usage,
        "tier_definitions": tier_defs,
        "evidence_plan": evidence_plan,
        "additional_evidence": additional_evidence,
        "analysis_timeframe_used": analysis_timeframe_used,
        "evaluation": evaluation,
        "options": options,
        "llm_rationale": llm_rationale,
        "recommended_tier": evaluation.get("recommended_tier", member_usage.get("current_tier")),
        "decision": evaluation.get("decision", "no_change"),
        "guardrails": {
            "pricing_claims_enabled": False,
            "aggressive_upsell_enabled": False,
            "insufficient_or_volatile_defaults_to_no_change": True,
        },
    }
    if follow_up and not final_member:
        story_output["requested_slot"] = "member_id"
        story_output["missing_slots"] = ["member_id"]
        story_output["needs_member_id"] = True

    return StoryResult(
        story_id=req.story_id,
        response_text=response_text,
        follow_up_question=follow_up,
        story_output=story_output,
        state_updates_global={"member": {"member_id": final_member}} if final_member else {},
        state_updates_domain={
            "member_id": final_member,
            "tier_timeframe": final_timeframe,
            "last_tier_recommendation": story_output.get("recommended_tier"),
            "last_story_summary": (
                "Evaluated membership tier fit with usage and feature signals; "
                f"decision={story_output.get('decision')}."
            ),
        },
    )
