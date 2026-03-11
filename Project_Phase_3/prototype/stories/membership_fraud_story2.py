from __future__ import annotations

import re
from typing import Any, Dict, List, Literal

from langchain_openai import ChatOpenAI
from pydantic import BaseModel, Field

from ..contracts import StoryRequest, StoryResult

IssueCategory = Literal["login", "billing", "renewal", "unknown"]

ISSUE_CATEGORIES: tuple[IssueCategory, ...] = ("login", "billing", "renewal", "unknown")
CLASSIFY_CONFIDENCE_THRESHOLD = 0.75
LLM_ACCEPT_THRESHOLD = 0.65

QUEUE_MAP = {
    "login": "membership_support_login_queue",
    "billing": "membership_support_billing_queue",
    "renewal": "membership_support_renewal_queue",
    "unknown": "membership_support_human_review_queue",
}

_CATEGORY_PATTERNS: Dict[str, List[str]] = {
    "login": [
        r"\blog[\s-]?in\b",
        r"\bsign[\s-]?in\b",
        r"\bpassword\b",
        r"\breset password\b",
        r"\bforgot password\b",
        r"\blocked out\b",
        r"\bcan(?:not|'t)\s+(?:access|log|sign)\b",
        r"\bverification code\b",
        r"\bmfa\b",
        r"\b2fa\b",
    ],
    "billing": [
        r"\bbilling\b",
        r"\bbill\b",
        r"\binvoice\b",
        r"\bcharged?\b",
        r"\bovercharged?\b",
        r"\brefund\b",
        r"\bpayment\b",
        r"\bcredit card\b",
        r"\bdebit card\b",
        r"\btransaction\b",
    ],
    "renewal": [
        r"\brenew(?:al)?\b",
        r"\bauto[\s-]?renew\b",
        r"\bsubscription\b",
        r"\bextend membership\b",
        r"\bexpiration\b",
        r"\bexpired\b",
        r"\bcancel(?:lation)?\b",
        r"\bplan term\b",
        r"\bnext cycle\b",
    ],
}


class IssueClassifierOutput(BaseModel):
    issue_category: IssueCategory = Field(description="One of: login, billing, renewal, unknown")
    issue_description: str = Field(default="")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    rationale: str = Field(default="")


def _compact_description(user_text: str, max_len: int = 140) -> str:
    cleaned = re.sub(r"\s+", " ", (user_text or "").strip())
    if not cleaned:
        return "No issue details were provided."
    if len(cleaned) <= max_len:
        return cleaned
    return f"{cleaned[: max_len - 3].rstrip()}..."


def _score_categories(user_text: str) -> Dict[str, int]:
    text = (user_text or "").lower()
    scores = {"login": 0, "billing": 0, "renewal": 0}
    for category, patterns in _CATEGORY_PATTERNS.items():
        for pattern in patterns:
            if re.search(pattern, text):
                scores[category] += 1
    return scores


def _classify_issue_deterministic(user_text: str) -> Dict[str, Any]:
    scores = _score_categories(user_text)
    ranked = sorted(scores.items(), key=lambda kv: kv[1], reverse=True)
    top_category, top_score = ranked[0]
    second_score = ranked[1][1]
    margin = top_score - second_score

    if top_score <= 0:
        return {
            "issue_category": "unknown",
            "issue_description": _compact_description(user_text),
            "confidence": 0.35,
            "classification_source": "deterministic",
            "classification_rationale": "No strong category keywords found.",
            "category_scores": scores,
        }

    if margin <= 0:
        return {
            "issue_category": "unknown",
            "issue_description": _compact_description(user_text),
            "confidence": 0.5,
            "classification_source": "deterministic",
            "classification_rationale": "Multiple categories scored equally; issue is ambiguous.",
            "category_scores": scores,
        }

    if top_score >= 3 and margin >= 2:
        confidence = 0.9
    elif top_score >= 2 and margin >= 1:
        confidence = 0.82
    else:
        confidence = 0.72

    return {
        "issue_category": top_category,
        "issue_description": _compact_description(user_text),
        "confidence": confidence,
        "classification_source": "deterministic",
        "classification_rationale": f"Keyword match score selected '{top_category}' (top={top_score}, margin={margin}).",
        "category_scores": scores,
    }


def _resolve_issue_with_llm(user_text: str, base: Dict[str, Any]) -> Dict[str, Any]:
    system = (
        "You are an issue classifier for member support triage.\n"
        "Classify the user issue into exactly one category: login, billing, renewal, unknown.\n"
        "Use 'unknown' only if category is not clear.\n"
        "Provide a concise issue_description and confidence from 0.0 to 1.0.\n"
        "Output only structured data."
    )
    user = (
        f"USER_QUERY: {user_text}\n"
        f"DETERMINISTIC_BASE_CATEGORY: {base.get('issue_category')}\n"
        f"DETERMINISTIC_BASE_CONFIDENCE: {base.get('confidence')}\n"
        f"DETERMINISTIC_BASE_SCORES: {base.get('category_scores')}"
    )
    try:
        llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        structured = llm.with_structured_output(IssueClassifierOutput)
        out = structured.invoke([("system", system), ("user", user)])
        candidate = out.issue_category if out.issue_category in ISSUE_CATEGORIES else "unknown"
        conf = float(out.confidence)
        if conf < LLM_ACCEPT_THRESHOLD:
            return base
        return {
            "issue_category": candidate,
            "issue_description": _compact_description(out.issue_description or user_text),
            "confidence": round(conf, 2),
            "classification_source": "llm",
            "classification_rationale": out.rationale or "LLM selected issue category.",
            "category_scores": base.get("category_scores", {}),
        }
    except Exception:
        return base


def _select_final_classification(user_text: str) -> Dict[str, Any]:
    base = _classify_issue_deterministic(user_text)
    needs_resolution = (
        base.get("issue_category") == "unknown"
        or float(base.get("confidence", 0.0)) < CLASSIFY_CONFIDENCE_THRESHOLD
    )
    if not needs_resolution:
        return base

    resolved = _resolve_issue_with_llm(user_text, base)
    # Keep deterministic output when LLM returns unknown but deterministic had a concrete label.
    if resolved.get("issue_category") == "unknown" and base.get("issue_category") != "unknown":
        return base
    return resolved


def _route_queue(issue_category: str, confidence: float) -> tuple[str, bool]:
    if issue_category == "unknown" or confidence < CLASSIFY_CONFIDENCE_THRESHOLD:
        return QUEUE_MAP["unknown"], True
    return QUEUE_MAP.get(issue_category, QUEUE_MAP["unknown"]), False


def _format_response(payload: Dict[str, Any]) -> str:
    return (
        "Issue triage result:\n"
        f"- Category: {payload.get('issue_category')}\n"
        f"- Description: {payload.get('issue_description')}\n"
        f"- Confidence: {float(payload.get('classification_confidence', 0.0)):.2f}\n"
        f"- Routed queue: {payload.get('routing_queue')}\n"
        f"- Human review required: {'yes' if payload.get('requires_human_review') else 'no'}"
    )


def run_membership_fraud_story2(req: StoryRequest) -> StoryResult:
    selected = _select_final_classification(req.user_query)
    category = str(selected.get("issue_category", "unknown"))
    confidence = float(selected.get("confidence", 0.0))
    queue_name, human_review = _route_queue(category, confidence)

    story_output = {
        "issue_category": category,
        "issue_description": selected.get("issue_description") or _compact_description(req.user_query),
        "classification_confidence": round(confidence, 2),
        "routing_queue": queue_name,
        "requires_human_review": human_review,
        "classification_source": selected.get("classification_source", "fallback"),
        "classification_rationale": selected.get("classification_rationale", "No rationale available."),
        "category_scores": selected.get("category_scores", {}),
        "guardrails": {
            "auto_resolve_enabled": False,
            "case_modification_enabled": False,
            "confidence_threshold": CLASSIFY_CONFIDENCE_THRESHOLD,
        },
    }

    return StoryResult(
        story_id=req.story_id,
        response_text=_format_response(story_output),
        story_output=story_output,
        state_updates_domain={
            "last_issue_category": category,
            "last_issue_queue": queue_name,
            "last_story_summary": (
                f"Triage classified '{category}' with confidence={story_output['classification_confidence']:.2f}."
            ),
        },
    )
