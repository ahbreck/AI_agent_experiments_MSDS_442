from __future__ import annotations

import json
import re
from pathlib import Path
from typing import Any, Dict, List, Literal, TypedDict

from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from ..contracts import StoryRequest, StoryResult

IssueCategory = Literal["login", "billing", "renewal", "unknown"]

ISSUE_CATEGORIES: tuple[IssueCategory, ...] = ("login", "billing", "renewal", "unknown")
CLASSIFY_CONFIDENCE_THRESHOLD = 0.75
LLM_ACCEPT_THRESHOLD = 0.65
RAG_CLASSIFY_CONFIDENCE_THRESHOLD = 0.8
RAG_RELEVANCE_THRESHOLD = 0.35
RAG_TOP_K = 3

PROJECT_PHASE_3 = Path(__file__).resolve().parents[2]
KB_ROOT = PROJECT_PHASE_3 / "kb" / "MembershipFraud"

CATEGORY_HELP_JSONL = {
    "login": KB_ROOT / "login_help_kb.jsonl",
    "billing": KB_ROOT / "billing_help_kb.jsonl",
    "renewal": KB_ROOT / "renewal_help_kb.jsonl",
}

CATEGORY_HELP_CHROMA_DIR = {
    "login": KB_ROOT / "login_help_chroma",
    "billing": KB_ROOT / "billing_help_chroma",
    "renewal": KB_ROOT / "renewal_help_chroma",
}

CATEGORY_HELP_COLLECTION = {
    "login": "membership_fraud_login_help",
    "billing": "membership_fraud_billing_help",
    "renewal": "membership_fraud_renewal_help",
}

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


class MembershipSupportState(TypedDict, total=False):
    user_text: str
    deterministic_classification: Dict[str, Any]
    final_classification: Dict[str, Any]
    issue_category: str
    classification_confidence: float
    routing_queue: str
    requires_human_review: bool
    rag_result: Dict[str, Any]
    story_output: Dict[str, Any]
    response_text: str
    audit_trace: List[str]


HELP_ROWS_CACHE: Dict[str, List[Dict[str, Any]]] = {}
HELP_STORE: Dict[str, Any] = {}
HELP_STORE_READY: Dict[str, bool] = {}


def _compact_description(user_text: str, max_len: int = 140) -> str:
    cleaned = re.sub(r"\s+", " ", (user_text or "").strip())
    if not cleaned:
        return "No issue details were provided."
    if len(cleaned) <= max_len:
        return cleaned
    return f"{cleaned[: max_len - 3].rstrip()}..."


def _tokenize(text: str) -> List[str]:
    return [tok for tok in re.split(r"[^a-z0-9]+", (text or "").lower()) if len(tok) >= 3]


def _load_category_help_rows(category: str) -> List[Dict[str, Any]]:
    if category in HELP_ROWS_CACHE:
        return HELP_ROWS_CACHE[category]

    p = CATEGORY_HELP_JSONL.get(category)
    if p is None or not p.exists():
        HELP_ROWS_CACHE[category] = []
        return HELP_ROWS_CACHE[category]

    rows: List[Dict[str, Any]] = []
    for line in p.read_text(encoding="utf-8").splitlines():
        txt = line.strip()
        if not txt:
            continue
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            continue
    HELP_ROWS_CACHE[category] = rows
    return rows


def _get_category_help_store(category: str):
    if HELP_STORE_READY.get(category):
        return HELP_STORE.get(category)
    HELP_STORE_READY[category] = True

    chroma_dir = CATEGORY_HELP_CHROMA_DIR.get(category)
    collection = CATEGORY_HELP_COLLECTION.get(category)
    if chroma_dir is None or collection is None or not chroma_dir.exists():
        HELP_STORE[category] = None
        return None

    try:
        from langchain_chroma import Chroma
        from langchain_openai import OpenAIEmbeddings
    except Exception:
        HELP_STORE[category] = None
        return None

    try:
        HELP_STORE[category] = Chroma(
            collection_name=collection,
            persist_directory=str(chroma_dir),
            embedding_function=OpenAIEmbeddings(model="text-embedding-3-small"),
        )
    except Exception:
        HELP_STORE[category] = None
    return HELP_STORE[category]


def _retrieve_category_help(category: str, query: str, k: int = RAG_TOP_K) -> List[Dict[str, Any]]:
    store = _get_category_help_store(category)
    out: List[Dict[str, Any]] = []
    if store is not None:
        try:
            pairs = store.similarity_search_with_score(query, k=k)
            for doc, distance in pairs:
                md = doc.metadata or {}
                dist = float(distance) if distance is not None else 1.0
                score = 1.0 / (1.0 + max(dist, 0.0))
                out.append(
                    {
                        "id": md.get("id") or f"{category}-help",
                        "question": md.get("question") or "",
                        "answer": md.get("answer") or "",
                        "score": score,
                        "text": doc.page_content,
                    }
                )
            if out:
                return out
        except Exception:
            pass

    rows = _load_category_help_rows(category)
    if not rows:
        return []

    q_tokens = set(_tokenize(query))
    scored: List[Dict[str, Any]] = []
    for i, row in enumerate(rows, start=1):
        q = str(row.get("question") or "")
        a = str(row.get("answer") or "")
        full = f"{q} {a}"
        row_tokens = set(_tokenize(full))
        overlap = len(q_tokens.intersection(row_tokens))
        if overlap <= 0:
            continue
        scored.append(
            {
                "id": row.get("id") or f"{category}-help-{i}",
                "question": q,
                "answer": a,
                "score": float(overlap) / max(len(q_tokens), 1),
                "text": full,
            }
        )
    scored.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return scored[:k]


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


def _needs_llm_resolution(base: Dict[str, Any]) -> bool:
    return base.get("issue_category") == "unknown" or float(base.get("confidence", 0.0)) < CLASSIFY_CONFIDENCE_THRESHOLD


def _route_queue(issue_category: str, confidence: float) -> tuple[str, bool]:
    # Guardrail: if confidence is below direct-response threshold, force human review.
    if issue_category == "unknown" or confidence < RAG_CLASSIFY_CONFIDENCE_THRESHOLD:
        return QUEUE_MAP["unknown"], True
    return QUEUE_MAP.get(issue_category, QUEUE_MAP["unknown"]), False


def _build_rag_direct_answer(user_text: str, issue_category: str, snippets: List[Dict[str, Any]]) -> str:
    if not snippets:
        return ""

    lines = ["Direct guidance from knowledge base:"]
    for s in snippets:
        ans = str(s.get("answer") or "").strip()
        sid = str(s.get("id") or "kb")
        if ans:
            lines.append(f"- {ans} [{sid}]")
    return "\n".join(lines)


def _format_response(payload: Dict[str, Any], rag_direct_answer: str | None = None) -> str:
    summary = (
        "Issue triage result:\n"
        f"- Category: {payload.get('issue_category')}\n"
        f"- Description: {payload.get('issue_description')}\n"
        f"- Confidence: {float(payload.get('classification_confidence', 0.0)):.2f}\n"
        f"- Routed queue: {payload.get('routing_queue')}\n"
        f"- Human review required: {'yes' if payload.get('requires_human_review') else 'no'}"
    )
    if rag_direct_answer:
        return f"{summary}\n\nSuggested direct response:\n{rag_direct_answer}"

    rag_attempted = bool(payload.get("rag_attempted"))
    rag_used = bool(payload.get("rag_used"))
    rag_reason = str(payload.get("rag_fallback_reason") or "").strip()
    if not rag_used:
        if rag_reason == "classification_confidence_below_threshold":
            return (
                f"{summary}\n\n"
                "I could not provide a direct knowledge-base response because classification confidence was below "
                "the direct-response threshold, so this has been flagged for human review."
            )
        if rag_reason in {"low_relevance", "no_kb_match"}:
            msg = "I could not find corresponding information in the issue knowledge base for a direct response."
            if rag_attempted:
                msg = (
                    f"{msg} The case has been routed for human review to avoid providing unsupported guidance."
                )
            return f"{summary}\n\n{msg}"
    return summary


def _maybe_retrieve_direct_answer(
    user_text: str,
    issue_category: str,
    confidence: float,
    requires_human_review: bool,
) -> Dict[str, Any]:
    out = {
        "rag_attempted": False,
        "rag_used": False,
        "rag_relevance_score": 0.0,
        "rag_snippets": [],
        "rag_direct_answer": None,
        "rag_fallback_reason": "retrieval_not_attempted",
        "rag_escalated_to_human_review": False,
    }

    if issue_category not in {"login", "billing", "renewal"}:
        out["rag_fallback_reason"] = "classification_confidence_below_threshold"
        return out
    if requires_human_review or confidence < RAG_CLASSIFY_CONFIDENCE_THRESHOLD:
        out["rag_fallback_reason"] = "classification_confidence_below_threshold"
        return out

    out["rag_attempted"] = True
    snippets = _retrieve_category_help(issue_category, user_text, k=RAG_TOP_K)
    out["rag_snippets"] = snippets
    if not snippets:
        out["rag_fallback_reason"] = "no_kb_match"
        return out

    top_score = float(snippets[0].get("score", 0.0))
    out["rag_relevance_score"] = round(top_score, 2)
    if top_score < RAG_RELEVANCE_THRESHOLD:
        out["rag_fallback_reason"] = "low_relevance"
        return out

    out["rag_used"] = True
    out["rag_direct_answer"] = _build_rag_direct_answer(user_text, issue_category, snippets)
    out["rag_fallback_reason"] = ""
    return out


def _append_trace(state: MembershipSupportState, message: str) -> List[str]:
    trace = list(state.get("audit_trace", []))
    trace.append(message)
    return trace


def _parse_request_node(state: MembershipSupportState) -> MembershipSupportState:
    user_text = str(state.get("user_text") or "").strip()
    return {
        "user_text": user_text,
        "audit_trace": _append_trace(state, "parse_request: accepted user query."),
    }


def _classify_deterministic_node(state: MembershipSupportState) -> MembershipSupportState:
    user_text = state.get("user_text", "")
    base = _classify_issue_deterministic(user_text)
    return {
        "deterministic_classification": base,
        "audit_trace": _append_trace(
            state,
            (
                "classify_deterministic: "
                f"category={base.get('issue_category')} confidence={float(base.get('confidence', 0.0)):.2f}"
            ),
        ),
    }


def _route_after_deterministic(state: MembershipSupportState) -> str:
    base = state.get("deterministic_classification", {})
    return "llm" if _needs_llm_resolution(base) else "route"


def _classify_with_llm_node(state: MembershipSupportState) -> MembershipSupportState:
    user_text = state.get("user_text", "")
    base = state.get("deterministic_classification", _classify_issue_deterministic(user_text))
    resolved = _resolve_issue_with_llm(user_text, base)
    if resolved.get("issue_category") == "unknown" and base.get("issue_category") != "unknown":
        resolved = base
    return {
        "final_classification": resolved,
        "audit_trace": _append_trace(
            state,
            (
                "classify_with_llm: "
                f"selected={resolved.get('issue_category')} source={resolved.get('classification_source')}"
            ),
        ),
    }


def _route_queue_node(state: MembershipSupportState) -> MembershipSupportState:
    selected = state.get("final_classification") or state.get("deterministic_classification") or {}
    category = str(selected.get("issue_category", "unknown"))
    confidence = float(selected.get("confidence", 0.0))
    queue_name, human_review = _route_queue(category, confidence)
    return {
        "final_classification": selected,
        "issue_category": category,
        "classification_confidence": confidence,
        "routing_queue": queue_name,
        "requires_human_review": human_review,
        "audit_trace": _append_trace(
            state,
            (
                "route_queue: "
                f"queue={queue_name} human_review={'yes' if human_review else 'no'} confidence={confidence:.2f}"
            ),
        ),
    }


def _retrieve_kb_node(state: MembershipSupportState) -> MembershipSupportState:
    rag_result = _maybe_retrieve_direct_answer(
        user_text=state.get("user_text", ""),
        issue_category=str(state.get("issue_category", "unknown")),
        confidence=float(state.get("classification_confidence", 0.0)),
        requires_human_review=bool(state.get("requires_human_review")),
    )
    return {
        "rag_result": rag_result,
        "audit_trace": _append_trace(
            state,
            (
                "retrieve_kb: "
                f"attempted={bool(rag_result.get('rag_attempted'))} used={bool(rag_result.get('rag_used'))}"
            ),
        ),
    }


def _guardrails_node(state: MembershipSupportState) -> MembershipSupportState:
    queue_name = str(state.get("routing_queue", QUEUE_MAP["unknown"]))
    human_review = bool(state.get("requires_human_review"))
    rag_result = dict(state.get("rag_result", {}))
    if (
        not human_review
        and bool(rag_result.get("rag_attempted"))
        and not bool(rag_result.get("rag_used"))
        and str(rag_result.get("rag_fallback_reason") or "") in {"low_relevance", "no_kb_match"}
    ):
        queue_name = QUEUE_MAP["unknown"]
        human_review = True
        rag_result["rag_escalated_to_human_review"] = True
    return {
        "routing_queue": queue_name,
        "requires_human_review": human_review,
        "rag_result": rag_result,
        "audit_trace": _append_trace(
            state,
            (
                "guardrails: "
                f"queue={queue_name} human_review={'yes' if human_review else 'no'} "
                f"fallback={str(rag_result.get('rag_fallback_reason') or 'none')}"
            ),
        ),
    }


def _compose_response_node(state: MembershipSupportState) -> MembershipSupportState:
    selected = state.get("final_classification") or state.get("deterministic_classification") or {}
    category = str(state.get("issue_category", selected.get("issue_category", "unknown")))
    confidence = float(state.get("classification_confidence", selected.get("confidence", 0.0)))
    rag_result = state.get("rag_result", {})
    rag_snippets = rag_result.get("rag_snippets", [])
    story_output = {
        "issue_category": category,
        "issue_description": selected.get("issue_description") or _compact_description(state.get("user_text", "")),
        "classification_confidence": round(confidence, 2),
        "routing_queue": state.get("routing_queue", QUEUE_MAP["unknown"]),
        "requires_human_review": bool(state.get("requires_human_review")),
        "classification_source": selected.get("classification_source", "fallback"),
        "classification_rationale": selected.get("classification_rationale", "No rationale available."),
        "category_scores": selected.get("category_scores", {}),
        "response_mode": "rag_direct_response" if rag_result.get("rag_used") else "queue_only",
        "rag_attempted": bool(rag_result.get("rag_attempted")),
        "rag_used": bool(rag_result.get("rag_used")),
        "rag_relevance_score": float(rag_result.get("rag_relevance_score", 0.0)),
        "rag_fallback_reason": str(rag_result.get("rag_fallback_reason") or ""),
        "rag_escalated_to_human_review": bool(rag_result.get("rag_escalated_to_human_review")),
        "rag_snippet_ids": [str(s.get("id")) for s in rag_snippets if s.get("id")],
        "rag_snippets": rag_snippets,
        "rag_thresholds": {
            "classification_confidence_min": RAG_CLASSIFY_CONFIDENCE_THRESHOLD,
            "retrieval_relevance_min": RAG_RELEVANCE_THRESHOLD,
            "top_k": RAG_TOP_K,
        },
        "guardrails": {
            "auto_resolve_enabled": False,
            "case_modification_enabled": False,
            "confidence_threshold": CLASSIFY_CONFIDENCE_THRESHOLD,
        },
        "audit_trace": state.get("audit_trace", []),
    }
    response_text = _format_response(story_output, rag_direct_answer=rag_result.get("rag_direct_answer"))
    return {
        "story_output": story_output,
        "response_text": response_text,
        "audit_trace": _append_trace(state, "compose_response: finalized response payload."),
    }


def _build_story_graph():
    g = StateGraph(MembershipSupportState)
    g.add_node("parse_request", _parse_request_node)
    g.add_node("classify_deterministic", _classify_deterministic_node)
    g.add_node("classify_with_llm", _classify_with_llm_node)
    g.add_node("route_queue", _route_queue_node)
    g.add_node("retrieve_kb", _retrieve_kb_node)
    g.add_node("guardrails", _guardrails_node)
    g.add_node("compose_response", _compose_response_node)
    g.set_entry_point("parse_request")
    g.add_edge("parse_request", "classify_deterministic")
    g.add_conditional_edges("classify_deterministic", _route_after_deterministic, {"llm": "classify_with_llm", "route": "route_queue"})
    g.add_edge("classify_with_llm", "route_queue")
    g.add_edge("route_queue", "retrieve_kb")
    g.add_edge("retrieve_kb", "guardrails")
    g.add_edge("guardrails", "compose_response")
    g.add_edge("compose_response", END)
    return g.compile()


MEMBERSHIP_SUPPORT_GRAPH = None


def _get_membership_support_graph():
    global MEMBERSHIP_SUPPORT_GRAPH
    if MEMBERSHIP_SUPPORT_GRAPH is None:
        MEMBERSHIP_SUPPORT_GRAPH = _build_story_graph()
    return MEMBERSHIP_SUPPORT_GRAPH


def get_membership_fraud_story2_mermaid() -> str:
    return _get_membership_support_graph().get_graph().draw_mermaid()


def run_membership_fraud_story2(req: StoryRequest) -> StoryResult:
    state_in: MembershipSupportState = {
        "user_text": req.user_query,
        "audit_trace": [],
    }
    state_out = _get_membership_support_graph().invoke(state_in)
    story_output = state_out.get("story_output", {})
    category = str(story_output.get("issue_category", "unknown"))
    queue_name = str(story_output.get("routing_queue", QUEUE_MAP["unknown"]))
    response_text = state_out.get("response_text") or "Issue triage result: unable to format response."

    return StoryResult(
        story_id=req.story_id,
        response_text=response_text,
        story_output=story_output,
        state_updates_domain={
            "last_issue_category": category,
            "last_issue_queue": queue_name,
            "last_story_summary": (
                f"Triage classified '{category}' with confidence={float(story_output.get('classification_confidence', 0.0)):.2f}."
            ),
        },
    )
