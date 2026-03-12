from __future__ import annotations

import json
import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional, TypedDict

from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from pydantic import BaseModel, Field

from ..contracts import StoryRequest, StoryResult
from ..utils import (
    extract_explicit_member_id,
    member_id_aliases,
    normalize_member_id,
    register_sqlite_alnum_normalizer,
)

PROJECT_PHASE_2 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_2 / "kb" / "MembershipFraud" / "membership_fraud.db"
SECURITY_HELP_JSONL_PATH = PROJECT_PHASE_2 / "kb" / "MembershipFraud" / "security_help_kb.jsonl"
SECURITY_HELP_CHROMA_DIR = PROJECT_PHASE_2 / "kb" / "MembershipFraud" / "security_help_chroma"
SECURITY_HELP_COLLECTION = "membership_fraud_security_help"

Timeframe = Literal["most_recent", "last_7_days", "last_30_days"]


class SecurityPlan(BaseModel):
    member_id: Optional[str] = None
    timeframe: Timeframe = "most_recent"
    max_events: int = Field(default=3, ge=1, le=10)
    ask_for_member_id: bool = False
    tone: Literal["calm", "neutral", "urgent"] = "neutral"


class SecurityState(TypedDict, total=False):
    user_text: str
    member_id: Optional[str]
    timeframe: Optional[Timeframe]
    plan: Dict[str, Any]
    retrieved_events: List[Dict[str, Any]]
    response_text: str
    follow_up_question: Optional[str]
    recommended_actions: List[str]
    latest_event: Dict[str, Any]
    security_help_snippets: List[Dict[str, Any]]


def _infer_timeframe(user_text: str) -> Timeframe:
    tl = user_text.lower()
    if any(k in tl for k in ["last week", "past 7", "last 7"]):
        return "last_7_days"
    if any(k in tl for k in ["last month", "past 30", "last 30"]):
        return "last_30_days"
    return "most_recent"


def _user_says_not_recognized(text: str) -> bool:
    t = text.lower()
    return any(
        phrase in t
        for phrase in [
            "don't recognize",
            "do not recognize",
            "wasn't me",
            "was not me",
            "not me",
            "i didn't do that",
        ]
    )


def _user_says_recognized(text: str) -> bool:
    t = text.lower()
    return any(
        phrase in t
        for phrase in [
            "that was me",
            "i recognize",
            "it was me",
            "yes that was me",
        ]
    )


def _user_asks_security_howto(text: str) -> bool:
    t = text.lower()
    howto_signals = [
        "how do i",
        "how to",
        "what are the steps",
        "where do i",
        "walk me through",
        "can you help me set up",
        "help me set up",
    ]
    security_topics = [
        "password",
        "mfa",
        "multi-factor",
        "multi factor",
        "2fa",
        "two-factor",
        "two factor",
        "sign out",
        "all sessions",
        "unknown device",
        "recovery",
        "account locked",
        "unlock account",
    ]

    howto = any(k in t for k in howto_signals)
    topic = any(k in t for k in security_topics)
    direct_password_or_mfa = (
        ("password" in t and any(k in t for k in ["change", "reset", "forgot"]))
        or ("mfa" in t)
        or ("2fa" in t)
        or ("multi-factor" in t)
        or ("two-factor" in t)
    )
    return (howto and topic) or direct_password_or_mfa


SECURITY_HELP_ROWS_CACHE: Optional[List[Dict[str, Any]]] = None
SECURITY_HELP_STORE = None
SECURITY_HELP_STORE_READY = False


def _tokenize(text: str) -> List[str]:
    return [tok for tok in re.split(r"[^a-z0-9]+", text.lower()) if len(tok) >= 3]


def _load_security_help_rows() -> List[Dict[str, Any]]:
    global SECURITY_HELP_ROWS_CACHE
    if SECURITY_HELP_ROWS_CACHE is not None:
        return SECURITY_HELP_ROWS_CACHE
    if not SECURITY_HELP_JSONL_PATH.exists():
        SECURITY_HELP_ROWS_CACHE = []
        return SECURITY_HELP_ROWS_CACHE

    rows: List[Dict[str, Any]] = []
    for line in SECURITY_HELP_JSONL_PATH.read_text(encoding="utf-8").splitlines():
        txt = line.strip()
        if not txt:
            continue
        try:
            obj = json.loads(txt)
            if isinstance(obj, dict):
                rows.append(obj)
        except Exception:
            continue
    SECURITY_HELP_ROWS_CACHE = rows
    return rows


def _get_security_help_store():
    global SECURITY_HELP_STORE, SECURITY_HELP_STORE_READY
    if SECURITY_HELP_STORE_READY:
        return SECURITY_HELP_STORE
    SECURITY_HELP_STORE_READY = True

    if not SECURITY_HELP_CHROMA_DIR.exists():
        return None

    try:
        from langchain_chroma import Chroma
        from langchain_openai import OpenAIEmbeddings
    except Exception:
        return None

    try:
        SECURITY_HELP_STORE = Chroma(
            collection_name=SECURITY_HELP_COLLECTION,
            persist_directory=str(SECURITY_HELP_CHROMA_DIR),
            embedding_function=OpenAIEmbeddings(model="text-embedding-3-small"),
        )
    except Exception:
        SECURITY_HELP_STORE = None
    return SECURITY_HELP_STORE


def _retrieve_security_help(query: str, k: int = 3) -> List[Dict[str, Any]]:
    store = _get_security_help_store()
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
                        "id": md.get("id") or "security-help",
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

    rows = _load_security_help_rows()
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
                "id": row.get("id") or f"security-help-{i}",
                "question": q,
                "answer": a,
                "score": float(overlap) / max(len(q_tokens), 1),
                "text": full,
            }
        )

    scored.sort(key=lambda x: x.get("score", 0.0), reverse=True)
    return scored[:k]


def _answer_security_howto(user_text: str, llm: ChatOpenAI) -> Optional[Dict[str, Any]]:
    snippets = _retrieve_security_help(user_text, k=3)
    if not snippets:
        return None
    if snippets[0].get("score", 0.0) < 0.2:
        return None

    context_parts = []
    for s in snippets:
        context_parts.append(
            f"[{s.get('id')}]\n"
            f"Q: {s.get('question')}\n"
            f"A: {s.get('answer')}"
        )
    context = "\n\n".join(context_parts)

    prompt = (
        "You are a security support assistant in a live chat.\n"
        "Answer the user's procedural question using only the provided snippets.\n"
        "Do not invent product-specific UI labels beyond snippet wording.\n"
        "Be concise and provide clear steps.\n"
        "If snippets are insufficient, say you don't have enough detail and suggest contacting support.\n"
        "Cite snippet ids in brackets (for example: [sec-help-003]).\n\n"
        f"USER QUESTION:\n{user_text}\n\n"
        f"SNIPPETS:\n{context}"
    )

    try:
        msg = str(llm.invoke(prompt).content).strip()
    except Exception:
        top = snippets[0]
        msg = f"{top.get('answer', '')} [{top.get('id')}]".strip()
    if not msg:
        return None
    return {"message": msg, "snippets": snippets}


def _read_security_events(member_id: str, timeframe: Timeframe, max_events: int = 5) -> List[Dict[str, Any]]:
    aliases = member_id_aliases(member_id)
    if not aliases:
        return []
    placeholders = ",".join(["?"] * len(aliases))

    with sqlite3.connect(DB_PATH) as conn:
        register_sqlite_alnum_normalizer(conn)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        if timeframe == "most_recent":
            sql = f"""
            SELECT event_id, member_id, event_ts, login_location, device_type, risk_level, trigger_reason, recommended_action
            FROM security_events
            WHERE NORM_ALNUM(member_id) IN ({placeholders})
            ORDER BY event_ts DESC
            LIMIT ?
            """
            return [dict(r) for r in cur.execute(sql, (*aliases, max_events)).fetchall()]

        days = 7 if timeframe == "last_7_days" else 30
        cutoff = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        sql = f"""
        SELECT event_id, member_id, event_ts, login_location, device_type, risk_level, trigger_reason, recommended_action
        FROM security_events
        WHERE NORM_ALNUM(member_id) IN ({placeholders}) AND event_ts >= ?
        ORDER BY event_ts DESC
        LIMIT ?
        """
        return [dict(r) for r in cur.execute(sql, (*aliases, cutoff, max_events)).fetchall()]


def _explain_security_event(event: Dict[str, Any]) -> str:
    return (
        f"Alert time: {event.get('event_ts')}\n"
        f"Location: {event.get('login_location')}\n"
        f"Device: {event.get('device_type')}\n"
        f"Flag reason: {event.get('trigger_reason')}\n"
        f"Risk level: {event.get('risk_level')}"
    )


def _guide_security_actions(event: Dict[str, Any]) -> List[str]:
    risk = str(event.get("risk_level") or "").lower()
    base = [
        "Review recent account activity and confirm whether you recognize this sign-in.",
        "If you do not recognize it, change your password and sign out of all sessions.",
    ]
    if risk == "high":
        return base + ["Enable MFA immediately.", "Contact support to secure the account."]
    if risk == "medium":
        return base + ["Enable MFA for added protection.", "Update account recovery options."]
    return base + ["No urgent action is needed if you recognize the login context."]


def _planner_node(state: SecurityState, llm: ChatOpenAI) -> SecurityState:
    user_text = state.get("user_text", "")
    existing_member_id = state.get("member_id")
    existing_timeframe = state.get("timeframe")
    fallback_timeframe = existing_timeframe or _infer_timeframe(user_text)

    structured = llm.with_structured_output(SecurityPlan)
    system = (
        "You are a security support planner.\n"
        "Extract member_id and timeframe from the user message.\n"
        "If member_id is missing but EXISTING_MEMBER_ID is provided, reuse it.\n"
        "If member_id is still missing, set ask_for_member_id=true.\n"
        "Timeframe mapping:\n"
        "- latest/most recent -> most_recent\n"
        "- last week/past 7 days -> last_7_days\n"
        "- last month/past 30 days -> last_30_days\n"
        "Return only structured output."
    )
    user = (
        f"EXISTING_MEMBER_ID: {existing_member_id}\n"
        f"EXISTING_TIMEFRAME: {existing_timeframe}\n"
        f"USER: {user_text}"
    )

    try:
        plan_obj = structured.invoke([("system", system), ("user", user)])
        plan = plan_obj.model_dump()
    except Exception:
        plan = {
            "member_id": existing_member_id,
            "timeframe": fallback_timeframe,
            "max_events": 3,
            "ask_for_member_id": existing_member_id is None,
            "tone": "neutral",
        }

    member_id = normalize_member_id(plan.get("member_id")) or existing_member_id
    timeframe = plan.get("timeframe") or fallback_timeframe
    ask_for_member = bool(plan.get("ask_for_member_id", False) and not member_id)

    if member_id:
        plan["ask_for_member_id"] = False
        plan["member_id"] = member_id
    plan["timeframe"] = timeframe

    return {
        "plan": plan,
        "member_id": member_id,
        "timeframe": timeframe,
        "follow_up_question": (
            "I can check that security alert, what is your member_id (e.g., MB001)?"
            if ask_for_member
            else None
        ),
    }


def _retrieve_node(state: SecurityState) -> SecurityState:
    member_id = state.get("member_id")
    if not member_id:
        return {}
    plan = state.get("plan", {})
    timeframe = state.get("timeframe") or "most_recent"
    max_events = int(plan.get("max_events", 3))
    events = _read_security_events(member_id=member_id, timeframe=timeframe, max_events=max_events)
    return {"retrieved_events": events}


def _respond_node(state: SecurityState, llm: ChatOpenAI) -> SecurityState:
    member_id = state.get("member_id")
    if not member_id:
        msg = state.get("follow_up_question") or "What is your member_id?"
        return {"response_text": msg, "follow_up_question": msg}

    events = state.get("retrieved_events") or []
    timeframe = state.get("timeframe") or "most_recent"
    if not events:
        msg = f"I could not find security alerts for {member_id} in timeframe={timeframe}."
        return {"response_text": msg, "follow_up_question": None}

    latest = events[0]
    explanation = _explain_security_event(latest)
    actions = _guide_security_actions(latest)
    user_text = state.get("user_text", "")
    recognized = _user_says_recognized(user_text)
    not_recognized = _user_says_not_recognized(user_text)
    asks_howto = _user_asks_security_howto(user_text)

    if asks_howto:
        rag = _answer_security_howto(user_text, llm)
        if rag:
            help_msg = rag["message"]
            if not_recognized:
                msg = (
                    "Because you do not recognize the login, take these actions now:\n"
                    f"- {actions[1]}\n"
                    f"- {actions[2] if len(actions) > 2 else 'Enable MFA for added protection.'}\n\n"
                    f"{help_msg}"
                )
            else:
                msg = help_msg
            return {
                "response_text": msg,
                "follow_up_question": None,
                "recommended_actions": actions,
                "latest_event": latest,
                "security_help_snippets": rag.get("snippets", []),
            }

    try:
        if not_recognized:
            prompt = (
                "You are a security support assistant in a live chat.\n"
                "Respond conversationally without greetings/signatures.\n"
                "The user does not recognize the login. Be clear and action-oriented.\n\n"
                f"EVENT DETAILS:\n{explanation}\n\n"
                f"RECOMMENDED ACTIONS:\n- " + "\n- ".join(actions)
            )
            msg = llm.invoke(prompt).content
            follow_up = None
        elif recognized:
            prompt = (
                "You are a security support assistant in a live chat.\n"
                "Respond conversationally without greetings/signatures.\n"
                "The user confirms they recognize the login. Reassure appropriately.\n\n"
                f"EVENT DETAILS:\n{explanation}\n"
            )
            msg = llm.invoke(prompt).content
            follow_up = None
        else:
            prompt = (
                "You are a security support assistant in a live chat.\n"
                "Respond conversationally without greetings/signatures.\n"
                "Provide alert details and recommended actions.\n"
                "End with one short question asking if they recognize the location or device.\n\n"
                f"EVENT DETAILS:\n{explanation}\n\n"
                f"RECOMMENDED ACTIONS:\n- " + "\n- ".join(actions)
            )
            msg = llm.invoke(prompt).content
            follow_up = "Do you recognize this location or device?"
    except Exception:
        msg = (
            f"Latest security alert for {member_id}:\n"
            f"- Time: {latest.get('event_ts')}\n"
            f"- Location: {latest.get('login_location')}\n"
            f"- Device: {latest.get('device_type')}\n"
            f"- Reason: {latest.get('trigger_reason')}\n"
            f"- Risk: {latest.get('risk_level')}\n"
            f"- Recommended action: {latest.get('recommended_action')}\n"
            "Suggested next steps:\n"
            + "\n".join([f"- {a}" for a in actions])
        )
        follow_up = "Do you recognize this location or device?"

    return {
        "response_text": msg,
        "follow_up_question": follow_up,
        "recommended_actions": actions,
        "latest_event": latest,
        "security_help_snippets": [],
    }


def _build_story_graph():
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
    builder = StateGraph(SecurityState)

    def plan_node(state: SecurityState) -> SecurityState:
        return _planner_node(state, llm)

    def retrieve_node(state: SecurityState) -> SecurityState:
        return _retrieve_node(state)

    def respond_node(state: SecurityState) -> SecurityState:
        return _respond_node(state, llm)

    def route_after_plan(state: SecurityState) -> str:
        return "ask" if state.get("follow_up_question") and not state.get("member_id") else "go"

    builder.add_node("plan", plan_node)
    builder.add_node("retrieve", retrieve_node)
    builder.add_node("respond", respond_node)
    builder.set_entry_point("plan")
    builder.add_conditional_edges("plan", route_after_plan, {"ask": "respond", "go": "retrieve"})
    builder.add_edge("retrieve", "respond")
    builder.add_edge("respond", END)
    return builder.compile()


SECURITY_GRAPH = None


def _get_security_graph():
    global SECURITY_GRAPH
    if SECURITY_GRAPH is None:
        SECURITY_GRAPH = _build_story_graph()
    return SECURITY_GRAPH


def get_membership_fraud_story1_mermaid() -> str:
    return _get_security_graph().get_graph().draw_mermaid()


def run_membership_fraud_story1(req: StoryRequest) -> StoryResult:
    member_from_query = extract_explicit_member_id(req.user_query)
    member_id = normalize_member_id(member_from_query or req.member.member_id or req.domain_context.get("member_id"))
    timeframe = req.domain_context.get("timeframe")

    state_in: SecurityState = {
        "user_text": req.user_query,
        "member_id": member_id,
        "timeframe": timeframe if timeframe in {"most_recent", "last_7_days", "last_30_days"} else None,
    }
    state_out = _get_security_graph().invoke(state_in)

    response_text = state_out.get("response_text") or "I can help with security alerts."
    final_member = state_out.get("member_id") or member_id
    final_timeframe = state_out.get("timeframe") or _infer_timeframe(req.user_query)
    retrieved_events = state_out.get("retrieved_events") or []
    follow_up = state_out.get("follow_up_question")

    story_output = {
        "member_id": final_member,
        "timeframe": final_timeframe,
        "plan": state_out.get("plan", {}),
        "retrieved_events": retrieved_events,
        "recommended_actions": state_out.get("recommended_actions", []),
        "latest_event": state_out.get("latest_event"),
        "security_help_snippets": state_out.get("security_help_snippets", []),
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
            "timeframe": final_timeframe,
            "last_story_summary": f"Returned {len(retrieved_events)} security event(s).",
        },
    )
