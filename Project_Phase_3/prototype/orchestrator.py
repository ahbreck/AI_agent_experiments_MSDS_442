from __future__ import annotations

from dataclasses import asdict
import os
import warnings
from typing import Any, Dict, List, Optional, Tuple, TypedDict

try:
    from dotenv import find_dotenv, load_dotenv
except ImportError:  # optional dependency
    find_dotenv = None
    load_dotenv = None
from langchain_openai import ChatOpenAI
from langgraph.graph import END, StateGraph
from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.memory import MemorySaver
from pydantic import BaseModel, Field

from .catalog import DOMAIN_TO_STORIES, STORY_CATALOG
from .contracts import CanonicalMember, GlobalState, RouteDecision, StoryRequest, StoryResult
from .utils import extract_explicit_member_id

# Suppress known noisy structured-output serializer warning from dependency internals.
warnings.filterwarnings(
    "ignore",
    message=r"Pydantic serializer warnings:.*field_name='parsed'.*",
    category=UserWarning,
    module=r"pydantic\.main",
)


CONT_HIGH = 0.70
FRESH_HIGH = 0.62
FRESH_MARGIN = 0.20
PENDING_TTL_TURNS = 3


class DomainRouteOutput(BaseModel):
    domain: str = Field(description="One of: business_marketing, data_science, membership_fraud, clarify")
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    rationale: str = Field(default="")


class StoryRouteOutput(BaseModel):
    story_id: str
    confidence: float = Field(default=0.0, ge=0.0, le=1.0)
    rationale: str = Field(default="")


class OrchestratorGraphState(TypedDict, total=False):
    user_query: str
    domain: Optional[str]
    story_id: Optional[str]
    reason: str
    metrics: Dict[str, Any]
    should_clarify: bool
    response: str
    story_output: Dict[str, Any]
    follow_up_question: Optional[str]


class AgenticOrchestrator:
    def __init__(
        self,
        state: Optional[GlobalState] = None,
        checkpointer: Optional[MemorySaver] = None,
        checkpoint_ns: str = "phase2_prototype",
    ):
        if load_dotenv and find_dotenv:
            load_dotenv(find_dotenv())
        if not os.getenv("OPENAI_API_KEY"):
            raise RuntimeError(
                "OPENAI_API_KEY is not set. Add it to your environment or .env before initializing AgenticOrchestrator."
            )
        self.state = state or GlobalState()
        self.checkpointer = checkpointer or MemorySaver()
        self.checkpoint_ns = checkpoint_ns
        self.llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)
        self.graph = self._build_graph()

    def _thread_config(self, thread_id: str) -> Dict[str, Any]:
        return {"configurable": {"thread_id": thread_id, "checkpoint_ns": self.checkpoint_ns}}

    def _load_thread_state(self, thread_id: str) -> GlobalState:
        ckpt_tuple = self.checkpointer.get_tuple(self._thread_config(thread_id))
        if not ckpt_tuple or not ckpt_tuple.metadata:
            return GlobalState(thread_id=thread_id)

        stored = ckpt_tuple.metadata.get("state")
        if not isinstance(stored, dict):
            return GlobalState(thread_id=thread_id)

        member_raw = stored.get("member") or {}
        member = CanonicalMember(
            member_id=member_raw.get("member_id"),
            member_id_raw=member_raw.get("member_id_raw"),
            member_id_source=member_raw.get("member_id_source"),
            confidence=float(member_raw.get("confidence", 0.0)),
        )

        return GlobalState(
            thread_id=stored.get("thread_id", thread_id),
            session_id=stored.get("session_id", "local"),
            messages=stored.get("messages", []),
            member=member,
            active_domain=stored.get("active_domain"),
            active_story_id=stored.get("active_story_id"),
            route_trace=stored.get("route_trace", []),
            domain_states=stored.get("domain_states", {}),
            pending_question=stored.get("pending_question"),
            pending_slot_type=stored.get("pending_slot_type"),
            pending_slot_target_story_id=stored.get("pending_slot_target_story_id"),
            pending_turn_created=stored.get("pending_turn_created"),
            turn_index=int(stored.get("turn_index", 0)),
            last_active_turn_index=stored.get("last_active_turn_index"),
            router_reason=stored.get("router_reason"),
            router_metrics=stored.get("router_metrics", {}),
            last_response=stored.get("last_response"),
            global_errors=stored.get("global_errors", []),
        )

    def _save_thread_state(self, thread_id: str) -> None:
        self.checkpointer.put(
            self._thread_config(thread_id),
            empty_checkpoint(),
            {"state": asdict(self.state)},
            {},
        )

    def _keyword_score(self, text: str, keywords: List[str]) -> int:
        tl = text.lower()
        return sum(1 for k in keywords if k in tl)

    def _score_to_conf(self, raw_score: int) -> float:
        if raw_score <= 0:
            return 0.0
        return round(min(0.95, 0.5 + raw_score * 0.12), 2)

    def _compute_domain_scores(self, user_query: str) -> Dict[str, float]:
        domain_conf: Dict[str, float] = {}
        for domain, story_ids in DOMAIN_TO_STORIES.items():
            best_raw = 0
            for sid in story_ids:
                cfg = STORY_CATALOG[sid]
                best_raw = max(best_raw, self._keyword_score(user_query, cfg.keywords))
            domain_conf[domain] = self._score_to_conf(best_raw)
        return domain_conf

    def _top_two_domains(self, domain_scores: Dict[str, float]) -> Tuple[str, float, Optional[str], float, float]:
        ranked = sorted(domain_scores.items(), key=lambda kv: kv[1], reverse=True)
        top_domain, top_score = ranked[0]
        second_domain, second_score = ranked[1] if len(ranked) > 1 else (None, 0.0)
        margin = round(top_score - second_score, 2)
        return top_domain, top_score, second_domain, second_score, margin

    def _compute_continuation_score(self) -> float:
        if not self.state.active_story_id or self.state.last_active_turn_index is None:
            return 0.0
        delta = self.state.turn_index - self.state.last_active_turn_index
        if delta <= 1:
            return 0.85
        if delta == 2:
            return 0.7
        if delta == 3:
            return 0.55
        return 0.2

    def _is_pending_valid(self) -> bool:
        if not self.state.pending_slot_type or self.state.pending_turn_created is None:
            return False
        return (self.state.turn_index - self.state.pending_turn_created) <= PENDING_TTL_TURNS

    def _slot_value_present(self, user_query: str) -> bool:
        if self.state.pending_slot_type == "member_id":
            return extract_explicit_member_id(user_query) is not None
        return False

    def _is_fresh_domain_high(self, top_score: float, margin: float) -> bool:
        return top_score >= FRESH_HIGH and margin >= FRESH_MARGIN

    def _story_router(self, domain: str, user_query: str) -> RouteDecision:
        candidate_ids = DOMAIN_TO_STORIES[domain]
        scored: List[tuple[str, int]] = []
        for sid in candidate_ids:
            cfg = STORY_CATALOG[sid]
            scored.append((sid, self._keyword_score(user_query, cfg.keywords)))

        scored.sort(key=lambda x: x[1], reverse=True)
        best_story, best_score = scored[0]
        confidence = min(0.99, 0.6 + best_score * 0.1)

        return RouteDecision(
            target=best_story,
            confidence=round(confidence, 2),
            rationale=f"Selected story='{best_story}' in domain='{domain}' by keyword score ({best_score}).",
            fallback_target=self.state.active_story_id,
            missing_slots=[],
        )

    def _coerce_domain(self, raw: str) -> Optional[str]:
        value = (raw or "").strip().lower()
        if value in DOMAIN_TO_STORIES:
            return value
        if value in {"membership", "fraud", "membershipfraud", "membership_fraud"}:
            return "membership_fraud"
        if value in {"business", "marketing", "business_marketing"}:
            return "business_marketing"
        if value in {"data", "data_science", "datascience"}:
            return "data_science"
        if value in {"clarify", "ambiguous", "unknown", "none"}:
            return "clarify"
        return None

    def _llm_domain_route(self, user_query: str) -> Tuple[str, float, str]:
        domain_list = ", ".join(sorted(DOMAIN_TO_STORIES.keys()))
        recent = self.state.messages[-6:]
        domain_guide = (
            "DOMAIN GUIDE:\n"
            "- business_marketing: campaign performance, campaign feedback themes, channel/segment breakdowns, weekly metrics, CTR/CAC/ROAS/spend, creative/messaging adjustments, lead prioritization, outreach drafts, template-driven follow-ups.\n"
            "- data_science: member workout analytics, training trends, heart-rate zones, performance anomalies, workout-type segmentation.\n"
            "- membership_fraud: security alerts, suspicious logins, risk events, account/device/location verification.\n"
            "- clarify: only when domain cannot be chosen reliably.\n"
            "\n"
            "DISAMBIGUATION RULES:\n"
            "- If query includes CTR/CAC/ROAS/campaign/channel/target segment/marketing performance, or leads/prospects/follow-up outreach, prefer business_marketing.\n"
            "- Do not route to data_science just because dates, trends, or analysis language are present.\n"
            "- DataScience usually involves workouts and often a member_id; business_marketing campaign analysis does not require member_id.\n"
        )
        system = (
            "You are a top-level orchestrator router.\n"
            "Choose exactly one domain based on the user message and recent chat context.\n"
            f"Valid domains: {domain_list}, clarify.\n"
            "Use clarify only if domain cannot be chosen reliably.\n"
            "If the current user message clearly indicates a different domain than the active domain, route to the new domain.\n"
            f"{domain_guide}"
        )
        user = (
            f"ACTIVE_DOMAIN: {self.state.active_domain}\n"
            f"ACTIVE_STORY: {self.state.active_story_id}\n"
            f"PENDING_SLOT_TYPE: {self.state.pending_slot_type}\n"
            f"RECENT_MESSAGES: {recent}\n"
            f"USER_QUERY: {user_query}"
        )
        structured = self.llm.with_structured_output(DomainRouteOutput)
        out = structured.invoke([("system", system), ("user", user)])
        domain = self._coerce_domain(out.domain) or "clarify"
        return domain, float(out.confidence), out.rationale

    def _llm_story_route(self, domain: str, user_query: str) -> Tuple[RouteDecision, str]:
        candidates = DOMAIN_TO_STORIES[domain]
        titles = {sid: STORY_CATALOG[sid].title for sid in candidates}
        keywords = {sid: STORY_CATALOG[sid].keywords for sid in candidates}

        system = (
            "You are a second-level story router.\n"
            "Select exactly one story_id from the allowed list for the chosen domain.\n"
            "Output only a valid allowed story_id."
        )
        user = (
            f"DOMAIN: {domain}\n"
            f"ALLOWED_STORY_IDS: {candidates}\n"
            f"STORY_TITLES: {titles}\n"
            f"STORY_KEYWORDS: {keywords}\n"
            f"ACTIVE_STORY: {self.state.active_story_id}\n"
            f"USER_QUERY: {user_query}"
        )
        structured = self.llm.with_structured_output(StoryRouteOutput)
        out = structured.invoke([("system", system), ("user", user)])

        picked = out.story_id if out.story_id in candidates else None
        if not picked:
            fallback = self._story_router(domain, user_query)
            return (
                RouteDecision(
                    target=fallback.target,
                    confidence=max(0.51, fallback.confidence),
                    rationale=f"LLM returned invalid story_id='{out.story_id}'. Fallback to keyword router.",
                    fallback_target=fallback.fallback_target,
                    missing_slots=fallback.missing_slots,
                ),
                "deterministic_fallback_invalid_story_id",
            )

        return (
            RouteDecision(
                target=picked,
                confidence=float(out.confidence),
                rationale=out.rationale or f"LLM selected story '{picked}' in domain '{domain}'.",
                fallback_target=self.state.active_story_id,
                missing_slots=[],
            ),
            "llm",
        )

    def _sync_member(self, user_query: str) -> None:
        norm = extract_explicit_member_id(user_query)
        if norm:
            self.state.member = CanonicalMember(
                member_id=norm,
                member_id_raw=norm,
                member_id_source="user_text",
                confidence=1.0,
            )

    def _merge_state_updates(self, result: StoryResult, domain: str) -> None:
        if result.state_updates_global:
            member_update = result.state_updates_global.get("member")
            if isinstance(member_update, dict) and member_update.get("member_id"):
                self.state.member.member_id = member_update["member_id"]

        if domain not in self.state.domain_states:
            self.state.domain_states[domain] = {"domain_name": domain, "domain_context": {}}

        if result.state_updates_domain:
            self.state.domain_states[domain].update(result.state_updates_domain)

    def _clear_pending_slot(self) -> None:
        self.state.pending_slot_type = None
        self.state.pending_slot_target_story_id = None
        self.state.pending_turn_created = None

    def _update_pending_from_result(self, result: StoryResult) -> None:
        self.state.pending_question = result.follow_up_question
        if result.follow_up_question and "member_id" in result.follow_up_question.lower():
            self.state.pending_slot_type = "member_id"
            self.state.pending_slot_target_story_id = result.story_id
            self.state.pending_turn_created = self.state.turn_index
        elif result.follow_up_question is None:
            self._clear_pending_slot()

    def _clarify_response(self, top_domain: Optional[str], second_domain: Optional[str]) -> str:
        if top_domain and second_domain:
            return (
                f"Do you want help with {top_domain} or {second_domain}? "
                f"I can also continue with {self.state.active_domain or 'your previous topic'} if that is what you meant."
            )
        return "I can route this to BusinessMarketing, DataScience, or MembershipFraud. Please clarify which area you want."

    def _route_turn_graph(self, user_query: str) -> Tuple[Optional[str], Optional[str], str, Dict[str, Any], bool]:
        domain_scores = self._compute_domain_scores(user_query)
        top_domain, top_score, second_domain, second_score, margin = self._top_two_domains(domain_scores)
        continuation_score = self._compute_continuation_score()
        pending_valid = self._is_pending_valid()

        metrics = {
            "continuation_score": continuation_score,
            "domain_scores": domain_scores,
            "top_domain": top_domain,
            "top_score": top_score,
            "second_domain": second_domain,
            "second_score": second_score,
            "margin": margin,
            "active_domain_before": self.state.active_domain,
            "active_story_before": self.state.active_story_id,
            "pending_slot_used": False,
            "router_style": "llm_langgraph",
            "domain_selected_by": "unknown",
            "story_selected_by": "unknown",
        }

        if pending_valid and self._slot_value_present(user_query) and self.state.pending_slot_target_story_id:
            sid = self.state.pending_slot_target_story_id
            dom = STORY_CATALOG[sid].domain
            metrics["pending_slot_used"] = True
            metrics["domain_selected_by"] = "pending_slot_resume"
            metrics["story_selected_by"] = "pending_slot_resume"
            return dom, sid, "pending_slot_fulfilled", metrics, False

        try:
            domain, domain_conf, domain_rationale = self._llm_domain_route(user_query)
            metrics["domain_llm_confidence"] = round(domain_conf, 2)
            metrics["domain_llm_rationale"] = domain_rationale
            metrics["domain_selected_by"] = "llm"
        except Exception as exc:
            self.state.global_errors.append(f"domain_router_llm_error: {exc}")
            domain = top_domain if self._is_fresh_domain_high(top_score, margin) else "clarify"
            metrics["domain_llm_confidence"] = 0.0
            metrics["domain_llm_rationale"] = "LLM failed; deterministic fallback applied."
            metrics["domain_selected_by"] = "deterministic_fallback"

        # Guardrail: when lexical signal is clearly strong, do not allow LLM clarify to block routing.
        if domain == "clarify" and self._is_fresh_domain_high(top_score, margin):
            domain = top_domain
            metrics["domain_llm_rationale"] = (
                f"{metrics.get('domain_llm_rationale', '')} "
                f"Overridden by strong lexical domain signal: top_domain={top_domain}, top_score={top_score}, margin={margin}."
            ).strip()
            metrics["domain_guardrail_override"] = "llm_clarify_to_top_domain"
            metrics["domain_selected_by"] = "lexical_guardrail_override"

        # Guardrail: when lexical signal is clearly strong, do not allow LLM to pick a different domain.
        if (
            domain in DOMAIN_TO_STORIES
            and top_domain in DOMAIN_TO_STORIES
            and domain != top_domain
            and self._is_fresh_domain_high(top_score, margin)
        ):
            domain = top_domain
            metrics["domain_llm_rationale"] = (
                f"{metrics.get('domain_llm_rationale', '')} "
                f"Overridden by strong lexical domain signal: top_domain={top_domain}, top_score={top_score}, margin={margin}."
            ).strip()
            metrics["domain_guardrail_override"] = "llm_domain_to_top_domain"
            metrics["domain_selected_by"] = "lexical_guardrail_override"

        if domain == "clarify":
            return None, None, "ambiguous_clarify_llm", metrics, True

        if domain not in DOMAIN_TO_STORIES:
            return None, None, "invalid_domain_clarify", metrics, True

        try:
            story_decision, story_selected_by = self._llm_story_route(domain, user_query)
            metrics["story_llm_confidence"] = round(story_decision.confidence, 2)
            metrics["story_llm_rationale"] = story_decision.rationale
            metrics["story_selected_by"] = story_selected_by
        except Exception as exc:
            self.state.global_errors.append(f"story_router_llm_error: {exc}")
            story_decision = self._story_router(domain, user_query)
            metrics["story_llm_confidence"] = 0.0
            metrics["story_llm_rationale"] = "LLM failed; deterministic story fallback applied."
            metrics["story_selected_by"] = "deterministic_fallback_exception"

        return domain, story_decision.target, "llm_domain_story_route", metrics, False

    def _build_graph(self):
        builder = StateGraph(OrchestratorGraphState)

        def route_node(gs: OrchestratorGraphState) -> OrchestratorGraphState:
            user_query = gs["user_query"]
            domain, story_id, reason, metrics, should_clarify = self._route_turn_graph(user_query)
            self.state.router_reason = reason
            self.state.router_metrics = metrics
            self.state.route_trace.append(
                {
                    "router": "decision_router",
                    "router_reason": reason,
                    "selected_domain": domain,
                    "selected_story_id": story_id,
                    "continuation_score": metrics.get("continuation_score"),
                    "domain_scores": metrics.get("domain_scores"),
                    "margin": metrics.get("margin"),
                    "pending_slot_used": metrics.get("pending_slot_used"),
                    "domain_llm_confidence": metrics.get("domain_llm_confidence"),
                    "story_llm_confidence": metrics.get("story_llm_confidence"),
                    "domain_selected_by": metrics.get("domain_selected_by"),
                    "story_selected_by": metrics.get("story_selected_by"),
                }
            )
            return {
                "domain": domain,
                "story_id": story_id,
                "reason": reason,
                "metrics": metrics,
                "should_clarify": should_clarify,
            }

        def clarify_node(gs: OrchestratorGraphState) -> OrchestratorGraphState:
            metrics = gs.get("metrics", {})
            text = self._clarify_response(metrics.get("top_domain"), metrics.get("second_domain"))
            self.state.last_response = text
            self.state.pending_question = text
            self.state.messages.append({"role": "assistant", "content": text})
            return {"response": text, "story_output": {}, "follow_up_question": text}

        def story_node(gs: OrchestratorGraphState) -> OrchestratorGraphState:
            domain = gs["domain"]
            story_id = gs["story_id"]
            user_query = gs["user_query"]
            assert domain is not None and story_id is not None

            self.state.active_domain = domain
            self.state.active_story_id = story_id
            if domain not in self.state.domain_states:
                self.state.domain_states[domain] = {"domain_name": domain, "domain_context": {}}

            req = StoryRequest(
                story_id=story_id,
                user_query=user_query,
                messages=self.state.messages,
                member=self.state.member,
                domain_context=self.state.domain_states[domain],
            )
            handler = STORY_CATALOG[story_id].handler
            result = handler(req)
            self._merge_state_updates(result, domain=domain)
            self._update_pending_from_result(result)
            self.state.last_response = result.response_text
            self.state.last_active_turn_index = self.state.turn_index
            self.state.messages.append({"role": "assistant", "content": result.response_text})

            return {
                "response": result.response_text,
                "story_output": result.story_output,
                "follow_up_question": result.follow_up_question,
            }

        def route_after_route(gs: OrchestratorGraphState) -> str:
            return "clarify" if gs.get("should_clarify") else "story"

        builder.add_node("route", route_node)
        builder.add_node("clarify", clarify_node)
        builder.add_node("invoke_story", story_node)
        builder.set_entry_point("route")
        builder.add_conditional_edges("route", route_after_route, {"clarify": "clarify", "story": "invoke_story"})
        builder.add_edge("clarify", END)
        builder.add_edge("invoke_story", END)
        return builder.compile()

    def get_orchestrator_mermaid(self) -> str:
        return self.graph.get_graph().draw_mermaid()

    def get_orchestrator_graph(self):
        return self.graph

    def invoke(self, user_query: str, thread_id: str = "default") -> Dict[str, Any]:
        self.state = self._load_thread_state(thread_id=thread_id)
        self.state.thread_id = thread_id
        self.state.turn_index += 1
        self.state.messages.append({"role": "user", "content": user_query})
        self._sync_member(user_query)

        out_state = self.graph.invoke({"user_query": user_query})
        reason = out_state.get("reason")
        metrics = out_state.get("metrics", {})
        response = out_state.get("response", "")
        story_output = out_state.get("story_output")
        follow_up_question = out_state.get("follow_up_question")

        self._save_thread_state(thread_id=thread_id)

        return {
            "response": response,
            "thread_id": thread_id,
            "active_domain": self.state.active_domain,
            "active_story_id": self.state.active_story_id,
            "story_output": story_output,
            "follow_up_question": follow_up_question,
            "router_reason": reason,
            "router_metrics": metrics,
            "route_trace": self.state.route_trace,
            "state": asdict(self.state),
        }
