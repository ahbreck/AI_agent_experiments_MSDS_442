from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional, Tuple

from langgraph.checkpoint.base import empty_checkpoint
from langgraph.checkpoint.memory import MemorySaver

from .catalog import DOMAIN_TO_STORIES, STORY_CATALOG
from .contracts import CanonicalMember, GlobalState, RouteDecision, StoryRequest, StoryResult
from .utils import extract_explicit_member_id


CONT_HIGH = 0.70
FRESH_HIGH = 0.62
FRESH_MARGIN = 0.20
PENDING_TTL_TURNS = 3


class AgenticOrchestrator:
    def __init__(
        self,
        state: Optional[GlobalState] = None,
        checkpointer: Optional[MemorySaver] = None,
        checkpoint_ns: str = "phase2_prototype",
    ):
        self.state = state or GlobalState()
        self.checkpointer = checkpointer or MemorySaver()
        self.checkpoint_ns = checkpoint_ns

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

    def _route_turn(self, user_query: str) -> Tuple[Optional[str], Optional[str], str, Dict[str, Any], bool]:
        domain_scores = self._compute_domain_scores(user_query)
        top_domain, top_score, second_domain, second_score, margin = self._top_two_domains(domain_scores)
        continuation_score = self._compute_continuation_score()

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
        }

        pending_valid = self._is_pending_valid()
        if pending_valid and self._slot_value_present(user_query) and self.state.pending_slot_target_story_id:
            sid = self.state.pending_slot_target_story_id
            dom = STORY_CATALOG[sid].domain
            metrics["pending_slot_used"] = True
            return dom, sid, "pending_slot_fulfilled", metrics, False

        if pending_valid and self._is_fresh_domain_high(top_score, margin) and self.state.active_domain and top_domain != self.state.active_domain:
            sid = self._story_router(top_domain, user_query).target
            return top_domain, sid, "pending_slot_abandoned_fresh_route", metrics, False

        if self._is_fresh_domain_high(top_score, margin):
            sid = self._story_router(top_domain, user_query).target
            reason = "continuation_but_fresh_domain" if (continuation_score >= CONT_HIGH and self.state.active_domain and top_domain != self.state.active_domain) else "fresh_domain_route"
            return top_domain, sid, reason, metrics, False

        if continuation_score >= CONT_HIGH and self.state.active_domain:
            if self.state.active_story_id and self.state.active_story_id in STORY_CATALOG:
                return self.state.active_domain, self.state.active_story_id, "continuation_to_active_story", metrics, False
            sid = self._story_router(self.state.active_domain, user_query).target
            return self.state.active_domain, sid, "continuation_to_active_domain", metrics, False

        return None, None, "ambiguous_clarify", metrics, True

    def invoke(self, user_query: str, thread_id: str = "default") -> Dict[str, Any]:
        self.state = self._load_thread_state(thread_id=thread_id)
        self.state.thread_id = thread_id
        self.state.turn_index += 1
        self.state.messages.append({"role": "user", "content": user_query})
        self._sync_member(user_query)

        domain, story_id, reason, metrics, should_clarify = self._route_turn(user_query)
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
            }
        )

        if should_clarify:
            top = metrics.get("top_domain")
            second = metrics.get("second_domain")
            text = self._clarify_response(top, second)
            self.state.last_response = text
            self.state.pending_question = text
            self.state.messages.append({"role": "assistant", "content": text})
            self._save_thread_state(thread_id=thread_id)
            return {
                "response": text,
                "thread_id": thread_id,
                "active_domain": self.state.active_domain,
                "active_story_id": self.state.active_story_id,
                "router_reason": reason,
                "router_metrics": metrics,
                "route_trace": self.state.route_trace,
                "state": asdict(self.state),
            }

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
        self._save_thread_state(thread_id=thread_id)

        return {
            "response": result.response_text,
            "thread_id": thread_id,
            "active_domain": domain,
            "active_story_id": story_id,
            "story_output": result.story_output,
            "follow_up_question": result.follow_up_question,
            "router_reason": reason,
            "router_metrics": metrics,
            "route_trace": self.state.route_trace,
            "state": asdict(self.state),
        }
