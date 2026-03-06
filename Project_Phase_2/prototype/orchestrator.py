from __future__ import annotations

from dataclasses import asdict
from typing import Any, Dict, List, Optional

from .catalog import DOMAIN_TO_STORIES, STORY_CATALOG
from .contracts import CanonicalMember, GlobalState, RouteDecision, StoryRequest, StoryResult
from .utils import normalize_member_id


class AgenticOrchestrator:
    def __init__(self, state: Optional[GlobalState] = None):
        self.state = state or GlobalState()

    def _keyword_score(self, text: str, keywords: List[str]) -> int:
        tl = text.lower()
        return sum(1 for k in keywords if k in tl)

    def _operator_router(self, user_query: str) -> RouteDecision:
        domain_scores: Dict[str, int] = {}
        for domain, story_ids in DOMAIN_TO_STORIES.items():
            best = 0
            for sid in story_ids:
                cfg = STORY_CATALOG[sid]
                best = max(best, self._keyword_score(user_query, cfg.keywords))
            domain_scores[domain] = best

        best_domain = max(domain_scores, key=domain_scores.get)
        best_score = domain_scores[best_domain]

        if best_score == 0:
            return RouteDecision(
                target="clarify",
                confidence=0.0,
                rationale="No domain keywords matched.",
                fallback_target=self.state.active_domain,
                missing_slots=[],
            )

        confidence = min(0.99, 0.55 + best_score * 0.1)
        return RouteDecision(
            target=best_domain,
            confidence=round(confidence, 2),
            rationale=f"Highest keyword score for domain='{best_domain}' ({best_score}).",
            fallback_target=self.state.active_domain,
            missing_slots=[],
        )

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
        norm = normalize_member_id(user_query)
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

    def invoke(self, user_query: str) -> Dict[str, Any]:
        self.state.messages.append({"role": "user", "content": user_query})
        self._sync_member(user_query)

        op_decision = self._operator_router(user_query)
        self.state.route_trace.append(
            {
                "router": "operator_router",
                "target": op_decision.target,
                "confidence": op_decision.confidence,
                "rationale": op_decision.rationale,
            }
        )

        if op_decision.target == "clarify":
            text = (
                "I can route this to BusinessMarketing, DataScience, or MembershipFraud. "
                "Please clarify which area you want."
            )
            self.state.last_response = text
            self.state.pending_question = text
            self.state.messages.append({"role": "assistant", "content": text})
            return {
                "response": text,
                "active_domain": None,
                "active_story_id": None,
                "route_trace": self.state.route_trace,
                "state": asdict(self.state),
            }

        domain = op_decision.target
        st_decision = self._story_router(domain, user_query)
        self.state.route_trace.append(
            {
                "router": "story_router",
                "target": st_decision.target,
                "confidence": st_decision.confidence,
                "rationale": st_decision.rationale,
            }
        )

        self.state.active_domain = domain
        self.state.active_story_id = st_decision.target

        if domain not in self.state.domain_states:
            self.state.domain_states[domain] = {"domain_name": domain, "domain_context": {}}

        req = StoryRequest(
            story_id=st_decision.target,
            user_query=user_query,
            messages=self.state.messages,
            member=self.state.member,
            domain_context=self.state.domain_states[domain],
        )

        handler = STORY_CATALOG[st_decision.target].handler
        result = handler(req)

        self._merge_state_updates(result, domain=domain)

        self.state.pending_question = result.follow_up_question
        self.state.last_response = result.response_text
        self.state.messages.append({"role": "assistant", "content": result.response_text})

        return {
            "response": result.response_text,
            "active_domain": domain,
            "active_story_id": st_decision.target,
            "story_output": result.story_output,
            "follow_up_question": result.follow_up_question,
            "route_trace": self.state.route_trace,
            "state": asdict(self.state),
        }
