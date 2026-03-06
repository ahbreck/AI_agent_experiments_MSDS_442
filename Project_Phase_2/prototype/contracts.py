from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional

DomainName = Literal["business_marketing", "data_science", "membership_fraud", "clarify"]


@dataclass
class CanonicalMember:
    member_id: Optional[str] = None
    member_id_raw: Optional[str] = None
    member_id_source: Optional[str] = None
    confidence: float = 0.0


@dataclass
class RouteDecision:
    target: str
    confidence: float
    rationale: str
    fallback_target: Optional[str] = None
    missing_slots: List[str] = field(default_factory=list)


@dataclass
class StoryRequest:
    story_id: str
    user_query: str
    messages: List[Dict[str, str]]
    member: CanonicalMember
    domain_context: Dict[str, Any] = field(default_factory=dict)
    story_input: Dict[str, Any] = field(default_factory=dict)


@dataclass
class StoryResult:
    story_id: str
    response_text: str
    story_output: Dict[str, Any] = field(default_factory=dict)
    state_updates_global: Dict[str, Any] = field(default_factory=dict)
    state_updates_domain: Dict[str, Any] = field(default_factory=dict)
    follow_up_question: Optional[str] = None
    handoff: Optional[Dict[str, Any]] = None


@dataclass
class GlobalState:
    thread_id: str = "default"
    session_id: str = "local"
    messages: List[Dict[str, str]] = field(default_factory=list)
    member: CanonicalMember = field(default_factory=CanonicalMember)
    active_domain: Optional[str] = None
    active_story_id: Optional[str] = None
    route_trace: List[Dict[str, Any]] = field(default_factory=list)
    domain_states: Dict[str, Dict[str, Any]] = field(default_factory=dict)
    pending_question: Optional[str] = None
    pending_slot_type: Optional[str] = None
    pending_slot_target_story_id: Optional[str] = None
    pending_turn_created: Optional[int] = None
    turn_index: int = 0
    last_active_turn_index: Optional[int] = None
    router_reason: Optional[str] = None
    router_metrics: Dict[str, Any] = field(default_factory=dict)
    last_response: Optional[str] = None
    global_errors: List[str] = field(default_factory=list)
