from __future__ import annotations

import re
import sqlite3
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Literal, Optional

from ..contracts import StoryRequest, StoryResult
from ..utils import extract_explicit_member_id, member_id_aliases

PROJECT_PHASE_2 = Path(__file__).resolve().parents[2]
DB_PATH = PROJECT_PHASE_2 / "kb" / "MembershipFraud" / "membership_fraud.db"

Timeframe = Literal["most_recent", "last_7_days", "last_30_days"]


def _infer_timeframe(user_text: str) -> Timeframe:
    tl = user_text.lower()
    if any(k in tl for k in ["last week", "past 7", "last 7"]):
        return "last_7_days"
    if any(k in tl for k in ["last month", "past 30", "last 30"]):
        return "last_30_days"
    return "most_recent"


def _read_security_events(member_id: str, timeframe: Timeframe, max_events: int = 5) -> List[Dict[str, Any]]:
    aliases = member_id_aliases(member_id)
    if not aliases:
        return []
    placeholders = ",".join(["?"] * len(aliases))

    with sqlite3.connect(DB_PATH) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()

        if timeframe == "most_recent":
            sql = f"""
            SELECT event_id, member_id, event_ts, login_location, device_type, risk_level, trigger_reason, recommended_action
            FROM security_events
            WHERE REPLACE(REPLACE(UPPER(member_id), '-', ''), '_', '') IN ({placeholders})
            ORDER BY event_ts DESC
            LIMIT ?
            """
            return [dict(r) for r in cur.execute(sql, (*aliases, max_events)).fetchall()]

        days = 7 if timeframe == "last_7_days" else 30
        cutoff = (datetime.now() - timedelta(days=days)).isoformat(timespec="seconds")
        sql = f"""
        SELECT event_id, member_id, event_ts, login_location, device_type, risk_level, trigger_reason, recommended_action
        FROM security_events
        WHERE REPLACE(REPLACE(UPPER(member_id), '-', ''), '_', '') IN ({placeholders}) AND event_ts >= ?
        ORDER BY event_ts DESC
        LIMIT ?
        """
        return [dict(r) for r in cur.execute(sql, (*aliases, cutoff, max_events)).fetchall()]


def _guide_actions(event: Dict[str, Any]) -> List[str]:
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


def run_membership_fraud_story1(req: StoryRequest) -> StoryResult:
    member_id = extract_explicit_member_id(req.user_query) or req.member.member_id
    timeframe = _infer_timeframe(req.user_query)

    if not member_id:
        ask = "I can check that security alert, what is your member_id (e.g., MB001)?"
        return StoryResult(
            story_id=req.story_id,
            response_text=ask,
            follow_up_question=ask,
            story_output={"needs_member_id": True, "timeframe": timeframe},
        )

    events = _read_security_events(member_id=member_id, timeframe=timeframe, max_events=3)
    if not events:
        msg = f"I could not find security alerts for {member_id} in timeframe={timeframe}."
        return StoryResult(
            story_id=req.story_id,
            response_text=msg,
            story_output={"member_id": member_id, "timeframe": timeframe, "retrieved_events": []},
            state_updates_global={"member": {"member_id": member_id}},
        )

    latest = events[0]
    actions = _guide_actions(latest)
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

    return StoryResult(
        story_id=req.story_id,
        response_text=msg,
        story_output={
            "member_id": member_id,
            "timeframe": timeframe,
            "retrieved_events": events,
            "recommended_actions": actions,
        },
        state_updates_global={"member": {"member_id": member_id}},
        state_updates_domain={"last_story_summary": f"Returned {len(events)} security event(s)."},
    )
