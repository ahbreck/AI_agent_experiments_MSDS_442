from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List

from .contracts import StoryRequest, StoryResult
from .stories.business_marketing_story1 import run_business_marketing_story1
from .stories.business_marketing_story2 import run_business_marketing_story2
from .stories.business_marketing_story3 import run_business_marketing_story3
from .stories.data_science_story1 import run_data_science_story1
from .stories.data_science_story2 import run_data_science_story2
from .stories.data_science_story3 import run_data_science_story3
from .stories.membership_fraud_story1 import run_membership_fraud_story1
from .stories.membership_fraud_story2 import run_membership_fraud_story2
from .stories.membership_fraud_story3 import run_membership_fraud_story3

StoryHandler = Callable[[StoryRequest], StoryResult]


@dataclass(frozen=True)
class StoryConfig:
    story_id: str
    domain: str
    title: str
    keywords: List[str]
    handler: StoryHandler


STORY_CATALOG: Dict[str, StoryConfig] = {
    "bm_story_1": StoryConfig(
        story_id="bm_story_1",
        domain="business_marketing",
        title="Campaign Feedback Themes + 3 Adjustments",
        keywords=["campaign", "feedback", "channel", "sentiment", "theme", "marketing"],
        handler=run_business_marketing_story1,
    ),
    "bm_story_2": StoryConfig(
        story_id="bm_story_2",
        domain="business_marketing",
        title="Weekly Campaign KPI Diagnostics",
        keywords=[
            "weekly",
            "metrics",
            "ctr",
            "cac",
            "roas",
            "spend",
            "underperform",
            "threshold",
            "segment",
            "campaign performance",
        ],
        handler=run_business_marketing_story2,
    ),
    "bm_story_3": StoryConfig(
        story_id="bm_story_3",
        domain="business_marketing",
        title="Lead Prioritization + Follow-up Drafts",
        keywords=[
            "lead",
            "prioritized leads",
            "prospect",
            "site behavior",
            "app behavior",
            "intent",
            "follow-up",
            "follow up",
            "outreach",
            "message template",
            "draft message",
            "cart abandonment",
            "trial",
        ],
        handler=run_business_marketing_story3,
    ),
    "ds_story_1": StoryConfig(
        story_id="ds_story_1",
        domain="data_science",
        title="Workout Data Visualization Builder",
        keywords=[
            "chart",
            "graph",
            "plot",
            "visual",
            "visualize",
            "visualization",
            "line chart",
            "bar chart",
            "scatter",
            "histogram",
            "box plot",
        ],
        handler=run_data_science_story1,
    ),
    "ds_story_2": StoryConfig(
        story_id="ds_story_2",
        domain="data_science",
        title="Workout Trend Analytics",
        keywords=["workout", "trend", "improv", "zone", "anomal", "performance", "strive", "cadence"],
        handler=run_data_science_story2,
    ),
    "ds_story_3": StoryConfig(
        story_id="ds_story_3",
        domain="data_science",
        title="Peer Benchmark Comparison + Improvement Suggestions",
        keywords=[
            "peer",
            "peers",
            "benchmark",
            "compared to peers",
            "compare to peers",
            "weekly workouts",
            "session length",
            "consistency",
        ],
        handler=run_data_science_story3,
    ),
    "mf_story_1": StoryConfig(
        story_id="mf_story_1",
        domain="membership_fraud",
        title="Security Event Explanation + Actions",
        keywords=[
            "fraud",
            "security alert",
            "suspicious login",
            "risk event",
            "unknown device",
            "new location",
            "account takeover",
            "compromised account",
            "device verification",
            "location verification",
        ],
        handler=run_membership_fraud_story1,
    ),
    "mf_story_2": StoryConfig(
        story_id="mf_story_2",
        domain="membership_fraud",
        title="Account Issue Triage + Queue Routing",
        keywords=[
            "cannot login",
            "can't log in",
            "password reset",
            "locked out",
            "billing",
            "charged",
            "invoice",
            "payment failed",
            "refund",
            "renewal",
            "auto renew",
            "subscription",
            "expired membership",
            "cancel membership",
        ],
        handler=run_membership_fraud_story2,
    ),
    "mf_story_3": StoryConfig(
        story_id="mf_story_3",
        domain="membership_fraud",
        title="Membership Tier Fit + Upgrade/Downgrade Guidance",
        keywords=[
            "membership tier",
            "tier optimization",
            "tier fit",
            "plan fit",
            "best plan",
            "upgrade",
            "downgrade",
            "membership level",
            "class limit",
            "utilization",
            "feature usage",
            "benefits usage",
            "should i change my plan",
        ],
        handler=run_membership_fraud_story3,
    ),
}

DOMAIN_TO_STORIES: Dict[str, List[str]] = {
    "business_marketing": ["bm_story_1", "bm_story_2", "bm_story_3"],
    "data_science": ["ds_story_1", "ds_story_2", "ds_story_3"],
    "membership_fraud": ["mf_story_1", "mf_story_2", "mf_story_3"],
}

# Planned expansion slots for stories not yet wired.
PLANNED_STORIES = {
    "business_marketing": [],
    "data_science": [],
    "membership_fraud": [],
}
