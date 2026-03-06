from __future__ import annotations

from dataclasses import dataclass
from typing import Callable, Dict, List

from .contracts import StoryRequest, StoryResult
from .stories.business_marketing_story1 import run_business_marketing_story1
from .stories.data_science_story2 import run_data_science_story2
from .stories.membership_fraud_story1 import run_membership_fraud_story1

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
    "ds_story_2": StoryConfig(
        story_id="ds_story_2",
        domain="data_science",
        title="Workout Trend Analytics",
        keywords=["workout", "trend", "improv", "zone", "anomal", "performance", "strive", "cadence"],
        handler=run_data_science_story2,
    ),
    "mf_story_1": StoryConfig(
        story_id="mf_story_1",
        domain="membership_fraud",
        title="Security Event Explanation + Actions",
        keywords=["fraud", "security", "login", "alert", "risk", "account", "device", "location"],
        handler=run_membership_fraud_story1,
    ),
}

DOMAIN_TO_STORIES: Dict[str, List[str]] = {
    "business_marketing": ["bm_story_1"],
    "data_science": ["ds_story_2"],
    "membership_fraud": ["mf_story_1"],
}

# Planned expansion slots for a total of 9 stories.
# Fill handlers/keywords as each story is built.
PLANNED_STORIES = {
    "business_marketing": ["bm_story_2", "bm_story_3"],
    "data_science": ["ds_story_1", "ds_story_3"],
    "membership_fraud": ["mf_story_2", "mf_story_3"],
}
