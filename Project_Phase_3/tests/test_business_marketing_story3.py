import sys
import unittest
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.contracts import CanonicalMember, StoryRequest  # noqa: E402
from prototype.stories.business_marketing_story3 import run_business_marketing_story3  # noqa: E402


class TestBusinessMarketingStory3(unittest.TestCase):
    def _invoke(self, query: str, messages=None, domain_context=None):
        req = StoryRequest(
            story_id="bm_story_3",
            user_query=query,
            messages=messages or [],
            member=CanonicalMember(),
            domain_context=domain_context or {},
        )
        return run_business_marketing_story3(req)

    def test_defaults_when_timeframe_and_channel_missing(self):
        out = self._invoke("Generate prioritized leads and draft follow-up messages for top 5")
        payload = out.story_output
        assumptions = payload.get("assumptions", [])
        self.assertEqual(payload.get("lookback_days"), 7)
        self.assertEqual(payload.get("channel"), "email")
        self.assertIn(payload.get("planning_source"), {"llm_validated", "deterministic_fallback_llm_unavailable"})
        self.assertTrue(any("defaulted to last 7 days" in x for x in assumptions))
        self.assertTrue(any("defaulted to email" in x for x in assumptions))

    def test_unsupported_lookback_is_coerced(self):
        out = self._invoke("Generate top 8 leads for last 10 days and draft email follow-ups")
        payload = out.story_output
        assumptions = payload.get("assumptions", [])
        self.assertEqual(payload.get("lookback_days"), 7)
        self.assertTrue(any("mapped to supported 7-day lookback" in x for x in assumptions))

    def test_suppression_filter_applies_for_email(self):
        out = self._invoke("Show top 20 leads for last 7 days and draft email outreach")
        payload = out.story_output
        ranked = payload.get("ranked_leads", [])
        ids = {r.get("lead_id") for r in ranked}
        self.assertNotIn("L003", ids)  # L003 is email-suppressed in seed data.
        self.assertGreaterEqual(int(payload.get("suppressed_excluded_count", 0)), 1)

    def test_channel_specific_suppression_for_call(self):
        out = self._invoke("Show top 20 leads for last 7 days and draft call outreach")
        payload = out.story_output
        ranked = payload.get("ranked_leads", [])
        ids = {r.get("lead_id") for r in ranked}
        self.assertIn("L003", ids)  # Email suppression should not remove call outreach.
        self.assertNotIn("L008", ids)  # L008 is call-suppressed in seed data.

    def test_ranked_leads_include_draft_payload(self):
        out = self._invoke("Generate top 5 cycling leads and draft email follow-up messages")
        payload = out.story_output
        ranked = payload.get("ranked_leads", [])
        self.assertGreater(len(ranked), 0)
        first = ranked[0]
        self.assertIn("template_id", first)
        self.assertIn("draft_message", first)
        self.assertIsInstance(first["draft_message"], dict)
        self.assertIn("body", first["draft_message"])

    def test_refinement_turn_can_carry_forward_missing_fields(self):
        prior_domain_context = {
            "bm_story_3_state": {
                "last_user_turn_number": 1,
                "last_resolved_plan": {
                    "lookback_days": 14,
                    "channel": "email",
                    "tone": "friendly",
                    "top_n": 10,
                    "primary_class_interest": ["Cycling"],
                },
                "field_resolution": {
                    "lookback_days": {"source": "explicit", "confidence": 0.9},
                    "channel": {"source": "explicit", "confidence": 0.9},
                    "primary_class_interest": {"source": "explicit", "confidence": 0.9},
                },
            }
        }
        msgs = [{"role": "user", "content": "Generate top leads and draft follow-ups"}]
        out = self._invoke(
            "Make tone consultative and show top 5",
            messages=msgs,
            domain_context=prior_domain_context,
        )
        payload = out.story_output
        self.assertNotIn("needs_request_details", payload)
        self.assertEqual(payload.get("lookback_days"), 14)
        self.assertEqual(payload.get("channel"), "email")
        self.assertEqual(payload.get("top_n"), 5)
        self.assertEqual(payload.get("tone"), "consultative")

    def test_refinement_top_n_bounds_ranked_leads(self):
        prior_domain_context = {
            "bm_story_3_state": {
                "last_user_turn_number": 2,
                "last_resolved_plan": {
                    "lookback_days": 14,
                    "channel": "email",
                    "tone": "friendly",
                    "top_n": 10,
                    "primary_class_interest": ["Cycling"],
                },
                "field_resolution": {
                    "lookback_days": {"source": "explicit", "confidence": 0.9},
                    "channel": {"source": "explicit", "confidence": 0.9},
                    "primary_class_interest": {"source": "explicit", "confidence": 0.9},
                },
            }
        }
        msgs = [
            {"role": "user", "content": "Generate top leads and draft follow-ups"},
            {"role": "user", "content": "Use last 14 days and email channel, focus on cycling"},
        ]
        out = self._invoke(
            "Make tone consultative and show top 5",
            messages=msgs,
            domain_context=prior_domain_context,
        )
        payload = out.story_output
        ranked = payload.get("ranked_leads", [])
        self.assertEqual(payload.get("top_n"), 5)
        self.assertLessEqual(len(ranked), 5)


if __name__ == "__main__":
    unittest.main()
