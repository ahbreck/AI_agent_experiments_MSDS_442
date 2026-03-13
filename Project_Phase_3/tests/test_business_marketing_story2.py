import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.contracts import CanonicalMember, StoryRequest  # noqa: E402
from prototype.stories.business_marketing_story2 import run_business_marketing_story2  # noqa: E402


class TestBusinessMarketingStory2(unittest.TestCase):
    def _invoke(self, query: str, messages=None, domain_context=None):
        req = StoryRequest(
            story_id="bm_story_2",
            user_query=query,
            messages=messages or [],
            member=CanonicalMember(),
            domain_context=domain_context or {},
        )
        return run_business_marketing_story2(req)

    def test_definitions_route_returns_metric_defs(self):
        out = self._invoke("What is CTR and ROAS? Give definitions only.")
        payload = out.story_output
        self.assertEqual(payload.get("intent"), "definitions")
        defs = payload.get("metric_definitions", {})
        self.assertIn("CTR", defs)
        self.assertIn("ROAS", defs)

    def test_no_available_weeks_short_circuits(self):
        with patch("prototype.stories.business_marketing_story2._get_available_weeks", return_value=[]):
            out = self._invoke("Show campaign performance for last 4 weeks by channel")
        self.assertIn("No weekly data is available", out.response_text)
        self.assertEqual(out.story_output.get("row_count"), 0)

    def test_compare_intent_defaults_group_by_campaign(self):
        out = self._invoke("Compare campaign metrics for last 4 weeks")
        payload = out.story_output
        self.assertEqual(payload.get("intent"), "compare_metrics")
        self.assertIn("campaign_id", payload.get("group_by", []))

    def test_analysis_response_schema_contains_core_fields(self):
        out = self._invoke("Show underperforming campaigns by channel for last 4 weeks")
        payload = out.story_output
        required = {
            "intent",
            "intent_source",
            "intent_confidence",
            "timeframe",
            "filters",
            "group_by",
            "metrics_requested",
            "row_count",
            "summary",
            "underperformers",
            "quality",
            "generated_on",
        }
        self.assertTrue(required.issubset(set(payload.keys())))
        self.assertIsInstance(payload.get("quality"), dict)
        self.assertIn("valid_threshold_rows", payload.get("quality", {}))

    def test_asks_clarification_for_ambiguous_overview_request(self):
        out = self._invoke("Can you help with campaign performance?")
        payload = out.story_output
        self.assertTrue(payload.get("needs_clarification"))
        self.assertEqual(payload.get("requested_slot"), "analysis_scope")
        self.assertIsNotNone(out.follow_up_question)
        self.assertIn("overview", out.response_text.lower())

    def test_grounded_narrative_can_override_template_response(self):
        mocked = {
            "narrative_text": "Grounded summary: CTR is stable, ROAS is below threshold for social.",
            "narrative_source": "llm_grounded",
            "narrative_confidence": 0.84,
            "used_fields": ["summary.overall", "underperformers"],
        }
        with patch("prototype.stories.business_marketing_story2._maybe_generate_grounded_narrative", return_value=mocked):
            out = self._invoke("Show underperforming campaigns by channel for last 4 weeks")
        payload = out.story_output
        self.assertEqual(out.response_text, mocked["narrative_text"])
        self.assertEqual(payload.get("response_source"), "llm_grounded")
        self.assertGreater(float(payload.get("response_confidence", 0.0)), 0.0)

    def test_scope_planner_can_refine_metrics_and_grouping(self):
        plan = {
            "intent": "compare_metrics",
            "metrics_requested": ["return_on_ad_spend"],
            "group_by": ["channel"],
            "ask_clarification": False,
            "follow_up_question": "",
            "concise": True,
            "rationale": "Focus on ROAS by channel for comparison intent.",
            "confidence": 0.88,
        }
        with patch("prototype.stories.business_marketing_story2._maybe_llm_scope_plan", return_value=plan):
            out = self._invoke("Compare campaign metrics for last 4 weeks")
        payload = out.story_output
        self.assertEqual(payload.get("planner_source"), "llm_scope_plan")
        self.assertEqual(payload.get("metrics_requested"), ["return_on_ad_spend"])
        self.assertEqual(payload.get("group_by"), ["channel"])

    def test_scope_planner_low_confidence_falls_back(self):
        low_conf_plan = {
            "intent": "compare_metrics",
            "metrics_requested": ["return_on_ad_spend"],
            "group_by": ["channel"],
            "ask_clarification": False,
            "follow_up_question": "",
            "concise": True,
            "rationale": "Low confidence mock.",
            "confidence": 0.21,
        }
        with patch("prototype.stories.business_marketing_story2._maybe_llm_scope_plan", return_value=low_conf_plan):
            out = self._invoke("Show underperforming campaigns by channel for last 4 weeks")
        payload = out.story_output
        self.assertEqual(payload.get("planner_source"), "deterministic_scope_low_llm_confidence")
        self.assertIn("channel", payload.get("group_by", []))
        self.assertIn("click_through_rate", payload.get("metrics_requested", []))

    def test_scope_planner_can_force_clarification(self):
        clarify_plan = {
            "intent": "overview",
            "metrics_requested": [],
            "group_by": [],
            "ask_clarification": True,
            "follow_up_question": "Do you want overview, comparison, or underperformers for last 4 weeks?",
            "concise": False,
            "rationale": "Need explicit analysis scope.",
            "confidence": 0.91,
        }
        with patch("prototype.stories.business_marketing_story2._maybe_llm_scope_plan", return_value=clarify_plan):
            out = self._invoke("show me campaign performance")
        payload = out.story_output
        self.assertTrue(payload.get("needs_clarification"))
        self.assertEqual(out.follow_up_question, clarify_plan["follow_up_question"])

    def test_critic_replan_loop_is_capped(self):
        critic_replan = {
            "action": "replan_once",
            "follow_up_question": "",
            "rationale": "Try one replan.",
            "confidence": 0.91,
        }
        with patch("prototype.stories.business_marketing_story2._maybe_llm_critic_decision", return_value=critic_replan):
            out = self._invoke("Compare campaign metrics for last 4 weeks")
        payload = out.story_output
        self.assertLessEqual(int(payload.get("replan_count", 0)), 1)
        self.assertIn(payload.get("critic_action"), {"continue", "replan_once"})

    def test_self_check_applies_fallback_for_unsupported_dimension_mentions(self):
        mocked = {
            "narrative_text": "Geography comparison shows strongest ROAS in the northeast region.",
            "narrative_source": "llm_grounded",
            "narrative_confidence": 0.93,
            "used_fields": ["summary.overall"],
        }
        with patch("prototype.stories.business_marketing_story2._maybe_generate_grounded_narrative", return_value=mocked):
            out = self._invoke("Show underperforming campaigns by geography for last 4 weeks")
        payload = out.story_output
        self.assertEqual(payload.get("response_source"), "deterministic_self_check_fallback")
        self.assertEqual((payload.get("self_check") or {}).get("status"), "fallback_applied")
        self.assertIn("Requested dimension(s) not available", out.response_text)

    def test_memory_carries_forward_missing_scope_fields_on_followup(self):
        domain_context = {
            "bm_story_2_state": {
                "last_user_turn_number": 1,
                "last_user_query": "Compare campaign metrics for last 4 weeks by channel for social.",
                "last_resolved_scope": {
                    "intent": "compare_metrics",
                    "metrics_requested": ["return_on_ad_spend"],
                    "group_by": ["channel"],
                    "filters": {"campaign_ids": None, "channels": None, "target_segments": None, "objectives": None},
                },
            }
        }
        msgs = [{"role": "user", "content": "Compare campaign metrics for last 4 weeks by channel for social."}]
        out = self._invoke("and keep same scope", messages=msgs, domain_context=domain_context)
        persisted = out.state_updates_domain.get("bm_story_2_state", {})
        self.assertIn("group_by", persisted.get("memory_applied_fields", []))
        self.assertIn("metrics_requested", persisted.get("memory_applied_fields", []))
        self.assertEqual(
            (persisted.get("last_resolved_scope", {}) or {}).get("metrics_requested"),
            ["return_on_ad_spend"],
        )
        self.assertIn(
            "channel",
            (persisted.get("last_resolved_scope", {}) or {}).get("group_by", []),
        )

    def test_memory_does_not_override_explicit_new_user_scope(self):
        domain_context = {
            "bm_story_2_state": {
                "last_user_turn_number": 1,
                "last_resolved_scope": {
                    "intent": "compare_metrics",
                    "metrics_requested": ["return_on_ad_spend"],
                    "group_by": ["channel"],
                    "filters": {"campaign_ids": None, "channels": None, "target_segments": None, "objectives": None},
                },
            }
        }
        msgs = [{"role": "user", "content": "Compare campaign metrics for last 4 weeks by channel for social."}]
        out = self._invoke("Show underperformers by campaign_id", messages=msgs, domain_context=domain_context)
        persisted = out.state_updates_domain.get("bm_story_2_state", {})
        resolved = persisted.get("last_resolved_scope", {}) or {}
        self.assertEqual(resolved.get("intent"), "underperformers_only")
        self.assertIn("campaign_id", resolved.get("group_by", []))

    def test_clarification_turn_persists_partial_scope_state(self):
        out = self._invoke("Can you help with campaign performance?")
        self.assertIsNotNone(out.follow_up_question)
        state = out.state_updates_domain.get("bm_story_2_state", {})
        self.assertEqual(state.get("pending_slot"), "analysis_scope")
        self.assertTrue(isinstance(state.get("last_partial_scope"), dict))


if __name__ == "__main__":
    unittest.main()
