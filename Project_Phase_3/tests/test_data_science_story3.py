import sys
import unittest
from pathlib import Path
from unittest.mock import patch
import os


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.contracts import CanonicalMember, StoryRequest  # noqa: E402
from prototype.stories.data_science_story3 import run_data_science_story3  # noqa: E402
from prototype.stories.data_science_story3 import PlanOutput, PeerDefinition, MetricSelection  # noqa: E402
from prototype.stories.data_science_story3 import compare_to_peers  # noqa: E402


class TestDataScienceStory3(unittest.TestCase):
    def _invoke(self, query: str, member_id: str | None = None, domain_context=None):
        req = StoryRequest(
            story_id="ds_story_3",
            user_query=query,
            messages=[],
            member=CanonicalMember(member_id=member_id),
            domain_context=domain_context or {},
        )
        return run_data_science_story3(req)

    def test_requires_member_id_when_missing(self):
        out = self._invoke("Compare my weekly workouts and consistency to peers.")
        payload = out.story_output
        self.assertTrue(payload.get("needs_clarification"))
        self.assertEqual(payload.get("requested_slot"), "member_id")
        self.assertIsNotNone(out.follow_up_question)

    def test_requests_metrics_clarification_when_generic_metrics_mentioned(self):
        out = self._invoke("Compare MB001 metrics to peers.")
        payload = out.story_output
        self.assertTrue(payload.get("needs_clarification"))
        self.assertEqual(payload.get("requested_slot"), "metrics")
        self.assertIn("plan_snapshot", payload)

    def test_clarification_resume_merges_metrics_and_proceeds(self):
        prior_state = {
            "ds_story_3_state": {
                "pending_slot": "metrics",
                "last_plan": {
                    "member_id": "MB001",
                    "timeframe": "last_8_weeks",
                    "peer_definition": {"scope": "all_members", "rationale": "Defaulted peer scope to all members."},
                    "metrics": {
                        "selected": ["weekly_workouts", "avg_session_length_min", "consistency_ratio"],
                        "inferred": ["weekly_workouts", "avg_session_length_min", "consistency_ratio"],
                    },
                    "assumptions": ["Used default metrics: weekly workouts, session length, consistency."],
                    "ambiguities": ["metrics"],
                    "needs_clarification": True,
                    "requested_slot": "metrics",
                    "clarifying_question": "Which metrics should I compare?",
                    "planner_confidence": 0.6,
                },
            }
        }
        out = self._invoke("weekly workouts and consistency", domain_context=prior_state)
        payload = out.story_output
        selected = payload.get("plan_snapshot", {}).get("metrics", {}).get("selected", [])
        assumptions = payload.get("plan_snapshot", {}).get("assumptions", [])
        self.assertFalse(payload.get("plan_snapshot", {}).get("needs_clarification"))
        self.assertIn("weekly_workouts", selected)
        self.assertIn("consistency_ratio", selected)
        self.assertFalse(any(str(a).startswith("Used default metrics:") for a in assumptions))

    def test_story_output_schema_and_guardrails(self):
        out = self._invoke("Compare MB001 weekly workouts and consistency to all peers for last 8 weeks.")
        payload = out.story_output
        required = {
            "member_id",
            "start_date",
            "end_date",
            "timeframe_label",
            "plan_snapshot",
            "member_metrics",
            "peer_benchmarks",
            "benchmark_availability",
            "comparisons",
            "primary_gap_metric",
            "suggestions",
            "guardrails",
            "generated_on",
        }
        self.assertTrue(required.issubset(set(payload.keys())))
        self.assertTrue(payload["guardrails"]["aggregated_peer_data_only"])
        self.assertFalse(payload["guardrails"]["medical_advice_enabled"])
        self.assertIn(payload.get("member_metrics", {}).get("primary_type"), {"cycling", "tread", "rowing", "strength", "yoga", None})

    def test_comparison_deltas_are_deterministic(self):
        out = self._invoke("For MB001, compare weekly workouts and session length to peers for last 8 weeks.")
        payload = out.story_output
        comparisons = payload.get("comparisons", {})

        for metric, cmp_row in comparisons.items():
            member_value = cmp_row.get("member_value")
            peer_value = cmp_row.get("peer_value")
            delta = cmp_row.get("delta")
            pct_delta = cmp_row.get("pct_delta")

            if peer_value is None:
                self.assertIsNone(delta)
                self.assertIsNone(pct_delta)
                continue

            expected_delta = round(float(member_value) - float(peer_value), 3)
            self.assertEqual(delta, expected_delta)
            if float(peer_value) > 0:
                self.assertEqual(pct_delta, round((expected_delta / float(peer_value)) * 100.0, 2))

    def test_suggestions_match_negative_gaps(self):
        out = self._invoke("Compare MB001 to all peers for weekly workouts, session length, and consistency.")
        payload = out.story_output
        comparisons = payload.get("comparisons", {})
        negative_metrics = {m for m, row in comparisons.items() if isinstance(row.get("delta"), (int, float)) and row.get("delta") < 0}
        known_metrics = {"weekly_workouts", "avg_session_length_min", "consistency_ratio"}

        suggestions = payload.get("suggestions", [])
        if negative_metrics:
            for suggestion in suggestions:
                self.assertIn(suggestion.get("metric"), negative_metrics)
        else:
            self.assertTrue(len(suggestions) >= 1)
            for suggestion in suggestions:
                self.assertIn(suggestion.get("metric"), known_metrics)

    def test_missing_benchmark_skips_recommendations_for_unavailable_metrics(self):
        mock_peer = {
            "peer_member_count": 1,
            "peer_workout_count": 3,
            "availability": {
                "weekly_workouts": False,
                "avg_session_length_min": True,
                "consistency_ratio": False,
            },
            "benchmarks": {"avg_session_length_min": 25.0},
            "limitation": "Insufficient aggregated peer benchmark data for this cohort/timeframe.",
        }
        with patch("prototype.stories.data_science_story3._read_peer_aggregate_metrics", return_value=mock_peer):
            out = self._invoke("Compare MB001 weekly workouts, session length, and consistency to all peers for last 8 weeks.")

        payload = out.story_output
        suggestions = payload.get("suggestions", [])
        suggestion_metrics = {s.get("metric") for s in suggestions}
        self.assertNotIn("weekly_workouts", suggestion_metrics)
        self.assertNotIn("consistency_ratio", suggestion_metrics)
        self.assertIn("aggregated benchmarks", out.response_text.lower())

    def test_llm_attempted_on_first_pass_when_enabled(self):
        query = "Compare MB001 weekly workouts and consistency to all peers over the last 8 weeks."
        with patch.dict(os.environ, {"PROTOTYPE_DS3_USE_LLM_PLAN": "1"}):
            with patch("prototype.stories.data_science_story3._maybe_llm_plan", return_value=None) as mocked_llm:
                out = self._invoke(query)
        mocked_llm.assert_called_once()
        self.assertEqual((out.story_output or {}).get("planner_source"), "deterministic_fallback_llm_unavailable")

    def test_llm_plan_is_used_when_available_and_valid(self):
        llm_plan = PlanOutput(
            member_id="MB001",
            timeframe="last_8_weeks",
            peer_definition=PeerDefinition(scope="all_members", rationale="llm"),
            metrics=MetricSelection(selected=["weekly_workouts"], inferred=["weekly_workouts"]),
            assumptions=["LLM selected weekly workouts as requested."],
            ambiguities=[],
            needs_clarification=False,
            requested_slot=None,
            clarifying_question=None,
            planner_confidence=0.9,
        )
        with patch.dict(os.environ, {"PROTOTYPE_DS3_USE_LLM_PLAN": "1"}):
            with patch("prototype.stories.data_science_story3._maybe_llm_plan", return_value=llm_plan) as mocked_llm:
                out = self._invoke("Compare MB001 metrics to peers.")
        mocked_llm.assert_called_once()
        self.assertEqual((out.story_output or {}).get("planner_source"), "llm")
        selected = ((out.story_output or {}).get("plan_snapshot") or {}).get("metrics", {}).get("selected", [])
        self.assertEqual(selected, ["weekly_workouts"])

    def test_hybrid_gate_falls_back_on_low_llm_confidence(self):
        low_conf_llm_plan = PlanOutput(
            member_id="MB001",
            timeframe="last_8_weeks",
            peer_definition=PeerDefinition(scope="all_members", rationale="llm"),
            metrics=MetricSelection(selected=["weekly_workouts"], inferred=["weekly_workouts"]),
            assumptions=["LLM plan"],
            ambiguities=[],
            needs_clarification=False,
            requested_slot=None,
            clarifying_question=None,
            planner_confidence=0.1,
        )
        with patch.dict(os.environ, {"PROTOTYPE_DS3_USE_LLM_PLAN": "1"}):
            with patch("prototype.stories.data_science_story3._maybe_llm_plan", return_value=low_conf_llm_plan):
                out = self._invoke("Compare MB001 metrics to peers.")
        self.assertEqual((out.story_output or {}).get("planner_source"), "deterministic_fallback_llm_low_confidence")

    def test_cohort_language_triggers_peer_definition_clarification(self):
        llm_plan = PlanOutput(
            member_id="MB001",
            timeframe="last_4_weeks",
            peer_definition=PeerDefinition(scope="same_primary_type", rationale="llm"),
            metrics=MetricSelection(selected=["weekly_workouts", "avg_session_length_min"], inferred=[]),
            assumptions=[],
            ambiguities=[],
            needs_clarification=False,
            requested_slot=None,
            clarifying_question=None,
            planner_confidence=0.9,
        )
        with patch.dict(os.environ, {"PROTOTYPE_DS3_USE_LLM_PLAN": "1"}):
            with patch("prototype.stories.data_science_story3._maybe_llm_plan", return_value=llm_plan):
                out = self._invoke("What should I improve compared to others in my cohort?", member_id="MB001")
        payload = out.story_output
        self.assertTrue(payload.get("needs_clarification"))
        self.assertEqual(payload.get("requested_slot"), "peer_definition")
        self.assertIsNotNone(out.follow_up_question)

    def test_not_everyone_phrase_never_uses_all_members_scope(self):
        llm_plan = PlanOutput(
            member_id="MB001",
            timeframe="last_8_weeks",
            peer_definition=PeerDefinition(scope="all_members", rationale="llm broad scope"),
            metrics=MetricSelection(selected=["weekly_workouts", "avg_session_length_min", "consistency_ratio"], inferred=[]),
            assumptions=[],
            ambiguities=[],
            needs_clarification=False,
            requested_slot=None,
            clarifying_question=None,
            planner_confidence=0.9,
        )
        with patch.dict(os.environ, {"PROTOTYPE_DS3_USE_LLM_PLAN": "1"}):
            with patch("prototype.stories.data_science_story3._maybe_llm_plan", return_value=llm_plan):
                out = self._invoke("Compare MB001 to others but not everyone.")
        scope = ((out.story_output or {}).get("plan_snapshot") or {}).get("peer_definition", {}).get("scope")
        self.assertNotEqual(scope, "all_members")

    def test_focus_on_what_matters_limits_to_priority_metrics(self):
        llm_plan = PlanOutput(
            member_id="MB001",
            timeframe="last_8_weeks",
            peer_definition=PeerDefinition(scope="similar_activity_band", rationale="llm"),
            metrics=MetricSelection(
                selected=["weekly_workouts", "avg_session_length_min", "consistency_ratio"],
                inferred=[],
            ),
            assumptions=[],
            ambiguities=[],
            needs_clarification=False,
            requested_slot=None,
            clarifying_question=None,
            planner_confidence=0.9,
        )
        with patch.dict(os.environ, {"PROTOTYPE_DS3_USE_LLM_PLAN": "1"}):
            with patch("prototype.stories.data_science_story3._maybe_llm_plan", return_value=llm_plan):
                out = self._invoke("Compare me to peers and focus on what matters most. My member ID is MB001.")
        selected = ((out.story_output or {}).get("plan_snapshot") or {}).get("metrics", {}).get("selected", [])
        self.assertEqual(selected, ["weekly_workouts", "consistency_ratio"])

    def test_strength_ranking_uses_percent_delta_not_raw_units(self):
        compared = compare_to_peers(
            member_data={"weekly_workouts": 3.0, "avg_session_length_min": 40.0},
            peer_data={
                "benchmarks": {"weekly_workouts": 2.0, "avg_session_length_min": 30.0},
                "availability": {"weekly_workouts": True, "avg_session_length_min": True},
            },
            selected_metrics=["weekly_workouts", "avg_session_length_min"],
        )
        # Weekly has smaller raw delta (1 vs 10) but larger percent delta (50% vs 33.33%).
        self.assertEqual(compared.get("strengths"), ["weekly_workouts"])


if __name__ == "__main__":
    unittest.main()
