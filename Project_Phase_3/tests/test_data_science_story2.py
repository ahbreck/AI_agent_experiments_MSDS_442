import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.contracts import CanonicalMember, StoryRequest  # noqa: E402
from prototype.stories.data_science_story2 import get_data_science_story2_mermaid, run_data_science_story2  # noqa: E402


def _sample_rows():
    return [
        {
            "workout_id": "W10",
            "member_id": "MB001",
            "date": "2026-02-01",
            "start_time_local": "07:30",
            "type": "cycling",
            "duration_min": 35.0,
            "calories": 350.0,
            "strive_score": 47.0,
            "output_kj": 190.0,
            "miles": 11.0,
            "average_speed_mph": 18.5,
            "avg_hr_bpm": 146.0,
        },
        {
            "workout_id": "W11",
            "member_id": "MB001",
            "date": "2026-02-08",
            "start_time_local": "18:15",
            "type": "cycling",
            "duration_min": 42.0,
            "calories": 430.0,
            "strive_score": 61.0,
            "output_kj": 245.0,
            "miles": 12.4,
            "average_speed_mph": 17.7,
            "avg_hr_bpm": 152.0,
        },
    ]


def _sample_rows_large():
    rows = []
    for i in range(10):
        rows.append(
            {
                "workout_id": f"W{i+1}",
                "member_id": "MB001",
                "date": f"2026-01-{(i + 1):02d}",
                "start_time_local": "07:30",
                "type": "cycling",
                "duration_min": 30.0 + i,
                "calories": 300.0 + (i * 8),
                "strive_score": 40.0 + i,
                "output_kj": 170.0 + (i * 6),
                "miles": 10.0 + (i * 0.3),
                "average_speed_mph": 17.0 + (i * 0.1),
                "avg_hr_bpm": 140.0 + i,
            }
        )
    return rows


class TestDataScienceStory2(unittest.TestCase):
    def _invoke(self, query: str, member_id: str | None = None):
        req = StoryRequest(
            story_id="ds_story_2",
            user_query=query,
            messages=[],
            member=CanonicalMember(member_id=member_id),
            domain_context={},
        )
        return run_data_science_story2(req)

    def test_mermaid_helper_returns_graph_text(self):
        mermaid = get_data_science_story2_mermaid()
        self.assertIsInstance(mermaid, str)
        self.assertIn("plan", mermaid)
        self.assertIn("interpret", mermaid)

    def test_requires_member_id_when_missing(self):
        out = self._invoke("Am I improving over time?")
        payload = out.story_output
        self.assertTrue(payload.get("needs_member_id"))
        self.assertEqual(payload.get("requested_slot"), "member_id")
        self.assertIsNotNone(out.follow_up_question)

    def test_deterministic_interpretation_fallback_metadata(self):
        tool_results = {
            "read_workouts": {"ok": True, "row_count": 2, "member_id": "MB001", "start_date": "2026-01-01", "end_date": "2026-02-28"},
            "step_1_summarize_time_series": {"trends": {"duration_min": {"slope_per_period": 0.05}}, "by_period_mean": [1, 2]},
        }
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}), patch(
            "prototype.stories.data_science_story2._read_workouts", return_value=_sample_rows()
        ), patch("prototype.stories.data_science_story2._execute_plan", return_value=tool_results), patch(
            "prototype.stories.data_science_story2._maybe_llm_interpret_results", return_value=None
        ):
            out = self._invoke("For MB001, analyze trend and drivers.", member_id="MB001")
        payload = out.story_output
        self.assertEqual(payload.get("interpretation_source"), "deterministic_template")
        self.assertEqual(float(payload.get("interpretation_confidence", -1)), 0.0)
        self.assertIn("Workout analysis for", out.response_text)

    def test_llm_interpretation_used_when_confident(self):
        llm_out = {
            "response_text": "1) Key takeaways\n- Trend is improving.\n2) Likely drivers\n- More cycling sessions.\n3) Next experiment\n- Keep weekday cadence stable.\n4) Limitations\n- Small sample.",
            "confidence": 0.91,
            "rationale": "Grounded in trend and segment summaries.",
        }
        tool_results = {
            "read_workouts": {"ok": True, "row_count": 2, "member_id": "MB001", "start_date": "2026-01-01", "end_date": "2026-02-28"}
        }
        with patch("prototype.stories.data_science_story2._read_workouts", return_value=_sample_rows()), patch(
            "prototype.stories.data_science_story2._execute_plan", return_value=tool_results
        ), patch("prototype.stories.data_science_story2._maybe_llm_interpret_results", return_value=llm_out):
            out = self._invoke("For MB001, summarize my progress.", member_id="MB001")
        payload = out.story_output
        self.assertEqual(payload.get("interpretation_source"), "llm_grounded")
        self.assertGreaterEqual(float(payload.get("interpretation_confidence", 0.0)), 0.9)
        self.assertIn("Grounded in trend", payload.get("interpretation_rationale", ""))
        self.assertEqual(out.response_text, llm_out["response_text"])

    def test_smoke_normal_success_without_replan(self):
        tool_results = {
            "read_workouts": {"ok": True, "row_count": 10, "member_id": "MB001", "start_date": "2026-01-01", "end_date": "2026-02-28"},
            "step_1_summarize_time_series": {"ok": True, "trends": {"duration_min": {"slope_per_period": 0.04}}, "period_count": 6},
        }
        with patch("prototype.stories.data_science_story2._read_workouts", return_value=_sample_rows_large()), patch(
            "prototype.stories.data_science_story2._execute_plan", return_value=tool_results
        ), patch("prototype.stories.data_science_story2._maybe_llm_interpret_results", return_value=None):
            out = self._invoke("For MB001, analyze my workout trends over time.", member_id="MB001")
        payload = out.story_output
        self.assertEqual(int(payload.get("replan_count", -1)), 0)
        self.assertEqual(payload.get("critic_action"), "continue")
        self.assertIsNone(out.follow_up_question)
        self.assertFalse(bool(payload.get("needs_clarification", False)))
        self.assertGreater(int(payload.get("row_count", 0)), 0)

    def test_smoke_zero_row_replan_then_success(self):
        tool_results = {
            "read_workouts": {"ok": True, "row_count": 10, "member_id": "MB001", "start_date": "2026-01-01", "end_date": "2026-02-28"},
            "step_1_summarize_time_series": {"ok": True, "trends": {"duration_min": {"slope_per_period": 0.03}}, "period_count": 5},
        }
        with patch("prototype.stories.data_science_story2._read_workouts", side_effect=[[], _sample_rows_large()]), patch(
            "prototype.stories.data_science_story2._execute_plan", return_value=tool_results
        ), patch("prototype.stories.data_science_story2._maybe_llm_interpret_results", return_value=None):
            out = self._invoke("For MB001, analyze cycling workouts and trend changes.", member_id="MB001")
        payload = out.story_output
        self.assertEqual(int(payload.get("replan_count", -1)), 1)
        self.assertIsNone(out.follow_up_question)
        self.assertFalse(bool(payload.get("needs_clarification", False)))
        self.assertGreaterEqual(len(payload.get("plan_history", [])), 2)
        self.assertGreater(int(payload.get("row_count", 0)), 0)

    def test_smoke_unresolved_ask_user_after_replan_exhausted(self):
        with patch("prototype.stories.data_science_story2._read_workouts", side_effect=[[], []]), patch(
            "prototype.stories.data_science_story2._maybe_llm_interpret_results", return_value=None
        ):
            out = self._invoke("For MB001, analyze cycling workouts and trend changes.", member_id="MB001")
        payload = out.story_output
        self.assertEqual(int(payload.get("replan_count", -1)), 1)
        self.assertEqual(payload.get("critic_action"), "ask_user")
        self.assertTrue(bool(payload.get("needs_clarification", False)))
        self.assertIsNotNone(out.follow_up_question)
        self.assertIn("scope", out.response_text.lower())


if __name__ == "__main__":
    unittest.main()
