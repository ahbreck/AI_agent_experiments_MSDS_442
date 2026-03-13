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


if __name__ == "__main__":
    unittest.main()
