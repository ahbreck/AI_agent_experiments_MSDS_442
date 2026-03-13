import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.contracts import CanonicalMember, StoryRequest  # noqa: E402
from prototype.stories.data_science_story1 import get_data_science_story1_mermaid, run_data_science_story1  # noqa: E402


def _sample_rows():
    return [
        {
            "workout_id": "W1",
            "member_id": "MB001",
            "date": "2026-02-01",
            "start_time_local": "07:30",
            "type": "cycling",
            "duration_min": 30.0,
            "calories": 320.0,
            "strive_score": 45.0,
            "avg_hr_bpm": 145.0,
            "output_kj": 180.0,
            "miles": 10.2,
            "average_speed_mph": 20.4,
            "cadence_rpm": 85.0,
            "resistance_percent": 42.0,
            "incline_percent": None,
        },
        {
            "workout_id": "W2",
            "member_id": "MB001",
            "date": "2026-02-08",
            "start_time_local": "18:15",
            "type": "cycling",
            "duration_min": 40.0,
            "calories": 420.0,
            "strive_score": 58.0,
            "avg_hr_bpm": 151.0,
            "output_kj": 230.0,
            "miles": 12.1,
            "average_speed_mph": 18.2,
            "cadence_rpm": 81.0,
            "resistance_percent": 45.0,
            "incline_percent": None,
        },
    ]


class TestDataScienceStory1(unittest.TestCase):
    def _invoke(self, query: str, member_id: str | None = None, domain_context=None, messages=None):
        req = StoryRequest(
            story_id="ds_story_1",
            user_query=query,
            messages=messages or [],
            member=CanonicalMember(member_id=member_id),
            domain_context=domain_context or {},
        )
        return run_data_science_story1(req)

    def test_mermaid_helper_returns_graph_text(self):
        mermaid = get_data_science_story1_mermaid()
        self.assertIsInstance(mermaid, str)
        self.assertIn("intake", mermaid)
        self.assertIn("finalize", mermaid)

    def test_requires_member_id_for_member_scoped_query(self):
        out = self._invoke("Show my workouts as a bar chart by weekday.")
        payload = out.story_output
        self.assertTrue(payload.get("needs_member_id"))
        self.assertEqual(payload.get("requested_slot"), "member_id")
        self.assertIsNotNone(out.follow_up_question)

    def test_same_chart_without_prior_plan_requests_chart_type(self):
        out = self._invoke("Use the same chart but segment by weekday.")
        payload = out.story_output
        self.assertTrue(payload.get("needs_clarification"))
        self.assertEqual(payload.get("requested_slot"), "chart_type")
        self.assertIsNotNone(out.follow_up_question)

    def test_underspecified_query_asks_for_chart_when_candidates_close(self):
        with patch("prototype.stories.data_science_story1._read_rows", return_value=_sample_rows()), patch(
            "prototype.stories.data_science_story1._score_candidate_plan", return_value=(0.52, {"point_count": 2})
        ):
            out = self._invoke("Show me a chart for MB001.", member_id="MB001")
        payload = out.story_output
        self.assertTrue(payload.get("needs_clarification"))
        self.assertEqual(payload.get("requested_slot"), "chart_type")
        self.assertEqual(payload.get("planner_source"), "deterministic_candidate_planner")
        self.assertIn("candidate_planner", payload.get("graph_pipeline", []))
        self.assertIn("ds_story_1_state", out.state_updates_domain)

    def test_success_response_contains_plotly_spec_and_state_updates(self):
        with patch("prototype.stories.data_science_story1._read_rows", return_value=_sample_rows()):
            out = self._invoke("For MB001, make a bar chart of calories by weekday.", member_id="MB001")
        payload = out.story_output
        self.assertEqual(payload.get("chart_type"), "bar")
        self.assertEqual((payload.get("chart_spec") or {}).get("library"), "plotly")
        self.assertIn("graph_pipeline", payload)
        self.assertIn("finalize", payload.get("graph_pipeline", []))
        self.assertIn("ds_story_1_state", out.state_updates_domain)
        self.assertIn("last_plan", (out.state_updates_domain.get("ds_story_1_state") or {}))


if __name__ == "__main__":
    unittest.main()
