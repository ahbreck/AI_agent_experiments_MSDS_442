import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.orchestrator import AgenticOrchestrator  # noqa: E402


class TestOrchestratorDataScienceRouting(unittest.TestCase):
    def _new_orchestrator(self) -> AgenticOrchestrator:
        return AgenticOrchestrator()

    def test_peer_benchmark_prompt_routes_to_ds_story_3(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            orch = self._new_orchestrator()
            with patch.object(orch, "_llm_domain_route", side_effect=RuntimeError("offline")):
                out = orch.invoke(
                    "Compare MB001 to peer benchmarks for weekly workouts and consistency.",
                    thread_id="test_ds_peer_route",
                )

        self.assertEqual(out.get("active_domain"), "data_science")
        self.assertEqual(out.get("active_story_id"), "ds_story_3")
        self.assertIn("comparisons", out.get("story_output") or {})

    def test_trend_prompt_routes_to_ds_story_2(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            orch = self._new_orchestrator()
            with patch.object(orch, "_llm_domain_route", side_effect=RuntimeError("offline")):
                out = orch.invoke(
                    "Analyze workout trend improvement for MB001 over the last 8 weeks.",
                    thread_id="test_ds_trend_route",
                )

        self.assertEqual(out.get("active_domain"), "data_science")
        self.assertEqual(out.get("active_story_id"), "ds_story_2")

    def test_ds_story3_pending_member_slot_resume(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            orch = self._new_orchestrator()
            with patch.object(orch, "_llm_domain_route", side_effect=RuntimeError("offline")):
                first = orch.invoke(
                    "Compare my weekly workouts and consistency to peer benchmarks and suggest improvements.",
                    thread_id="test_ds_pending_slot",
                )
                second = orch.invoke("MB001", thread_id="test_ds_pending_slot")

        self.assertEqual(first.get("active_story_id"), "ds_story_3")
        self.assertTrue((first.get("story_output") or {}).get("needs_clarification"))
        self.assertEqual((first.get("story_output") or {}).get("requested_slot"), "member_id")
        self.assertEqual(second.get("active_story_id"), "ds_story_3")
        self.assertEqual((second.get("story_output") or {}).get("member_id"), "MB001")


if __name__ == "__main__":
    unittest.main()
