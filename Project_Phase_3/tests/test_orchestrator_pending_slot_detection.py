import os
import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.contracts import StoryResult  # noqa: E402
from prototype.orchestrator import AgenticOrchestrator  # noqa: E402


class TestOrchestratorPendingSlotDetection(unittest.TestCase):
    def _new_orchestrator(self) -> AgenticOrchestrator:
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            return AgenticOrchestrator()

    def test_detects_member_slot_from_non_literal_followup_text(self):
        orch = self._new_orchestrator()
        orch.state.turn_index = 3

        result = StoryResult(
            story_id="mf_story_1",
            response_text="Please share your member ID.",
            follow_up_question="Could you share your member ID so I can continue?",
        )
        orch._update_pending_from_result(result)

        self.assertEqual(orch.state.pending_slot_type, "member_id")
        self.assertEqual(orch.state.pending_slot_target_story_id, "mf_story_1")
        self.assertEqual(orch.state.pending_turn_created, 3)

    def test_prefers_structured_slot_signal(self):
        orch = self._new_orchestrator()
        orch.state.turn_index = 4

        result = StoryResult(
            story_id="ds_story_2",
            response_text="Need member identifier.",
            follow_up_question="What is your ID?",
            story_output={"requested_slot": "member_id", "missing_slots": ["member_id"]},
        )
        orch._update_pending_from_result(result)

        self.assertEqual(orch.state.pending_slot_type, "member_id")
        self.assertEqual(orch.state.pending_slot_target_story_id, "ds_story_2")
        self.assertEqual(orch.state.pending_turn_created, 4)


if __name__ == "__main__":
    unittest.main()
