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


class TestOrchestratorMembershipFraudRouting(unittest.TestCase):
    def _new_orchestrator(self) -> AgenticOrchestrator:
        return AgenticOrchestrator()

    def test_login_triage_routes_to_mf_story_2(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            orch = self._new_orchestrator()

            # Force deterministic fallback path (no live LLM dependency).
            with patch.object(orch, "_llm_domain_route", side_effect=RuntimeError("offline")):
                out = orch.invoke(
                    "I cannot login and need a password reset because I am locked out.",
                    thread_id="test_mf_route_login",
                )

        self.assertEqual(out.get("active_domain"), "membership_fraud")
        self.assertEqual(out.get("active_story_id"), "mf_story_2")
        self.assertEqual((out.get("story_output") or {}).get("issue_category"), "login")

    def test_security_alert_routes_to_mf_story_1(self):
        with patch.dict(os.environ, {"OPENAI_API_KEY": "test-key"}):
            orch = self._new_orchestrator()

            # Force deterministic fallback path (no live LLM dependency).
            with patch.object(orch, "_llm_domain_route", side_effect=RuntimeError("offline")):
                out = orch.invoke(
                    "I got a suspicious login alert from a new device in another location.",
                    thread_id="test_mf_route_security",
                )

        self.assertEqual(out.get("active_domain"), "membership_fraud")
        self.assertEqual(out.get("active_story_id"), "mf_story_1")
        self.assertIn("retrieved_events", out.get("story_output") or {})


if __name__ == "__main__":
    unittest.main()
