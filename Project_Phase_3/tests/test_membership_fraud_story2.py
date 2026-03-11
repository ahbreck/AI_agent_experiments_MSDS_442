import sys
import unittest
from pathlib import Path
from unittest.mock import patch


REPO_ROOT = Path(__file__).resolve().parents[2]
PHASE3_ROOT = REPO_ROOT / "Project_Phase_3"
if str(PHASE3_ROOT) not in sys.path:
    sys.path.insert(0, str(PHASE3_ROOT))

from prototype.contracts import CanonicalMember, StoryRequest  # noqa: E402
from prototype.stories.membership_fraud_story2 import run_membership_fraud_story2  # noqa: E402


class TestMembershipFraudStory2(unittest.TestCase):
    def _invoke(self, query: str):
        req = StoryRequest(
            story_id="mf_story_2",
            user_query=query,
            messages=[],
            member=CanonicalMember(),
            domain_context={},
        )
        return run_membership_fraud_story2(req)

    def test_classifies_login_high_confidence(self):
        out = self._invoke("I cannot login and need a password reset because I am locked out.")
        payload = out.story_output
        self.assertEqual(payload.get("issue_category"), "login")
        self.assertGreaterEqual(float(payload.get("classification_confidence", 0.0)), 0.75)
        self.assertFalse(payload.get("requires_human_review"))

    def test_classifies_billing_high_confidence(self):
        out = self._invoke("I was charged twice and need a refund on my latest invoice.")
        payload = out.story_output
        self.assertEqual(payload.get("issue_category"), "billing")
        self.assertGreaterEqual(float(payload.get("classification_confidence", 0.0)), 0.75)
        self.assertFalse(payload.get("requires_human_review"))

    def test_classifies_renewal_high_confidence(self):
        out = self._invoke("My subscription renewal failed and my membership looks expired.")
        payload = out.story_output
        self.assertEqual(payload.get("issue_category"), "renewal")
        self.assertGreaterEqual(float(payload.get("classification_confidence", 0.0)), 0.75)
        self.assertFalse(payload.get("requires_human_review"))

    def test_ambiguous_routes_human_review(self):
        out = self._invoke("Something is wrong with my account and I need help.")
        payload = out.story_output
        self.assertTrue(payload.get("requires_human_review"))
        self.assertEqual(payload.get("routing_queue"), "membership_support_human_review_queue")

    def test_invalid_llm_resolution_falls_back_to_deterministic(self):
        mocked = {
            "issue_category": "unknown",
            "issue_description": "ambiguous",
            "confidence": 0.99,
            "classification_source": "llm",
            "classification_rationale": "mocked",
            "category_scores": {"login": 1, "billing": 0, "renewal": 0},
        }
        with patch("prototype.stories.membership_fraud_story2._resolve_issue_with_llm", return_value=mocked):
            out = self._invoke("I cannot login to my account.")
        payload = out.story_output
        self.assertEqual(payload.get("issue_category"), "login")
        self.assertEqual(payload.get("classification_source"), "deterministic")

    def test_story_output_schema_fields_present(self):
        out = self._invoke("I was charged unexpectedly after auto renewal.")
        payload = out.story_output
        required = {
            "issue_category",
            "issue_description",
            "classification_confidence",
            "routing_queue",
            "requires_human_review",
            "classification_source",
            "classification_rationale",
            "guardrails",
        }
        self.assertTrue(required.issubset(set(payload.keys())))
        self.assertGreaterEqual(float(payload["classification_confidence"]), 0.0)
        self.assertLessEqual(float(payload["classification_confidence"]), 1.0)
        self.assertFalse(payload["guardrails"]["auto_resolve_enabled"])


if __name__ == "__main__":
    unittest.main()
