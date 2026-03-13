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
            "response_mode",
            "rag_attempted",
            "rag_used",
            "rag_relevance_score",
            "rag_fallback_reason",
            "rag_escalated_to_human_review",
            "rag_snippet_ids",
            "rag_thresholds",
            "guardrails",
        }
        self.assertTrue(required.issubset(set(payload.keys())))
        self.assertGreaterEqual(float(payload["classification_confidence"]), 0.0)
        self.assertLessEqual(float(payload["classification_confidence"]), 1.0)
        self.assertFalse(payload["guardrails"]["auto_resolve_enabled"])

    def test_high_confidence_strong_retrieval_uses_rag_response(self):
        snippets = [
            {
                "id": "login-help-001",
                "question": "Forgot password",
                "answer": "Use the forgot password flow.",
                "score": 0.92,
                "text": "Forgot password guidance",
            }
        ]
        with patch("prototype.stories.membership_fraud_story2._retrieve_category_help", return_value=snippets), patch(
            "prototype.stories.membership_fraud_story2._build_rag_direct_answer",
            return_value="Reset your password and retry login. [login-help-001]",
        ):
            out = self._invoke("I cannot login and need a password reset because I am locked out.")
        payload = out.story_output
        self.assertTrue(payload.get("rag_attempted"))
        self.assertTrue(payload.get("rag_used"))
        self.assertEqual(payload.get("response_mode"), "rag_direct_response")
        self.assertIn("login-help-001", payload.get("rag_snippet_ids", []))
        self.assertIn("[login-help-001]", out.response_text)

    def test_high_confidence_weak_retrieval_falls_back_to_queue_only(self):
        weak_snippets = [
            {
                "id": "billing-help-002",
                "question": "Payment failed",
                "answer": "Check payment settings.",
                "score": 0.05,
                "text": "weak match",
            }
        ]
        with patch("prototype.stories.membership_fraud_story2._retrieve_category_help", return_value=weak_snippets), patch(
            "prototype.stories.membership_fraud_story2._build_rag_direct_answer"
        ) as rag_answer:
            out = self._invoke("I was charged twice and need a refund on my latest invoice.")
        payload = out.story_output
        self.assertTrue(payload.get("rag_attempted"))
        self.assertFalse(payload.get("rag_used"))
        self.assertEqual(payload.get("response_mode"), "queue_only")
        self.assertEqual(payload.get("rag_fallback_reason"), "low_relevance")
        self.assertTrue(payload.get("rag_escalated_to_human_review"))
        self.assertTrue(payload.get("requires_human_review"))
        self.assertEqual(payload.get("routing_queue"), "membership_support_human_review_queue")
        self.assertIn("could not find corresponding information", out.response_text.lower())
        rag_answer.assert_not_called()

    def test_high_confidence_no_retrieval_match_acknowledges_kb_gap(self):
        with patch("prototype.stories.membership_fraud_story2._retrieve_category_help", return_value=[]), patch(
            "prototype.stories.membership_fraud_story2._build_rag_direct_answer"
        ) as rag_answer:
            out = self._invoke("I cannot login and need a password reset because I am locked out.")
        payload = out.story_output
        self.assertTrue(payload.get("rag_attempted"))
        self.assertFalse(payload.get("rag_used"))
        self.assertEqual(payload.get("rag_fallback_reason"), "no_kb_match")
        self.assertTrue(payload.get("rag_escalated_to_human_review"))
        self.assertTrue(payload.get("requires_human_review"))
        self.assertEqual(payload.get("routing_queue"), "membership_support_human_review_queue")
        self.assertIn("could not find corresponding information", out.response_text.lower())
        rag_answer.assert_not_called()

    def test_low_confidence_does_not_attempt_rag(self):
        with patch("prototype.stories.membership_fraud_story2._retrieve_category_help") as retriever:
            out = self._invoke("Need help with my account, not sure what is wrong.")
        payload = out.story_output
        self.assertTrue(payload.get("requires_human_review"))
        self.assertFalse(payload.get("rag_attempted"))
        self.assertFalse(payload.get("rag_used"))
        self.assertEqual(payload.get("routing_queue"), "membership_support_human_review_queue")
        self.assertEqual(payload.get("rag_fallback_reason"), "classification_confidence_below_threshold")
        self.assertIn("below the direct-response threshold", out.response_text.lower())
        retriever.assert_not_called()

    def test_rag_direct_response_is_snippet_grounded(self):
        snippets = [
            {
                "id": "billing-help-004",
                "question": "Invoice total looks different",
                "answer": "Check tax, discounts, and plan change proration details listed on the invoice breakdown.",
                "score": 0.92,
                "text": "billing guidance",
            }
        ]
        with patch("prototype.stories.membership_fraud_story2._retrieve_category_help", return_value=snippets):
            out = self._invoke("I was charged twice and need a refund on my latest invoice.")
        self.assertIn("Direct guidance from knowledge base:", out.response_text)
        self.assertIn("billing-help-004", out.response_text)
        self.assertIn("tax, discounts, and plan change proration", out.response_text)

    def test_low_confidence_query_uses_llm_resolution_path(self):
        mocked_llm = {
            "issue_category": "billing",
            "issue_description": "Need refund",
            "confidence": 0.88,
            "classification_source": "llm",
            "classification_rationale": "LLM disambiguated short query.",
            "category_scores": {"login": 0, "billing": 1, "renewal": 0},
        }
        snippets = [
            {
                "id": "billing-help-200",
                "question": "Refund request",
                "answer": "Open billing history and submit a refund request from the charge details page.",
                "score": 0.9,
                "text": "Refund request instructions",
            }
        ]
        with patch("prototype.stories.membership_fraud_story2._resolve_issue_with_llm", return_value=mocked_llm) as resolver, patch(
            "prototype.stories.membership_fraud_story2._retrieve_category_help", return_value=snippets
        ):
            out = self._invoke("Need refund.")
        payload = out.story_output
        self.assertTrue(resolver.called)
        self.assertEqual(payload.get("issue_category"), "billing")
        self.assertEqual(payload.get("classification_source"), "llm")
        self.assertFalse(payload.get("requires_human_review"))

    def test_out_of_scope_issue_routes_human_without_rag_attempt(self):
        with patch("prototype.stories.membership_fraud_story2._retrieve_category_help") as retriever:
            out = self._invoke("How do I change my profile photo?")
        payload = out.story_output
        self.assertEqual(payload.get("issue_category"), "unknown")
        self.assertTrue(payload.get("requires_human_review"))
        self.assertFalse(payload.get("rag_attempted"))
        self.assertEqual(payload.get("rag_fallback_reason"), "classification_confidence_below_threshold")
        retriever.assert_not_called()

    def test_story_output_includes_graph_audit_trace(self):
        out = self._invoke("I was charged twice and need a refund on my latest invoice.")
        payload = out.story_output
        trace = payload.get("audit_trace")
        self.assertIsInstance(trace, list)
        self.assertGreaterEqual(len(trace), 5)
        trace_text = " ".join(str(t) for t in trace)
        self.assertIn("parse_request", trace_text)
        self.assertIn("classify_deterministic", trace_text)
        self.assertIn("route_queue", trace_text)
        self.assertIn("retrieve_kb", trace_text)
        self.assertIn("guardrails", trace_text)


if __name__ == "__main__":
    unittest.main()
