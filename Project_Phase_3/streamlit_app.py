from __future__ import annotations

import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

import streamlit as st


def _configure_import_path() -> None:
    cwd = Path.cwd()
    if (cwd / "prototype").exists():
        sys.path.insert(0, str(cwd))
        return
    if (cwd / "Project_Phase_3" / "prototype").exists():
        sys.path.insert(0, str(cwd / "Project_Phase_3"))


_configure_import_path()

from prototype.orchestrator import AgenticOrchestrator  # noqa: E402


@st.cache_resource
def _get_orchestrator() -> AgenticOrchestrator:
    return AgenticOrchestrator()


def _build_plotly_figure(story_output: Optional[Dict[str, Any]]):
    if not isinstance(story_output, dict):
        return None

    chart_spec = story_output.get("chart_spec")
    if not isinstance(chart_spec, dict):
        return None
    if chart_spec.get("library") != "plotly":
        return None

    try:
        import plotly.graph_objects as go
    except Exception:
        return None

    data = chart_spec.get("data", [])
    layout = chart_spec.get("layout", {})
    try:
        return go.Figure(data=data, layout=layout)
    except Exception:
        return None


def _render_debug_block(out: Dict[str, Any]) -> None:
    rm = out.get("router_metrics", {}) or {}
    st.markdown("**Routing Debug**")
    st.json(
        {
            "router_reason": out.get("router_reason"),
            "continuation_score": rm.get("continuation_score"),
            "domain_scores": rm.get("domain_scores"),
            "margin": rm.get("margin"),
            "domain_selected_by": rm.get("domain_selected_by"),
            "story_selected_by": rm.get("story_selected_by"),
            "domain_guardrail_override": rm.get("domain_guardrail_override"),
        }
    )

    so = out.get("story_output") or {}
    if isinstance(so, dict):
        ep = so.get("evidence_plan") or {}
        lr = so.get("llm_rationale") or {}
        plan_snapshot = so.get("plan_snapshot") or {}

        if ep or lr:
            st.markdown("**LLM Story Debug**")
            st.json(
                {
                    "planner_source": ep.get("planner_source"),
                    "planner_confidence": ep.get("planner_confidence"),
                    "llm_rationale_source": lr.get("source"),
                    "llm_rationale_confidence": lr.get("confidence"),
                }
            )

        if so.get("planner_source") or plan_snapshot:
            st.markdown("**Planning Debug**")
            st.json(
                {
                    "planner_source": so.get("planner_source"),
                    "planner_confidence": so.get(
                        "planner_confidence", plan_snapshot.get("planner_confidence")
                    ),
                    "needs_clarification": so.get(
                        "needs_clarification", plan_snapshot.get("needs_clarification")
                    ),
                    "requested_slot": so.get(
                        "requested_slot", plan_snapshot.get("requested_slot")
                    ),
                }
            )


def _render_assistant_message(msg: Dict[str, Any], debug: bool, show_story_output: bool, render_chart: bool) -> None:
    with st.chat_message("assistant"):
        st.markdown(msg["response"])
        st.caption(
            f"routed domain={msg.get('active_domain', 'unknown')} "
            f"story={msg.get('active_story_id', 'unknown')}"
        )

        if render_chart:
            fig = _build_plotly_figure(msg.get("story_output"))
            if fig is not None:
                st.plotly_chart(fig, use_container_width=True)

        if debug:
            _render_debug_block(msg)

        if show_story_output and msg.get("story_output") is not None:
            st.markdown("**story_output**")
            try:
                st.code(json.dumps(msg["story_output"], indent=2, default=str), language="json")
            except Exception:
                st.write(msg["story_output"])


def main() -> None:
    st.set_page_config(page_title="Phase 3 Chatbot", layout="wide")
    st.title("Project Phase 3 Chatbot")

    with st.sidebar:
        st.subheader("Session")
        thread_id = st.text_input("Thread ID", value=st.session_state.get("thread_id", "default"))
        st.session_state["thread_id"] = thread_id.strip() or "default"
        render_chart = st.checkbox("Render Plotly chart", value=True)
        show_story_output = st.checkbox("Show raw story_output", value=False)
        debug = st.checkbox("Show debug fields", value=False)
        if st.button("Clear chat history"):
            st.session_state["messages"] = []
            st.rerun()

    if "messages" not in st.session_state:
        st.session_state["messages"] = []

    for msg in st.session_state["messages"]:
        if msg["role"] == "user":
            with st.chat_message("user"):
                st.markdown(msg["content"])
        else:
            _render_assistant_message(msg, debug, show_story_output, render_chart)

    prompt = st.chat_input("Ask a question...")
    if not prompt:
        return

    st.session_state["messages"].append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    orchestrator = _get_orchestrator()
    with st.spinner("Thinking..."):
        out = orchestrator.invoke(prompt, thread_id=st.session_state["thread_id"])

    assistant_message = {
        "role": "assistant",
        "response": out.get("response", ""),
        "active_domain": out.get("active_domain"),
        "active_story_id": out.get("active_story_id"),
        "router_reason": out.get("router_reason"),
        "router_metrics": out.get("router_metrics"),
        "story_output": out.get("story_output"),
    }
    st.session_state["messages"].append(assistant_message)
    _render_assistant_message(assistant_message, debug, show_story_output, render_chart)


if __name__ == "__main__":
    main()
