# Project Phase 3 Prototype

## Application Overview
This prototype is an agentic demonstration of an AI assistant for Peloton Fitness. It routes each user request to one of three business domains, selects the best matching in-domain story, runs the story workflow, and returns a grounded response while preserving multi-turn state.

Core architecture:
- Top-level orchestrator router (domain routing).
- In-domain story router.
- Layered state (`GlobalState` + per-domain context).
- Memory checkpointer (`MemorySaver`) keyed by `thread_id` for multi-user simulation.
- Catalog-driven stories in `Project_Phase_3/prototype/catalog.py`.

## Domains and User Stories
The application currently covers three domains with three user stories in each domain:

### 1) Business & Marketing
- `bm_story_1` - **Campaign Feedback Themes + Adjustments**: summarizes campaign feedback themes and recommends campaign changes.
- `bm_story_2` - **Weekly Campaign Metric Diagnostics**: analyzes weekly metric performance (for example CTR/CAC/ROAS) and highlights underperformance drivers.
- `bm_story_3` - **Lead Prioritization + Follow-up Drafts**: prioritizes sales leads from behavior and intent signals, then drafts follow-up outreach.

### 2) Data Science
- `ds_story_1` - **Workout Data Visualization Builder**: creates chart-oriented views of workout data based on user analysis goals.
- `ds_story_2` - **Workout Trend Analytics**: analyzes workout trends and performance patterns (for example zones, cadence, improvement signals).
- `ds_story_3` - **Peer Benchmark Comparison + Improvement Suggestions**: compares a member's workout profile to peers and suggests improvement actions.

### 3) Membership & Fraud
- `mf_story_1` - **Security Event Explanation + Actions**: explains suspicious security events and fields user questions about related account-protection actions.
- `mf_story_2` - **Account Issue Triage + Queue Routing**: classifies support issues (login, billing, renewal) and routes to the right support queue.
- `mf_story_3` - **Membership Tier Fit + Upgrade/Downgrade Guidance**: evaluates tier utilization and recommends keeping, upgrading, or downgrading membership tier.

## Environment Setup (Conda, Recommended)
This repo ships an `environment.yml` at the repository root. Use it as the source of truth.

From repo root:

```powershell
conda env create -f .\environment.yml
conda activate msds_442
```

If the environment already exists:

```powershell
conda env update -f .\environment.yml --prune
conda activate msds_442
```

## Configure Secrets
Copy `.\.env.example` to `.\.env` at repo root, then fill values:

```text
OPENAI_API_KEY=...
```

`prototype/orchestrator.py` loads `.env` via `python-dotenv`.

## Verify You Are Using the Intended Python
From repo root:

```powershell
python -c "import sys; print(sys.executable)"
python -c "import dotenv; print('python-dotenv OK')"
python -c "import os; print('OPENAI_API_KEY set:', bool(os.getenv('OPENAI_API_KEY')))"
```

If the executable path is not your conda env, activate the env in that terminal and retry.

## VS Code / Notebook Consistency
- In VS Code, select the same interpreter used above (`Python: Select Interpreter`).
- In notebooks, select the matching kernel (same `msds_442` environment).
- Run this in both terminal and notebook to confirm parity:

```python
import sys
print(sys.executable)
```

## Run the Prototype
From repo root:

```powershell
.\scripts\run_phase3_cli.ps1
```

Notebook wrapper:
- `Project_Phase_3/Prototype_Orchestrator_Wrapper.ipynb`

Direct run (if your `msds_442` environment is already active):

```powershell
python .\Project_Phase_3\prototype_cli.py
```

Streamlit chatbot UI:

```powershell
python -m streamlit run .\Project_Phase_3\streamlit_app.py
```

The Streamlit app supports:
- Persistent multi-turn chat history.
- Configurable `thread_id` to simulate separate users/sessions.
- Plotly rendering when `story_output.chart_spec.library == "plotly"`.
- Optional debug metadata and raw `story_output` display.

## Run Tests
From repo root:

```powershell
.\scripts\run_phase3_tests.ps1
```

Direct run (if your `msds_442` environment is already active):

```powershell
python -m unittest Project_Phase_3.tests.test_membership_fraud_story2 Project_Phase_3.tests.test_orchestrator_membership_fraud_routing Project_Phase_3.tests.test_business_marketing_story3
```

These helper scripts first try the active conda environment, then `conda run`, then common local conda env paths.

## Build Membership-Fraud Issue KB Vectors (RAG)
From repo root (typically run once after creating/updating KB JSONL files):

```powershell
python .\Project_Phase_3\kb\MembershipFraud\build_issue_help_chroma.py --category all --rebuild
```

This builds:
- `login_help_chroma`
- `billing_help_chroma`
- `renewal_help_chroma`

Re-run the command only when the corresponding `*_help_kb.jsonl` content changes.

## Add a New Story
1. Add a handler in `Project_Phase_3/prototype/stories/`.
2. Register it in `Project_Phase_3/prototype/catalog.py` (`STORY_CATALOG` and `DOMAIN_TO_STORIES`).
3. Keep handler signature: `StoryRequest -> StoryResult`.
4. Add tests under `Project_Phase_3/tests/`.

## Multi-User Threads
- Use `orchestrator.invoke(query, thread_id="user_a")`.
- Different `thread_id` values maintain separate state snapshots in `MemorySaver`.

## Troubleshooting
- `ModuleNotFoundError: dotenv`:
  - Your active interpreter is not the expected env. Re-activate conda env and verify `sys.executable`.
- `OPENAI_API_KEY is not set`:
  - Confirm `.env` exists at repo root and contains `OPENAI_API_KEY=...`.
  - Confirm `python-dotenv` is installed in the active environment.
