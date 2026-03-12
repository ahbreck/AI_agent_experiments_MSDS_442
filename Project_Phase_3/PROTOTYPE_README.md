# Project Phase 3 Prototype

## Features
- Top-level orchestrator router (domain routing).
- In-domain story router.
- Layered state (`GlobalState` + per-domain context).
- Memory checkpointer (`MemorySaver`) keyed by `thread_id` for multi-user simulation.
- Current wired stories:
  - `bm_story_1`
  - `bm_story_2`
  - `bm_story_3`
  - `ds_story_2`
  - `mf_story_1`
  - `mf_story_2`

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

## Run Tests
From repo root:

```powershell
.\scripts\run_phase3_tests.ps1
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

Direct run (if your `msds_442` environment is already active):

```powershell
python -m unittest Project_Phase_3.tests.test_membership_fraud_story2 Project_Phase_3.tests.test_orchestrator_membership_fraud_routing Project_Phase_3.tests.test_business_marketing_story3
```

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
