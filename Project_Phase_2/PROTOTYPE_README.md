# Project Phase 2 Prototype (3 stories wired)

## What this adds
- Top-level operator router (domain routing)
- In-domain story router
- Layered state (`GlobalState` + per-domain context)
- 3 working story handlers:
  - `bm_story_1`
  - `ds_story_2`
  - `mf_story_1`
- Catalog-driven structure for adding the remaining 6 stories

## Run
From `Project_Phase_2`:

```powershell
python .\prototype_cli.py
```

## Add a new future story
1. Add a handler in `prototype/stories/`.
2. Register it in `prototype/catalog.py` (`STORY_CATALOG` and `DOMAIN_TO_STORIES`).
3. Keep handler signature: `StoryRequest -> StoryResult`.
