# Project Phase 2 Prototype (3 stories wired)

## What this adds
- Top-level operator router (domain routing)
- In-domain story router
- Layered state (`GlobalState` + per-domain context)
- Memory checkpointer (`MemorySaver`) keyed by `thread_id` for multi-user simulation
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

## ID normalization (source-of-truth)
To normalize ID fields in DB and CSV artifacts to uppercase alphanumeric (remove `_`, `-`, and other non-alphanumeric chars):

```powershell
python .\kb\normalize_ids_source_of_truth.py --dry-run
python .\kb\normalize_ids_source_of_truth.py --apply
```

The script creates timestamped backups before `--apply`.

To add cross-domain `member_id` linkage into BusinessMarketing campaign feedback:

```powershell
python .\kb\BusinessMarketing\add_member_ids_to_campaign_feedback.py --dry-run
python .\kb\BusinessMarketing\add_member_ids_to_campaign_feedback.py --apply
```

## Add a new future story
1. Add a handler in `prototype/stories/`.
2. Register it in `prototype/catalog.py` (`STORY_CATALOG` and `DOMAIN_TO_STORIES`).
3. Keep handler signature: `StoryRequest -> StoryResult`.

## Multi-user threads
- Use `orchestrator.invoke(query, thread_id=\"user_a\")`.
- Different `thread_id` values maintain separate state snapshots in `MemorySaver`.
