"""
Add missing call-channel templates for BusinessMarketing Story 3 intents.

Examples:
  python .\\Project_Phase_3\\kb\\BusinessMarketing\\add_call_templates_story3.py --dry-run
  python .\\Project_Phase_3\\kb\\BusinessMarketing\\add_call_templates_story3.py --apply
"""

from __future__ import annotations

import argparse
import shutil
import sqlite3
from contextlib import closing
from datetime import datetime
from pathlib import Path
from typing import Iterable, Tuple


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "brand_feedback.db"


TEMPLATES: Tuple[Tuple[str, str, str, str, str, str, str, int, int], ...] = (
    (
        "TPL_TRIAL_CALL_FRIEND_01",
        "trial_engaged",
        "call",
        "friendly",
        "",
        "Call opener: Thank them for trying {primary_class_interest} and ask how the trial felt.",
        "Ask permission to suggest one next class based on their goal.",
        100,
        1,
    ),
    (
        "TPL_CONSIDER_CALL_FRIEND_01",
        "considering",
        "call",
        "friendly",
        "",
        "Call opener: Mention their interest in {primary_class_interest} and offer to simplify options.",
        "Ask if they want a short recommendation for this week.",
        100,
        1,
    ),
    (
        "TPL_BROWSE_CALL_FRIEND_01",
        "browsing",
        "call",
        "friendly",
        "",
        "Call opener: Thank them for checking out {primary_class_interest} and ask what they want to focus on first.",
        "Offer one beginner-friendly next step and confirm interest.",
        100,
        1,
    ),
)


def backup(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = path.with_suffix(path.suffix + f".bak_call_templates_{stamp}")
    shutil.copy2(path, out)
    return out


def upsert_templates(conn: sqlite3.Connection, apply: bool) -> Tuple[int, int]:
    existing = {
        str(r[0]) for r in conn.execute("SELECT template_id FROM message_templates").fetchall()
    }
    to_insert = [t for t in TEMPLATES if t[0] not in existing]
    to_update = [t for t in TEMPLATES if t[0] in existing]

    if apply:
        for t in TEMPLATES:
            conn.execute(
                """
                INSERT INTO message_templates(
                  template_id, intent, primary_class_interest, channel, tone,
                  subject_template, body_template, cta_template, priority, is_active
                )
                VALUES (?, ?, NULL, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(template_id) DO UPDATE SET
                  intent = excluded.intent,
                  primary_class_interest = excluded.primary_class_interest,
                  channel = excluded.channel,
                  tone = excluded.tone,
                  subject_template = excluded.subject_template,
                  body_template = excluded.body_template,
                  cta_template = excluded.cta_template,
                  priority = excluded.priority,
                  is_active = excluded.is_active,
                  updated_at = CURRENT_TIMESTAMP
                """,
                t,
            )
    return len(to_insert), len(to_update)


def print_templates(rows: Iterable[tuple]) -> None:
    for r in rows:
        print(r)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--db-path", type=Path, default=DB_PATH)
    args = ap.parse_args()

    if args.apply and args.dry_run:
        raise SystemExit("Use only one of --apply or --dry-run.")
    apply = bool(args.apply)
    mode = "APPLY" if apply else "DRY-RUN"

    with closing(sqlite3.connect(str(args.db_path))) as conn:
        print(f"[{mode}] add missing Story 3 call templates")
        print(f"db_path={args.db_path}")
        if apply:
            b = backup(args.db_path)
            print(f"backup={b}")
            conn.execute("BEGIN")
        inserted, updated = upsert_templates(conn, apply=apply)
        if apply:
            conn.commit()
        print(f"would_insert={inserted}" if not apply else f"inserted_or_upserted={inserted + updated}")
        print(f"would_update={updated}" if not apply else f"updated={updated}")

        rows = conn.execute(
            """
            SELECT template_id, intent, channel, tone, is_active
            FROM message_templates
            WHERE channel = 'call'
            ORDER BY template_id
            """
        ).fetchall()
        print("call_templates:")
        print_templates(rows)
        if not apply:
            print("No DB changes were written (dry-run).")


if __name__ == "__main__":
    main()
