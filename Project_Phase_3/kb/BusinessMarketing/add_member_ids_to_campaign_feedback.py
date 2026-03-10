r"""
Add/backfill member_id in BusinessMarketing campaign_feedback (CSV + SQLite).

Mapping is deterministic from feedback_id to MB001..MB050 so CSV and DB stay aligned.

Examples:
  python .\Project_Phase_2\kb\BusinessMarketing\add_member_ids_to_campaign_feedback.py --dry-run
  python .\Project_Phase_2\kb\BusinessMarketing\add_member_ids_to_campaign_feedback.py --apply
"""

from __future__ import annotations

import argparse
import csv
import shutil
import sqlite3
import zlib
from datetime import datetime
from pathlib import Path


ROOT = Path(__file__).resolve().parent
CSV_PATH = ROOT / "campaign_feedback_synthetic.csv"
DB_PATH = ROOT / "brand_feedback.db"

MEMBER_POOL_SIZE = 50


def member_for_feedback_id(feedback_id: str) -> str:
    h = zlib.crc32(feedback_id.encode("utf-8")) % MEMBER_POOL_SIZE
    return f"MB{h + 1:03d}"


def backup(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = path.with_suffix(path.suffix + f".bak_{stamp}")
    shutil.copy2(path, out)
    return out


def update_csv(apply: bool) -> int:
    with CSV_PATH.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = list(reader.fieldnames or [])

    if "member_id" not in fieldnames:
        fieldnames.append("member_id")

    updates = 0
    for row in rows:
        fid = str(row.get("feedback_id") or "")
        new_member = member_for_feedback_id(fid)
        old_member = str(row.get("member_id") or "")
        if old_member != new_member:
            row["member_id"] = new_member
            updates += 1

    if apply:
        with CSV_PATH.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return updates


def update_sqlite(apply: bool) -> int:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cols = [r[1] for r in cur.execute("PRAGMA table_info(campaign_feedback)")]
        if "member_id" not in cols and apply:
            cur.execute("ALTER TABLE campaign_feedback ADD COLUMN member_id TEXT")
            conn.commit()

        rows = list(cur.execute("SELECT rowid, feedback_id, COALESCE(member_id, '') FROM campaign_feedback"))
        updates = 0
        for rowid, feedback_id, old_member in rows:
            new_member = member_for_feedback_id(str(feedback_id))
            if str(old_member or "") != new_member:
                updates += 1
                if apply:
                    cur.execute("UPDATE campaign_feedback SET member_id = ? WHERE rowid = ?", (new_member, rowid))

        if apply:
            conn.commit()
        return updates


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    if args.apply and args.dry_run:
        raise SystemExit("Use only one of --apply or --dry-run.")

    do_apply = args.apply
    mode = "APPLY" if do_apply else "DRY-RUN"
    print(f"[{mode}] campaign_feedback member_id backfill")

    if do_apply:
        print(f"Backup: {backup(CSV_PATH)}")
        print(f"Backup: {backup(DB_PATH)}")

    csv_updates = update_csv(apply=do_apply)
    db_updates = update_sqlite(apply=do_apply)
    print(f"csv_updates={csv_updates}")
    print(f"db_updates={db_updates}")


if __name__ == "__main__":
    main()
