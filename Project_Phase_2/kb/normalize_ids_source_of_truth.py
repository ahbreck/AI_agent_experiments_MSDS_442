r"""
Normalize source-of-truth ID fields in SQLite/CSV artifacts to alphanumeric-only uppercase.

This script is idempotent for already-normalized data.

Examples:
  python .\Project_Phase_2\kb\normalize_ids_source_of_truth.py --dry-run
  python .\Project_Phase_2\kb\normalize_ids_source_of_truth.py --apply
"""

from __future__ import annotations

import argparse
import csv
import re
import shutil
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


PROJECT_PHASE_2 = Path(__file__).resolve().parents[1]
KB = PROJECT_PHASE_2 / "kb"


SQLITE_TARGETS: Dict[Path, Dict[str, List[str]]] = {
    KB / "MembershipFraud" / "membership_fraud.db": {"security_events": ["event_id", "member_id"]},
    KB / "DataScience" / "peloton_workouts.sqlite": {"workouts": ["member_id"]},
    KB / "BusinessMarketing" / "brand_feedback.db": {
        "campaign_feedback": ["feedback_id", "campaign_id", "member_id"],
        "campaigns": ["campaign_id"],
    },
}

CSV_TARGETS: Dict[Path, List[str]] = {
    KB / "MembershipFraud" / "security_events.csv": ["event_id", "member_id"],
    KB / "DataScience" / "workout_history_synthetic.csv": ["member_id"],
    KB / "BusinessMarketing" / "campaigns_synthetic.csv": ["campaign_id"],
    KB / "BusinessMarketing" / "campaign_feedback_synthetic.csv": ["feedback_id", "campaign_id", "member_id"],
}


def normalize_id(raw: str) -> str:
    return re.sub(r"[^A-Z0-9]", "", (raw or "").upper())


def normalize_member_id(raw: str) -> str:
    token = normalize_id(raw)
    m = re.search(r"MB(\d+)", token)
    if m:
        return f"MB{m.group(1)}"
    m = re.search(r"M(\d+)", token)
    if m:
        return f"MB{m.group(1)}"
    return token


def normalize_for_column(column: str, raw: str) -> str:
    if column == "member_id":
        return normalize_member_id(raw)
    return normalize_id(raw)


def build_backup(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    backup = path.with_suffix(path.suffix + f".bak_{stamp}")
    shutil.copy2(path, backup)
    return backup


def collision_map(pairs: Iterable[Tuple[str, str]]) -> Dict[str, List[str]]:
    bucket: Dict[str, set] = defaultdict(set)
    for old, new in pairs:
        if old != new:
            bucket[new].add(old)
    out: Dict[str, List[str]] = {}
    for new_val, old_vals in bucket.items():
        if len(old_vals) > 1:
            out[new_val] = sorted(old_vals)
    return out


def normalize_sqlite(path: Path, table_columns: Dict[str, List[str]], apply: bool) -> Dict[str, object]:
    summary: Dict[str, object] = {"path": str(path), "type": "sqlite", "updated_cells": 0, "collisions": {}}
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        all_updates: List[Tuple[str, str, str, object]] = []
        all_pairs: List[Tuple[str, str]] = []

        for table, cols in table_columns.items():
            rows = [dict(r) for r in cur.execute(f"SELECT rowid, * FROM {table}").fetchall()]
            for row in rows:
                rowid = row["rowid"]
                for col in cols:
                    old = str(row.get(col) or "")
                    new = normalize_for_column(col, old)
                    all_pairs.append((old, new))
                    if old != new:
                        all_updates.append((table, col, new, rowid))

        collisions = collision_map(all_pairs)
        summary["collisions"] = collisions
        summary["updated_cells"] = len(all_updates)

        if apply:
            if collisions:
                raise RuntimeError(f"Collision(s) in {path}: {collisions}")
            for table, col, new, rowid in all_updates:
                cur.execute(f"UPDATE {table} SET {col} = ? WHERE rowid = ?", (new, rowid))
            conn.commit()

    return summary


def normalize_csv(path: Path, columns: List[str], apply: bool) -> Dict[str, object]:
    summary: Dict[str, object] = {"path": str(path), "type": "csv", "updated_cells": 0, "collisions": {}}
    with path.open("r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        rows = list(reader)
        fieldnames = reader.fieldnames or []

    pairs: List[Tuple[str, str]] = []
    updates = 0
    for row in rows:
        for col in columns:
            if col not in row:
                continue
            old = str(row.get(col) or "")
            new = normalize_for_column(col, old)
            pairs.append((old, new))
            if old != new:
                row[col] = new
                updates += 1

    collisions = collision_map(pairs)
    summary["collisions"] = collisions
    summary["updated_cells"] = updates

    if apply:
        if collisions:
            raise RuntimeError(f"Collision(s) in {path}: {collisions}")
        with path.open("w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)

    return summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", action="store_true", help="Apply updates in-place.")
    ap.add_argument("--dry-run", action="store_true", help="Preview changes without writing files.")
    args = ap.parse_args()

    if args.apply and args.dry_run:
        raise SystemExit("Use only one of --apply or --dry-run.")

    do_apply = args.apply
    mode = "APPLY" if do_apply else "DRY-RUN"
    print(f"[{mode}] Normalizing ID fields to uppercase alphanumeric.")

    backups: List[Path] = []
    summaries: List[Dict[str, object]] = []

    try:
        if do_apply:
            for p in list(SQLITE_TARGETS.keys()) + list(CSV_TARGETS.keys()):
                backup = build_backup(p)
                backups.append(backup)
                print(f"Backup: {backup}")

        for db_path, table_cols in SQLITE_TARGETS.items():
            summaries.append(normalize_sqlite(db_path, table_cols, apply=do_apply))

        for csv_path, cols in CSV_TARGETS.items():
            summaries.append(normalize_csv(csv_path, cols, apply=do_apply))

        total_updates = 0
        any_collision = False
        for s in summaries:
            updates = int(s["updated_cells"])
            collisions = s["collisions"]
            total_updates += updates
            if collisions:
                any_collision = True
            print(f"{s['type']} | {s['path']} | updated_cells={updates} | collisions={bool(collisions)}")
            if collisions:
                print(f"  collision_details={collisions}")

        if do_apply and any_collision:
            raise RuntimeError("Collision detected after apply attempt. Restore from backups and resolve collisions.")

        if do_apply:
            print(f"Completed apply. total_updated_cells={total_updates}")
        else:
            print(f"Dry-run complete. total_potential_updated_cells={total_updates}")
    except Exception as exc:
        print(f"ERROR: {exc}")
        if backups:
            print("Backups were created before apply mode. You can restore from them if needed.")
        raise


if __name__ == "__main__":
    main()
