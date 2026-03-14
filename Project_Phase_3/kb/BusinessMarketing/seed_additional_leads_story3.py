"""
Seed additional lead records and engagement signals for BusinessMarketing Story 3.

Examples:
  python .\\Project_Phase_3\\kb\\BusinessMarketing\\seed_additional_leads_story3.py --dry-run
  python .\\Project_Phase_3\\kb\\BusinessMarketing\\seed_additional_leads_story3.py --apply --count 40
"""

from __future__ import annotations

import argparse
import random
import shutil
import sqlite3
from contextlib import closing
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Dict, List, Sequence, Tuple


ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "brand_feedback.db"

INTERESTS = ("Cycling", "Yoga", "Strength", "Running")

FIRST_NAMES = [
    "Aiden", "Amelia", "Benjamin", "Charlotte", "Daniel", "Evelyn", "Henry", "Harper",
    "Jackson", "Luna", "Logan", "Mila", "Nathan", "Nora", "Owen", "Penelope",
    "Samuel", "Scarlett", "Sebastian", "Stella", "Wyatt", "Zoe", "Caleb", "Chloe",
    "Dylan", "Ella", "Grayson", "Grace", "Hudson", "Hazel", "Isaac", "Layla",
    "Julian", "Leah", "Leo", "Lily", "Mateo", "Madison", "Miles", "Naomi",
    "Oliver", "Paisley", "Ryan", "Riley", "Thomas", "Sofia", "Xavier", "Violet",
]

COMPANY_WORD_1 = [
    "North", "Summit", "Urban", "Prime", "Elevate", "Peak", "Bright", "Core",
    "Velocity", "Pioneer", "Atlas", "Vertex", "Pulse", "Beacon", "Nimbus", "Blue",
]
COMPANY_WORD_2 = [
    "Fitness", "Wellness", "Studios", "Dynamics", "Labs", "Group", "Collective", "Works",
    "Partners", "Ventures", "Performance", "Systems", "Solutions", "Health", "Training", "Hub",
]


@dataclass(frozen=True)
class LeadSeed:
    lead_id: str
    member_id: str | None
    first_name: str
    company_name: str | None
    email: str
    phone: str | None
    interest: str


def _backup(path: Path) -> Path:
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    out = path.with_suffix(path.suffix + f".bak_leads_expand_{stamp}")
    shutil.copy2(path, out)
    return out


def _max_lead_num(conn: sqlite3.Connection) -> int:
    row = conn.execute(
        "SELECT MAX(CAST(SUBSTR(lead_id, 2) AS INTEGER)) FROM leads WHERE lead_id LIKE 'L%'"
    ).fetchone()
    return int(row[0] or 0)


def _member_pool(conn: sqlite3.Connection) -> List[str]:
    rows = conn.execute(
        "SELECT member_id FROM member_registry WHERE member_id LIKE 'MB%' ORDER BY member_id"
    ).fetchall()
    return [str(r[0]) for r in rows]


def _as_of_date_for_story3(conn: sqlite3.Connection) -> date:
    row = conn.execute(
        """
        SELECT MAX(as_of_date)
        FROM lead_engagement_signals
        WHERE as_of_date <= DATE('now')
        """
    ).fetchone()
    if row and row[0]:
        return date.fromisoformat(str(row[0]))
    row = conn.execute("SELECT MAX(as_of_date) FROM lead_engagement_signals").fetchone()
    if row and row[0]:
        return date.fromisoformat(str(row[0]))
    return date.today()


def _counts_by_window_interest(conn: sqlite3.Connection, as_of: date) -> Dict[Tuple[int, str], int]:
    rows = conn.execute(
        """
        SELECT lookback_days, COALESCE(primary_class_interest, 'Unknown'), COUNT(*)
        FROM lead_engagement_signals
        WHERE as_of_date = ?
        GROUP BY lookback_days, COALESCE(primary_class_interest, 'Unknown')
        """,
        (as_of.isoformat(),),
    ).fetchall()
    out: Dict[Tuple[int, str], int] = {}
    for lookback, interest, count in rows:
        out[(int(lookback), str(interest))] = int(count)
    return out


def _build_lead_seeds(
    start_lead_num: int,
    count: int,
    members: Sequence[str],
) -> List[LeadSeed]:
    leads: List[LeadSeed] = []
    for i in range(count):
        lead_num = start_lead_num + i + 1
        lead_id = f"L{lead_num:03d}"
        first_name = FIRST_NAMES[(lead_num - 1) % len(FIRST_NAMES)]
        interest = INTERESTS[(lead_num - 1) % len(INTERESTS)]

        company_name: str | None = None
        if lead_num % 4 != 0:
            company_name = f"{COMPANY_WORD_1[lead_num % len(COMPANY_WORD_1)]} {COMPANY_WORD_2[(lead_num * 3) % len(COMPANY_WORD_2)]}"

        member_id: str | None = None
        if members and (lead_num % 3 != 0):
            member_id = members[(lead_num - 1) % len(members)]

        email = f"{first_name.lower()}.{lead_num:03d}@example.com"
        phone: str | None = None
        if lead_num % 5 != 0:
            phone = f"555-{2000 + lead_num:04d}"

        leads.append(
            LeadSeed(
                lead_id=lead_id,
                member_id=member_id,
                first_name=first_name,
                company_name=company_name,
                email=email,
                phone=phone,
                interest=interest,
            )
        )
    return leads


def _signal_values_for(lead_num: int, lookback_days: int, rng: random.Random) -> Tuple[int, int, int, int, int]:
    # Deterministic but varied distributions; biased so top-N has enough separation.
    base = 1 + (lead_num % 9)
    pages_boost = {7: 1, 14: 2, 30: 3}[lookback_days]
    pages_viewed = max(0, base + pages_boost + rng.randint(-1, 2))

    cart_threshold = 8 if lookback_days == 7 else 7 if lookback_days == 14 else 6
    cart_abandonments = 1 if pages_viewed >= cart_threshold and rng.random() < 0.45 else 0
    if pages_viewed >= cart_threshold + 3 and rng.random() < 0.25:
        cart_abandonments = 2

    trial_used = 1 if (pages_viewed >= (6 + pages_boost) and rng.random() < 0.4) else 0

    days_since_last_visit = min(lookback_days + 5, max(0, rng.randint(0, lookback_days + 3)))
    email_opens = max(0, min(14, int(pages_viewed * 0.6) + rng.randint(0, 3)))
    return pages_viewed, cart_abandonments, trial_used, days_since_last_visit, email_opens


def _insert_leads_and_signals(
    conn: sqlite3.Connection,
    leads: Sequence[LeadSeed],
    as_of: date,
    seed: int,
    apply: bool,
) -> Tuple[int, int]:
    rng = random.Random(seed + 99)
    inserted_leads = 0
    inserted_signals = 0

    for lead in leads:
        if apply:
            conn.execute(
                """
                INSERT INTO leads(lead_id, member_id, first_name, company_name, email, phone)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    lead.lead_id,
                    lead.member_id,
                    lead.first_name,
                    lead.company_name,
                    lead.email,
                    lead.phone,
                ),
            )
        inserted_leads += 1

        lead_num = int(lead.lead_id[1:])
        for lookback_days in (7, 14, 30):
            pages_viewed, cart_abandonments, trial_used, dslv, email_opens = _signal_values_for(
                lead_num, lookback_days, rng
            )
            last_visit_at = (as_of - timedelta(days=dslv)).isoformat()
            if apply:
                conn.execute(
                    """
                    INSERT INTO lead_engagement_signals(
                      lead_id, member_id, as_of_date, lookback_days, pages_viewed,
                      primary_class_interest, cart_abandonments, trial_used,
                      days_since_last_visit, email_opens, last_visit_at
                    )
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        lead.lead_id,
                        lead.member_id,
                        as_of.isoformat(),
                        lookback_days,
                        pages_viewed,
                        lead.interest,
                        cart_abandonments,
                        trial_used,
                        dslv,
                        email_opens,
                        last_visit_at,
                    ),
                )
            inserted_signals += 1

    return inserted_leads, inserted_signals


def _print_count_delta(before: Dict[Tuple[int, str], int], after: Dict[Tuple[int, str], int]) -> None:
    print("Counts by lookback + interest (delta):")
    keys = sorted(set(before) | set(after), key=lambda k: (k[0], k[1]))
    for lookback, interest in keys:
        b = before.get((lookback, interest), 0)
        a = after.get((lookback, interest), 0)
        d = a - b
        print(f"  lookback={lookback:>2} interest={interest:<8} before={b:<3} after={a:<3} delta={d:+}")


def _projected_counts(
    before: Dict[Tuple[int, str], int],
    leads: Sequence[LeadSeed],
) -> Dict[Tuple[int, str], int]:
    out = dict(before)
    for lead in leads:
        for lookback_days in (7, 14, 30):
            key = (lookback_days, lead.interest)
            out[key] = int(out.get(key, 0)) + 1
    return out


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true", help="Write changes to SQLite DB.")
    parser.add_argument("--dry-run", action="store_true", help="Show planned changes only.")
    parser.add_argument("--count", type=int, default=40, help="Number of leads to add.")
    parser.add_argument("--seed", type=int, default=442, help="Random seed for deterministic generation.")
    parser.add_argument("--db-path", type=Path, default=DB_PATH, help="Path to brand_feedback.db")
    args = parser.parse_args()

    if args.apply and args.dry_run:
        raise SystemExit("Use only one of --apply or --dry-run.")
    if args.count <= 0:
        raise SystemExit("--count must be > 0.")

    apply_mode = bool(args.apply)
    mode = "APPLY" if apply_mode else "DRY-RUN"

    with closing(sqlite3.connect(str(args.db_path))) as conn:
        conn.execute("PRAGMA foreign_keys = ON")
        as_of = _as_of_date_for_story3(conn)
        before = _counts_by_window_interest(conn, as_of)
        start_num = _max_lead_num(conn)
        members = _member_pool(conn)

        leads = _build_lead_seeds(
            start_lead_num=start_num,
            count=int(args.count),
            members=members,
        )

        print(f"[{mode}] Seeding additional Story 3 leads")
        print(f"db_path={args.db_path}")
        print(f"as_of_date={as_of.isoformat()}")
        print(f"existing_max_lead_id=L{start_num:03d}")
        print(f"planned_new_leads={len(leads)} planned_new_signals={len(leads) * 3}")
        print(f"new_lead_id_range={leads[0].lead_id}..{leads[-1].lead_id}")

        if apply_mode:
            backup_path = _backup(args.db_path)
            print(f"backup={backup_path}")
            conn.execute("BEGIN")
            inserted_leads, inserted_signals = _insert_leads_and_signals(
                conn=conn,
                leads=leads,
                as_of=as_of,
                seed=int(args.seed),
                apply=True,
            )
            conn.commit()
        else:
            inserted_leads, inserted_signals = _insert_leads_and_signals(
                conn=conn,
                leads=leads,
                as_of=as_of,
                seed=int(args.seed),
                apply=False,
            )

        after = _counts_by_window_interest(conn, as_of)
        print(f"inserted_leads={inserted_leads}")
        print(f"inserted_signals={inserted_signals}")
        if apply_mode:
            _print_count_delta(before, after)
        else:
            _print_count_delta(before, _projected_counts(before, leads))

        if not apply_mode:
            print("No DB changes were written (dry-run).")


if __name__ == "__main__":
    main()
