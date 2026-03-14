from __future__ import annotations

import sqlite3
from contextlib import closing
from pathlib import Path
from typing import Dict, List, Tuple

ROOT = Path(__file__).resolve().parent
DB_PATH = ROOT / "membership_fraud.db"

TIERS: List[Tuple[str, int, str, str]] = [
    ("Basic", 8, "core_classes", "light usage with mostly standard classes"),
    ("Plus", 16, "core_classes+guest_passes", "steady weekly usage with occasional premium needs"),
    ("Premium", 30, "premium_content+priority_booking", "frequent usage with regular premium feature demand"),
    ("Elite", 60, "all_access+concierge_support", "high-volume members who rely on full feature access"),
]


def _feature_level_for_index(i: int) -> str:
    if i % 5 == 0:
        return "high"
    if i % 2 == 0:
        return "medium"
    return "low"


def _pick_recommended_tier(current_i: int, util_3mo: float, util_6mo: float, feature_level: str) -> int:
    if util_3mo >= 90.0 or util_6mo >= 88.0:
        return min(current_i + 1, len(TIERS) - 1)
    if util_3mo <= 55.0 and util_6mo <= 58.0 and feature_level == "low":
        return max(current_i - 1, 0)
    return current_i


def _interpretation(current: str, recommended: str, util_3mo: float, feature_level: str) -> str:
    if current == recommended:
        return "Current tier appears to fit observed usage and feature needs."
    if util_3mo >= 90.0:
        return "You are consistently near your class limit; a higher tier may reduce limit pressure."
    if util_3mo <= 55.0 and feature_level == "low":
        return "Usage appears below current tier capacity; a lower tier may better match your pattern."
    return "Recommendation balances class utilization and feature-level signals."


def seed() -> None:
    with closing(sqlite3.connect(DB_PATH)) as conn:
        cur = conn.cursor()

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS membership_tier_definitions (
              tier TEXT PRIMARY KEY,
              included_monthly_classes INTEGER NOT NULL,
              feature_access TEXT NOT NULL,
              intended_user_profile TEXT NOT NULL
            )
            """
        )

        cur.execute(
            """
            CREATE TABLE IF NOT EXISTS membership_tier_optimization (
              member_id TEXT PRIMARY KEY,
              current_tier TEXT NOT NULL,
              avg_monthly_classes REAL NOT NULL,
              tier_class_limit INTEGER NOT NULL,
              feature_usage_level TEXT NOT NULL,
              tier_utilization_pct REAL NOT NULL,
              recommended_tier TEXT NOT NULL,
              interpretation_summary TEXT NOT NULL,
              avg_monthly_classes_3mo REAL,
              avg_monthly_classes_6mo REAL,
              tier_utilization_pct_3mo REAL,
              tier_utilization_pct_6mo REAL,
              classes_stddev_6mo REAL,
              months_observed INTEGER,
              data_quality TEXT,
              volatility_index REAL
            )
            """
        )

        member_rows = cur.execute(
            "SELECT DISTINCT member_id FROM security_events WHERE member_id IS NOT NULL ORDER BY member_id"
        ).fetchall()
        member_ids = [str(r[0]) for r in member_rows]

        cur.execute("DELETE FROM membership_tier_definitions")
        cur.execute("DELETE FROM membership_tier_optimization")

        cur.executemany(
            """
            INSERT INTO membership_tier_definitions (
              tier, included_monthly_classes, feature_access, intended_user_profile
            ) VALUES (?, ?, ?, ?)
            """,
            TIERS,
        )

        rows: List[Tuple] = []
        for pos, member_id in enumerate(member_ids, start=1):
            current_i = (pos - 1) % len(TIERS)
            current_tier, tier_limit, _, _ = TIERS[current_i]

            factor_1mo = 0.38 + ((pos * 7) % 78) / 100.0  # 0.38..1.15
            factor_3mo = 0.35 + ((pos * 11) % 74) / 100.0  # 0.35..1.08
            factor_6mo = 0.32 + ((pos * 13) % 70) / 100.0  # 0.32..1.01

            avg_1mo = round(max(1.0, min(tier_limit * 1.35, tier_limit * factor_1mo)), 1)
            avg_3mo = round(max(1.0, min(tier_limit * 1.25, tier_limit * factor_3mo)), 1)
            avg_6mo = round(max(1.0, min(tier_limit * 1.2, tier_limit * factor_6mo)), 1)

            util_1mo = round((avg_1mo / tier_limit) * 100.0, 1)
            util_3mo = round((avg_3mo / tier_limit) * 100.0, 1)
            util_6mo = round((avg_6mo / tier_limit) * 100.0, 1)

            feature_level = _feature_level_for_index(pos)
            std_6mo = round(1.2 + ((pos * 5) % 8) * 0.65, 2)
            volatility = round(std_6mo / max(avg_6mo, 1.0), 3)

            months_observed = 2 if pos % 13 == 0 else 6
            data_quality = "low" if months_observed < 3 else "good"

            rec_i = _pick_recommended_tier(current_i=current_i, util_3mo=util_3mo, util_6mo=util_6mo, feature_level=feature_level)
            recommended_tier = TIERS[rec_i][0]
            interpretation = _interpretation(
                current=current_tier,
                recommended=recommended_tier,
                util_3mo=util_3mo,
                feature_level=feature_level,
            )

            rows.append(
                (
                    member_id,
                    current_tier,
                    avg_1mo,
                    tier_limit,
                    feature_level,
                    util_1mo,
                    recommended_tier,
                    interpretation,
                    avg_3mo,
                    avg_6mo,
                    util_3mo,
                    util_6mo,
                    std_6mo,
                    months_observed,
                    data_quality,
                    volatility,
                )
            )

        cur.executemany(
            """
            INSERT INTO membership_tier_optimization (
              member_id,
              current_tier,
              avg_monthly_classes,
              tier_class_limit,
              feature_usage_level,
              tier_utilization_pct,
              recommended_tier,
              interpretation_summary,
              avg_monthly_classes_3mo,
              avg_monthly_classes_6mo,
              tier_utilization_pct_3mo,
              tier_utilization_pct_6mo,
              classes_stddev_6mo,
              months_observed,
              data_quality,
              volatility_index
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )

        conn.commit()
        print(f"Seeded membership tier tables for {len(member_ids)} members.")


if __name__ == "__main__":
    seed()
