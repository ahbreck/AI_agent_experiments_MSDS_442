"""
Import workout history CSV into a SQLite database (workouts table).

Example:
  python import_workouts_to_sqlite.py --csv workout_history_synthetic.csv --db peloton_workouts.sqlite --replace
"""

import argparse
import sqlite3
import pandas as pd


SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS workouts (
  workout_id TEXT PRIMARY KEY,
  member_id TEXT NOT NULL,
  date TEXT NOT NULL,
  start_time_local TEXT,
  type TEXT NOT NULL,
  duration_min REAL NOT NULL,

  zone1_minutes REAL,
  zone2_minutes REAL,
  zone3_minutes REAL,
  zone4_minutes REAL,
  zone5_minutes REAL,

  cadence_rpm REAL,
  resistance_percent REAL,
  incline_percent REAL,
  miles REAL,
  average_speed_mph REAL,
  calories REAL,

  strive_score REAL,
  avg_hr_bpm REAL,
  output_kj REAL
);

CREATE INDEX IF NOT EXISTS idx_workouts_member_date ON workouts(member_id, date);
CREATE INDEX IF NOT EXISTS idx_workouts_type ON workouts(type);
"""


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--csv", required=True, help="Path to workout_history CSV")
    ap.add_argument("--db", required=True, help="Path to SQLite .sqlite file")
    ap.add_argument("--replace", action="store_true", help="Drop and recreate workouts table before import")
    args = ap.parse_args()

    df = pd.read_csv(args.csv)

    with sqlite3.connect(args.db) as conn:
        cur = conn.cursor()

        if args.replace:
            cur.execute("DROP TABLE IF EXISTS workouts;")
            conn.commit()

        for stmt in [s.strip() for s in SCHEMA_SQL.split(";") if s.strip()]:
            cur.execute(stmt)
        conn.commit()

        df.to_sql("workouts", conn, if_exists="append", index=False)
        conn.commit()

        n = cur.execute("SELECT COUNT(*) FROM workouts;").fetchone()[0]

    print(f"Imported {len(df)} rows. Table now has {n} rows. DB: {args.db}")


if __name__ == "__main__":
    main()
