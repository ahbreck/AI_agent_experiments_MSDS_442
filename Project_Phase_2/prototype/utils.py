from __future__ import annotations

import re
from datetime import date, timedelta
from typing import Optional, Tuple


def normalize_member_id(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = raw.strip().upper()

    m = re.search(r"\bMB-\d{3}\b", text)
    if m:
        return m.group(0)

    m = re.search(r"\bM\d{3}\b", text)
    if m:
        return "MB-" + m.group(0)[1:]

    m = re.search(r"\b\d{3}\b", text)
    if m:
        return "MB-" + m.group(0)

    return None


def parse_last_n_weeks(user_text: str, default_weeks: int) -> Tuple[str, str, str]:
    tl = user_text.lower()
    weeks = default_weeks
    m = re.search(r"last\s+(\d+)\s+weeks?", tl)
    if m:
        weeks = max(1, int(m.group(1)))
    elif "last month" in tl:
        weeks = 4

    end = date.today()
    start = end - timedelta(days=weeks * 7)
    return start.isoformat(), end.isoformat(), f"last_{weeks}_weeks"


def parse_date_range_from_text(user_text: str, default_weeks: int = 8) -> Tuple[str, str]:
    start, end, _ = parse_last_n_weeks(user_text, default_weeks=default_weeks)
    return start, end
