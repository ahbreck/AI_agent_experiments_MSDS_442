from __future__ import annotations

import re
from datetime import date, timedelta
from typing import List, Optional, Tuple


def normalize_token_alnum(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    token = re.sub(r"[^A-Z0-9]", "", raw.strip().upper())
    return token or None


def normalize_member_id(raw: Optional[str]) -> Optional[str]:
    token = normalize_token_alnum(raw)
    if not token:
        return None

    m = re.search(r"MB(\d{3})", token)
    if m:
        return f"MB{m.group(1)}"

    m = re.search(r"M(\d{3})", token)
    if m:
        return f"MB{m.group(1)}"

    m = re.search(r"(\d{3})", token)
    if m:
        return f"MB{m.group(1)}"

    return None


def normalize_campaign_id(raw: Optional[str]) -> Optional[str]:
    token = normalize_token_alnum(raw)
    if not token:
        return None
    m = re.search(r"CAMP(\d+)", token)
    if m:
        return f"CAMP{m.group(1)}"
    return None


def member_id_aliases(raw: Optional[str]) -> List[str]:
    norm = normalize_member_id(raw)
    if not norm:
        return []
    digits = norm[-3:]
    return [norm, f"M{digits}", f"MB-{digits}"]


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
