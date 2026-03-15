from __future__ import annotations

import os
import re
import sqlite3
from datetime import date, timedelta
from typing import Any, Callable, Dict, List, Optional, Tuple


def normalize_token_alnum(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    token = re.sub(r"[^A-Z0-9]", "", raw.strip().upper())
    return token or None


def resolve_openai_client_config() -> Dict[str, str]:
    api_key = os.getenv("OPENAI_API_KEY") or os.getenv("OPENAI_COMPATIBLE_API_KEY")
    base_url = (
        os.getenv("OPENAI_BASE_URL")
        or os.getenv("OPENAI_API_BASE")
        or os.getenv("OPENAI_COMPATIBLE_BASE_URL")
    )

    config: Dict[str, str] = {}
    if base_url:
        config["base_url"] = base_url
    if api_key:
        config["api_key"] = api_key
    elif base_url:
        config["api_key"] = os.getenv("OPENAI_COMPATIBLE_DUMMY_API_KEY", "not-needed")
    return config


def has_openai_client_config() -> bool:
    return bool(resolve_openai_client_config())


def build_chat_openai(*, model: str, temperature: float = 0, **kwargs: Any):
    from langchain_openai import ChatOpenAI

    return ChatOpenAI(model=model, temperature=temperature, **resolve_openai_client_config(), **kwargs)


def build_openai_embeddings(*, model: str = "text-embedding-3-small", **kwargs: Any):
    from langchain_openai import OpenAIEmbeddings

    return OpenAIEmbeddings(model=model, **resolve_openai_client_config(), **kwargs)


def normalize_id(raw: Optional[str]) -> Optional[str]:
    return normalize_token_alnum(raw)


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


def extract_explicit_member_id(raw: Optional[str]) -> Optional[str]:
    if not raw:
        return None
    text = raw.strip().upper()
    m = re.search(r"\bMB[^A-Z0-9]*\d{3}\b", text)
    if m:
        return normalize_member_id(m.group(0))
    m = re.search(r"\bM[^A-Z0-9]*\d{3}\b", text)
    if m:
        return normalize_member_id(m.group(0))
    return None


def member_id_aliases(raw: Optional[str]) -> List[str]:
    norm = normalize_member_id(raw)
    if not norm:
        return []
    digits = norm[-3:]
    aliases: List[str] = []
    for candidate in (norm, f"M{digits}", digits):
        normalized = normalize_token_alnum(candidate)
        if normalized and normalized not in aliases:
            aliases.append(normalized)
    return aliases


def sql_norm_alnum(value: Any) -> str:
    if value is None:
        return ""
    return normalize_token_alnum(str(value)) or ""


def register_sqlite_alnum_normalizer(conn: sqlite3.Connection, function_name: str = "NORM_ALNUM") -> None:
    fn: Callable[[Any], str] = sql_norm_alnum
    conn.create_function(function_name, 1, fn)


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
