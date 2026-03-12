"""
Build Chroma vector stores for Membership Fraud issue-help KBs.

Examples:
  python build_issue_help_chroma.py --category all --rebuild
  python build_issue_help_chroma.py --category login --rebuild
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
from pathlib import Path
from typing import Any, Dict, List

from dotenv import find_dotenv, load_dotenv
from langchain_chroma import Chroma
from langchain_core.documents import Document
from langchain_openai import OpenAIEmbeddings


THIS_DIR = Path(__file__).resolve().parent
REPO_ROOT = THIS_DIR.parents[2]

KB_CONFIG: Dict[str, Dict[str, Any]] = {
    "login": {
        "jsonl": THIS_DIR / "login_help_kb.jsonl",
        "persist_dir": THIS_DIR / "login_help_chroma",
        "collection": "membership_fraud_login_help",
    },
    "billing": {
        "jsonl": THIS_DIR / "billing_help_kb.jsonl",
        "persist_dir": THIS_DIR / "billing_help_chroma",
        "collection": "membership_fraud_billing_help",
    },
    "renewal": {
        "jsonl": THIS_DIR / "renewal_help_kb.jsonl",
        "persist_dir": THIS_DIR / "renewal_help_chroma",
        "collection": "membership_fraud_renewal_help",
    },
}


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            text = line.strip()
            if not text:
                continue
            try:
                obj = json.loads(text)
            except json.JSONDecodeError as exc:
                raise ValueError(f"Invalid JSON at line {line_no} in {path}: {exc}") from exc
            if not isinstance(obj, dict):
                raise ValueError(f"Expected object at line {line_no} in {path}.")
            rows.append(obj)
    return rows


def _to_documents(rows: List[Dict[str, Any]], prefix: str) -> List[Document]:
    docs: List[Document] = []
    for i, row in enumerate(rows, start=1):
        q = str(row.get("question") or "").strip()
        a = str(row.get("answer") or "").strip()
        if not q or not a:
            continue
        doc_id = str(row.get("id") or f"{prefix}-help-{i:03d}")
        page = f"Question: {q}\nAnswer: {a}\nCategory: {prefix}"
        docs.append(
            Document(
                page_content=page,
                metadata={
                    "id": doc_id,
                    "question": q,
                    "answer": a,
                    "category": prefix,
                },
            )
        )
    return docs


def _build_for_category(
    category: str,
    embedding_model: str,
    rebuild: bool,
    api_key: str,
) -> None:
    cfg = KB_CONFIG[category]
    jsonl_path = Path(cfg["jsonl"]).resolve()
    persist_dir = Path(cfg["persist_dir"]).resolve()
    collection = str(cfg["collection"])

    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL file not found for category '{category}': {jsonl_path}")

    rows = _load_jsonl(jsonl_path)
    docs = _to_documents(rows, prefix=category)
    if not docs:
        raise ValueError(f"No valid question/answer rows found in {jsonl_path}")

    if rebuild and persist_dir.exists():
        shutil.rmtree(persist_dir)
    persist_dir.mkdir(parents=True, exist_ok=True)

    embeddings = OpenAIEmbeddings(model=embedding_model, api_key=api_key)
    store = Chroma(
        collection_name=collection,
        persist_directory=str(persist_dir),
        embedding_function=embeddings,
    )
    ids = [d.metadata["id"] for d in docs]
    store.add_documents(docs, ids=ids)

    print(f"[{category}] indexed {len(docs)} docs into collection='{collection}'.")
    print(f"[{category}] persist dir: {persist_dir}")
    print(f"[{category}] source JSONL: {jsonl_path}")


def main() -> None:
    load_dotenv(find_dotenv(usecwd=True))
    root_env = REPO_ROOT / ".env"
    if root_env.exists():
        load_dotenv(root_env, override=False)

    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--category",
        choices=["all", "login", "billing", "renewal"],
        default="all",
        help="Which issue KB to index",
    )
    ap.add_argument("--embedding-model", default="text-embedding-3-small", help="OpenAI embedding model")
    ap.add_argument("--rebuild", action="store_true", help="Delete existing persist directory before building")
    args = ap.parse_args()

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Add it to environment variables or to "
            f"the repo root .env file at: {root_env}"
        )

    categories = ["login", "billing", "renewal"] if args.category == "all" else [args.category]
    for category in categories:
        _build_for_category(
            category=category,
            embedding_model=args.embedding_model,
            rebuild=bool(args.rebuild),
            api_key=api_key,
        )


if __name__ == "__main__":
    main()
