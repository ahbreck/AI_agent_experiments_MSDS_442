"""
Build a Chroma vector store for Membership Fraud security-help Q/A.

Example:
  python build_security_help_chroma.py --rebuild
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
DEFAULT_JSONL = THIS_DIR / "security_help_kb.jsonl"
DEFAULT_PERSIST_DIR = THIS_DIR / "security_help_chroma"
DEFAULT_COLLECTION = "membership_fraud_security_help"


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


def _to_documents(rows: List[Dict[str, Any]]) -> List[Document]:
    docs: List[Document] = []
    for i, row in enumerate(rows, start=1):
        q = str(row.get("question") or "").strip()
        a = str(row.get("answer") or "").strip()
        if not q or not a:
            continue
        doc_id = str(row.get("id") or f"sec-help-{i:03d}")
        topic = str(row.get("topic") or "general")
        tags = row.get("tags") or []
        if not isinstance(tags, list):
            tags = [str(tags)]
        tags_str = ", ".join(str(t) for t in tags if str(t).strip())
        page = (
            f"Question: {q}\n"
            f"Answer: {a}\n"
            f"Topic: {topic}\n"
            f"Tags: {tags_str}"
        )
        docs.append(
            Document(
                page_content=page,
                metadata={
                    "id": doc_id,
                    "question": q,
                    "answer": a,
                    "topic": topic,
                    "tags": tags,
                },
            )
        )
    return docs


def main() -> None:
    # Support both notebook-style auto-discovery and explicit repo-root .env loading.
    load_dotenv(find_dotenv(usecwd=True))
    root_env = REPO_ROOT / ".env"
    if root_env.exists():
        load_dotenv(root_env, override=False)

    ap = argparse.ArgumentParser()
    ap.add_argument("--jsonl", default=str(DEFAULT_JSONL), help="Path to security help JSONL file")
    ap.add_argument("--persist-dir", default=str(DEFAULT_PERSIST_DIR), help="Path to Chroma persistence directory")
    ap.add_argument("--collection", default=DEFAULT_COLLECTION, help="Chroma collection name")
    ap.add_argument("--embedding-model", default="text-embedding-3-small", help="OpenAI embedding model")
    ap.add_argument("--rebuild", action="store_true", help="Delete existing persist directory before building")
    args = ap.parse_args()

    jsonl_path = Path(args.jsonl).resolve()
    persist_dir = Path(args.persist_dir).resolve()
    if not jsonl_path.exists():
        raise FileNotFoundError(f"JSONL file not found: {jsonl_path}")

    rows = _load_jsonl(jsonl_path)
    docs = _to_documents(rows)
    if not docs:
        raise ValueError(f"No valid question/answer rows found in {jsonl_path}")

    if args.rebuild and persist_dir.exists():
        shutil.rmtree(persist_dir)
    persist_dir.mkdir(parents=True, exist_ok=True)

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        raise RuntimeError(
            "OPENAI_API_KEY is not set. Add it to environment variables or to "
            f"the repo root .env file at: {root_env}"
        )

    embeddings = OpenAIEmbeddings(model=args.embedding_model, api_key=api_key)
    store = Chroma(
        collection_name=args.collection,
        persist_directory=str(persist_dir),
        embedding_function=embeddings,
    )

    ids = [d.metadata["id"] for d in docs]
    store.add_documents(docs, ids=ids)
    print(f"Indexed {len(docs)} docs into collection='{args.collection}'.")
    print(f"Persist dir: {persist_dir}")
    print(f"Source JSONL: {jsonl_path}")


if __name__ == "__main__":
    main()
