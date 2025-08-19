"""
Incremental FAISS indexer using SQLite source.

- Reads new rows from SQLite `messages` (by rowid)
- Computes embeddings (Sentence-Transformers)
- Appends to FAISS index and updates mapping parquet

Run (WSL/x64 venv):
  python3 scripts/faiss_index_worker.py

Requires: sentence-transformers, faiss-cpu, polars, numpy
"""

from __future__ import annotations

import os
import time
import sqlite3
from typing import Tuple

import faiss  # type: ignore
import numpy as np
import polars as pl
from sentence_transformers import SentenceTransformer


SQLITE_DB = os.path.join("data", "sqlite", "messages.db")
PARQUET_SILVER = os.path.join("hvdc_logs", "silver", "messages.parquet")
MAPPING_PARQUET = os.path.join("hvdc_logs", "mapping.parquet")
FAISS_IDX = os.path.join("hvdc_logs", "faiss.idx")
MODEL = "all-MiniLM-L6-v2"
BATCH = 128
SLEEP = 30  # seconds


def ensure_paths() -> None:
    os.makedirs(os.path.dirname(SQLITE_DB), exist_ok=True)
    os.makedirs(os.path.dirname(MAPPING_PARQUET), exist_ok=True)


def ensure_meta(con: sqlite3.Connection) -> None:
    cur = con.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS index_meta (k TEXT PRIMARY KEY, v TEXT)"
    )
    con.commit()


def get_last_rowid(con: sqlite3.Connection) -> int:
    cur = con.cursor()
    cur.execute("SELECT v FROM index_meta WHERE k='last_rowid'")
    r = cur.fetchone()
    return int(r[0]) if r else 0


def set_last_rowid(con: sqlite3.Connection, rid: int) -> None:
    cur = con.cursor()
    cur.execute(
        "INSERT OR REPLACE INTO index_meta(k,v) VALUES('last_rowid',?)",
        (str(rid),),
    )
    con.commit()


def load_index() -> faiss.Index | None:
    if os.path.exists(FAISS_IDX):
        idx = faiss.read_index(FAISS_IDX)
        print("Loaded FAISS: ntotal=", idx.ntotal)
        return idx
    return None


def append_mapping(ids: list[str], start_vec_id: int) -> None:
    existing = (
        pl.read_parquet(MAPPING_PARQUET)
        if os.path.exists(MAPPING_PARQUET)
        else pl.DataFrame({"id": [], "vec_id": []})
    )
    new_map = pl.DataFrame(
        {"id": ids, "vec_id": list(range(start_vec_id, start_vec_id + len(ids)))}
    )
    out = pl.concat([existing, new_map]) if len(existing) else new_map
    out.write_parquet(MAPPING_PARQUET)


def main() -> None:
    ensure_paths()
    model = SentenceTransformer(MODEL)
    con = sqlite3.connect(SQLITE_DB)
    ensure_meta(con)
    last = get_last_rowid(con)
    index = load_index()

    while True:
        cur = con.cursor()
        cur.execute(
            "SELECT rowid,id,body FROM messages WHERE rowid > ? ORDER BY rowid LIMIT ?",
            (last, BATCH),
        )
        rows = cur.fetchall()
        if not rows:
            time.sleep(SLEEP)
            continue

        rowids, ids, texts = zip(*rows)
        embs = model.encode(list(texts), convert_to_numpy=True)
        faiss.normalize_L2(embs)

        if index is None:
            d = embs.shape[1]
            index = faiss.IndexFlatIP(d)
        start_vec = index.ntotal
        index.add(embs)
        faiss.write_index(index, FAISS_IDX)
        append_mapping(list(ids), start_vec)

        last = int(rowids[-1])
        set_last_rowid(con, last)
        print(f"Indexed up to rowid={last}; ntotal={index.ntotal}")
        time.sleep(1)


if __name__ == "__main__":
    main()


