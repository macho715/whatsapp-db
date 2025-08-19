from __future__ import annotations

import os
import sqlite3
from typing import Optional, List

import faiss  # type: ignore
import polars as pl
from fastapi import FastAPI
from pydantic import BaseModel, Field
from sentence_transformers import SentenceTransformer


SQLITE_DB = os.path.join("data", "sqlite", "messages.db")
MAPPING = os.path.join("hvdc_logs", "mapping.parquet")
FAISS_IDX = os.path.join("hvdc_logs", "faiss.idx")
MODEL = "all-MiniLM-L6-v2"

app = FastAPI(title="HVDC Search API (Option B)")

model = SentenceTransformer(MODEL)
index = faiss.read_index(FAISS_IDX) if os.path.exists(FAISS_IDX) else None
mapping = pl.read_parquet(MAPPING) if os.path.exists(MAPPING) else pl.DataFrame({"id": [], "vec_id": []})


class QueryReq(BaseModel):
    q: str = Field(..., min_length=1)
    top_k: int = 5
    group_name: Optional[str] = None


def sqlite_fts_query(q: str, group: Optional[str], limit: int) -> List[str]:
    con = sqlite3.connect(SQLITE_DB)
    cur = con.cursor()
    if group:
        cur.execute(
            "SELECT id FROM messages_fts WHERE messages_fts MATCH ? AND group_name=? LIMIT ?",
            (q, group, limit),
        )
    else:
        cur.execute(
            "SELECT id FROM messages_fts WHERE messages_fts MATCH ? LIMIT ?",
            (q, limit),
        )
    ids = [r[0] for r in cur.fetchall()]
    con.close()
    return ids


@app.post("/search")
def search(req: QueryReq):
    # 1) keyword candidates via FTS5
    candidates = sqlite_fts_query(req.q, req.group_name, limit=500)
    if not candidates and index is None:
        return {"items": []}

    # 2) FAISS rank
    if not candidates:
        # global FAISS
        q_emb = model.encode([req.q], convert_to_numpy=True)
        faiss.normalize_L2(q_emb)
        D, I = index.search(q_emb, req.top_k)
        vec_ids = list(I[0])
    else:
        # restrict by candidates
        mp = mapping.filter(pl.col("id").is_in(candidates))
        if mp.height == 0:
            vec_ids = []
        else:
            q_emb = model.encode([req.q], convert_to_numpy=True)
            faiss.normalize_L2(q_emb)
            D, I = index.search(q_emb, min(max(req.top_k, 100), index.ntotal))
            allowed = set(mp["vec_id"].to_list())
            vec_ids = [vid for vid in I[0] if vid in allowed][: req.top_k]

    if not vec_ids:
        return {"items": []}

    vec_df = pl.DataFrame({"vec_id": vec_ids})
    ids_df = vec_df.join(mapping, on="vec_id", how="inner").select(["id"])  # vec_id -> id
    ids = ids_df["id"].to_list()

    # 3) fetch rows
    con = sqlite3.connect(SQLITE_DB)
    cur = con.cursor()
    qmarks = ",".join(["?"] * len(ids))
    cur.execute(
        f"SELECT id,date_gst,group_name,sender,body FROM messages WHERE id IN ({qmarks})",
        ids,
    )
    rows = cur.fetchall()
    con.close()

    items = [
        {"id": r[0], "date_gst": r[1], "group_name": r[2], "sender": r[3], "body": r[4]}
        for r in rows
    ]
    return {"items": items}


