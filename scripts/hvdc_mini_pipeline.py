"""
Minimal Option-B pipeline: Bronze(JSONL) -> Parquet(Polars) -> FAISS index

Run under WSL hvdc311 venv (recommended):
  source ~/hvdc311/bin/activate
  python3 scripts/hvdc_mini_pipeline.py

Requires (WSL):
  pip install polars pyarrow sentence-transformers faiss-cpu
"""

from __future__ import annotations

import os
from glob import glob
import polars as pl
import faiss  # type: ignore
from sentence_transformers import SentenceTransformer

MODEL_NAME = "all-MiniLM-L6-v2"
BRONZE_GLOB = os.path.join("hvdc_logs", "bronze", "*.jsonl")
PARQUET_OUT = os.path.join("hvdc_logs", "silver", "messages.parquet")
FAISS_OUT = os.path.join("hvdc_logs", "faiss.idx")
MAPPING_OUT = os.path.join("hvdc_logs", "mapping.parquet")


def ensure_dirs() -> None:
    os.makedirs(os.path.dirname(PARQUET_OUT), exist_ok=True)
    os.makedirs(os.path.dirname(FAISS_OUT), exist_ok=True)


def bronze_to_parquet() -> pl.DataFrame:
    files = glob(BRONZE_GLOB)
    if not files:
        print(f"No bronze files matched: {BRONZE_GLOB}")
    # Lazy scan NDJSON for scalability
    ldf = pl.scan_ndjson(BRONZE_GLOB)
    df = (
        ldf.with_columns(
            pl.col("date_gst")
            .cast(pl.Utf8)
            .str.strptime(pl.Datetime, fmt="%Y-%m-%d %H:%M", strict=False)
            .alias("date_gst")
        )
        .select([
            pl.col("id").cast(pl.Utf8),
            pl.col("date_gst"),
            pl.col("group_name").cast(pl.Utf8),
            pl.col("sender").cast(pl.Utf8).alias("sender"),
            pl.col("summary").cast(pl.Utf8).alias("body"),
            pl.col("sla_breaches").cast(pl.Int64).fill_null(0),
        ])
        .collect()
    )
    # Partition key-friendly date string
    df = df.with_columns(pl.col("date_gst").dt.date().cast(pl.Utf8).alias("date"))
    df.write_parquet(PARQUET_OUT)
    print(f"Wrote parquet: {PARQUET_OUT}  (rows={len(df)})")
    return df


def build_faiss(df: pl.DataFrame | None = None) -> None:
    if df is None:
        df = pl.read_parquet(PARQUET_OUT)
    if df.is_empty():
        print("No rows to index; skip FAISS build")
        return
    texts = df["body"].to_list()
    model = SentenceTransformer(MODEL_NAME)
    emb = model.encode(texts, convert_to_numpy=True, show_progress_bar=True)
    # Use cosine (inner product on L2-normalized vectors)
    faiss.normalize_L2(emb)
    d = emb.shape[1]
    index = faiss.IndexFlatIP(d)
    index.add(emb)
    faiss.write_index(index, FAISS_OUT)
    mapping = pl.DataFrame({"id": df["id"]})
    mapping = mapping.with_columns(pl.Series("vec_id", list(range(len(mapping)))))
    mapping.write_parquet(MAPPING_OUT)
    print(f"FAISS index: {FAISS_OUT}  | mapping: {MAPPING_OUT}")


def main() -> None:
    ensure_dirs()
    df = bronze_to_parquet()
    build_faiss(df)
    print("Option-B mini pipeline done.")


if __name__ == "__main__":
    main()


