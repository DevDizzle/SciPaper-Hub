"""Embed normalized records and upsert into Vertex Vector Search."""

from __future__ import annotations

import random
from io import BytesIO
from typing import Iterable, List

import pandas as pd

from common.config import get_settings
from common.gcs import GCSClient
from common.logging import configure_logging
from service.embed_vertex import VertexEmbeddingClient
from service.vector_search import VertexVectorSearchClient

configure_logging()

BATCH_SIZE = 256
PROBE_COUNT = 100


def _chunks(sequence: List[int], size: int) -> Iterable[List[int]]:
    for i in range(0, len(sequence), size):
        yield sequence[i : i + size]


def index_snapshot(snapshot: str, parquet_blob: str | None = None) -> None:
    settings = get_settings()
    client = GCSClient(settings.project_id)
    blob_name = parquet_blob or f"normalized/{snapshot}/records.parquet"
    parquet_bytes = client.download_bytes(settings.data_bucket, blob_name)
    df = pd.read_parquet(BytesIO(parquet_bytes))

    if df.empty:
        return

    embed_client = VertexEmbeddingClient()
    vector_client = VertexVectorSearchClient()

    indices = list(range(len(df)))
    for batch_indices in _chunks(indices, BATCH_SIZE):
        batch_df = df.iloc[batch_indices]
        embeddings = embed_client.embed_batch(batch_df["abstract"].tolist())
        items = []
        for idx, embedding in zip(batch_indices, embeddings):
            row = df.iloc[idx]
            items.append(
                {
                    "id": row["base_id"],
                    "vector": embedding,
                    "metadata": {
                        "title": row["title"],
                        "abstract": row["abstract"],
                        "authors": row["authors"],
                        "primary_category": row["primary_category"],
                        "categories": row["categories"],
                        "published_at": row["published_at"],
                        "updated_at": row["updated_at"],
                        "link_abs": row["link_abs"],
                        "link_pdf": row["link_pdf"],
                        "ingest_snapshot": row["ingest_snapshot"],
                    },
                }
            )
        vector_client.upsert(items)

        probe_ids = random.sample([item["id"] for item in items], min(PROBE_COUNT, len(items)))
        if probe_ids:
            datapoints = vector_client.get_datapoints(probe_ids)
            if len(datapoints) != len(probe_ids):
                missing = set(probe_ids) - set(datapoints)
                raise RuntimeError(f"Probe mismatch detected: missing {len(missing)} ids")


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Index normalized arXiv snapshot")
    parser.add_argument("snapshot", help="Snapshot identifier")
    parser.add_argument("--blob", help="Override normalized parquet blob path")
    args = parser.parse_args()
    index_snapshot(args.snapshot, parquet_blob=args.blob)

