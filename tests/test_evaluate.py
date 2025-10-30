"""Tests for the offline evaluation pipeline."""

from __future__ import annotations

from pathlib import Path

import pandas as pd

from pipelines.evaluate import evaluate


def test_evaluate_computes_metrics(tmp_path: Path) -> None:
    data = [
        {
            "arxiv_id": "1",
            "base_id": "1",
            "version": 1,
            "title": "AI paper",
            "abstract": "machine learning and neural networks",
            "authors": ["Author A"],
            "primary_category": "cs.AI",
            "categories": ["cs.AI"],
            "published_at": "2024-01-01T00:00:00Z",
            "updated_at": "2024-01-01T00:00:00Z",
        },
        {
            "arxiv_id": "2",
            "base_id": "2",
            "version": 1,
            "title": "Another AI paper",
            "abstract": "deep learning techniques",
            "authors": ["Author B"],
            "primary_category": "cs.AI",
            "categories": ["cs.AI"],
            "published_at": "2024-01-02T00:00:00Z",
            "updated_at": "2024-01-02T00:00:00Z",
        },
        {
            "arxiv_id": "3",
            "base_id": "3",
            "version": 1,
            "title": "Vision paper",
            "abstract": "computer vision and image recognition",
            "authors": ["Author C"],
            "primary_category": "cs.CV",
            "categories": ["cs.CV"],
            "published_at": "2024-01-03T00:00:00Z",
            "updated_at": "2024-01-03T00:00:00Z",
        },
        {
            "arxiv_id": "4",
            "base_id": "4",
            "version": 1,
            "title": "Another vision paper",
            "abstract": "image recognition and computer vision",
            "authors": ["Author D"],
            "primary_category": "cs.CV",
            "categories": ["cs.CV"],
            "published_at": "2024-01-04T00:00:00Z",
            "updated_at": "2024-01-04T00:00:00Z",
        },
    ]
    df = pd.DataFrame(data)
    records_path = tmp_path / "records.parquet"
    df.to_parquet(records_path, index=False)

    result = evaluate(str(records_path), k=2, train_fraction=0.75)

    assert result.hit_rate >= 0.0
    assert result.ndcg >= 0.0
    assert "cs.CV" in result.by_category
    assert result.by_category["cs.CV"].hit_rate >= 0.0

