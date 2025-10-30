"""Tests for drift detection pipeline."""

from __future__ import annotations

import logging
from pathlib import Path

import pandas as pd

from pipelines.check_drift import check_drift


def test_check_drift_detects_large_changes(tmp_path: Path, caplog) -> None:
    ref_df = pd.DataFrame({"primary_category": ["cs.AI", "cs.AI", "cs.CV"]})
    new_df = pd.DataFrame({"primary_category": ["cs.AI", "cs.CV", "cs.CV"]})

    ref_path = tmp_path / "ref.parquet"
    new_path = tmp_path / "new.parquet"
    ref_df.to_parquet(ref_path, index=False)
    new_df.to_parquet(new_path, index=False)

    with caplog.at_level(logging.WARNING):
        drift = check_drift(str(ref_path), str(new_path), threshold=0.1)

    assert drift["cs.CV"] > 0.1
    assert any("Detected drift" in record.message for record in caplog.records)

