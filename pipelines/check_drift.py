"""Detect distribution drift in normalized records."""

from __future__ import annotations

import argparse
import logging
from typing import Dict

import pandas as pd

from common.logging import configure_logging

configure_logging()


def _load_counts(path: str) -> Dict[str, float]:
    df = pd.read_parquet(path)
    counts = df["primary_category"].fillna("<unknown>").value_counts(normalize=True)
    return counts.to_dict()


def check_drift(reference_path: str, new_path: str, threshold: float = 0.2) -> Dict[str, float]:
    logging.info(
        "Checking primary_category drift. Reference: %s, New: %s", reference_path, new_path
    )
    reference_counts = _load_counts(reference_path)
    new_counts = _load_counts(new_path)

    categories = set(reference_counts) | set(new_counts)
    drift_scores: Dict[str, float] = {}
    flagged = []

    for category in sorted(categories):
        ref_value = reference_counts.get(category, 0.0)
        new_value = new_counts.get(category, 0.0)
        diff = abs(ref_value - new_value)
        drift_scores[category] = diff
        if diff > threshold:
            flagged.append((category, diff, ref_value, new_value))

    if flagged:
        for category, diff, ref_value, new_value in flagged:
            logging.warning(
                "Detected drift in category '%s': reference=%.3f new=%.3f diff=%.3f",
                category,
                ref_value,
                new_value,
                diff,
            )
    else:
        logging.info("No significant drift detected (threshold %.2f).", threshold)

    return drift_scores


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(description="Check for primary_category drift")
    parser.add_argument("reference", help="Reference records.parquet path")
    parser.add_argument("new", help="Newly generated records.parquet path")
    parser.add_argument(
        "--threshold", type=float, default=0.2, help="Drift threshold for value differences"
    )
    args = parser.parse_args()
    check_drift(args.reference, args.new, threshold=args.threshold)


if __name__ == "__main__":  # pragma: no cover
    main()

