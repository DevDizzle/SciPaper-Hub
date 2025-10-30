"""Offline evaluation pipeline for the recommendation model."""

from __future__ import annotations

import argparse
import logging
import math
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence, Tuple

import numpy as np
import pandas as pd
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity

from common.logging import configure_logging

configure_logging()


@dataclass
class EvaluationResult:
    hit_rate: float
    ndcg: float
    by_category: Dict[str, "EvaluationResult"]


def _chronological_split(
    df: pd.DataFrame, timestamp_col: str = "published_at", train_fraction: float = 0.8
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    df = df.copy()
    df[timestamp_col] = pd.to_datetime(df[timestamp_col], errors="coerce")
    df = df.dropna(subset=[timestamp_col])
    df = df.sort_values(timestamp_col)
    split_idx = max(1, int(len(df) * train_fraction))
    split_idx = min(split_idx, len(df) - 1)
    return df.iloc[:split_idx], df.iloc[split_idx:]


def _build_index(train_df: pd.DataFrame) -> Tuple[TfidfVectorizer, np.ndarray]:
    vectorizer = TfidfVectorizer(stop_words="english")
    matrix = vectorizer.fit_transform(train_df["abstract"].fillna(""))
    return vectorizer, matrix


def _rank_neighbors(
    vectorizer: TfidfVectorizer, matrix: np.ndarray, abstract: str, top_k: int
) -> Sequence[int]:
    query_vec = vectorizer.transform([abstract])
    similarities = cosine_similarity(query_vec, matrix)[0]
    top_indices = np.argsort(similarities)[::-1][:top_k]
    return top_indices


def _dcg(relevances: Sequence[int]) -> float:
    return sum((2 ** rel - 1) / math.log2(idx + 2) for idx, rel in enumerate(relevances))


def _ndcg(relevances: Sequence[int]) -> float:
    if not relevances:
        return 0.0
    ideal = sorted(relevances, reverse=True)
    ideal_dcg = _dcg(ideal)
    if ideal_dcg == 0:
        return 0.0
    return _dcg(relevances) / ideal_dcg


def _evaluate_group(
    relevances: Iterable[Sequence[int]],
) -> EvaluationResult:
    hit_rates: List[float] = []
    ndcgs: List[float] = []
    for rel in relevances:
        hit_rates.append(1.0 if any(rel) else 0.0)
        ndcgs.append(_ndcg(rel))
    hit_rate = float(np.mean(hit_rates)) if hit_rates else 0.0
    ndcg = float(np.mean(ndcgs)) if ndcgs else 0.0
    return EvaluationResult(hit_rate=hit_rate, ndcg=ndcg, by_category={})


def evaluate(records_path: str, k: int = 10, train_fraction: float = 0.8) -> EvaluationResult:
    logging.info("Loading records from %s", records_path)
    df = pd.read_parquet(records_path)
    if df.empty:
        raise ValueError("No records found in the provided parquet file.")

    train_df, test_df = _chronological_split(df, train_fraction=train_fraction)
    logging.info("Train size: %s, Test size: %s", len(train_df), len(test_df))

    vectorizer, matrix = _build_index(train_df)

    overall_relevances: List[Sequence[int]] = []
    per_category: Dict[str, List[Sequence[int]]] = defaultdict(list)

    for _, row in test_df.iterrows():
        abstract = row.get("abstract", "") or ""
        category = row.get("primary_category", "")
        if not abstract.strip():
            continue
        neighbor_indices = _rank_neighbors(vectorizer, matrix, abstract, k)
        relevances = [
            1
            if train_df.iloc[idx].get("primary_category", "") == category and category
            else 0
            for idx in neighbor_indices
        ]
        overall_relevances.append(relevances)
        per_category[category].append(relevances)

    result = _evaluate_group(overall_relevances)
    result.by_category = {
        category: _evaluate_group(rels) for category, rels in per_category.items()
    }

    logging.info("Overall HitRate@%s: %.4f", k, result.hit_rate)
    logging.info("Overall nDCG@%s: %.4f", k, result.ndcg)

    for category, metrics in sorted(result.by_category.items()):
        logging.info(
            "Category %s -> HitRate@%s: %.4f, nDCG@%s: %.4f",
            category or "<unknown>",
            k,
            metrics.hit_rate,
            k,
            metrics.ndcg,
        )

    return result


def main() -> None:  # pragma: no cover
    parser = argparse.ArgumentParser(description="Offline evaluation for recommendations")
    parser.add_argument("records", help="Path to records.parquet produced by normalize.py")
    parser.add_argument("--k", type=int, default=10, help="Number of neighbors to retrieve")
    parser.add_argument(
        "--train-fraction",
        type=float,
        default=0.8,
        help="Fraction of data to use for training portion of evaluation",
    )
    args = parser.parse_args()
    evaluate(args.records, k=args.k, train_fraction=args.train_fraction)


if __name__ == "__main__":  # pragma: no cover
    main()

