"""Vertex AI embedding client for gemini-embedding-001."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass, field
from typing import List, Sequence

import vertexai

try:  # pragma: no cover - fallback import path for older SDKs
    from vertexai.language_models import TextEmbeddingModel
except ImportError:  # pragma: no cover
    from vertexai.preview.language_models import TextEmbeddingModel  # type: ignore

from common.config import get_settings

MODEL_ID = "gemini-embedding-001"


def _hash_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


@dataclass
class VertexEmbeddingClient:
    """Client wrapper around Vertex AI text embeddings."""

    model_id: str = MODEL_ID
    _model: TextEmbeddingModel = field(init=False, repr=False)
    _cache: dict[str, List[float]] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        settings = get_settings()
        vertexai.init(project=settings.project_id, location=settings.vertex_location or settings.region)
        self._model = TextEmbeddingModel.from_pretrained(self.model_id)

    def _get_from_cache(self, text: str) -> List[float] | None:
        return self._cache.get(_hash_text(text))

    def _store_cache(self, text: str, embedding: List[float]) -> None:
        self._cache[_hash_text(text)] = embedding

    def embed_text(self, text: str) -> List[float]:
        cached = self._get_from_cache(text)
        if cached is not None:
            return cached
        embeddings = self._model.get_embeddings([text])
        vector = list(embeddings[0].values)
        self._store_cache(text, vector)
        return vector

    def embed_batch(self, texts: Sequence[str]) -> List[List[float]]:
        embeddings = []
        uncached_texts: list[str] = []
        uncached_indices: list[int] = []

        for idx, text in enumerate(texts):
            cached = self._get_from_cache(text)
            if cached is not None:
                embeddings.append(cached)
            else:
                embeddings.append([])  # placeholder
                uncached_texts.append(text)
                uncached_indices.append(idx)

        if uncached_texts:
            responses = self._model.get_embeddings(uncached_texts)
            for i, response in enumerate(responses):
                vector = list(response.values)
                original_idx = uncached_indices[i]
                embeddings[original_idx] = vector
                self._store_cache(uncached_texts[i], vector)

        return embeddings


__all__ = ["VertexEmbeddingClient", "MODEL_ID"]
