"""FastAPI app exposing the /search endpoint."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, List, Optional

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, HttpUrl

from common.logging import configure_logging
from service import arxiv_client
from service.embed_vertex import VertexEmbeddingClient
from service.vector_search import VertexVectorSearchClient

configure_logging()
app = FastAPI(title="SciPaper Hub Similarity Service")
_embedding_client = VertexEmbeddingClient()
_vector_client = VertexVectorSearchClient()


class SearchRequest(BaseModel):
    url: HttpUrl
    k: Optional[int] = 10


class MatchResponse(BaseModel):
    score: float
    title: str
    authors: List[str]
    primary_category: str
    abstract_snippet: str
    link_abs: str
    link_pdf: Optional[str]


class QueryResponse(BaseModel):
    arxiv_id: str
    title: str
    abstract_snippet: str
    link_abs: str
    primary_category: str


class SearchResponse(BaseModel):
    query: QueryResponse
    matches: List[MatchResponse]
    as_of: datetime


@app.post("/search", response_model=SearchResponse)
async def search(request: SearchRequest) -> SearchResponse:
    arxiv_id = arxiv_client.parse_arxiv_url(str(request.url))
    if not arxiv_id:
        raise HTTPException(status_code=400, detail="Unsupported arXiv URL format")
    try:
        record = arxiv_client.get_by_id(arxiv_id)
    except arxiv_client.ArxivNotFoundError as exc:  # pragma: no cover - network failure
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    embedding = _embedding_client.embed_text(record["abstract"])  # type: ignore[arg-type]
    k = request.k or 10
    neighbors = _vector_client.search(embedding, k=k)

    query_payload = QueryResponse(
        arxiv_id=record["arxiv_id"],
        title=record["title"],
        abstract_snippet=_snippet(record["abstract"]),
        link_abs=record["links"].get("abs", f"https://arxiv.org/abs/{record['arxiv_id']}")
        if isinstance(record.get("links"), dict)
        else f"https://arxiv.org/abs/{record['arxiv_id']}",
        primary_category=record.get("primary_category", ""),
    )

    matches: List[MatchResponse] = []
    for neighbor in neighbors:
        metadata = neighbor.get("metadata", {})
        matches.append(
            MatchResponse(
                score=_to_similarity(neighbor.get("score", 0.0)),
                title=metadata.get("title", ""),
                authors=_parse_list(metadata.get("authors")),
                primary_category=metadata.get("primary_category", ""),
                abstract_snippet=_snippet(metadata.get("abstract", "")),
                link_abs=metadata.get("link_abs", ""),
                link_pdf=metadata.get("link_pdf"),
            )
        )

    return SearchResponse(query=query_payload, matches=matches, as_of=datetime.now(timezone.utc))


def _snippet(text: str, length: int = 400) -> str:
    return (text or "").strip()[:length]


def _parse_list(raw: Any) -> List[str]:
    if isinstance(raw, list):
        return [str(x) for x in raw]
    if isinstance(raw, str):
        raw = raw.strip()
        if raw.startswith("["):
            try:
                import json

                loaded = json.loads(raw)
                if isinstance(loaded, list):
                    return [str(x) for x in loaded]
            except json.JSONDecodeError:  # pragma: no cover - best effort
                pass
        return [item.strip() for item in raw.split(";") if item.strip()]
    return []


def _to_similarity(distance: Any) -> float:
    try:
        value = float(distance)
    except (TypeError, ValueError):  # pragma: no cover - defensive
        return 0.0
    # Vertex Vector Search returns distance; for cosine distance we convert to similarity.
    return max(0.0, min(1.0, 1.0 - value))


__all__ = ["app"]
