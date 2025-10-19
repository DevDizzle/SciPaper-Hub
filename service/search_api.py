"""FastAPI app exposing the /search endpoint."""

from __future__ import annotations

import os
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from common.config import get_settings
from common.logging import configure_logging
from service import arxiv_client
from service.embed_vertex import embed_text
from service.vector_search import VectorSearchClient, VectorSearchConfig

configure_logging()
app = FastAPI(title="PaperRec Search API", version="0.1.0")


class SearchRequest(BaseModel):
    url: str
    k: int = 5


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"error": str(exc)})


_settings = get_settings()
_index_endpoint = os.getenv("INDEX_ENDPOINT")
_deployed_index_id = os.getenv("DEPLOYED_INDEX_ID")

_vector_client: Optional[VectorSearchClient] = None


def _get_vector_client() -> VectorSearchClient:
    global _vector_client
    if _vector_client is None:
        cfg = VectorSearchConfig(
            project_id=_settings.project_id,
            region=_settings.region,
            index_endpoint=_index_endpoint,
            deployed_index_id=_deployed_index_id,
        )
        _vector_client = VectorSearchClient(cfg)
    return _vector_client


async def _maybe_fetch_abstract(url: str) -> str:
    arxiv_id = arxiv_client.parse_arxiv_url(url)
    if not arxiv_id:
        raise HTTPException(status_code=400, detail="Unsupported arXiv URL format")
    try:
        record = arxiv_client.get_by_id(arxiv_id)
    except arxiv_client.ArxivNotFoundError as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc
    abstract = record.get("abstract")
    if not abstract:
        raise HTTPException(status_code=500, detail="No abstract returned from arXiv.")
    return abstract


@app.post("/search")
async def search(req: SearchRequest):
    if not _index_endpoint or not _deployed_index_id:
        raise HTTPException(
            status_code=500,
            detail="Vector index endpoint not configured (INDEX_ENDPOINT/DEPLOYED_INDEX_ID).",
        )

    abstract = await _maybe_fetch_abstract(req.url)
    vec = embed_text(abstract)
    neighbors = _get_vector_client().search(vec, k=req.k)
    return {"query_url": req.url, "k": req.k, **neighbors}


__all__ = ["app"]
