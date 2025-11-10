"""FastAPI app exposing the /search endpoint."""

from __future__ import annotations

import os
import hashlib
from typing import Optional
import logging

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

# --- Provenance and A/B Testing Configuration ---
B_DEPLOYED_INDEX_ID = os.getenv("B_DEPLOYED_INDEX_ID")
GIT_SHA = os.getenv("GIT_SHA", "unknown")
IMAGE_DIGEST = os.getenv("IMAGE_DIGEST", "unknown")


class SearchRequest(BaseModel):
    url: str
    k: int = 5


@app.get("/healthz")
def healthz():
    return {"ok": True}


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"error": str(exc)})


_vector_client_A: Optional[VectorSearchClient] = None
_vector_client_B: Optional[VectorSearchClient] = None


@app.on_event("startup")
def _init_clients():
    global _vector_client_A, _vector_client_B
    settings = get_settings()
    index_endpoint = os.getenv("INDEX_ENDPOINT_ID")
    deployed_index_id_A = os.getenv("DEPLOYED_INDEX_ID")

    if deployed_index_id_A:
        cfg_A = VectorSearchConfig(
            project_id=settings.project_id,
            region=settings.region,
            index_endpoint=index_endpoint,
            deployed_index_id=deployed_index_id_A,
            vertex_location=settings.vertex_location,
        )
        _vector_client_A = VectorSearchClient(cfg_A)
        logging.info("Initialized vector client A for index %s", deployed_index_id_A)

    if B_DEPLOYED_INDEX_ID:
        cfg_B = VectorSearchConfig(
            project_id=settings.project_id,
            region=settings.region,
            index_endpoint=index_endpoint,
            deployed_index_id=B_DEPLOYED_INDEX_ID,
            vertex_location=settings.vertex_location,
        )
        _vector_client_B = VectorSearchClient(cfg_B)
        logging.info("Initialized vector client B for index %s", B_DEPLOYED_INDEX_ID)


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
async def search(req: SearchRequest, request: Request):
    # A/B testing logic
    remote_ip = request.client.host or "127.0.0.1"
    ip_hash = int(hashlib.md5(remote_ip.encode()).hexdigest(), 16)

    if B_DEPLOYED_INDEX_ID and _vector_client_B and (ip_hash % 100 < 10):
        user_group = "B"
        model_version = "v2_768d"  # Example name
        client_to_use = _vector_client_B
    else:
        user_group = "A"
        model_version = "v1_3072d"  # Example name
        client_to_use = _vector_client_A

    if not client_to_use:
        raise HTTPException(
            status_code=500,
            detail="Vector index client not initialized.",
        )

    logging.info("Fetching abstract from arXiv...")
    abstract = await _maybe_fetch_abstract(req.url)
    logging.info("Abstract fetched successfully.")

    logging.info("Embedding abstract...")
    vec = embed_text(abstract)
    logging.info("Abstract embedded successfully.")

    logging.info("Searching for neighbors...")
    neighbors = client_to_use.search(vec, k=req.k)
    logging.info("Neighbors found successfully.")

    # Provenance logging
    request_id = request.headers.get("X-Cloud-Trace-Context", "no-trace")

    neighbors_list = neighbors.get("neighbors", [])
    first_neighbor = neighbors_list[0] if neighbors_list else {}
    first_meta = first_neighbor.get("metadata", {})
    data_snapshot_id = first_meta.get("ingest_snapshot", "unknown")

    logging.info(
        "RECO_RESPONSE",
        extra={
            "json_fields": {
                "query_url": req.url,
                "k": req.k,
                "recommendations": [n["id"] for n in neighbors_list],
                # --- Provenance Fields ---
                "request_id": request_id,
                "user_group": user_group,
                "model_version": model_version,
                "data_snapshot_id": data_snapshot_id,
                "pipeline_git_sha": GIT_SHA,
                "container_image_digest": IMAGE_DIGEST,
            }
        },
    )

    return {"query_url": req.url, "k": req.k, **neighbors}


__all__ = ["app"]