"""FastAPI app exposing the /search endpoint."""

from __future__ import annotations

import hashlib
from typing import Optional
import logging

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from google.cloud import aiplatform

from common.config import get_settings
from common.logging import configure_logging
from service import arxiv_client
from service.embed_vertex import embed_text
from service.vector_search import VectorSearchClient, VectorSearchConfig

configure_logging()

app = FastAPI(title="PaperRec Search API", version="0.1.0")

# --- Provenance and A/B Testing Configuration ---
# These are now loaded via get_settings()
# B_DEPLOYED_INDEX_ID = os.getenv("B_DEPLOYED_INDEX_ID")
# GIT_SHA = os.getenv("GIT_SHA", "unknown")
# IMAGE_DIGEST = os.getenv("IMAGE_DIGEST", "unknown")


class SearchRequest(BaseModel):
    url: str
    k: int = 5


@app.get("/healthz")
@app.get("/healhz")
def healthz():
    """Health endpoint used by Cloud Run uptime checks.

    Historically the path was sometimes misspelled as `/healhz` in
    infrastructure configs, so we serve both spellings to avoid 404s.
    """
    return {"ok": True}


@app.exception_handler(Exception)
async def unhandled(request: Request, exc: Exception):
    return JSONResponse(status_code=500, content={"error": str(exc)})


_vector_client_A: Optional[VectorSearchClient] = None
_vector_client_B: Optional[VectorSearchClient] = None


@app.on_event("startup")
def _init_clients():
    global _vector_client_A, _vector_client_B
    try:
        settings = get_settings()
        logging.info(f"Settings loaded: Project ID={settings.project_id}, Region={settings.region}, Vertex Location={settings.vertex_location}")

        # Initialize Vertex AI SDK
        try:
            aiplatform.init(project=settings.project_id, location=settings.vertex_location or settings.region)
            logging.info("Vertex AI SDK initialized.")
        except Exception as e:
            logging.error(f"Failed to initialize Vertex AI SDK: {e}", exc_info=True)
            raise

        # Initialize _vector_client_A
        deployed_index_id_A = settings.deployed_index_id
        if deployed_index_id_A:
            try:
                cfg_A = VectorSearchConfig(
                    project_id=settings.project_id,
                    region=settings.region,
                    index_endpoint=settings.index_endpoint_id,
                    deployed_index_id=deployed_index_id_A,
                    vertex_location=settings.vertex_location,
                )
                _vector_client_A = VectorSearchClient(cfg_A)
                logging.info("Initialized vector client A for index %s", deployed_index_id_A)
            except Exception as e:
                logging.error(f"Failed to initialize vector client A: {e}", exc_info=True)
                raise
        else:
            logging.warning("DEPLOYED_INDEX_ID not set, vector client A will not be initialized.")

        # Initialize _vector_client_B
        if settings.b_deployed_index_id:
            try:
                cfg_B = VectorSearchConfig(
                    project_id=settings.project_id,
                    region=settings.region,
                    index_endpoint=settings.index_endpoint_id,
                    deployed_index_id=settings.b_deployed_index_id,
                    vertex_location=settings.vertex_location,
                )
                _vector_client_B = VectorSearchClient(cfg_B)
                logging.info("Initialized vector client B for index %s", settings.b_deployed_index_id)
            except Exception as e:
                logging.error(f"Failed to initialize vector client B: {e}", exc_info=True)
                raise
        else:
            logging.warning("B_DEPLOYED_INDEX_ID not set, vector client B will not be initialized.")

    except Exception as e:
        logging.critical(f"Application startup failed: {e}", exc_info=True)
        # Re-raise to ensure Cloud Run reports failure
        raise


async def _maybe_fetch_abstract(url: str) -> str:
    arxiv_id = arxiv_client.parse_arxiv_url(url)
    if not arxiv_id:
        raise HTTPException(
            status_code=400, detail=f"Could not parse arXiv ID from URL: {url}"
        )
    entry = arxiv_client.get_by_id(arxiv_id)
    return str(entry["abstract"])


@app.post("/search")
async def search(req: SearchRequest, request: Request):
    settings = get_settings()
    # A/B testing logic
    remote_ip: str = request.client.host if request.client and request.client.host else "127.0.0.1"
    ip_hash = int(hashlib.md5(remote_ip.encode()).hexdigest(), 16)

    client_to_use: Optional[VectorSearchClient] = None
    user_group: str
    model_version: str

    if settings.b_deployed_index_id and _vector_client_B and (ip_hash % 100 < 10):
        user_group = "B"
        model_version = "v3_768d"  # Example name
        client_to_use = _vector_client_B
    else:
        user_group = "A"
        model_version = "v1_768d"  # Example name
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

    log_payload = {
        "query_url": req.url,
        "k": req.k,
        "recommendations": [n["id"] for n in neighbors_list],
        # --- Provenance Fields ---
        "request_id": request_id,
        "user_group": user_group,
        "model_version": model_version,
        "data_snapshot_id": data_snapshot_id,
        "pipeline_git_sha": settings.git_sha or "unknown",
        "container_image_digest": settings.image_digest or "unknown",
    }

    logging.info("RECO_RESPONSE", extra={"json_fields": log_payload})

    return {"query_url": req.url, "k": req.k, **neighbors}















__all__ = ["app"]

# Forcing a redeployment to fix stale service version.
