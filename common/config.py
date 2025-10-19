"""Configuration helpers for SciPaper Hub services.

This module centralizes environment configuration and secret lookup logic
for both the online service and the nightly batch pipelines.
"""

from __future__ import annotations

import os
from dataclasses import dataclass
from functools import lru_cache
from typing import Optional


@dataclass(frozen=True)
class Settings:
    """Runtime configuration loaded from environment variables."""

    project_id: str
    region: str
    data_bucket: str
    vector_collection_id: str
    vertex_location: Optional[str] = None

    @property
    def gcs_bucket_uri(self) -> str:
        return f"gs://{self.data_bucket}" if self.data_bucket else ""


REQUIRED_VARS = {
    "PROJECT_ID": "Google Cloud project id used for API calls.",
    "REGION": "Default region for Vertex AI resources.",
    "DATA_BUCKET": "Cloud Storage bucket for pipeline artifacts.",
    "VECTOR_COLLECTION_ID": "Vertex Vector Search collection identifier.",
}


def _get_env(name: str, *, required: bool = True) -> str:
    value = os.getenv(name)
    if required and not value:
        raise RuntimeError(f"Missing required environment variable: {name}")
    return value or ""


@lru_cache()
def get_settings() -> Settings:
    """Load :class:`Settings` from the environment."""

    values = {var.lower(): _get_env(var) for var in REQUIRED_VARS}
    vertex_location = os.getenv("VERTEX_LOCATION")
    return Settings(vertex_location=vertex_location, **values)


__all__ = ["Settings", "get_settings"]
