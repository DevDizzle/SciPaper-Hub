"""Logging utilities used by the SciPaper Hub project."""

from __future__ import annotations

import logging
import os
import sys
from typing import Optional

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def configure_logging(level: int = logging.INFO, *, fmt: Optional[str] = None) -> None:
    """Configure root logging with sane defaults.

    Tries to use the Google Cloud Logging library if in a production environment,
    otherwise falls back to a standard basicConfig setup for local/test environments.
    """
    if os.getenv("ENV") == "production":
        try:
            import google.cloud.logging
            client = google.cloud.logging.Client()
            client.setup_logging(log_level=level)
            logging.info("Successfully configured Google Cloud Logging handler for production.")
            return
        except ImportError:
            logging.info("google-cloud-logging not found, using basicConfig even in production ENV.")

    # Fallback for local, test, or non-GCP environments
    logging.basicConfig(
        level=level,
        format=fmt or _DEFAULT_FORMAT,
        stream=sys.stdout,
        force=True,
    )


__all__ = ["configure_logging"]
