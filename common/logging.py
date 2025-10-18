"""Logging utilities used by the SciPaper Hub project."""

from __future__ import annotations

import logging
import sys
from typing import Optional

_DEFAULT_FORMAT = "%(asctime)s %(levelname)s [%(name)s] %(message)s"


def configure_logging(level: int = logging.INFO, *, fmt: Optional[str] = None) -> None:
    """Configure root logging with sane defaults.

    Parameters
    ----------
    level:
        Logging level for the root logger.
    fmt:
        Optional logging format string. When omitted, a structured default
        format suitable for Cloud Logging ingestion is used.
    """

    logging.basicConfig(
        level=level,
        format=fmt or _DEFAULT_FORMAT,
        stream=sys.stdout,
        force=True,
    )


__all__ = ["configure_logging"]
