"""Nightly harvester for arXiv categories."""

from __future__ import annotations

import argparse
import time
from datetime import datetime, timedelta, timezone
from typing import Dict, List
from xml.etree import ElementTree as ET

import subprocess
import shlex
import urllib.parse

from common.config import get_settings
from common.gcs import GCSClient
from common.logging import configure_logging

configure_logging()

DEFAULT_CATEGORIES = ["cs.AI", "cs.LG", "cs.CL", "cs.RO", "cs.CV"]
MAX_RESULTS = 2000
SLEEP_SECONDS = 3.0


def _build_window(start_offset_days: int) -> tuple[str, str]:
    """Computes the start and end timestamps for the harvest window."""
    now = datetime.now(timezone.utc)
    today_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    start_date = today_start - timedelta(days=start_offset_days)
    end_date = today_start - timedelta(days=start_offset_days - 1)
    return start_date.strftime("%Y%m%d%H%M"), end_date.strftime("%Y%m%d%H%M")


def _build_search_query(categories: List[str], start_offset_days: int) -> str:
    """Construct the arXiv API search query."""
    start, end = _build_window(start_offset_days)
    category_clause = " OR ".join(f"cat:{cat}" for cat in categories)
    return f"({category_clause}) AND submittedDate:[{start} TO {end}]"


def _extract_entry_count(xml_bytes: bytes) -> int:
    feed = ET.fromstring(xml_bytes)
    return len(feed.findall("{http://www.w3.org/2005/Atom}entry"))


def harvest(
    *,
    mode: str = "incremental",
    categories: List[str] | None = None,
    start_offset_days: int = 1,
    snapshot: str | None = None,
) -> Dict[str, object]:
    """Harvests arXiv metadata for the given categories and time window."""
    if mode != "incremental":
        raise ValueError("Only 'incremental' mode is currently supported.")

    settings = get_settings()
    snapshot_id = snapshot or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_prefix = f"harvest/{snapshot_id}"
    
    final_categories = categories or DEFAULT_CATEGORIES

    client = GCSClient(settings.project_id)
    search_query = _build_search_query(final_categories, start_offset_days)

    params: Dict[str, str | int] = {
        "search_query": search_query,
        "start": 0,
        "max_results": MAX_RESULTS,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }

    start_ts = time.time()
    page = 0
    total_entries = 0


    while True:
        url = "http://export.arxiv.org/api/query?" + urllib.parse.urlencode(params)
        command = f"wget -qO- '{url}'"
        try:
            xml_bytes = subprocess.check_output(shlex.split(command), timeout=120)
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"wget command failed with exit code {e.returncode}") from e
        entry_count = _extract_entry_count(xml_bytes)
        if entry_count == 0:
            break

        blob_name = f"{base_prefix}/page_{page:05d}.xml"
        client.upload_text(settings.data_bucket, blob_name, xml_bytes.decode("utf-8"), content_type="application/xml")
        total_entries += entry_count

        if entry_count < MAX_RESULTS:
            break
        page += 1
        params["start"] = int(params["start"]) + MAX_RESULTS
        time.sleep(SLEEP_SECONDS)

    duration = time.time() - start_ts
    manifest = {
        "snapshot": snapshot_id,
        "search_query": search_query,
        "pages": page + 1,
        "count": total_entries,
        "duration_seconds": duration,
        "bucket": settings.data_bucket,
        "prefix": base_prefix,
        "mode": mode,
        "categories": final_categories,
        "start_offset_days": start_offset_days,
    }
    client.upload_json(settings.data_bucket, f"{base_prefix}/manifest.json", manifest)
    return manifest


if __name__ == "__main__":  # pragma: no cover
    import json

    parser = argparse.ArgumentParser(description="Nightly harvester for arXiv categories.")
    parser.add_argument(
        "--mode",
        default="incremental",
        help="Harvest mode. Currently only 'incremental' is supported.",
    )
    parser.add_argument(
        "--categories",
        nargs="+",
        default=DEFAULT_CATEGORIES,
        help="List of arXiv categories to harvest.",
    )
    parser.add_argument(
        "--start_offset_days",
        type=int,
        default=1,
        help="How many days back to start harvesting from (e.g., 1 means yesterday).",
    )
    parser.add_argument("--snapshot", help="Optional snapshot override.")
    args = parser.parse_args()

    manifest = harvest(
        mode=args.mode,
        categories=args.categories,
        start_offset_days=args.start_offset_days,
        snapshot=args.snapshot,
    )
    print(json.dumps(manifest))
