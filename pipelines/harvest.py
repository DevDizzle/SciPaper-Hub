"""Nightly harvester for arXiv categories."""

from __future__ import annotations

import time
from datetime import datetime, timedelta, timezone
from typing import Dict
from xml.etree import ElementTree as ET

import requests

from common.config import get_settings
from common.gcs import GCSClient
from common.logging import configure_logging

configure_logging()

CATEGORIES = ["cs.AI", "cs.LG", "cs.CL", "cs.RO", "cs.CV"]
MAX_RESULTS = 2000
SLEEP_SECONDS = 3.0


def _build_window() -> tuple[str, str]:
    now = datetime.now(timezone.utc)
    today_start = datetime(now.year, now.month, now.day, tzinfo=timezone.utc)
    yesterday_start = today_start - timedelta(days=1)
    return yesterday_start.strftime("%Y%m%d%H%M"), today_start.strftime("%Y%m%d%H%M")


def _build_search_query() -> str:
    start, end = _build_window()
    category_clause = " OR ".join(f"cat:{cat}" for cat in CATEGORIES)
    return f"({category_clause}) AND submittedDate:[{start} TO {end}]"


def _extract_entry_count(xml_bytes: bytes) -> int:
    feed = ET.fromstring(xml_bytes)
    return len(feed.findall("{http://www.w3.org/2005/Atom}entry"))


def harvest(snapshot: str | None = None) -> Dict[str, object]:
    settings = get_settings()
    snapshot_id = snapshot or datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    base_prefix = f"harvest/{snapshot_id}"

    client = GCSClient(settings.project_id)
    search_query = _build_search_query()

    params = {
        "search_query": search_query,
        "start": 0,
        "max_results": MAX_RESULTS,
        "sortBy": "submittedDate",
        "sortOrder": "descending",
    }

    start_ts = time.time()
    page = 0
    total_entries = 0
    session = requests.Session()

    while True:
        response = session.get("http://export.arxiv.org/api/query", params=params, timeout=120)
        response.raise_for_status()
        xml_bytes = response.content
        entry_count = _extract_entry_count(xml_bytes)
        if entry_count == 0:
            break

        blob_name = f"{base_prefix}/page_{page:05d}.xml"
        client.upload_text(settings.data_bucket, blob_name, xml_bytes.decode("utf-8"), content_type="application/xml")
        total_entries += entry_count

        if entry_count < MAX_RESULTS:
            break
        page += 1
        params["start"] += MAX_RESULTS
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
    }
    client.upload_json(settings.data_bucket, f"{base_prefix}/manifest.json", manifest)
    return manifest


if __name__ == "__main__":  # pragma: no cover
    harvest()
