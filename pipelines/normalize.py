"""Normalize harvested arXiv Atom feeds into parquet records."""

from __future__ import annotations

import json
from io import BytesIO
from typing import Dict, Iterable, List, Optional, cast
from xml.etree import ElementTree as ET

import pandas as pd
import pandera.pandas as pa

from pandera.pandas import Column
from pandera.dtypes import String

from common.config import get_settings
from common.gcs import GCSClient
from common.logging import configure_logging
from service.arxiv_client import parse_entry

configure_logging()


def _load_manifest(client: GCSClient, bucket: str, snapshot: str) -> Dict[str, object]:
    manifest_blob = f"harvest/{snapshot}/manifest.json"
    manifest_text = client.download_text(bucket, manifest_blob)
    return json.loads(manifest_text)


def _iter_entries(client: GCSClient, bucket: str, prefix: str) -> Iterable[Dict[str, object]]:
    for blob_name in sorted(client.list_blobs(bucket, prefix)):
        if not blob_name.endswith(".xml"):
            continue
        xml_text = client.download_text(bucket, blob_name)
        feed = ET.fromstring(xml_text)
        for entry in feed.findall("{http://www.w3.org/2005/Atom}entry"):
            yield parse_entry(entry)


def normalize(snapshot: str, output_blob: str | None = None) -> str:
    settings = get_settings()
    client = GCSClient(settings.project_id)
    manifest = _load_manifest(client, settings.data_bucket, snapshot)
    prefix: str = str(manifest["prefix"])
    ingest_snapshot: str = str(manifest["snapshot"])

    records: Dict[str, Dict[str, object]] = {}
    for record_entry in _iter_entries(client, settings.data_bucket, prefix):
        base_id: str = str(record_entry["base_id"])
        existing: Optional[Dict[str, object]] = records.get(base_id)
        if not existing or int(str(record_entry["version"])) > int(str(existing.get("version", 0))):
            records[base_id] = record_entry

    rows = []
    for record_value in records.values():
        links: Dict[str, str] = record_value.get("links", {})  # type: ignore
        rows.append(
            {
                "arxiv_id": str(record_value["arxiv_id"]),
                "base_id": str(record_value["base_id"]),
                "version": int(str(record_value["version"])),
                "title": str(record_value["title"]),
                "abstract": str(record_value["abstract"]),
                "authors": list(cast(List[str], record_value.get("authors", []))),
                "primary_category": str(record_value.get("primary_category", "")),
                "categories": list(cast(List[str], record_value.get("categories", []))),
                "published_at": str(record_value.get("published_at", "")),
                "updated_at": str(record_value.get("updated_at", "")),
                "link_abs": links.get("abs", f"https://arxiv.org/abs/{record_value['arxiv_id']}"),
                "link_pdf": links.get("pdf", f"https://arxiv.org/pdf/{record_value['arxiv_id']}.pdf"),
                "ingest_snapshot": ingest_snapshot,
            }
        )

    df = pd.DataFrame(rows)

    schema = pa.DataFrameSchema(
        {
            "base_id": Column(String, nullable=False),
            "abstract": Column(String, nullable=False),
            "published_at": Column(
                String,
                nullable=False,
                checks=pa.Check(
                    lambda s: pd.to_datetime(s, errors="coerce").notna().all(),
                    element_wise=False,
                    error="published_at must be parseable as datetime",
                ),
            ),
        }
    )

    schema.validate(df)
    output_blob = output_blob or f"normalized/{snapshot}/records.parquet"
    buffer = BytesIO()
    df.to_parquet(buffer, index=False)
    client.upload_bytes(settings.data_bucket, output_blob, buffer.getvalue(), content_type="application/octet-stream")
    return output_blob


if __name__ == "__main__":  # pragma: no cover
    import argparse

    parser = argparse.ArgumentParser(description="Normalize arXiv harvest snapshot")
    parser.add_argument("snapshot", help="Harvest snapshot identifier")
    parser.add_argument("--output", help="Destination blob for parquet data")
    args = parser.parse_args()
    normalize(args.snapshot, output_blob=args.output)
