"""Tests for the normalize pipeline."""

from __future__ import annotations

import io
import json
from dataclasses import dataclass
from typing import Dict, Iterable

import pandas as pd

from pipelines import normalize as normalize_module


@dataclass
class _Settings:
    project_id: str
    data_bucket: str


class _FakeGCSClient:
    def __init__(self, project_id: str | None = None) -> None:  # pragma: no cover - trivial init
        self.project_id = project_id
        self.uploaded: Dict[str, bytes] = {}

    def download_text(self, bucket: str, blob_name: str) -> str:
        if blob_name.endswith("manifest.json"):
            return json.dumps({"prefix": "snapshots/demo", "snapshot": "demo"})
        return """
        <feed xmlns=\"http://www.w3.org/2005/Atom\">
            <entry>
                <id>http://arxiv.org/abs/1234.5678v1</id>
                <title>First Paper</title>
                <summary>First abstract</summary>
                <published>2024-01-01T00:00:00Z</published>
                <updated>2024-01-01T00:00:00Z</updated>
                <category term=\"cs.AI\" />
            </entry>
            <entry>
                <id>http://arxiv.org/abs/1234.5678v2</id>
                <title>First Paper Revision</title>
                <summary>Revised abstract</summary>
                <published>2024-01-02T00:00:00Z</published>
                <updated>2024-01-02T00:00:00Z</updated>
                <category term=\"cs.AI\" />
            </entry>
            <entry>
                <id>http://arxiv.org/abs/2345.6789v1</id>
                <title>Second Paper</title>
                <summary>Second abstract</summary>
                <published>2024-01-03T00:00:00Z</published>
                <updated>2024-01-03T00:00:00Z</updated>
                <category term=\"cs.CV\" />
            </entry>
        </feed>
        """

    def list_blobs(self, bucket: str, prefix: str) -> Iterable[str]:
        return [f"{prefix}/feed.xml"]

    def upload_bytes(self, bucket: str, blob_name: str, data: bytes, *, content_type: str) -> None:
        self.uploaded[blob_name] = data


def test_normalize_builds_parquet(monkeypatch) -> None:
    fake_client = _FakeGCSClient()

    monkeypatch.setattr(normalize_module, "GCSClient", lambda project_id: fake_client)
    monkeypatch.setattr(
        normalize_module,
        "get_settings",
        lambda: _Settings(project_id="proj", data_bucket="bucket"),
    )

    output_blob = normalize_module.normalize("demo-snapshot", output_blob="normalized/demo.parquet")

    assert output_blob == "normalized/demo.parquet"
    assert "normalized/demo.parquet" in fake_client.uploaded

    df = pd.read_parquet(io.BytesIO(fake_client.uploaded["normalized/demo.parquet"]))
    assert set(df["primary_category"]) == {"cs.AI", "cs.CV"}
    assert all(pd.to_datetime(df["published_at"], errors="raise"))

