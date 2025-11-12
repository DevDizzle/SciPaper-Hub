"""Google Cloud Storage helper abstractions."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Any, Dict, Iterable, Optional

from google.cloud.storage import Client


@dataclass
class GCSClient:
    """Light-weight wrapper around :mod:`google.cloud.storage` client."""

    project_id: Optional[str] = None

    def __post_init__(self) -> None:
        self._client = Client(project=self.project_id) if self.project_id else Client()

    def upload_text(self, bucket: str, blob_name: str, text: str, *, content_type: str = "text/plain") -> None:
        bucket_ref = self._client.bucket(bucket)
        blob = bucket_ref.blob(blob_name)
        blob.upload_from_string(text, content_type=content_type)

    def upload_bytes(self, bucket: str, blob_name: str, data: bytes, *, content_type: str) -> None:
        bucket_ref = self._client.bucket(bucket)
        blob = bucket_ref.blob(blob_name)
        blob.upload_from_string(data, content_type=content_type)

    def upload_json(self, bucket: str, blob_name: str, payload: Dict[str, Any]) -> None:
        self.upload_text(bucket, blob_name, json.dumps(payload, ensure_ascii=False), content_type="application/json")

    def download_text(self, bucket: str, blob_name: str) -> str:
        bucket_ref = self._client.bucket(bucket)
        blob = bucket_ref.blob(blob_name)
        return blob.download_as_text()

    def download_bytes(self, bucket: str, blob_name: str) -> bytes:
        bucket_ref = self._client.bucket(bucket)
        blob = bucket_ref.blob(blob_name)
        return blob.download_as_bytes()

    def list_blobs(self, bucket: str, prefix: str) -> Iterable[str]:
        bucket_ref = self._client.bucket(bucket)
        for blob in bucket_ref.list_blobs(prefix=prefix):
            yield blob.name


__all__ = ["GCSClient"]
