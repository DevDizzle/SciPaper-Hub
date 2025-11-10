"""Tests for the search API service."""

from __future__ import annotations

import importlib
import logging
from typing import Any, Dict

import pytest
from fastapi.testclient import TestClient

from common import config as config_module


class _DummyVectorClient:
    def search(self, vec, k: int = 5) -> Dict[str, Any]:  # pragma: no cover - simple stub
        return {"neighbors": [{"id": "paper-1"}, {"id": "paper-2"}]}


@pytest.fixture
def search_api(monkeypatch: pytest.MonkeyPatch):
    env = {
        "PROJECT_ID": "test-project",
        "REGION": "us-central1",
        "DATA_BUCKET": "test-bucket",
        "VECTOR_COLLECTION_ID": "collection",
        "INDEX_ENDPOINT_ID": "endpoint",
        "DEPLOYED_INDEX_ID": "deployed",
    }
    for key, value in env.items():
        monkeypatch.setenv(key, value)

    config_module.get_settings.cache_clear()

    module = importlib.import_module("service.search_api")
    importlib.reload(module)

    async def _mock_fetch(url: str) -> str:
        return "example abstract"

    monkeypatch.setattr(module, "_maybe_fetch_abstract", _mock_fetch)
    monkeypatch.setattr(module, "embed_text", lambda text: [0.1, 0.2, 0.3])

    # Patch the client variables directly after the app startup has run
    module._vector_client_A = _DummyVectorClient()
    module._vector_client_B = _DummyVectorClient()

    yield module

    config_module.get_settings.cache_clear()


def test_search_logs_recommendations(search_api, caplog) -> None:
    client = TestClient(search_api.app)

    with caplog.at_level(logging.INFO):
        response = client.post(
            "/search",
            json={"url": "https://arxiv.org/abs/1234.5678", "k": 2},
        )

    assert response.status_code == 200
    body = response.json()
    assert body["neighbors"][0]["id"] == "paper-1"

    reco_logs = [record for record in caplog.records if record.getMessage() == "RECO_RESPONSE"]
    assert reco_logs, "Expected structured RECO_RESPONSE log"
    assert reco_logs[0].json_fields["recommendations"] == ["paper-1", "paper-2"]

