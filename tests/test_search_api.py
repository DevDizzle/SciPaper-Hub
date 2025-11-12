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
        "B_DEPLOYED_INDEX_ID": "deployed-b",  # Add B_DEPLOYED_INDEX_ID
        "GIT_SHA": "test-sha",  # Add GIT_SHA
        "IMAGE_DIGEST": "test-digest",  # Add IMAGE_DIGEST
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
    module._vector_client_A = _DummyVectorClient()  # type: ignore
    module._vector_client_B = _DummyVectorClient()  # type: ignore

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


@pytest.mark.parametrize(
    "ip_hash_value,expected_user_group,expected_model_version",
    [
        (0, "B", "v3_768d"),  # 0 % 100 < 10, so group B
        (9, "B", "v3_768d"),  # 9 % 100 < 10, so group B
        (10, "A", "v1_768d"),  # 10 % 100 is not < 10, so group A
        (99, "A", "v1_768d"),  # 99 % 100 is not < 10, so group A
    ],
)
def test_search_ab_testing_logic(
    search_api, monkeypatch, caplog, ip_hash_value, expected_user_group, expected_model_version
) -> None:
    client = TestClient(search_api.app)

    # Mock hashlib.md5 to control the IP hash for A/B testing
    class MockMD5:
        def __init__(self, s: bytes):
            pass

        def hexdigest(self) -> str:
            return f"{ip_hash_value:032x}"  # Pad to 32 hex chars

    monkeypatch.setattr("hashlib.md5", MockMD5)

    with caplog.at_level(logging.INFO):
        response = client.post(
            "/search",
            json={"url": "https://arxiv.org/abs/1234.5678", "k": 2},
            headers={"X-Cloud-Trace-Context": "test-request-id"},
        )

    assert response.status_code == 200
    reco_logs = [record for record in caplog.records if record.getMessage() == "RECO_RESPONSE"]
    assert reco_logs, "Expected structured RECO_RESPONSE log"

    log_fields = reco_logs[0].json_fields
    assert log_fields["user_group"] == expected_user_group
    assert log_fields["model_version"] == expected_model_version
    assert log_fields["pipeline_git_sha"] == "test-sha"
    assert log_fields["container_image_digest"] == "test-digest"
    assert log_fields["request_id"] == "test-request-id"
    assert log_fields["data_snapshot_id"] == "unknown"  # Our dummy client doesn't provide this


def test_search_provenance_logging(search_api, caplog) -> None:
    client = TestClient(search_api.app)

    with caplog.at_level(logging.INFO):
        response = client.post(
            "/search",
            json={"url": "https://arxiv.org/abs/1234.5678", "k": 2},
            headers={"X-Cloud-Trace-Context": "another-test-request-id"},
        )

    assert response.status_code == 200
    reco_logs = [record for record in caplog.records if record.getMessage() == "RECO_RESPONSE"]
    assert reco_logs, "Expected structured RECO_RESPONSE log"

    log_fields = reco_logs[0].json_fields
    assert log_fields["query_url"] == "https://arxiv.org/abs/1234.5678"
    assert log_fields["k"] == 2
    assert log_fields["recommendations"] == ["paper-1", "paper-2"]
    assert log_fields["request_id"] == "another-test-request-id"
    assert log_fields["user_group"] in ["A", "B"]  # Depends on the actual IP hash
    assert log_fields["model_version"] in ["v1_768d", "v3_768d"]  # Depends on the actual IP hash
    assert log_fields["data_snapshot_id"] == "unknown"
    assert log_fields["pipeline_git_sha"] == "test-sha"
    assert log_fields["container_image_digest"] == "test-digest"

