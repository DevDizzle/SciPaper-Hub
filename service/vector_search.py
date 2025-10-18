"""Vertex Vector Search client utilities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

from google.cloud import aiplatform_v1beta1 as aiplatform_vs

from common.config import get_settings


@dataclass
class VertexVectorSearchClient:
    """Wrapper around the Vertex AI Vector Search collections API."""

    collection_id: str | None = None

    def __post_init__(self) -> None:
        settings = get_settings()
        region = settings.region
        endpoint = f"{region}-aiplatform.googleapis.com"
        self._client = aiplatform_vs.VectorSearchServiceClient(
            client_options={"api_endpoint": endpoint}
        )
        collection_id = self.collection_id or settings.vector_collection_id
        self._collection_path = (
            f"projects/{settings.project_id}/locations/{region}/collections/{collection_id}"
        )

    def upsert(self, items: Sequence[Dict[str, object]]) -> None:
        datapoints: List[aiplatform_vs.IndexDatapoint] = []
        for item in items:
            metadata = {}
            for key, value in item.get("metadata", {}).items():
                if isinstance(value, (dict, list)):
                    metadata[key] = json.dumps(value)
                else:
                    metadata[key] = str(value)
            datapoints.append(
                aiplatform_vs.IndexDatapoint(
                    datapoint_id=str(item["id"]),
                    feature_vector=item["vector"],
                    metadata=metadata,
                )
            )
        request = aiplatform_vs.UpsertDatapointsRequest(
            collection=self._collection_path,
            datapoints=datapoints,
        )
        self._client.upsert_datapoints(request=request)

    def search(self, vector: Sequence[float], k: int = 10) -> List[Dict[str, object]]:
        query = aiplatform_vs.Query(
            datapoint=aiplatform_vs.IndexDatapoint(feature_vector=vector),
            neighbor_count=k,
        )
        request = aiplatform_vs.SearchRequest(
            collection=self._collection_path,
            queries=[query],
        )
        response = self._client.search(request=request)
        results: List[Dict[str, object]] = []
        if not response.results:
            return results
        for neighbor in response.results[0].neighbors:
            results.append(
                {
                    "id": neighbor.datapoint.datapoint_id,
                    "score": neighbor.distance,
                    "metadata": dict(neighbor.datapoint.metadata),
                }
            )
        return results

    def get_datapoints(self, ids: Iterable[str]) -> Dict[str, aiplatform_vs.IndexDatapoint]:
        datapoints: Dict[str, aiplatform_vs.IndexDatapoint] = {}
        for chunk in _chunked(list(ids), 100):
            request = aiplatform_vs.GetDatapointsRequest(
                collection=self._collection_path,
                ids=chunk,
            )
            response = self._client.get_datapoints(request=request)
            for dp in response.datapoints:
                datapoints[dp.datapoint_id] = dp
        return datapoints


def _chunked(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


__all__ = ["VertexVectorSearchClient"]
