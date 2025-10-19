"""Vertex Vector Search client utilities."""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Dict, Iterable, List, Sequence

from google.cloud.aiplatform_v1.services.index_endpoint_service_client import IndexEndpointServiceClient
from google.cloud.aiplatform_v1.services.match_service_client import MatchServiceClient
from google.cloud.aiplatform_v1.types import index_endpoint as index_endpoint_v1
from google.cloud.aiplatform_v1.types import match_service as match_service_v1

from common.config import get_settings


@dataclass
class VertexVectorSearchClient:
    """Wrapper around the Vertex AI Vector Search matching engine API."""

    def __post_init__(self) -> None:
        settings = get_settings()
        region = settings.region
        endpoint = f"{region}-aiplatform.googleapis.com"
        
        # Get the public endpoint domain name from the index endpoint
        index_endpoint_client = IndexEndpointServiceClient(client_options={"api_endpoint": endpoint})
        index_endpoint_path = f"projects/{settings.project_id}/locations/{region}/indexEndpoints/3967620694178529280"
        index_endpoint = index_endpoint_client.get_index_endpoint(name=index_endpoint_path)
        public_endpoint = index_endpoint.public_endpoint_domain_name

        self._client = MatchServiceClient(
            client_options={"api_endpoint": public_endpoint}
        )
        self._deployed_index_id = "papers_v1_deployed"

    def upsert(self, items: Sequence[Dict[str, object]]) -> None:
        pass

    def search(self, vector: Sequence[float], k: int = 10) -> List[Dict[str, object]]:
        request = match_service_v1.FindNeighborsRequest(
            deployed_index_id=self._deployed_index_id,
            queries=[match_service_v1.FindNeighborsRequest.Query(
                datapoint=index_endpoint_v1.IndexDatapoint(
                    feature_vector=vector
                ),
                neighbor_count=k
            )]
        )
        response = self._client.find_neighbors(request=request)
        results: List[Dict[str, object]] = []
        for neighbor in response.nearest_neighbors[0].neighbors:
            datapoint = neighbor.datapoint
            results.append(
                {
                    "id": datapoint.datapoint_id,
                    "score": neighbor.distance,
                    "metadata": {},
                }
            )
        return results

    def get_datapoints(self, ids: Iterable[str]) -> Dict[str, index_endpoint_v1.IndexDatapoint]:
        return {}


def _chunked(items: Sequence[str], size: int) -> Iterable[Sequence[str]]:
    for i in range(0, len(items), size):
        yield items[i : i + size]


__all__ = ["VertexVectorSearchClient"]