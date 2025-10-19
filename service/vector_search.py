from typing import List, Dict, Any
from dataclasses import dataclass
import os

from google.api_core.client_options import ClientOptions
from google.cloud import aiplatform_v1 as aiplatform_vs


@dataclass
class VectorSearchConfig:
    project_id: str
    region: str
    index_endpoint: str
    deployed_index_id: str


class VectorSearchClient:
    def __init__(self, cfg: VectorSearchConfig):
        api = f"{cfg.region}-aiplatform.googleapis.com"
        self._match = aiplatform_vs.MatchServiceClient(
            client_options=ClientOptions(api_endpoint=api)
        )
        self._index_endpoint = cfg.index_endpoint
        self._deployed_index_id = cfg.deployed_index_id

    def search(self, query_vector: List[float], k: int = 5) -> Dict[str, Any]:
        query = aiplatform_vs.FindNeighborsRequest.Query(
            datapoint=aiplatform_vs.IndexDatapoint(feature_vector=query_vector),
            neighbor_count=k,
        )
        request = aiplatform_vs.FindNeighborsRequest(
            index_endpoint=self._index_endpoint,
            deployed_index_id=self._deployed_index_id,
            queries=[query],
            return_full_datapoint=True,
        )
        resp = self._match.find_neighbors(request=request)
        return {
            "neighbors": [
                {
                    "id": n.datapoint.datapoint_id,
                    "distance": n.distance,
                    "labels": {kv.key: kv.value for kv in n.datapoint.restricts},
                    "crowding_tag": n.datapoint.crowding_tag,
                    "raw": a,
                }
                for a in resp.nearest_neighbors
                for n in a.neighbors
            ]
        }


__all__ = ["VectorSearchConfig", "VectorSearchClient"]
