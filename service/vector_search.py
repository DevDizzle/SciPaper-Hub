"""Helpers for interacting with Vertex AI Vector Search."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence

from google.api_core.client_options import ClientOptions
from google.cloud import aiplatform_v1 as aiplatform_vs
from google.protobuf import json_format, struct_pb2

from common.config import Settings, get_settings


def _struct_to_dict(struct: Optional[struct_pb2.Struct]) -> Dict[str, Any]:
    if not struct:
        return {}
    return json_format.MessageToDict(
        struct,
        preserving_proto_field_name=True,
    )


def _build_api_endpoint(location: str) -> str:
    return f"{location}-aiplatform.googleapis.com"


def _resolve_match_service_endpoint(
    endpoint: aiplatform_vs.types.IndexEndpoint, deployed_index_id: str
) -> str:
    """Return the correct data plane hostname for MatchService calls."""
    if endpoint.public_endpoint_domain_name:
        return endpoint.public_endpoint_domain_name
    for deployed in endpoint.deployed_indexes:
        if deployed.id == deployed_index_id:
            private_endpoints = deployed.private_endpoints
            if private_endpoints and private_endpoints.match_grpc_address:
                return private_endpoints.match_grpc_address
            break
    raise RuntimeError(
        "No MatchService endpoint found for deployed index '%s' on index endpoint '%s'"
        % (deployed_index_id, endpoint.name),
    )


def _make_datapoint(item: Mapping[str, Any]) -> aiplatform_vs.IndexDatapoint:
    metadata = item.get("metadata") or {}
    struct = None
    if metadata:
        struct = struct_pb2.Struct()
        struct.update(metadata)
    return aiplatform_vs.IndexDatapoint(
        datapoint_id=item["id"],
        feature_vector=list(item["vector"]),
        embedding_metadata=struct,
    )


@dataclass
class VectorSearchConfig:
    project_id: str
    region: str
    index_endpoint: str
    deployed_index_id: str
    vertex_location: Optional[str] = None

    @property
    def location(self) -> str:
        return self.vertex_location or self.region

    @property
    def api_endpoint(self) -> str:
        return _build_api_endpoint(self.location)

    @property
    def index_endpoint_path(self) -> str:
        return (
            f"projects/{self.project_id}/locations/{self.location}/"
            f"indexEndpoints/{self.index_endpoint}"
        )


class VectorSearchClient:
    def __init__(self, cfg: VectorSearchConfig, *, transport: str = "grpc"):
        self._config = cfg
        endpoint_client = aiplatform_vs.IndexEndpointServiceClient(
            client_options=ClientOptions(api_endpoint=cfg.api_endpoint)
        )
        endpoint = endpoint_client.get_index_endpoint(name=cfg.index_endpoint_path)
        self._match_endpoint = _resolve_match_service_endpoint(
            endpoint, cfg.deployed_index_id
        )
        endpoint_client.transport.close()
        self._match = aiplatform_vs.MatchServiceClient(
            client_options=ClientOptions(api_endpoint=self._match_endpoint),
            transport=transport,
        )
        self._index_endpoint = cfg.index_endpoint_path
        self._deployed_index_id = cfg.deployed_index_id

    def search(self, query_vector: Sequence[float], k: int = 5) -> Dict[str, Any]:
        query = aiplatform_vs.FindNeighborsRequest.Query(
            datapoint=aiplatform_vs.IndexDatapoint(feature_vector=list(query_vector)),
            neighbor_count=k,
        )
        request = aiplatform_vs.FindNeighborsRequest(
            index_endpoint=self._index_endpoint,
            deployed_index_id=self._deployed_index_id,
            queries=[query],
            return_full_datapoint=True,
        )
        response = self._match.find_neighbors(request=request)

        neighbors: List[Dict[str, Any]] = []
        if response.nearest_neighbors:
            # We issue a single query, so inspect the first response entry.
            for neighbor in response.nearest_neighbors[0].neighbors:
                datapoint = neighbor.datapoint
                neighbors.append(
                    {
                        "id": datapoint.datapoint_id,
                        "distance": neighbor.distance,
                        "metadata": _struct_to_dict(datapoint.embedding_metadata),
                    }
                )

        return {"neighbors": neighbors}


class VertexVectorSearchClient:
    """Client used by the indexing pipeline to mutate and probe vectors."""

    def __init__(
        self,
        *,
        settings: Optional[Settings] = None,
        index_client: Optional[aiplatform_vs.IndexServiceClient] = None,
        endpoint_client: Optional[aiplatform_vs.IndexEndpointServiceClient] = None,
        match_client: Optional[aiplatform_vs.MatchServiceClient] = None,
    ) -> None:
        self._settings = settings or get_settings()
        location = self._settings.vertex_location or self._settings.region
        self._project_id = self._settings.project_id
        self._location = location
        self._index_endpoint_id = self._settings.index_endpoint_id
        self._deployed_index_id = self._settings.deployed_index_id
        self._index_resource_name: Optional[str] = None
        self._index_endpoint: Optional[aiplatform_vs.types.IndexEndpoint] = None
        control_plane_options = ClientOptions(api_endpoint=_build_api_endpoint(location))
        self._index_client = index_client or aiplatform_vs.IndexServiceClient(
            client_options=control_plane_options
        )
        self._endpoint_client = (
            endpoint_client
            or aiplatform_vs.IndexEndpointServiceClient(
                client_options=control_plane_options
            )
        )
        if match_client is not None:
            self._match_client = match_client
            self._match_endpoint = getattr(
                getattr(match_client, "transport", None),
                "_host",
                None,
            )
        else:
            index_endpoint = self._endpoint_client.get_index_endpoint(
                name=self.index_endpoint_path
            )
            self._index_endpoint = index_endpoint
            match_endpoint = _resolve_match_service_endpoint(
                index_endpoint, self._deployed_index_id
            )
            self._match_endpoint = match_endpoint
            self._match_client = aiplatform_vs.MatchServiceClient(
                client_options=ClientOptions(api_endpoint=match_endpoint),
                transport="grpc",
            )

    @property
    def index_endpoint_path(self) -> str:
        return (
            f"projects/{self._project_id}/locations/{self._location}/"
            f"indexEndpoints/{self._index_endpoint_id}"
        )

    @property
    def index_resource_name(self) -> str:
        if self._index_resource_name:
            return self._index_resource_name
        if not self._index_endpoint:
            self._index_endpoint = self._endpoint_client.get_index_endpoint(
                name=self.index_endpoint_path
            )
        endpoint = self._index_endpoint
        for deployed in endpoint.deployed_indexes:
            if deployed.id == self._deployed_index_id and deployed.index:
                self._index_resource_name = deployed.index
                break
        if not self._index_resource_name:
            raise RuntimeError(
                "Deployed index id '%s' not found on index endpoint '%s'"
                % (self._deployed_index_id, self.index_endpoint_path)
            )
        return self._index_resource_name

    def upsert(self, items: Iterable[Mapping[str, Any]]) -> None:
        datapoints = [_make_datapoint(item) for item in items]
        if not datapoints:
            return
        request = aiplatform_vs.UpsertDatapointsRequest(
            index=self.index_resource_name,
            datapoints=datapoints,
        )
        self._index_client.upsert_datapoints(request=request)

    def get_datapoints(self, ids: Sequence[str]) -> Dict[str, Dict[str, Any]]:
        if not ids:
            return {}
        request = aiplatform_vs.ReadIndexDatapointsRequest(
            index_endpoint=self.index_endpoint_path,
            deployed_index_id=self._deployed_index_id,
            ids=list(ids),
        )
        response = self._match_client.read_index_datapoints(request=request)

        results: Dict[str, Dict[str, Any]] = {}
        for datapoint in response.datapoints:
            results[datapoint.datapoint_id] = {
                "vector": list(datapoint.feature_vector),
                "metadata": _struct_to_dict(datapoint.embedding_metadata),
            }
        return results


__all__ = [
    "VectorSearchConfig",
    "VectorSearchClient",
    "VertexVectorSearchClient",
]