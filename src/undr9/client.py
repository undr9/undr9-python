from __future__ import annotations

import inspect
from collections.abc import AsyncIterator, Iterator
from collections.abc import Mapping
from typing import Any

from ._transport import AsyncHttpTransport, HttpTransport
from .models import (
    ClusterTopology,
    Edge,
    FailoverPlan,
    IndexSnapshot,
    IntegrityReport,
    MaintenanceResponse,
    MaintenanceStatus,
    Node,
    PropertyValue,
    QueryResponse,
    QueryStreamFrame,
    ReplicationRecord,
    ReplicationStatusResponse,
    TransactionCommitResult,
    TransactionSummary,
    ServiceStatus,
)


def _resolve_node_lookup(
    node_id: str | None = None,
    *,
    id: str | None = None,
    key: str | None = None,
) -> tuple[str, str]:
    if node_id is not None and id is not None:
        raise ValueError("pass either positional node_id or id=, not both")

    resolved_id = id if id is not None else node_id
    provided = [value is not None for value in (resolved_id, key)]
    if sum(provided) != 1:
        raise ValueError("pass exactly one of id= or key=")

    if key is not None:
        return ("key", key)
    return ("id", resolved_id)


def _first_node_from_query(response: QueryResponse, *, key: str) -> Node:
    if not response.nodes:
        raise LookupError(f"no node found for unique key {key!r}")
    return response.nodes[0]


def _upsert_node_operation(node: Node) -> dict[str, Any]:
    return {"UpsertNode": node.to_dict()}


def _upsert_edge_operation(edge: Edge) -> dict[str, Any]:
    return {"UpsertEdge": edge.to_dict()}


def _delete_node_operation(node_id: str) -> dict[str, Any]:
    return {"DeleteNode": {"node_id": node_id}}


def _delete_edge_operation(edge_id: str) -> dict[str, Any]:
    return {"DeleteEdge": {"edge_id": edge_id}}


class SyncTransaction:
    def __init__(self, client: "SyncUndr9Client", summary: TransactionSummary):
        self._client = client
        self.summary = summary

    @property
    def transaction_id(self) -> str:
        return self.summary.transaction_id

    def refresh(self) -> TransactionSummary:
        self.summary = self._client.get_transaction(self.transaction_id)
        return self.summary

    def upsert_node(self, node: Node) -> TransactionSummary:
        self.summary = self._client.stage_transaction_operation(
            self.transaction_id,
            _upsert_node_operation(node),
        )
        return self.summary

    def upsert_edge(self, edge: Edge) -> TransactionSummary:
        self.summary = self._client.stage_transaction_operation(
            self.transaction_id,
            _upsert_edge_operation(edge),
        )
        return self.summary

    def delete_node(self, node_id: str) -> TransactionSummary:
        self.summary = self._client.stage_transaction_operation(
            self.transaction_id,
            _delete_node_operation(node_id),
        )
        return self.summary

    def delete_edge(self, edge_id: str) -> TransactionSummary:
        self.summary = self._client.stage_transaction_operation(
            self.transaction_id,
            _delete_edge_operation(edge_id),
        )
        return self.summary

    def query(self, payload: dict[str, Any]) -> QueryResponse:
        return self._client.transaction_query(self.transaction_id, payload)

    def query_stream(self, payload: dict[str, Any]) -> Iterator[QueryStreamFrame]:
        return self._client.transaction_query_stream(self.transaction_id, payload)

    def commit(self) -> TransactionCommitResult:
        return self._client.commit_transaction(self.transaction_id)

    def rollback(self) -> TransactionSummary:
        self.summary = self._client.rollback_transaction(self.transaction_id)
        return self.summary


class AsyncTransaction:
    def __init__(self, client: "AsyncUndr9Client", summary: TransactionSummary):
        self._client = client
        self.summary = summary

    @property
    def transaction_id(self) -> str:
        return self.summary.transaction_id

    async def refresh(self) -> TransactionSummary:
        self.summary = await self._client.get_transaction(self.transaction_id)
        return self.summary

    async def upsert_node(self, node: Node) -> TransactionSummary:
        self.summary = await self._client.stage_transaction_operation(
            self.transaction_id,
            _upsert_node_operation(node),
        )
        return self.summary

    async def upsert_edge(self, edge: Edge) -> TransactionSummary:
        self.summary = await self._client.stage_transaction_operation(
            self.transaction_id,
            _upsert_edge_operation(edge),
        )
        return self.summary

    async def delete_node(self, node_id: str) -> TransactionSummary:
        self.summary = await self._client.stage_transaction_operation(
            self.transaction_id,
            _delete_node_operation(node_id),
        )
        return self.summary

    async def delete_edge(self, edge_id: str) -> TransactionSummary:
        self.summary = await self._client.stage_transaction_operation(
            self.transaction_id,
            _delete_edge_operation(edge_id),
        )
        return self.summary

    async def query(self, payload: dict[str, Any]) -> QueryResponse:
        return await self._client.transaction_query(self.transaction_id, payload)

    def query_stream(self, payload: dict[str, Any]) -> AsyncIterator[QueryStreamFrame]:
        return self._client.transaction_query_stream(self.transaction_id, payload)

    async def commit(self) -> TransactionCommitResult:
        return await self._client.commit_transaction(self.transaction_id)

    async def rollback(self) -> TransactionSummary:
        self.summary = await self._client.rollback_transaction(self.transaction_id)
        return self.summary


class SyncUndr9Client:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        transport: Any | None = None,
        timeout: float | None = 10.0,
        max_retries: int = 0,
        retry_backoff_seconds: float = 0.25,
        retry_non_idempotent_requests: bool = False,
        headers: Mapping[str, str] | None = None,
        user_agent: str | None = None,
        http2: bool = False,
        follow_redirects: bool = False,
        verify: bool | str = True,
        max_connections: int | None = None,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        write_timeout: float | None = None,
        pool_timeout: float | None = None,
    ):
        self._transport = transport or HttpTransport(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_non_idempotent_requests=retry_non_idempotent_requests,
            headers=headers,
            user_agent=user_agent,
            http2=http2,
            follow_redirects=follow_redirects,
            verify=verify,
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
            pool_timeout=pool_timeout,
        )

    def close(self) -> None:
        close_method = getattr(self._transport, "close", None)
        if close_method is not None:
            close_method()

    def __enter__(self) -> "SyncUndr9Client":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()

    def create_node(
        self,
        node_id: str,
        node_type: str,
        properties: dict[str, PropertyValue] | None = None,
        vectors: dict[str, list[float]] | None = None,
    ) -> Node:
        return self.upsert_node(
            Node(
                id=node_id,
                node_type=node_type,
                properties=properties or {},
                vectors=vectors or {},
            )
        )

    def upsert_node(self, node: Node) -> Node:
        payload = self._transport.request("POST", "/v1/nodes", node.to_dict())
        return Node.from_dict(payload)

    def get_node(
        self,
        node_id: str | None = None,
        *,
        id: str | None = None,
        key: str | None = None,
    ) -> Node:
        lookup_kind, lookup_value = _resolve_node_lookup(node_id, id=id, key=key)
        if lookup_kind == "key":
            return _first_node_from_query(
                self.get_node_by_unique_key(lookup_value),
                key=lookup_value,
            )

        payload = self._transport.request("GET", f"/v1/nodes/{lookup_value}")
        return Node.from_dict(payload)

    def update_node(self, node: Node) -> Node:
        payload = self._transport.request("PUT", f"/v1/nodes/{node.id}", node.to_dict())
        return Node.from_dict(payload)

    def delete_node(self, node_id: str) -> None:
        self._transport.request("DELETE", f"/v1/nodes/{node_id}")

    def create_edge(
        self,
        edge_id: str,
        source: str,
        target: str,
        edge_type: str,
        properties: dict[str, PropertyValue] | None = None,
    ) -> Edge:
        return self.upsert_edge(
            Edge(
                id=edge_id,
                source=source,
                target=target,
                edge_type=edge_type,
                properties=properties or {},
            )
        )

    def upsert_edge(self, edge: Edge) -> Edge:
        payload = self._transport.request("POST", "/v1/edges", edge.to_dict())
        return Edge.from_dict(payload)

    def get_edge(self, edge_id: str) -> Edge:
        payload = self._transport.request("GET", f"/v1/edges/{edge_id}")
        return Edge.from_dict(payload)

    def update_edge(self, edge: Edge) -> Edge:
        payload = self._transport.request("PUT", f"/v1/edges/{edge.id}", edge.to_dict())
        return Edge.from_dict(payload)

    def delete_edge(self, edge_id: str) -> None:
        self._transport.request("DELETE", f"/v1/edges/{edge_id}")

    def query(self, payload: dict[str, Any]) -> QueryResponse:
        return QueryResponse.from_dict(self._transport.request("POST", "/v1/query", payload))

    def query_stream(self, payload: dict[str, Any]) -> Iterator[QueryStreamFrame]:
        for frame in self._transport.stream_request("POST", "/v1/query/stream", payload):
            yield QueryStreamFrame.from_dict(frame)

    def begin_transaction(self, isolation_level: str = "Snapshot") -> SyncTransaction:
        payload = {"isolation_level": isolation_level}
        summary = TransactionSummary.from_dict(
            self._transport.request("POST", "/v1/transactions/begin", payload)
        )
        return SyncTransaction(self, summary)

    def list_transactions(self) -> list[TransactionSummary]:
        payload = self._transport.request("GET", "/v1/transactions")
        return [TransactionSummary.from_dict(item) for item in payload]

    def get_transaction(self, transaction_id: str) -> TransactionSummary:
        payload = self._transport.request("GET", f"/v1/transactions/{transaction_id}")
        return TransactionSummary.from_dict(payload)

    def transaction(self, transaction_id: str) -> SyncTransaction:
        return SyncTransaction(self, self.get_transaction(transaction_id))

    def stage_transaction_operation(
        self,
        transaction_id: str,
        operation: dict[str, Any],
    ) -> TransactionSummary:
        payload = self._transport.request(
            "POST",
            f"/v1/transactions/{transaction_id}/operations",
            operation,
        )
        return TransactionSummary.from_dict(payload)

    def transaction_query(self, transaction_id: str, payload: dict[str, Any]) -> QueryResponse:
        response = self._transport.request(
            "POST",
            f"/v1/transactions/{transaction_id}/query",
            payload,
        )
        return QueryResponse.from_dict(response)

    def transaction_query_stream(
        self,
        transaction_id: str,
        payload: dict[str, Any],
    ) -> Iterator[QueryStreamFrame]:
        for frame in self._transport.stream_request(
            "POST",
            f"/v1/transactions/{transaction_id}/query/stream",
            payload,
        ):
            yield QueryStreamFrame.from_dict(frame)

    def commit_transaction(self, transaction_id: str) -> TransactionCommitResult:
        payload = self._transport.request("POST", f"/v1/transactions/{transaction_id}/commit")
        return TransactionCommitResult.from_dict(payload)

    def rollback_transaction(self, transaction_id: str) -> TransactionSummary:
        payload = self._transport.request("POST", f"/v1/transactions/{transaction_id}/rollback")
        return TransactionSummary.from_dict(payload)

    def health(self) -> ServiceStatus:
        payload = self._transport.request("GET", "/healthz")
        return ServiceStatus.from_dict(payload)

    def readiness(self) -> ServiceStatus:
        payload = self._transport.request("GET", "/readyz")
        return ServiceStatus.from_dict(payload)

    def metrics(self) -> str:
        return self._transport.text_request("GET", "/metrics")

    def admin_compact(self) -> MaintenanceResponse:
        payload = self._transport.request("POST", "/v1/admin/compact")
        return MaintenanceResponse.from_dict(payload)

    def admin_backup(self, destination: str) -> MaintenanceResponse:
        payload = self._transport.request(
            "POST",
            "/v1/admin/backup",
            {"destination": destination},
        )
        return MaintenanceResponse.from_dict(payload)

    def admin_restore(self, source: str, target_lsn: int | None = None) -> MaintenanceResponse:
        payload = self._transport.request(
            "POST",
            "/v1/admin/restore",
            {"source": source, "target_lsn": target_lsn},
        )
        return MaintenanceResponse.from_dict(payload)

    def admin_repair(self) -> IntegrityReport:
        payload = self._transport.request("POST", "/v1/admin/repair")
        return IntegrityReport.from_dict(payload)

    def admin_rebuild_indexes(self) -> IndexSnapshot:
        payload = self._transport.request("POST", "/v1/admin/rebuild-indexes")
        return IndexSnapshot.from_dict(payload)

    def admin_integrity(self) -> IntegrityReport:
        payload = self._transport.request("GET", "/v1/admin/integrity")
        return IntegrityReport.from_dict(payload)

    def admin_maintenance_status(self) -> MaintenanceStatus:
        payload = self._transport.request("GET", "/v1/admin/maintenance/status")
        return MaintenanceStatus.from_dict(payload)

    def replication_status(self) -> ReplicationStatusResponse:
        payload = self._transport.request("GET", "/v1/admin/replication/status")
        return ReplicationStatusResponse.from_dict(payload)

    def replication_history(self, after_source_lsn: int | None = None) -> list[ReplicationRecord]:
        path = "/v1/admin/replication/history"
        if after_source_lsn is not None:
            path = f"{path}?after_source_lsn={after_source_lsn}"
        payload = self._transport.request("GET", path)
        return [ReplicationRecord.from_dict(item) for item in payload]

    def configure_as_leader(self) -> ReplicationStatusResponse:
        payload = self._transport.request("POST", "/v1/admin/replication/leader")
        return ReplicationStatusResponse.from_dict(payload)

    def configure_as_follower(
        self,
        leader_node_id: str,
        leader_address: str,
    ) -> ReplicationStatusResponse:
        payload = self._transport.request(
            "POST",
            "/v1/admin/replication/follower",
            {
                "leader_node_id": leader_node_id,
                "leader_address": leader_address,
            },
        )
        return ReplicationStatusResponse.from_dict(payload)

    def acknowledge_replica(
        self,
        replica_node_id: str,
        source_lsn: int,
    ) -> ReplicationStatusResponse:
        payload = self._transport.request(
            "POST",
            "/v1/admin/replication/ack",
            {
                "replica_node_id": replica_node_id,
                "source_lsn": source_lsn,
            },
        )
        return ReplicationStatusResponse.from_dict(payload)

    def apply_replication_records(
        self,
        records: list[ReplicationRecord],
    ) -> ReplicationStatusResponse:
        payload = self._transport.request(
            "POST",
            "/v1/admin/replication/apply",
            {"records": [record.to_dict() for record in records]},
        )
        return ReplicationStatusResponse.from_dict(payload)

    def cluster_topology(self) -> ClusterTopology:
        payload = self._transport.request("GET", "/v1/admin/cluster/topology")
        return ClusterTopology.from_dict(payload)

    def register_cluster_node(self, node_id: str, address: str) -> ClusterTopology:
        payload = self._transport.request(
            "POST",
            "/v1/admin/cluster/nodes",
            {"node_id": node_id, "address": address},
        )
        return ClusterTopology.from_dict(payload)

    def mark_cluster_node_health(self, node_id: str, healthy: bool) -> ClusterTopology:
        payload = self._transport.request(
            "POST",
            f"/v1/admin/cluster/nodes/{node_id}/health",
            {"healthy": healthy},
        )
        return ClusterTopology.from_dict(payload)

    def promote_cluster_node(self, node_id: str) -> FailoverPlan:
        payload = self._transport.request(
            "POST",
            "/v1/admin/cluster/promote",
            {"node_id": node_id},
        )
        return FailoverPlan.from_dict(payload)

    def get_node_by_unique_key(self, unique_key: str) -> QueryResponse:
        payload = {"GetNodeByUniqueKey": {"unique_key": unique_key}}
        return self.query(payload)

    def search_by_label(self, label: str, limit: int | None = None) -> QueryResponse:
        payload = {"SearchByLabel": {"label": label, "limit": limit}}
        return self.query(payload)

    def filter_nodes(
        self,
        *,
        where: dict[str, Any],
        label: str | None = None,
        limit: int | None = None,
    ) -> QueryResponse:
        payload = {
            "FilterNodes": {
                "label": label,
                "where": where,
                "limit": limit,
            }
        }
        return self.query(payload)

    def vector_search(
        self,
        query_vector: list[float],
        limit: int,
        node_type: str | None = None,
        vector_name: str | None = None,
        top_k: int | None = None,
    ) -> QueryResponse:
        payload = {
            "VectorSearch": {
                "query_vector": query_vector,
                "node_type": node_type,
                "vector_name": vector_name,
                "limit": limit,
                "top_k": top_k,
            }
        }
        return self.query(payload)

    def time_range(
        self,
        from_epoch_ms: int,
        to_epoch_ms: int,
        limit: int = 10,
        field: str = "timestamp",
    ) -> QueryResponse:
        payload = {
            "TimeRange": {
                "field": field,
                "from_epoch_ms": from_epoch_ms,
                "to_epoch_ms": to_epoch_ms,
                "limit": limit,
            }
        }
        return self.query(payload)

    def traverse(
        self,
        *,
        start_node_id: str,
        direction: str,
        edge_type: str | None = None,
        max_hops: int | None = None,
        limit: int | None = None,
        timeout_ms: int | None = None,
        edge_types: list[str] | None = None,
        node_labels: list[str] | None = None,
    ) -> QueryResponse:
        payload = {
            "Traverse": {
                "start_node_id": start_node_id,
                "edge_type": edge_type,
                "direction": direction,
                "max_hops": max_hops,
                "limit": limit,
                "timeout_ms": timeout_ms,
                "constraints": {
                    "edge_types": edge_types or [],
                    "node_labels": node_labels or [],
                },
            }
        }
        return self.query(payload)

    def shortest_path(
        self,
        *,
        source_node_id: str,
        target_node_id: str,
        direction: str,
        max_depth: int | None = None,
        limit: int | None = None,
        timeout_ms: int | None = None,
        edge_types: list[str] | None = None,
        node_labels: list[str] | None = None,
    ) -> QueryResponse:
        payload = {
            "ShortestPath": {
                "source_node_id": source_node_id,
                "target_node_id": target_node_id,
                "direction": direction,
                "max_depth": max_depth,
                "limit": limit,
                "timeout_ms": timeout_ms,
                "constraints": {
                    "edge_types": edge_types or [],
                    "node_labels": node_labels or [],
                },
            }
        }
        return self.query(payload)

    def ranked_retrieval(
        self,
        *,
        query_vector: list[float] | None = None,
        reference_node_id: str | None = None,
        edge_type: str | None = None,
        from_epoch_ms: int | None = None,
        to_epoch_ms: int | None = None,
        vector_name: str | None = None,
        limit: int = 10,
        top_k: int | None = None,
        now_epoch_ms: int,
        retrieval_profile: str | None = "v1-default",
    ) -> QueryResponse:
        payload = {
            "RankedRetrieval": {
                "query_vector": query_vector,
                "reference_node_id": reference_node_id,
                "edge_type": edge_type,
                "from_epoch_ms": from_epoch_ms,
                "to_epoch_ms": to_epoch_ms,
                "vector_name": vector_name,
                "limit": limit,
                "top_k": top_k,
                "now_epoch_ms": now_epoch_ms,
                "retrieval_profile": retrieval_profile,
            }
        }
        return self.query(payload)


class AsyncUndr9Client:
    def __init__(
        self,
        base_url: str,
        api_key: str,
        transport: Any | None = None,
        timeout: float | None = 10.0,
        max_retries: int = 0,
        retry_backoff_seconds: float = 0.25,
        retry_non_idempotent_requests: bool = False,
        headers: Mapping[str, str] | None = None,
        user_agent: str | None = None,
        http2: bool = False,
        follow_redirects: bool = False,
        verify: bool | str = True,
        max_connections: int | None = None,
        max_keepalive_connections: int | None = None,
        keepalive_expiry: float | None = None,
        connect_timeout: float | None = None,
        read_timeout: float | None = None,
        write_timeout: float | None = None,
        pool_timeout: float | None = None,
    ):
        self._transport = transport or AsyncHttpTransport(
            base_url=base_url,
            api_key=api_key,
            timeout=timeout,
            max_retries=max_retries,
            retry_backoff_seconds=retry_backoff_seconds,
            retry_non_idempotent_requests=retry_non_idempotent_requests,
            headers=headers,
            user_agent=user_agent,
            http2=http2,
            follow_redirects=follow_redirects,
            verify=verify,
            max_connections=max_connections,
            max_keepalive_connections=max_keepalive_connections,
            keepalive_expiry=keepalive_expiry,
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            write_timeout=write_timeout,
            pool_timeout=pool_timeout,
        )

    async def _request(self, method: str, path: str, payload: dict[str, Any] | None = None) -> Any:
        response = self._transport.request(method, path, payload)
        if inspect.isawaitable(response):
            return await response
        return response

    async def aclose(self) -> None:
        close_method = getattr(self._transport, "aclose", None)
        if close_method is not None:
            result = close_method()
            if inspect.isawaitable(result):
                await result

    async def __aenter__(self) -> "AsyncUndr9Client":
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self.aclose()

    async def create_node(
        self,
        node_id: str,
        node_type: str,
        properties: dict[str, PropertyValue] | None = None,
        vectors: dict[str, list[float]] | None = None,
    ) -> Node:
        payload = Node(
            id=node_id,
            node_type=node_type,
            properties=properties or {},
            vectors=vectors or {},
        )
        response = await self._request("POST", "/v1/nodes", payload.to_dict())
        return Node.from_dict(response)

    async def get_node(
        self,
        node_id: str | None = None,
        *,
        id: str | None = None,
        key: str | None = None,
    ) -> Node:
        lookup_kind, lookup_value = _resolve_node_lookup(node_id, id=id, key=key)
        if lookup_kind == "key":
            return _first_node_from_query(
                await self.get_node_by_unique_key(lookup_value),
                key=lookup_value,
            )

        payload = await self._request("GET", f"/v1/nodes/{lookup_value}")
        return Node.from_dict(payload)

    async def update_node(self, node: Node) -> Node:
        payload = await self._request("PUT", f"/v1/nodes/{node.id}", node.to_dict())
        return Node.from_dict(payload)

    async def delete_node(self, node_id: str) -> None:
        await self._request("DELETE", f"/v1/nodes/{node_id}")

    async def create_edge(
        self,
        edge_id: str,
        source: str,
        target: str,
        edge_type: str,
        properties: dict[str, PropertyValue] | None = None,
    ) -> Edge:
        payload = Edge(
            id=edge_id,
            source=source,
            target=target,
            edge_type=edge_type,
            properties=properties or {},
        )
        response = await self._request("POST", "/v1/edges", payload.to_dict())
        return Edge.from_dict(response)

    async def get_edge(self, edge_id: str) -> Edge:
        payload = await self._request("GET", f"/v1/edges/{edge_id}")
        return Edge.from_dict(payload)

    async def update_edge(self, edge: Edge) -> Edge:
        payload = await self._request("PUT", f"/v1/edges/{edge.id}", edge.to_dict())
        return Edge.from_dict(payload)

    async def delete_edge(self, edge_id: str) -> None:
        await self._request("DELETE", f"/v1/edges/{edge_id}")

    async def query(self, payload: dict[str, Any]) -> QueryResponse:
        return QueryResponse.from_dict(await self._request("POST", "/v1/query", payload))

    async def query_stream(self, payload: dict[str, Any]) -> AsyncIterator[QueryStreamFrame]:
        async for frame in self._transport.stream_request("POST", "/v1/query/stream", payload):
            yield QueryStreamFrame.from_dict(frame)

    async def begin_transaction(self, isolation_level: str = "Snapshot") -> AsyncTransaction:
        payload = {"isolation_level": isolation_level}
        summary = TransactionSummary.from_dict(
            await self._request("POST", "/v1/transactions/begin", payload)
        )
        return AsyncTransaction(self, summary)

    async def list_transactions(self) -> list[TransactionSummary]:
        payload = await self._request("GET", "/v1/transactions")
        return [TransactionSummary.from_dict(item) for item in payload]

    async def get_transaction(self, transaction_id: str) -> TransactionSummary:
        payload = await self._request("GET", f"/v1/transactions/{transaction_id}")
        return TransactionSummary.from_dict(payload)

    async def transaction(self, transaction_id: str) -> AsyncTransaction:
        return AsyncTransaction(self, await self.get_transaction(transaction_id))

    async def stage_transaction_operation(
        self,
        transaction_id: str,
        operation: dict[str, Any],
    ) -> TransactionSummary:
        payload = await self._request(
            "POST",
            f"/v1/transactions/{transaction_id}/operations",
            operation,
        )
        return TransactionSummary.from_dict(payload)

    async def transaction_query(self, transaction_id: str, payload: dict[str, Any]) -> QueryResponse:
        response = await self._request(
            "POST",
            f"/v1/transactions/{transaction_id}/query",
            payload,
        )
        return QueryResponse.from_dict(response)

    async def transaction_query_stream(
        self,
        transaction_id: str,
        payload: dict[str, Any],
    ) -> AsyncIterator[QueryStreamFrame]:
        async for frame in self._transport.stream_request(
            "POST",
            f"/v1/transactions/{transaction_id}/query/stream",
            payload,
        ):
            yield QueryStreamFrame.from_dict(frame)

    async def commit_transaction(self, transaction_id: str) -> TransactionCommitResult:
        payload = await self._request("POST", f"/v1/transactions/{transaction_id}/commit")
        return TransactionCommitResult.from_dict(payload)

    async def rollback_transaction(self, transaction_id: str) -> TransactionSummary:
        payload = await self._request("POST", f"/v1/transactions/{transaction_id}/rollback")
        return TransactionSummary.from_dict(payload)

    async def health(self) -> ServiceStatus:
        payload = await self._request("GET", "/healthz")
        return ServiceStatus.from_dict(payload)

    async def readiness(self) -> ServiceStatus:
        payload = await self._request("GET", "/readyz")
        return ServiceStatus.from_dict(payload)

    async def metrics(self) -> str:
        response = self._transport.text_request("GET", "/metrics")
        if inspect.isawaitable(response):
            return await response
        return response

    async def admin_compact(self) -> MaintenanceResponse:
        payload = await self._request("POST", "/v1/admin/compact")
        return MaintenanceResponse.from_dict(payload)

    async def admin_backup(self, destination: str) -> MaintenanceResponse:
        payload = await self._request(
            "POST",
            "/v1/admin/backup",
            {"destination": destination},
        )
        return MaintenanceResponse.from_dict(payload)

    async def admin_restore(
        self,
        source: str,
        target_lsn: int | None = None,
    ) -> MaintenanceResponse:
        payload = await self._request(
            "POST",
            "/v1/admin/restore",
            {"source": source, "target_lsn": target_lsn},
        )
        return MaintenanceResponse.from_dict(payload)

    async def admin_repair(self) -> IntegrityReport:
        payload = await self._request("POST", "/v1/admin/repair")
        return IntegrityReport.from_dict(payload)

    async def admin_rebuild_indexes(self) -> IndexSnapshot:
        payload = await self._request("POST", "/v1/admin/rebuild-indexes")
        return IndexSnapshot.from_dict(payload)

    async def admin_integrity(self) -> IntegrityReport:
        payload = await self._request("GET", "/v1/admin/integrity")
        return IntegrityReport.from_dict(payload)

    async def admin_maintenance_status(self) -> MaintenanceStatus:
        payload = await self._request("GET", "/v1/admin/maintenance/status")
        return MaintenanceStatus.from_dict(payload)

    async def replication_status(self) -> ReplicationStatusResponse:
        payload = await self._request("GET", "/v1/admin/replication/status")
        return ReplicationStatusResponse.from_dict(payload)

    async def replication_history(
        self,
        after_source_lsn: int | None = None,
    ) -> list[ReplicationRecord]:
        path = "/v1/admin/replication/history"
        if after_source_lsn is not None:
            path = f"{path}?after_source_lsn={after_source_lsn}"
        payload = await self._request("GET", path)
        return [ReplicationRecord.from_dict(item) for item in payload]

    async def configure_as_leader(self) -> ReplicationStatusResponse:
        payload = await self._request("POST", "/v1/admin/replication/leader")
        return ReplicationStatusResponse.from_dict(payload)

    async def configure_as_follower(
        self,
        leader_node_id: str,
        leader_address: str,
    ) -> ReplicationStatusResponse:
        payload = await self._request(
            "POST",
            "/v1/admin/replication/follower",
            {
                "leader_node_id": leader_node_id,
                "leader_address": leader_address,
            },
        )
        return ReplicationStatusResponse.from_dict(payload)

    async def acknowledge_replica(
        self,
        replica_node_id: str,
        source_lsn: int,
    ) -> ReplicationStatusResponse:
        payload = await self._request(
            "POST",
            "/v1/admin/replication/ack",
            {
                "replica_node_id": replica_node_id,
                "source_lsn": source_lsn,
            },
        )
        return ReplicationStatusResponse.from_dict(payload)

    async def apply_replication_records(
        self,
        records: list[ReplicationRecord],
    ) -> ReplicationStatusResponse:
        payload = await self._request(
            "POST",
            "/v1/admin/replication/apply",
            {"records": [record.to_dict() for record in records]},
        )
        return ReplicationStatusResponse.from_dict(payload)

    async def cluster_topology(self) -> ClusterTopology:
        payload = await self._request("GET", "/v1/admin/cluster/topology")
        return ClusterTopology.from_dict(payload)

    async def register_cluster_node(self, node_id: str, address: str) -> ClusterTopology:
        payload = await self._request(
            "POST",
            "/v1/admin/cluster/nodes",
            {"node_id": node_id, "address": address},
        )
        return ClusterTopology.from_dict(payload)

    async def mark_cluster_node_health(self, node_id: str, healthy: bool) -> ClusterTopology:
        payload = await self._request(
            "POST",
            f"/v1/admin/cluster/nodes/{node_id}/health",
            {"healthy": healthy},
        )
        return ClusterTopology.from_dict(payload)

    async def promote_cluster_node(self, node_id: str) -> FailoverPlan:
        payload = await self._request(
            "POST",
            "/v1/admin/cluster/promote",
            {"node_id": node_id},
        )
        return FailoverPlan.from_dict(payload)

    async def get_node_by_unique_key(self, unique_key: str) -> QueryResponse:
        payload = {"GetNodeByUniqueKey": {"unique_key": unique_key}}
        return await self.query(payload)

    async def search_by_label(self, label: str, limit: int | None = None) -> QueryResponse:
        payload = {"SearchByLabel": {"label": label, "limit": limit}}
        return await self.query(payload)

    async def filter_nodes(
        self,
        *,
        where: dict[str, Any],
        label: str | None = None,
        limit: int | None = None,
    ) -> QueryResponse:
        payload = {
            "FilterNodes": {
                "label": label,
                "where": where,
                "limit": limit,
            }
        }
        return await self.query(payload)

    async def vector_search(
        self,
        query_vector: list[float],
        limit: int,
        node_type: str | None = None,
        vector_name: str | None = None,
        top_k: int | None = None,
    ) -> QueryResponse:
        payload = {
            "VectorSearch": {
                "query_vector": query_vector,
                "node_type": node_type,
                "vector_name": vector_name,
                "limit": limit,
                "top_k": top_k,
            }
        }
        return await self.query(payload)

    async def time_range(
        self,
        from_epoch_ms: int,
        to_epoch_ms: int,
        limit: int = 10,
        field: str = "timestamp",
    ) -> QueryResponse:
        payload = {
            "TimeRange": {
                "field": field,
                "from_epoch_ms": from_epoch_ms,
                "to_epoch_ms": to_epoch_ms,
                "limit": limit,
            }
        }
        return await self.query(payload)

    async def ranked_retrieval(self, **kwargs: Any) -> QueryResponse:
        payload = {
            "RankedRetrieval": {
                "query_vector": kwargs.get("query_vector"),
                "reference_node_id": kwargs.get("reference_node_id"),
                "edge_type": kwargs.get("edge_type"),
                "from_epoch_ms": kwargs.get("from_epoch_ms"),
                "to_epoch_ms": kwargs.get("to_epoch_ms"),
                "vector_name": kwargs.get("vector_name"),
                "limit": kwargs.get("limit", 10),
                "top_k": kwargs.get("top_k"),
                "now_epoch_ms": kwargs["now_epoch_ms"],
                "retrieval_profile": kwargs.get("retrieval_profile", "v1-default"),
            }
        }
        return await self.query(payload)

    async def traverse(self, **kwargs: Any) -> QueryResponse:
        payload = {
            "Traverse": {
                "start_node_id": kwargs["start_node_id"],
                "edge_type": kwargs.get("edge_type"),
                "direction": kwargs["direction"],
                "max_hops": kwargs.get("max_hops"),
                "limit": kwargs.get("limit"),
                "timeout_ms": kwargs.get("timeout_ms"),
                "constraints": {
                    "edge_types": kwargs.get("edge_types") or [],
                    "node_labels": kwargs.get("node_labels") or [],
                },
            }
        }
        return await self.query(payload)

    async def shortest_path(self, **kwargs: Any) -> QueryResponse:
        payload = {
            "ShortestPath": {
                "source_node_id": kwargs["source_node_id"],
                "target_node_id": kwargs["target_node_id"],
                "direction": kwargs["direction"],
                "max_depth": kwargs.get("max_depth"),
                "limit": kwargs.get("limit"),
                "timeout_ms": kwargs.get("timeout_ms"),
                "constraints": {
                    "edge_types": kwargs.get("edge_types") or [],
                    "node_labels": kwargs.get("node_labels") or [],
                },
            }
        }
        return await self.query(payload)
