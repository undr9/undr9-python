from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


@dataclass(slots=True)
class PropertyValue:
    kind: str
    value: Any

    @classmethod
    def string(cls, value: str) -> "PropertyValue":
        return cls("String", value)

    @classmethod
    def integer(cls, value: int) -> "PropertyValue":
        return cls("Integer", value)

    @classmethod
    def float(cls, value: float) -> "PropertyValue":
        return cls("Float", value)

    @classmethod
    def boolean(cls, value: bool) -> "PropertyValue":
        return cls("Boolean", value)

    @classmethod
    def string_list(cls, value: list[str]) -> "PropertyValue":
        return cls("StringList", value)

    @classmethod
    def float_list(cls, value: list[float]) -> "PropertyValue":
        return cls("FloatList", value)

    def to_dict(self) -> dict[str, Any]:
        return {"kind": self.kind, "value": self.value}

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "PropertyValue":
        return cls(kind=payload["kind"], value=payload["value"])


@dataclass(slots=True)
class Node:
    id: str
    node_type: str
    properties: dict[str, PropertyValue] = field(default_factory=dict)
    vectors: dict[str, list[float]] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "node_type": self.node_type,
            "properties": {key: value.to_dict() for key, value in self.properties.items()},
            "vectors": self.vectors,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Node":
        return cls(
            id=payload["id"],
            node_type=payload["node_type"],
            properties={
                key: PropertyValue.from_dict(value)
                for key, value in payload.get("properties", {}).items()
            },
            vectors=payload.get("vectors", {}),
        )


@dataclass(slots=True)
class Edge:
    id: str
    source: str
    target: str
    edge_type: str
    properties: dict[str, PropertyValue] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "source": self.source,
            "target": self.target,
            "edge_type": self.edge_type,
            "properties": {key: value.to_dict() for key, value in self.properties.items()},
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "Edge":
        return cls(
            id=payload["id"],
            source=payload["source"],
            target=payload["target"],
            edge_type=payload["edge_type"],
            properties={
                key: PropertyValue.from_dict(value)
                for key, value in payload.get("properties", {}).items()
            },
        )


@dataclass(slots=True)
class ScoreBreakdown:
    structural: float
    semantic: float
    temporal: float
    importance: float
    confidence: float

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ScoreBreakdown":
        return cls(
            structural=payload["structural"],
            semantic=payload["semantic"],
            temporal=payload["temporal"],
            importance=payload["importance"],
            confidence=payload["confidence"],
        )


@dataclass(slots=True)
class RankedNodeResult:
    node: Node
    score: float
    breakdown: ScoreBreakdown

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "RankedNodeResult":
        return cls(
            node=Node.from_dict(payload["node"]),
            score=payload["score"],
            breakdown=ScoreBreakdown.from_dict(payload["breakdown"]),
        )


@dataclass(slots=True)
class GraphPath:
    node_ids: list[str]
    edge_ids: list[str]
    hop_count: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "GraphPath":
        return cls(
            node_ids=payload.get("node_ids", []),
            edge_ids=payload.get("edge_ids", []),
            hop_count=payload.get("hop_count", 0),
        )


@dataclass(slots=True)
class QueryStreamFrame:
    frame_type: str
    plan_kind: str | None = None
    retrieval_profile: str | None = None
    node: Node | None = None
    edge: Edge | None = None
    result: RankedNodeResult | None = None
    path: GraphPath | None = None
    item_count: int | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "QueryStreamFrame":
        frame_type = payload["type"]
        return cls(
            frame_type=frame_type,
            plan_kind=payload.get("plan_kind"),
            retrieval_profile=payload.get("retrieval_profile"),
            node=Node.from_dict(payload["node"]) if payload.get("node") else None,
            edge=Edge.from_dict(payload["edge"]) if payload.get("edge") else None,
            result=RankedNodeResult.from_dict(payload["result"]) if payload.get("result") else None,
            path=GraphPath.from_dict(payload["path"]) if payload.get("path") else None,
            item_count=payload.get("item_count"),
        )


@dataclass(slots=True)
class QueryResponse:
    plan_kind: str
    nodes: list[Node]
    edges: list[Edge]
    ranked_results: list[RankedNodeResult]
    path: GraphPath | None = None
    retrieval_profile: str | None = None

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "QueryResponse":
        return cls(
            plan_kind=payload["plan_kind"],
            nodes=[Node.from_dict(item) for item in payload.get("nodes", [])],
            edges=[Edge.from_dict(item) for item in payload.get("edges", [])],
            ranked_results=[
                RankedNodeResult.from_dict(item) for item in payload.get("ranked_results", [])
            ],
            path=GraphPath.from_dict(payload["path"]) if payload.get("path") else None,
            retrieval_profile=payload.get("retrieval_profile"),
        )


@dataclass(slots=True)
class TransactionSummary:
    transaction_id: str
    isolation_level: str
    state: str
    started_at_revision: int
    staged_operation_count: int
    touched_node_count: int
    touched_edge_count: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TransactionSummary":
        return cls(
            transaction_id=payload["transaction_id"],
            isolation_level=payload["isolation_level"],
            state=payload["state"],
            started_at_revision=payload["started_at_revision"],
            staged_operation_count=payload["staged_operation_count"],
            touched_node_count=payload["touched_node_count"],
            touched_edge_count=payload["touched_edge_count"],
        )


@dataclass(slots=True)
class TransactionCommitResult:
    transaction_id: str
    committed_revision: int
    committed_lsn: int
    staged_operation_count: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "TransactionCommitResult":
        return cls(
            transaction_id=payload["transaction_id"],
            committed_revision=payload["committed_revision"],
            committed_lsn=payload["committed_lsn"],
            staged_operation_count=payload["staged_operation_count"],
        )


@dataclass(slots=True)
class MaintenanceResponse:
    status: str
    operation: str
    elapsed_ms: int
    detail: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MaintenanceResponse":
        return cls(
            status=payload["status"],
            operation=payload["operation"],
            elapsed_ms=payload["elapsed_ms"],
            detail=payload["detail"],
        )


@dataclass(slots=True)
class MaintenanceStatus:
    in_progress: bool
    last_operation: str | None
    last_outcome: str | None
    detail: str | None
    started_at_ms: int | None
    finished_at_ms: int | None
    elapsed_ms: int | None
    last_node_count: int
    last_edge_count: int
    max_node_count: int
    max_edge_count: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "MaintenanceStatus":
        return cls(
            in_progress=payload["in_progress"],
            last_operation=payload.get("last_operation"),
            last_outcome=payload.get("last_outcome"),
            detail=payload.get("detail"),
            started_at_ms=payload.get("started_at_ms"),
            finished_at_ms=payload.get("finished_at_ms"),
            elapsed_ms=payload.get("elapsed_ms"),
            last_node_count=payload["last_node_count"],
            last_edge_count=payload["last_edge_count"],
            max_node_count=payload["max_node_count"],
            max_edge_count=payload["max_edge_count"],
        )


@dataclass(slots=True)
class IntegrityReport:
    manifest_present: bool
    node_snapshot_valid: bool
    edge_snapshot_valid: bool
    wal_replay_valid: bool
    node_count: int
    edge_count: int
    issues: list[str]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "IntegrityReport":
        return cls(
            manifest_present=payload["manifest_present"],
            node_snapshot_valid=payload["node_snapshot_valid"],
            edge_snapshot_valid=payload["edge_snapshot_valid"],
            wal_replay_valid=payload["wal_replay_valid"],
            node_count=payload["node_count"],
            edge_count=payload["edge_count"],
            issues=payload.get("issues", []),
        )


@dataclass(slots=True)
class IndexSnapshot:
    format_version: int
    node_count: int
    unique_key_count: int
    adjacency_key_count: int
    reverse_adjacency_key_count: int
    label_bucket_count: int
    temporal_bucket_count: int
    vector_space_count: int
    vector_candidate_count: int
    vector_backend: str
    vector_runtime_ready: bool
    vector_dimensions: dict[str, int]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "IndexSnapshot":
        return cls(
            format_version=payload["format_version"],
            node_count=payload["node_count"],
            unique_key_count=payload["unique_key_count"],
            adjacency_key_count=payload["adjacency_key_count"],
            reverse_adjacency_key_count=payload["reverse_adjacency_key_count"],
            label_bucket_count=payload["label_bucket_count"],
            temporal_bucket_count=payload["temporal_bucket_count"],
            vector_space_count=payload["vector_space_count"],
            vector_candidate_count=payload["vector_candidate_count"],
            vector_backend=payload["vector_backend"],
            vector_runtime_ready=payload["vector_runtime_ready"],
            vector_dimensions=payload.get("vector_dimensions", {}),
        )


@dataclass(slots=True)
class WriteBatch:
    nodes_upserted: list[Node] = field(default_factory=list)
    edges_upserted: list[Edge] = field(default_factory=list)
    deleted_node_ids: list[str] = field(default_factory=list)
    deleted_edge_ids: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        return {
            "nodes_upserted": [node.to_dict() for node in self.nodes_upserted],
            "edges_upserted": [edge.to_dict() for edge in self.edges_upserted],
            "deleted_node_ids": self.deleted_node_ids,
            "deleted_edge_ids": self.deleted_edge_ids,
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "WriteBatch":
        return cls(
            nodes_upserted=[Node.from_dict(item) for item in payload.get("nodes_upserted", [])],
            edges_upserted=[Edge.from_dict(item) for item in payload.get("edges_upserted", [])],
            deleted_node_ids=payload.get("deleted_node_ids", []),
            deleted_edge_ids=payload.get("deleted_edge_ids", []),
        )


@dataclass(slots=True)
class ReplicaProgress:
    replica_node_id: str
    last_acked_source_lsn: int
    last_applied_source_lsn: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReplicaProgress":
        return cls(
            replica_node_id=payload["replica_node_id"],
            last_acked_source_lsn=payload["last_acked_source_lsn"],
            last_applied_source_lsn=payload["last_applied_source_lsn"],
        )


@dataclass(slots=True)
class ReplicationStatus:
    mode: str
    local_node_id: str
    leader_node_id: str | None
    current_term: int
    last_applied_lsn: int
    last_committed_source_lsn: int
    last_pulled_source_lsn: int
    last_applied_source_lsn: int
    replicas: list[ReplicaProgress]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReplicationStatus":
        return cls(
            mode=payload["mode"],
            local_node_id=payload["local_node_id"],
            leader_node_id=payload.get("leader_node_id"),
            current_term=payload["current_term"],
            last_applied_lsn=payload["last_applied_lsn"],
            last_committed_source_lsn=payload["last_committed_source_lsn"],
            last_pulled_source_lsn=payload["last_pulled_source_lsn"],
            last_applied_source_lsn=payload["last_applied_source_lsn"],
            replicas=[ReplicaProgress.from_dict(item) for item in payload.get("replicas", [])],
        )


@dataclass(slots=True)
class ReplicationStatusResponse:
    status: ReplicationStatus
    replica_lag: dict[str, int]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReplicationStatusResponse":
        return cls(
            status=ReplicationStatus.from_dict(payload["status"]),
            replica_lag=payload.get("replica_lag", {}),
        )


@dataclass(slots=True)
class ReplicationRecord:
    source_node_id: str
    source_term: int
    source_lsn: int
    batch: WriteBatch

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_node_id": self.source_node_id,
            "source_term": self.source_term,
            "source_lsn": self.source_lsn,
            "batch": self.batch.to_dict(),
        }

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ReplicationRecord":
        return cls(
            source_node_id=payload["source_node_id"],
            source_term=payload["source_term"],
            source_lsn=payload["source_lsn"],
            batch=WriteBatch.from_dict(payload["batch"]),
        )


@dataclass(slots=True)
class ClusterNode:
    node_id: str
    address: str
    role: str
    healthy: bool

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ClusterNode":
        return cls(
            node_id=payload["node_id"],
            address=payload["address"],
            role=payload["role"],
            healthy=payload["healthy"],
        )


@dataclass(slots=True)
class ClusterTopology:
    term: int
    leader_node_id: str | None
    nodes: list[ClusterNode]

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ClusterTopology":
        return cls(
            term=payload["term"],
            leader_node_id=payload.get("leader_node_id"),
            nodes=[ClusterNode.from_dict(item) for item in payload.get("nodes", [])],
        )


@dataclass(slots=True)
class FailoverPlan:
    old_leader_node_id: str | None
    new_leader_node_id: str
    term: int

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "FailoverPlan":
        return cls(
            old_leader_node_id=payload.get("old_leader_node_id"),
            new_leader_node_id=payload["new_leader_node_id"],
            term=payload["term"],
        )


@dataclass(slots=True)
class ServiceStatus:
    service: str
    status: str

    @classmethod
    def from_dict(cls, payload: dict[str, Any]) -> "ServiceStatus":
        return cls(
            service=payload["service"],
            status=payload["status"],
        )
