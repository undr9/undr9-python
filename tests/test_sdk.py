import asyncio
import io
import pathlib
import sys
import unittest
import urllib.error
from unittest import mock

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from undr9 import (
    AsyncUndr9Client,
    ReplicationRecord,
    Node,
    PropertyValue,
    SyncUndr9Client,
    Undr9ApiError,
    Undr9ConnectionError,
    WriteBatch,
)
from undr9 import _transport as transport_module
from undr9._transport import AsyncHttpTransport, HttpTransport


class FakeTransport:
    def __init__(self):
        self.calls = []
        self.transaction_summary = {
            "transaction_id": "tx_1",
            "isolation_level": "Snapshot",
            "state": "Active",
            "started_at_revision": 4,
            "staged_operation_count": 0,
            "touched_node_count": 0,
            "touched_edge_count": 0,
        }

    def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if path == "/healthz" and method == "GET":
            return {"service": "undr9", "status": "ok"}
        if path == "/readyz" and method == "GET":
            return {"service": "undr9", "status": "ready"}
        if path == "/v1/nodes" and method == "POST":
            return payload
        if path == "/v1/query" and method == "POST":
            return {
                "plan_kind": "VectorSimilarity",
                "nodes": [
                    {
                        "id": "node_a",
                        "node_type": "memory",
                        "properties": {
                            "unique_key": {"kind": "String", "value": "alpha"}
                        },
                        "vectors": {"default": [1.0, 0.0]},
                    }
                ],
                "edges": [],
                "ranked_results": [
                    {
                        "node": {
                            "id": "node_a",
                            "node_type": "memory",
                            "properties": {
                                "unique_key": {"kind": "String", "value": "alpha"}
                            },
                            "vectors": {"default": [1.0, 0.0]},
                        },
                        "score": 0.99,
                        "breakdown": {
                            "structural": 0.0,
                            "semantic": 0.99,
                            "temporal": 0.0,
                            "importance": 0.0,
                            "confidence": 0.0,
                        },
                    }
                ],
            }
        if path == "/v1/nodes/node_a" and method == "GET":
            return {
                "id": "node_a",
                "node_type": "memory",
                "properties": {
                    "unique_key": {"kind": "String", "value": "alpha"}
                },
            }
        if path == "/v1/transactions/begin" and method == "POST":
            return dict(self.transaction_summary, isolation_level=payload["isolation_level"])
        if path == "/v1/transactions" and method == "GET":
            return [self.transaction_summary]
        if path == "/v1/transactions/tx_1" and method == "GET":
            return self.transaction_summary
        if path == "/v1/transactions/tx_1/operations" and method == "POST":
            next_count = self.transaction_summary["staged_operation_count"] + 1
            return dict(
                self.transaction_summary,
                staged_operation_count=next_count,
                touched_node_count=1,
            )
        if path == "/v1/transactions/tx_1/query" and method == "POST":
            return {
                "plan_kind": "ExactLookup",
                "nodes": [
                    {
                        "id": "node_a",
                        "node_type": "memory",
                        "properties": {
                            "unique_key": {"kind": "String", "value": "alpha"}
                        },
                        "vectors": {"default": [1.0, 0.0]},
                    }
                ],
                "edges": [],
                "ranked_results": [],
            }
        if path == "/v1/transactions/tx_1/commit" and method == "POST":
            return {
                "transaction_id": "tx_1",
                "committed_revision": 5,
                "committed_lsn": 5,
                "staged_operation_count": 1,
            }
        if path == "/v1/transactions/tx_1/rollback" and method == "POST":
            return dict(self.transaction_summary, state="RolledBack")
        if path == "/v1/admin/compact" and method == "POST":
            return {
                "status": "ok",
                "operation": "compact",
                "elapsed_ms": 12,
                "detail": "storage compacted",
            }
        if path == "/v1/admin/backup" and method == "POST":
            return {
                "status": "ok",
                "operation": "backup",
                "elapsed_ms": 22,
                "detail": f"backup created at {payload['destination']}",
            }
        if path == "/v1/admin/restore" and method == "POST":
            return {
                "status": "ok",
                "operation": "restore",
                "elapsed_ms": 33,
                "detail": f"storage restored from {payload['source']}",
            }
        if path == "/v1/admin/repair" and method == "POST":
            return {
                "manifest_present": True,
                "node_snapshot_valid": True,
                "edge_snapshot_valid": True,
                "wal_replay_valid": True,
                "node_count": 2,
                "edge_count": 1,
                "issues": [],
            }
        if path == "/v1/admin/rebuild-indexes" and method == "POST":
            return {
                "format_version": 1,
                "node_count": 2,
                "unique_key_count": 1,
                "adjacency_key_count": 1,
                "reverse_adjacency_key_count": 1,
                "label_bucket_count": 1,
                "temporal_bucket_count": 1,
                "vector_space_count": 1,
                "vector_candidate_count": 2,
                "vector_backend": "hnsw",
                "vector_runtime_ready": True,
                "vector_dimensions": {"default": 2},
            }
        if path == "/v1/admin/integrity" and method == "GET":
            return {
                "manifest_present": True,
                "node_snapshot_valid": True,
                "edge_snapshot_valid": True,
                "wal_replay_valid": True,
                "node_count": 2,
                "edge_count": 1,
                "issues": [],
            }
        if path == "/v1/admin/maintenance/status" and method == "GET":
            return {
                "in_progress": False,
                "last_operation": "rebuild_indexes",
                "last_outcome": "success",
                "detail": "index snapshot rebuilt",
                "started_at_ms": 100,
                "finished_at_ms": 120,
                "elapsed_ms": 20,
                "last_node_count": 2,
                "last_edge_count": 1,
                "max_node_count": 100,
                "max_edge_count": 100,
            }
        if path == "/v1/admin/replication/status" and method == "GET":
            return {
                "status": {
                    "mode": "Leader",
                    "local_node_id": "leader-1",
                    "leader_node_id": "leader-1",
                    "current_term": 2,
                    "last_applied_lsn": 4,
                    "last_committed_source_lsn": 4,
                    "last_pulled_source_lsn": 0,
                    "last_applied_source_lsn": 4,
                    "replicas": [
                        {
                            "replica_node_id": "replica-1",
                            "last_acked_source_lsn": 3,
                            "last_applied_source_lsn": 3,
                        }
                    ],
                },
                "replica_lag": {"replica-1": 1},
            }
        if path == "/v1/admin/replication/history?after_source_lsn=0" and method == "GET":
            return [
                {
                    "source_node_id": "leader-1",
                    "source_term": 2,
                    "source_lsn": 4,
                    "batch": {
                        "nodes_upserted": [],
                        "edges_upserted": [],
                        "deleted_node_ids": [],
                        "deleted_edge_ids": [],
                    },
                }
            ]
        if path == "/v1/admin/replication/leader" and method == "POST":
            return self.request("GET", "/v1/admin/replication/status")
        if path == "/v1/admin/replication/follower" and method == "POST":
            return self.request("GET", "/v1/admin/replication/status")
        if path == "/v1/admin/replication/ack" and method == "POST":
            return self.request("GET", "/v1/admin/replication/status")
        if path == "/v1/admin/replication/apply" and method == "POST":
            return self.request("GET", "/v1/admin/replication/status")
        if path == "/v1/admin/cluster/topology" and method == "GET":
            return {
                "term": 3,
                "leader_node_id": "leader-1",
                "nodes": [
                    {
                        "node_id": "leader-1",
                        "address": "127.0.0.1:9100",
                        "role": "Primary",
                        "healthy": True,
                    },
                    {
                        "node_id": "replica-1",
                        "address": "127.0.0.1:9101",
                        "role": "Replica",
                        "healthy": True,
                    },
                ],
            }
        if path == "/v1/admin/cluster/nodes" and method == "POST":
            return self.request("GET", "/v1/admin/cluster/topology")
        if path == "/v1/admin/cluster/nodes/replica-1/health" and method == "POST":
            topology = self.request("GET", "/v1/admin/cluster/topology")
            topology["nodes"][1]["healthy"] = payload["healthy"]
            return topology
        if path == "/v1/admin/cluster/promote" and method == "POST":
            return {
                "old_leader_node_id": "leader-1",
                "new_leader_node_id": payload["node_id"],
                "term": 4,
            }
        return None

    def text_request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if path == "/metrics" and method == "GET":
            return '# HELP undr9_requests_total Total requests\nundr9_requests_total 1\n'
        return ""

    def stream_request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if path == "/v1/transactions/tx_1/query/stream":
            frames = [
                {"type": "meta", "plan_kind": "ExactLookup", "retrieval_profile": None},
                {
                    "type": "node",
                    "node": {
                        "id": "node_a",
                        "node_type": "memory",
                        "properties": {
                            "unique_key": {"kind": "String", "value": "alpha"}
                        },
                        "vectors": {"default": [1.0, 0.0]},
                    },
                },
                {"type": "end", "item_count": 1},
            ]
        else:
            frames = [
            {"type": "meta", "plan_kind": "VectorSimilarity", "retrieval_profile": None},
            {
                "type": "node",
                "node": {
                    "id": "node_a",
                    "node_type": "memory",
                    "properties": {
                        "unique_key": {"kind": "String", "value": "alpha"}
                    },
                    "vectors": {"default": [1.0, 0.0]},
                },
            },
            {
                "type": "ranked_result",
                "result": {
                    "node": {
                        "id": "node_a",
                        "node_type": "memory",
                        "properties": {
                            "unique_key": {"kind": "String", "value": "alpha"}
                        },
                        "vectors": {"default": [1.0, 0.0]},
                    },
                    "score": 0.99,
                    "breakdown": {
                        "structural": 0.0,
                        "semantic": 0.99,
                        "temporal": 0.0,
                        "importance": 0.0,
                        "confidence": 0.0,
                    },
                },
            },
            {"type": "end", "item_count": 2},
            ]
        for frame in frames:
            yield frame


class AsyncFakeTransport:
    def __init__(self):
        self.calls = []
        self.closed = False
        self.transaction_summary = {
            "transaction_id": "tx_1",
            "isolation_level": "Snapshot",
            "state": "Active",
            "started_at_revision": 4,
            "staged_operation_count": 0,
            "touched_node_count": 0,
            "touched_edge_count": 0,
        }

    async def request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if path == "/healthz" and method == "GET":
            return {"service": "undr9", "status": "ok"}
        if path == "/readyz" and method == "GET":
            return {"service": "undr9", "status": "ready"}
        if path == "/v1/nodes/node_a" and method == "GET":
            return {
                "id": "node_a",
                "node_type": "memory",
                "properties": {
                    "unique_key": {"kind": "String", "value": "alpha"}
                },
            }
        if path == "/v1/query" and method == "POST":
            return {
                "plan_kind": "VectorSimilarity",
                "nodes": [
                    {
                        "id": "node_a",
                        "node_type": "memory",
                        "properties": {
                            "unique_key": {"kind": "String", "value": "alpha"}
                        },
                        "vectors": {"default": [1.0, 0.0]},
                    }
                ],
                "edges": [],
                "ranked_results": [
                    {
                        "node": {
                            "id": "node_a",
                            "node_type": "memory",
                            "properties": {
                                "unique_key": {"kind": "String", "value": "alpha"}
                            },
                            "vectors": {"default": [1.0, 0.0]},
                        },
                        "score": 0.99,
                        "breakdown": {
                            "structural": 0.0,
                            "semantic": 0.99,
                            "temporal": 0.0,
                            "importance": 0.0,
                            "confidence": 0.0,
                        },
                    }
                ],
            }
        if path == "/v1/transactions/begin" and method == "POST":
            return dict(self.transaction_summary, isolation_level=payload["isolation_level"])
        if path == "/v1/transactions" and method == "GET":
            return [self.transaction_summary]
        if path == "/v1/transactions/tx_1" and method == "GET":
            return self.transaction_summary
        if path == "/v1/transactions/tx_1/operations" and method == "POST":
            next_count = self.transaction_summary["staged_operation_count"] + 1
            return dict(
                self.transaction_summary,
                staged_operation_count=next_count,
                touched_node_count=1,
            )
        if path == "/v1/transactions/tx_1/query" and method == "POST":
            return {
                "plan_kind": "ExactLookup",
                "nodes": [
                    {
                        "id": "node_a",
                        "node_type": "memory",
                        "properties": {
                            "unique_key": {"kind": "String", "value": "alpha"}
                        },
                        "vectors": {"default": [1.0, 0.0]},
                    }
                ],
                "edges": [],
                "ranked_results": [],
            }
        if path == "/v1/transactions/tx_1/commit" and method == "POST":
            return {
                "transaction_id": "tx_1",
                "committed_revision": 5,
                "committed_lsn": 5,
                "staged_operation_count": 1,
            }
        if path == "/v1/transactions/tx_1/rollback" and method == "POST":
            return dict(self.transaction_summary, state="RolledBack")
        if path == "/v1/admin/compact" and method == "POST":
            return {
                "status": "ok",
                "operation": "compact",
                "elapsed_ms": 12,
                "detail": "storage compacted",
            }
        if path == "/v1/admin/backup" and method == "POST":
            return {
                "status": "ok",
                "operation": "backup",
                "elapsed_ms": 22,
                "detail": f"backup created at {payload['destination']}",
            }
        if path == "/v1/admin/restore" and method == "POST":
            return {
                "status": "ok",
                "operation": "restore",
                "elapsed_ms": 33,
                "detail": f"storage restored from {payload['source']}",
            }
        if path == "/v1/admin/repair" and method == "POST":
            return {
                "manifest_present": True,
                "node_snapshot_valid": True,
                "edge_snapshot_valid": True,
                "wal_replay_valid": True,
                "node_count": 2,
                "edge_count": 1,
                "issues": [],
            }
        if path == "/v1/admin/rebuild-indexes" and method == "POST":
            return {
                "format_version": 1,
                "node_count": 2,
                "unique_key_count": 1,
                "adjacency_key_count": 1,
                "reverse_adjacency_key_count": 1,
                "label_bucket_count": 1,
                "temporal_bucket_count": 1,
                "vector_space_count": 1,
                "vector_candidate_count": 2,
                "vector_backend": "hnsw",
                "vector_runtime_ready": True,
                "vector_dimensions": {"default": 2},
            }
        if path == "/v1/admin/integrity" and method == "GET":
            return {
                "manifest_present": True,
                "node_snapshot_valid": True,
                "edge_snapshot_valid": True,
                "wal_replay_valid": True,
                "node_count": 2,
                "edge_count": 1,
                "issues": [],
            }
        if path == "/v1/admin/maintenance/status" and method == "GET":
            return {
                "in_progress": False,
                "last_operation": "rebuild_indexes",
                "last_outcome": "success",
                "detail": "index snapshot rebuilt",
                "started_at_ms": 100,
                "finished_at_ms": 120,
                "elapsed_ms": 20,
                "last_node_count": 2,
                "last_edge_count": 1,
                "max_node_count": 100,
                "max_edge_count": 100,
            }
        if path == "/v1/admin/replication/status" and method == "GET":
            return {
                "status": {
                    "mode": "Leader",
                    "local_node_id": "leader-1",
                    "leader_node_id": "leader-1",
                    "current_term": 2,
                    "last_applied_lsn": 4,
                    "last_committed_source_lsn": 4,
                    "last_pulled_source_lsn": 0,
                    "last_applied_source_lsn": 4,
                    "replicas": [
                        {
                            "replica_node_id": "replica-1",
                            "last_acked_source_lsn": 3,
                            "last_applied_source_lsn": 3,
                        }
                    ],
                },
                "replica_lag": {"replica-1": 1},
            }
        if path == "/v1/admin/replication/history?after_source_lsn=0" and method == "GET":
            return [
                {
                    "source_node_id": "leader-1",
                    "source_term": 2,
                    "source_lsn": 4,
                    "batch": {
                        "nodes_upserted": [],
                        "edges_upserted": [],
                        "deleted_node_ids": [],
                        "deleted_edge_ids": [],
                    },
                }
            ]
        if path == "/v1/admin/replication/leader" and method == "POST":
            return await self.request("GET", "/v1/admin/replication/status")
        if path == "/v1/admin/replication/follower" and method == "POST":
            return await self.request("GET", "/v1/admin/replication/status")
        if path == "/v1/admin/replication/ack" and method == "POST":
            return await self.request("GET", "/v1/admin/replication/status")
        if path == "/v1/admin/replication/apply" and method == "POST":
            return await self.request("GET", "/v1/admin/replication/status")
        if path == "/v1/admin/cluster/topology" and method == "GET":
            return {
                "term": 3,
                "leader_node_id": "leader-1",
                "nodes": [
                    {
                        "node_id": "leader-1",
                        "address": "127.0.0.1:9100",
                        "role": "Primary",
                        "healthy": True,
                    },
                    {
                        "node_id": "replica-1",
                        "address": "127.0.0.1:9101",
                        "role": "Replica",
                        "healthy": True,
                    },
                ],
            }
        if path == "/v1/admin/cluster/nodes" and method == "POST":
            return await self.request("GET", "/v1/admin/cluster/topology")
        if path == "/v1/admin/cluster/nodes/replica-1/health" and method == "POST":
            topology = await self.request("GET", "/v1/admin/cluster/topology")
            topology["nodes"][1]["healthy"] = payload["healthy"]
            return topology
        if path == "/v1/admin/cluster/promote" and method == "POST":
            return {
                "old_leader_node_id": "leader-1",
                "new_leader_node_id": payload["node_id"],
                "term": 4,
            }
        return None

    async def text_request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if path == "/metrics" and method == "GET":
            return '# HELP undr9_requests_total Total requests\nundr9_requests_total 1\n'
        return ""

    async def stream_request(self, method, path, payload=None):
        self.calls.append((method, path, payload))
        if path == "/v1/transactions/tx_1/query/stream":
            frames = [
                {"type": "meta", "plan_kind": "ExactLookup", "retrieval_profile": None},
                {
                    "type": "node",
                    "node": {
                        "id": "node_a",
                        "node_type": "memory",
                        "properties": {
                            "unique_key": {"kind": "String", "value": "alpha"}
                        },
                        "vectors": {"default": [1.0, 0.0]},
                    },
                },
                {"type": "end", "item_count": 1},
            ]
        else:
            frames = [
            {"type": "meta", "plan_kind": "VectorSimilarity", "retrieval_profile": None},
            {
                "type": "node",
                "node": {
                    "id": "node_a",
                    "node_type": "memory",
                    "properties": {
                        "unique_key": {"kind": "String", "value": "alpha"}
                    },
                    "vectors": {"default": [1.0, 0.0]},
                },
            },
            {"type": "end", "item_count": 1},
            ]
        for frame in frames:
            yield frame

    async def aclose(self):
        self.closed = True


class SyncClientTests(unittest.TestCase):
    def test_sync_client_forwards_advanced_transport_options(self):
        with mock.patch("undr9.client.HttpTransport") as mocked_transport:
            mocked_transport.return_value = object()
            client = SyncUndr9Client(
                "http://localhost:8080",
                api_key="test",
                timeout=8.0,
                max_retries=2,
                retry_backoff_seconds=0.2,
                retry_non_idempotent_requests=True,
                headers={"x-trace-id": "trace-1"},
                user_agent="undr9-sdk-test/1.0",
                http2=True,
                follow_redirects=True,
                verify=False,
                max_connections=32,
                max_keepalive_connections=8,
                keepalive_expiry=12.0,
                connect_timeout=1.0,
                read_timeout=2.0,
                write_timeout=3.0,
                pool_timeout=4.0,
            )

        self.assertIsNotNone(client)
        mocked_transport.assert_called_once_with(
            base_url="http://localhost:8080",
            api_key="test",
            timeout=8.0,
            max_retries=2,
            retry_backoff_seconds=0.2,
            retry_non_idempotent_requests=True,
            headers={"x-trace-id": "trace-1"},
            user_agent="undr9-sdk-test/1.0",
            http2=True,
            follow_redirects=True,
            verify=False,
            max_connections=32,
            max_keepalive_connections=8,
            keepalive_expiry=12.0,
            connect_timeout=1.0,
            read_timeout=2.0,
            write_timeout=3.0,
            pool_timeout=4.0,
        )

    def test_create_node_serializes_vectors_and_properties(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        node = client.create_node(
            "node_a",
            "memory",
            {
                "unique_key": PropertyValue.string("alpha"),
            },
            vectors={"default": [1.0, 0.0], "title": [0.7, 0.3]},
        )

        self.assertEqual(node.id, "node_a")
        self.assertEqual(transport.calls[0][0], "POST")
        self.assertEqual(transport.calls[0][1], "/v1/nodes")
        self.assertEqual(
            transport.calls[0][2]["properties"]["unique_key"]["kind"],
            "String",
        )
        self.assertEqual(transport.calls[0][2]["vectors"]["default"], [1.0, 0.0])
        self.assertEqual(node.vectors["title"], [0.7, 0.3])

    def test_health_readiness_metrics_and_context_manager(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        health = client.health()
        ready = client.readiness()
        metrics = client.metrics()

        self.assertEqual(health.status, "ok")
        self.assertEqual(ready.status, "ready")
        self.assertIn("undr9_requests_total", metrics)

        with SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport) as managed:
            self.assertIsNotNone(managed)

    def test_get_node_supports_id_and_key_lookup(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        by_id = client.get_node(id="node_a")
        by_key = client.get_node(key="alpha")

        self.assertEqual(by_id.id, "node_a")
        self.assertEqual(by_key.properties["unique_key"].value, "alpha")
        self.assertEqual(transport.calls[0], ("GET", "/v1/nodes/node_a", None))
        self.assertEqual(
            transport.calls[1],
            ("POST", "/v1/query", {"GetNodeByUniqueKey": {"unique_key": "alpha"}}),
        )

    def test_get_node_rejects_ambiguous_lookup_arguments(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        with self.assertRaisesRegex(ValueError, "exactly one of id= or key="):
            client.get_node()
        with self.assertRaisesRegex(ValueError, "exactly one of id= or key="):
            client.get_node(id="node_a", key="alpha")
        with self.assertRaisesRegex(ValueError, "pass either positional node_id or id=, not both"):
            client.get_node("node_a", id="node_a")

    def test_vector_search_parses_ranked_results_and_passes_current_options(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        response = client.vector_search(
            [1.0, 0.0],
            limit=3,
            node_type="memory",
            vector_name="default",
            top_k=25,
        )

        self.assertEqual(response.plan_kind, "VectorSimilarity")
        self.assertEqual(len(response.ranked_results), 1)
        self.assertEqual(response.ranked_results[0].node.id, "node_a")
        self.assertEqual(
            transport.calls[0][2]["VectorSearch"]["vector_name"],
            "default",
        )
        self.assertEqual(transport.calls[0][2]["VectorSearch"]["top_k"], 25)

    def test_filter_nodes_builds_expected_payload(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        response = client.filter_nodes(
            label="user",
            where={
                "op": "or",
                "conditions": [
                    {
                        "op": "gt",
                        "field": "score",
                        "value": {"kind": "Integer", "value": 90},
                    },
                    {
                        "op": "eq",
                        "field": "unique_key",
                        "value": {"kind": "String", "value": "alice"},
                    },
                ],
            },
            limit=50,
        )

        self.assertEqual(response.plan_kind, "VectorSimilarity")
        self.assertEqual(transport.calls[0][1], "/v1/query")
        self.assertEqual(transport.calls[0][2]["FilterNodes"]["label"], "user")
        self.assertEqual(transport.calls[0][2]["FilterNodes"]["limit"], 50)
        self.assertEqual(
            transport.calls[0][2]["FilterNodes"]["where"]["conditions"][0]["field"],
            "score",
        )

    def test_query_stream_returns_typed_frames(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        frames = list(client.query_stream({"GetNodeById": {"node_id": "node_a"}}))

        self.assertEqual([frame.frame_type for frame in frames], ["meta", "node", "ranked_result", "end"])
        self.assertEqual(frames[0].plan_kind, "VectorSimilarity")
        self.assertEqual(frames[1].node.id, "node_a")
        self.assertEqual(frames[2].result.score, 0.99)
        self.assertEqual(frames[3].item_count, 2)
        self.assertEqual(transport.calls[0][1], "/v1/query/stream")

    def test_transaction_helpers_cover_stage_query_stream_commit_and_rollback(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        tx = client.begin_transaction()
        self.assertEqual(tx.transaction_id, "tx_1")

        staged = tx.upsert_node(
            Node(
                id="node_a",
                node_type="memory",
                properties={"unique_key": PropertyValue.string("alpha")},
                vectors={"default": [1.0, 0.0]},
            )
        )
        self.assertEqual(staged.staged_operation_count, 1)

        query_response = tx.query({"GetNodeById": {"node_id": "node_a"}})
        self.assertEqual(query_response.plan_kind, "ExactLookup")

        frames = list(tx.query_stream({"GetNodeById": {"node_id": "node_a"}}))
        self.assertEqual([frame.frame_type for frame in frames], ["meta", "node", "end"])
        self.assertEqual(frames[1].node.id, "node_a")

        commit = tx.commit()
        self.assertEqual(commit.committed_lsn, 5)

        rolled_back = client.rollback_transaction("tx_1")
        self.assertEqual(rolled_back.state, "RolledBack")

    def test_list_and_get_transactions_return_typed_summaries(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        summaries = client.list_transactions()
        summary = client.get_transaction("tx_1")

        self.assertEqual(len(summaries), 1)
        self.assertEqual(summary.transaction_id, "tx_1")
        self.assertEqual(summary.state, "Active")

    def test_admin_helpers_return_typed_responses(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        compact = client.admin_compact()
        backup = client.admin_backup("/tmp/backup")
        restore = client.admin_restore("/tmp/backup", target_lsn=42)
        repair = client.admin_repair()
        indexes = client.admin_rebuild_indexes()
        integrity = client.admin_integrity()
        status = client.admin_maintenance_status()

        self.assertEqual(compact.operation, "compact")
        self.assertEqual(backup.detail, "backup created at /tmp/backup")
        self.assertEqual(restore.operation, "restore")
        self.assertTrue(repair.manifest_present)
        self.assertEqual(indexes.vector_backend, "hnsw")
        self.assertEqual(integrity.node_count, 2)
        self.assertEqual(status.last_operation, "rebuild_indexes")

    def test_replication_and_cluster_helpers_return_typed_responses(self):
        transport = FakeTransport()
        client = SyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        status = client.replication_status()
        history = client.replication_history(after_source_lsn=0)
        follower = client.configure_as_follower("leader-1", "127.0.0.1:9100")
        leader = client.configure_as_leader()
        ack = client.acknowledge_replica("replica-1", 4)
        apply_result = client.apply_replication_records(
            [
                ReplicationRecord(
                    source_node_id="leader-1",
                    source_term=2,
                    source_lsn=4,
                    batch=WriteBatch(),
                )
            ]
        )
        topology = client.cluster_topology()
        registered = client.register_cluster_node("replica-1", "127.0.0.1:9101")
        marked = client.mark_cluster_node_health("replica-1", False)
        plan = client.promote_cluster_node("replica-1")

        self.assertEqual(status.status.mode, "Leader")
        self.assertEqual(history[0].source_lsn, 4)
        self.assertEqual(follower.status.local_node_id, "leader-1")
        self.assertEqual(leader.status.mode, "Leader")
        self.assertEqual(ack.replica_lag["replica-1"], 1)
        self.assertEqual(apply_result.status.current_term, 2)
        self.assertEqual(topology.nodes[0].role, "Primary")
        self.assertEqual(registered.term, 3)
        self.assertEqual(marked.nodes[1].healthy, False)
        self.assertEqual(plan.new_leader_node_id, "replica-1")


class AsyncClientTests(unittest.IsolatedAsyncioTestCase):
    async def test_async_client_forwards_advanced_transport_options(self):
        with mock.patch("undr9.client.AsyncHttpTransport") as mocked_transport:
            mocked_transport.return_value = object()
            client = AsyncUndr9Client(
                "http://localhost:8080",
                api_key="test",
                timeout=8.0,
                max_retries=2,
                retry_backoff_seconds=0.2,
                retry_non_idempotent_requests=True,
                headers={"x-trace-id": "trace-1"},
                user_agent="undr9-sdk-test/1.0",
                http2=True,
                follow_redirects=True,
                verify=False,
                max_connections=32,
                max_keepalive_connections=8,
                keepalive_expiry=12.0,
                connect_timeout=1.0,
                read_timeout=2.0,
                write_timeout=3.0,
                pool_timeout=4.0,
            )

        self.assertIsNotNone(client)
        mocked_transport.assert_called_once_with(
            base_url="http://localhost:8080",
            api_key="test",
            timeout=8.0,
            max_retries=2,
            retry_backoff_seconds=0.2,
            retry_non_idempotent_requests=True,
            headers={"x-trace-id": "trace-1"},
            user_agent="undr9-sdk-test/1.0",
            http2=True,
            follow_redirects=True,
            verify=False,
            max_connections=32,
            max_keepalive_connections=8,
            keepalive_expiry=12.0,
            connect_timeout=1.0,
            read_timeout=2.0,
            write_timeout=3.0,
            pool_timeout=4.0,
        )

    async def test_async_client_uses_async_transport_contract(self):
        transport = AsyncFakeTransport()
        client = AsyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        response = await client.vector_search(
            [1.0, 0.0],
            limit=1,
            vector_name="default",
            top_k=10,
        )

        self.assertEqual(response.ranked_results[0].score, 0.99)
        self.assertTrue(transport.calls)
        self.assertEqual(transport.calls[0][2]["VectorSearch"]["vector_name"], "default")
        self.assertEqual(transport.calls[0][2]["VectorSearch"]["top_k"], 10)

    async def test_async_client_context_manager_closes_transport(self):
        transport = AsyncFakeTransport()
        async with AsyncUndr9Client(
            "http://localhost:8080",
            api_key="test",
            transport=transport,
        ) as client:
            await client.search_by_label("memory", limit=5)

        self.assertEqual(transport.calls[0][2]["SearchByLabel"]["label"], "memory")
        self.assertTrue(transport.closed)

    async def test_async_health_readiness_and_metrics(self):
        transport = AsyncFakeTransport()
        client = AsyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        health = await client.health()
        ready = await client.readiness()
        metrics = await client.metrics()

        self.assertEqual(health.service, "undr9")
        self.assertEqual(ready.status, "ready")
        self.assertIn("undr9_requests_total", metrics)

    async def test_async_get_node_supports_id_and_key_lookup(self):
        transport = AsyncFakeTransport()
        client = AsyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        by_id = await client.get_node(id="node_a")
        by_key = await client.get_node(key="alpha")

        self.assertEqual(by_id.id, "node_a")
        self.assertEqual(by_key.properties["unique_key"].value, "alpha")
        self.assertEqual(transport.calls[0], ("GET", "/v1/nodes/node_a", None))
        self.assertEqual(
            transport.calls[1],
            ("POST", "/v1/query", {"GetNodeByUniqueKey": {"unique_key": "alpha"}}),
        )

    async def test_async_get_node_rejects_ambiguous_lookup_arguments(self):
        transport = AsyncFakeTransport()
        client = AsyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        with self.assertRaisesRegex(ValueError, "exactly one of id= or key="):
            await client.get_node()
        with self.assertRaisesRegex(ValueError, "exactly one of id= or key="):
            await client.get_node(id="node_a", key="alpha")
        with self.assertRaisesRegex(ValueError, "pass either positional node_id or id=, not both"):
            await client.get_node("node_a", id="node_a")

    async def test_async_query_stream_returns_typed_frames(self):
        transport = AsyncFakeTransport()
        client = AsyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        frames = []
        async for frame in client.query_stream({"GetNodeById": {"node_id": "node_a"}}):
            frames.append(frame)

        self.assertEqual([frame.frame_type for frame in frames], ["meta", "node", "end"])
        self.assertEqual(frames[1].node.id, "node_a")
        self.assertEqual(frames[2].item_count, 1)

    async def test_async_transaction_helpers_cover_full_flow(self):
        transport = AsyncFakeTransport()
        client = AsyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        tx = await client.begin_transaction()
        self.assertEqual(tx.transaction_id, "tx_1")

        staged = await tx.upsert_node(
            Node(
                id="node_a",
                node_type="memory",
                properties={"unique_key": PropertyValue.string("alpha")},
                vectors={"default": [1.0, 0.0]},
            )
        )
        self.assertEqual(staged.staged_operation_count, 1)

        query_response = await tx.query({"GetNodeById": {"node_id": "node_a"}})
        self.assertEqual(query_response.plan_kind, "ExactLookup")

        frames = []
        async for frame in tx.query_stream({"GetNodeById": {"node_id": "node_a"}}):
            frames.append(frame)
        self.assertEqual([frame.frame_type for frame in frames], ["meta", "node", "end"])

        commit = await tx.commit()
        self.assertEqual(commit.committed_revision, 5)

        rolled_back = await client.rollback_transaction("tx_1")
        self.assertEqual(rolled_back.state, "RolledBack")

    async def test_async_admin_and_cluster_helpers_return_typed_responses(self):
        transport = AsyncFakeTransport()
        client = AsyncUndr9Client("http://localhost:8080", api_key="test", transport=transport)

        compact = await client.admin_compact()
        status = await client.admin_maintenance_status()
        replication = await client.replication_status()
        history = await client.replication_history(after_source_lsn=0)
        topology = await client.cluster_topology()
        plan = await client.promote_cluster_node("replica-1")

        self.assertEqual(compact.operation, "compact")
        self.assertEqual(status.last_outcome, "success")
        self.assertEqual(replication.status.local_node_id, "leader-1")
        self.assertEqual(history[0].source_node_id, "leader-1")
        self.assertEqual(topology.nodes[1].node_id, "replica-1")
        self.assertEqual(plan.term, 4)


class HttpTransportTests(unittest.TestCase):
    def test_urllib_path_applies_custom_headers(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"ok": true}'

        transport = HttpTransport(
            "http://localhost:8080",
            api_key="test",
            headers={"x-trace-id": "trace-1"},
            user_agent="undr9-sdk-test/1.0",
        )
        transport._client = None

        with mock.patch("urllib.request.urlopen", return_value=FakeResponse()) as mocked_urlopen:
            transport.request("GET", "/healthz")

        request = mocked_urlopen.call_args.args[0]
        self.assertEqual(request.headers["X-api-key"], "test")
        self.assertEqual(request.headers["X-trace-id"], "trace-1")
        self.assertEqual(request.headers["User-agent"], "undr9-sdk-test/1.0")

    def test_request_passes_timeout_to_urlopen(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"ok": true}'

        transport = HttpTransport("http://localhost:8080", api_key="test", timeout=5.0)
        transport._client = None

        with mock.patch("urllib.request.urlopen", return_value=FakeResponse()) as mocked_urlopen:
            payload = transport.request("GET", "/healthz")

        self.assertEqual(payload["ok"], True)
        self.assertEqual(mocked_urlopen.call_args.kwargs["timeout"], 5.0)

    def test_retries_safe_request_on_connection_error_when_enabled(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"ok": true}'

        transport = HttpTransport(
            "http://localhost:8080",
            api_key="test",
            timeout=5.0,
            max_retries=2,
            retry_backoff_seconds=0.1,
        )
        transport._client = None

        with (
            mock.patch(
                "urllib.request.urlopen",
                side_effect=[urllib.error.URLError("temporary"), FakeResponse()],
            ) as mocked_urlopen,
            mock.patch("time.sleep") as mocked_sleep,
        ):
            payload = transport.request("GET", "/healthz")

        self.assertEqual(payload["ok"], True)
        self.assertEqual(mocked_urlopen.call_count, 2)
        mocked_sleep.assert_called_once_with(0.1)

    def test_does_not_retry_non_idempotent_write_by_default(self):
        transport = HttpTransport(
            "http://localhost:8080",
            api_key="test",
            timeout=5.0,
            max_retries=3,
            retry_backoff_seconds=0.1,
        )
        transport._client = None

        with (
            mock.patch(
                "urllib.request.urlopen",
                side_effect=urllib.error.URLError("temporary"),
            ) as mocked_urlopen,
            mock.patch("time.sleep") as mocked_sleep,
        ):
            with self.assertRaises(Undr9ConnectionError):
                transport.request("POST", "/v1/nodes", {"id": "node_a"})

        self.assertEqual(mocked_urlopen.call_count, 1)
        mocked_sleep.assert_not_called()

    def test_retries_query_post_but_not_writes(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def read(self):
                return b'{"ok": true}'

        transport = HttpTransport(
            "http://localhost:8080",
            api_key="test",
            timeout=5.0,
            max_retries=1,
            retry_backoff_seconds=0.25,
        )
        transport._client = None

        with (
            mock.patch(
                "urllib.request.urlopen",
                side_effect=[urllib.error.URLError("temporary"), FakeResponse()],
            ) as mocked_urlopen,
            mock.patch("time.sleep") as mocked_sleep,
        ):
            payload = transport.request("POST", "/v1/query", {"GetNodeById": {"node_id": "node_a"}})

        self.assertEqual(payload["ok"], True)
        self.assertEqual(mocked_urlopen.call_count, 2)
        mocked_sleep.assert_called_once_with(0.25)

    def test_http_error_with_non_json_body_falls_back_to_stable_sdk_error(self):
        transport = HttpTransport("http://localhost:8080", api_key="test", timeout=5.0)
        transport._client = None
        error = urllib.error.HTTPError(
            url="http://localhost:8080/v1/query",
            code=502,
            msg="Bad Gateway",
            hdrs=None,
            fp=io.BytesIO(b"<html>proxy error</html>"),
        )
        self.addCleanup(error.close)

        with mock.patch("urllib.request.urlopen", side_effect=error):
            with self.assertRaises(Undr9ApiError) as raised:
                transport.request("POST", "/v1/query", {"GetNodeById": {"node_id": "node_a"}})

        self.assertEqual(raised.exception.status_code, 502)
        self.assertEqual(raised.exception.code, "http_error")
        self.assertEqual(raised.exception.message, "Bad Gateway")

    def test_stream_request_parses_ndjson_frames(self):
        class FakeResponse:
            def __enter__(self):
                return self

            def __exit__(self, exc_type, exc, tb):
                return False

            def __iter__(self):
                return iter(
                    [
                        b'{"type":"meta","plan_kind":"VectorSimilarity"}\n',
                        b'{"type":"end","item_count":0}\n',
                    ]
                )

        transport = HttpTransport("http://localhost:8080", api_key="test", timeout=5.0)
        transport._client = None

        with mock.patch("urllib.request.urlopen", return_value=FakeResponse()):
            frames = list(transport.stream_request("POST", "/v1/query/stream", {"GetNodeById": {"node_id": "node_a"}}))

        self.assertEqual(frames[0]["type"], "meta")
        self.assertEqual(frames[1]["item_count"], 0)


@unittest.skipIf(transport_module.httpx is None, "httpx is not installed in the current interpreter")
class SyncHttpTransportClientTests(unittest.TestCase):
    def test_request_uses_httpx_client_and_text_request(self):
        class FakeResponse:
            def __init__(self, payload=None, text=""):
                self._payload = payload
                self.text = text
                self.content = b"" if payload is None else b'{"ok": true}'

            def raise_for_status(self):
                return None

            def json(self):
                return self._payload

        class FakeSyncClient:
            def __init__(self, *args, **kwargs):
                self.kwargs = kwargs
                self.calls = []

            def request(self, method, url, json=None):
                self.calls.append((method, url, json))
                if url == "/metrics":
                    return FakeResponse(payload=None, text="undr9_requests_total 1\n")
                return FakeResponse(payload={"service": "undr9", "status": "ok"})

            def close(self):
                return None

        with mock.patch("undr9._transport.httpx.Client", FakeSyncClient):
            transport = HttpTransport(
                "http://localhost:8080",
                api_key="test",
                timeout=2.5,
                headers={"x-trace-id": "trace-1"},
                user_agent="undr9-sdk-test/1.0",
                http2=True,
                follow_redirects=True,
                verify=False,
                max_connections=20,
                max_keepalive_connections=5,
                keepalive_expiry=9.0,
                connect_timeout=0.5,
                read_timeout=1.5,
                write_timeout=2.5,
                pool_timeout=3.5,
            )
            payload = transport.request("GET", "/healthz")
            metrics = transport.text_request("GET", "/metrics")
            transport.close()

        self.assertEqual(payload["status"], "ok")
        self.assertEqual(metrics, "undr9_requests_total 1\n")
        self.assertEqual(transport._client.kwargs["headers"]["x-trace-id"], "trace-1")
        self.assertEqual(transport._client.kwargs["headers"]["user-agent"], "undr9-sdk-test/1.0")
        self.assertEqual(transport._client.kwargs["timeout"].connect, 0.5)
        self.assertEqual(transport._client.kwargs["timeout"].read, 1.5)
        self.assertEqual(transport._client.kwargs["timeout"].write, 2.5)
        self.assertEqual(transport._client.kwargs["timeout"].pool, 3.5)
        self.assertEqual(transport._client.kwargs["limits"].max_connections, 20)
        self.assertEqual(transport._client.kwargs["limits"].max_keepalive_connections, 5)
        self.assertEqual(transport._client.kwargs["limits"].keepalive_expiry, 9.0)
        self.assertEqual(transport._client.kwargs["http2"], True)
        self.assertEqual(transport._client.kwargs["follow_redirects"], True)
        self.assertEqual(transport._client.kwargs["verify"], False)
        self.assertEqual(transport._client.calls[0], ("GET", "/healthz", None))


@unittest.skipIf(transport_module.httpx is None, "httpx is not installed in the current interpreter")
class AsyncHttpTransportTests(unittest.IsolatedAsyncioTestCase):
    async def test_request_uses_async_client_and_parses_json(self):
        class FakeResponse:
            def __init__(self):
                self.content = b'{"ok": true}'

            def raise_for_status(self):
                return None

            def json(self):
                return {"ok": True}

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                self.kwargs = kwargs
                self.calls = []

            async def request(self, method, url, json=None):
                self.calls.append((method, url, json))
                return FakeResponse()

            async def aclose(self):
                return None

        with mock.patch("undr9._transport.httpx.AsyncClient", FakeAsyncClient):
            transport = AsyncHttpTransport(
                "http://localhost:8080",
                api_key="test",
                timeout=2.5,
                headers={"x-trace-id": "trace-1"},
                user_agent="undr9-sdk-test/1.0",
                http2=True,
                follow_redirects=True,
                verify=False,
                max_connections=20,
                max_keepalive_connections=5,
                keepalive_expiry=9.0,
                connect_timeout=0.5,
                read_timeout=1.5,
                write_timeout=2.5,
                pool_timeout=3.5,
            )
            payload = await transport.request("POST", "/v1/query", {"GetNodeById": {"node_id": "a"}})
            await transport.aclose()

        self.assertEqual(payload["ok"], True)
        self.assertEqual(transport._client.kwargs["headers"]["x-trace-id"], "trace-1")
        self.assertEqual(transport._client.kwargs["headers"]["user-agent"], "undr9-sdk-test/1.0")
        self.assertEqual(transport._client.kwargs["timeout"].connect, 0.5)
        self.assertEqual(transport._client.kwargs["timeout"].read, 1.5)
        self.assertEqual(transport._client.kwargs["timeout"].write, 2.5)
        self.assertEqual(transport._client.kwargs["timeout"].pool, 3.5)
        self.assertEqual(transport._client.kwargs["limits"].max_connections, 20)
        self.assertEqual(transport._client.kwargs["limits"].max_keepalive_connections, 5)
        self.assertEqual(transport._client.kwargs["limits"].keepalive_expiry, 9.0)
        self.assertEqual(transport._client.kwargs["http2"], True)
        self.assertEqual(transport._client.kwargs["follow_redirects"], True)
        self.assertEqual(transport._client.kwargs["verify"], False)
        self.assertEqual(
            transport._client.calls[0],
            ("POST", "/v1/query", {"GetNodeById": {"node_id": "a"}}),
        )

    async def test_async_transport_retries_retryable_status_codes(self):
        request = transport_module.httpx.Request("POST", "http://localhost:8080/v1/query")
        first_response = transport_module.httpx.Response(
            503,
            headers={"Retry-After": "0"},
            request=request,
            json={"code": "busy", "message": "try later", "details": []},
        )
        second_response = transport_module.httpx.Response(
            200,
            request=request,
            json={"ok": True},
        )

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                self.calls = []

            async def request(self, method, url, json=None):
                self.calls.append((method, url, json))
                if len(self.calls) == 1:
                    raise transport_module.httpx.HTTPStatusError(
                        "temporary",
                        request=request,
                        response=first_response,
                    )
                return second_response

            async def aclose(self):
                return None

        with (
            mock.patch("undr9._transport.httpx.AsyncClient", FakeAsyncClient),
            mock.patch("undr9._transport.asyncio.sleep", new=mock.AsyncMock()) as mocked_sleep,
        ):
            transport = AsyncHttpTransport(
                "http://localhost:8080",
                api_key="test",
                timeout=2.5,
                max_retries=1,
                retry_backoff_seconds=0.1,
            )
            payload = await transport.request(
                "POST",
                "/v1/query",
                {"GetNodeById": {"node_id": "a"}},
            )
            await transport.aclose()

        self.assertEqual(payload["ok"], True)
        self.assertEqual(len(transport._client.calls), 2)
        mocked_sleep.assert_not_awaited()

    async def test_async_stream_request_parses_ndjson_frames(self):
        class FakeStreamResponse:
            def __init__(self):
                self.headers = {}

            async def __aenter__(self):
                return self

            async def __aexit__(self, exc_type, exc, tb):
                return False

            def raise_for_status(self):
                return None

            async def aiter_lines(self):
                for line in [
                    '{"type":"meta","plan_kind":"VectorSimilarity"}',
                    '{"type":"end","item_count":0}',
                ]:
                    yield line

        class FakeAsyncClient:
            def __init__(self, *args, **kwargs):
                self.stream_calls = []

            def stream(self, method, url, json=None):
                self.stream_calls.append((method, url, json))
                return FakeStreamResponse()

            async def aclose(self):
                return None

        with mock.patch("undr9._transport.httpx.AsyncClient", FakeAsyncClient):
            transport = AsyncHttpTransport("http://localhost:8080", api_key="test", timeout=2.5)
            frames = []
            async for frame in transport.stream_request(
                "POST",
                "/v1/query/stream",
                {"GetNodeById": {"node_id": "a"}},
            ):
                frames.append(frame)
            await transport.aclose()

        self.assertEqual(frames[0]["type"], "meta")
        self.assertEqual(frames[1]["item_count"], 0)
        self.assertEqual(
            transport._client.stream_calls[0],
            ("POST", "/v1/query/stream", {"GetNodeById": {"node_id": "a"}}),
        )


if __name__ == "__main__":
    unittest.main()
