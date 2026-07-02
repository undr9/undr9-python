import os
import pathlib
import sys
import unittest
import uuid

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / "src"))

from undr9 import AsyncUndr9Client, Node, PropertyValue, SyncUndr9Client


LIVE_TEST_FLAG = "UNDR9_SDK_LIVE_TESTS"
BASE_URL_ENV = "UNDR9_SDK_BASE_URL"
ADMIN_API_KEY_ENV = "UNDR9_SDK_ADMIN_API_KEY"
WRITER_API_KEY_ENV = "UNDR9_SDK_WRITER_API_KEY"
READER_API_KEY_ENV = "UNDR9_SDK_READER_API_KEY"


def _require_live_config() -> tuple[str, str, str, str]:
    if os.environ.get(LIVE_TEST_FLAG) != "1":
        raise unittest.SkipTest(f"set {LIVE_TEST_FLAG}=1 to run live SDK contract tests")

    base_url = os.environ.get(BASE_URL_ENV, "http://127.0.0.1:8080")
    admin_api_key = os.environ.get(ADMIN_API_KEY_ENV)
    writer_api_key = os.environ.get(WRITER_API_KEY_ENV)
    reader_api_key = os.environ.get(READER_API_KEY_ENV)

    missing = [
        name
        for name, value in (
            (ADMIN_API_KEY_ENV, admin_api_key),
            (WRITER_API_KEY_ENV, writer_api_key),
            (READER_API_KEY_ENV, reader_api_key),
        )
        if not value
    ]
    if missing:
        raise unittest.SkipTest(
            "missing required live SDK contract env vars: " + ", ".join(missing)
        )

    return base_url, admin_api_key, writer_api_key, reader_api_key


class LiveSyncContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        base_url, admin_api_key, writer_api_key, reader_api_key = _require_live_config()
        cls.admin_client = SyncUndr9Client(
            base_url,
            api_key=admin_api_key,
            user_agent="undr9-python-sdk-live-contract/sync",
            headers={"x-undr9-test-suite": "python-live-sync"},
            max_retries=2,
            retry_backoff_seconds=0.1,
            max_connections=20,
            max_keepalive_connections=10,
            connect_timeout=2.0,
            read_timeout=5.0,
            write_timeout=5.0,
            pool_timeout=2.0,
        )
        cls.writer_client = SyncUndr9Client(
            base_url,
            api_key=writer_api_key,
            user_agent="undr9-python-sdk-live-contract/sync",
            headers={"x-undr9-test-suite": "python-live-sync"},
            max_retries=2,
            retry_backoff_seconds=0.1,
            max_connections=20,
            max_keepalive_connections=10,
        )
        cls.reader_client = SyncUndr9Client(
            base_url,
            api_key=reader_api_key,
            user_agent="undr9-python-sdk-live-contract/sync",
            headers={"x-undr9-test-suite": "python-live-sync"},
            max_retries=2,
            retry_backoff_seconds=0.1,
            max_connections=20,
            max_keepalive_connections=10,
        )

    @classmethod
    def tearDownClass(cls):
        cls.reader_client.close()
        cls.writer_client.close()
        cls.admin_client.close()

    def setUp(self):
        self._cleanup_node_ids = []

    def tearDown(self):
        for node_id in reversed(self._cleanup_node_ids):
            try:
                self.writer_client.delete_node(node_id)
            except Exception:
                pass

    def _new_node_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    def test_sync_core_query_and_stream_contract(self):
        node_id = self._new_node_id("sdk_live_sync")
        unique_key = f"{node_id}_key"
        self._cleanup_node_ids.append(node_id)

        created = self.writer_client.create_node(
            node_id=node_id,
            node_type="memory",
            properties={
                "unique_key": PropertyValue.string(unique_key),
                "timestamp": PropertyValue.integer(1_717_171_717_000),
                "score": PropertyValue.integer(97),
            },
            vectors={"default": [1.0, 0.0]},
        )

        self.assertEqual(created.id, node_id)
        self.assertEqual(created.properties["unique_key"].value, unique_key)

        fetched = self.writer_client.get_node(id=node_id)
        self.assertEqual(fetched.node_type, "memory")

        fetched_by_key = self.reader_client.get_node(key=unique_key)
        self.assertEqual(fetched_by_key.id, node_id)

        by_key = self.reader_client.get_node_by_unique_key(unique_key)
        self.assertEqual(by_key.nodes[0].id, node_id)

        filtered = self.reader_client.filter_nodes(
            label="memory",
            where={
                "op": "and",
                "conditions": [
                    {
                        "op": "gt",
                        "field": "score",
                        "value": {"kind": "Integer", "value": 90},
                    },
                    {
                        "op": "eq",
                        "field": "unique_key",
                        "value": {"kind": "String", "value": unique_key},
                    },
                ],
            },
            limit=10,
        )
        self.assertTrue(any(node.id == node_id for node in filtered.nodes))

        vector_results = self.reader_client.vector_search(
            [1.0, 0.0],
            limit=5,
            node_type="memory",
            vector_name="default",
            top_k=10,
        )
        self.assertTrue(any(result.node.id == node_id for result in vector_results.ranked_results))

        frames = list(self.reader_client.query_stream({"GetNodeById": {"node_id": node_id}}))
        self.assertEqual(frames[0].frame_type, "meta")
        self.assertEqual(frames[-1].frame_type, "end")
        self.assertTrue(any(frame.node and frame.node.id == node_id for frame in frames[1:-1]))

    def test_sync_transactions_and_observability_contract(self):
        node_id = self._new_node_id("sdk_live_tx")
        unique_key = f"{node_id}_key"
        self._cleanup_node_ids.append(node_id)

        health = self.admin_client.health()
        readiness = self.admin_client.readiness()
        metrics = self.admin_client.metrics()
        integrity = self.admin_client.admin_integrity()

        self.assertEqual(health.status, "ok")
        self.assertIn(readiness.status, {"ok", "ready"})
        self.assertIn("undr9_", metrics)
        self.assertTrue(integrity.manifest_present)

        tx = self.writer_client.begin_transaction()
        tx.upsert_node(
            Node(
                id=node_id,
                node_type="memory",
                properties={
                    "unique_key": PropertyValue.string(unique_key),
                    "timestamp": PropertyValue.integer(1_818_181_818_000),
                },
                vectors={"default": [0.0, 1.0]},
            )
        )

        snapshot = tx.query({"GetNodeById": {"node_id": node_id}})
        self.assertEqual(snapshot.nodes[0].id, node_id)

        streamed = list(tx.query_stream({"GetNodeById": {"node_id": node_id}}))
        self.assertEqual(streamed[0].frame_type, "meta")
        self.assertTrue(any(frame.node and frame.node.id == node_id for frame in streamed[1:-1]))

        commit = tx.commit()
        self.assertEqual(commit.transaction_id, tx.transaction_id)

        by_key = self.reader_client.get_node_by_unique_key(unique_key)
        self.assertTrue(any(node.id == node_id for node in by_key.nodes))


class LiveAsyncContractTests(unittest.IsolatedAsyncioTestCase):
    @classmethod
    def setUpClass(cls):
        cls.base_url, cls.admin_api_key, cls.writer_api_key, cls.reader_api_key = _require_live_config()

    async def asyncSetUp(self):
        self._cleanup_node_ids = []
        self.writer_client = AsyncUndr9Client(
            self.base_url,
            api_key=self.writer_api_key,
            user_agent="undr9-python-sdk-live-contract/async",
            headers={"x-undr9-test-suite": "python-live-async"},
            max_retries=2,
            retry_backoff_seconds=0.1,
            max_connections=20,
            max_keepalive_connections=10,
            http2=False,
        )
        self.reader_client = AsyncUndr9Client(
            self.base_url,
            api_key=self.reader_api_key,
            user_agent="undr9-python-sdk-live-contract/async",
            headers={"x-undr9-test-suite": "python-live-async"},
            max_retries=2,
            retry_backoff_seconds=0.1,
            max_connections=20,
            max_keepalive_connections=10,
            http2=False,
        )

    async def asyncTearDown(self):
        for node_id in reversed(self._cleanup_node_ids):
            try:
                await self.writer_client.delete_node(node_id)
            except Exception:
                pass
        await self.reader_client.aclose()
        await self.writer_client.aclose()

    def _new_node_id(self, prefix: str) -> str:
        return f"{prefix}_{uuid.uuid4().hex[:10]}"

    async def test_async_core_contract(self):
        node_id = self._new_node_id("sdk_live_async")
        unique_key = f"{node_id}_key"
        self._cleanup_node_ids.append(node_id)

        created = await self.writer_client.create_node(
            node_id=node_id,
            node_type="memory",
            properties={
                "unique_key": PropertyValue.string(unique_key),
                "timestamp": PropertyValue.integer(1_919_191_919_000),
            },
            vectors={"default": [0.5, 0.5]},
        )
        self.assertEqual(created.id, node_id)

        labeled = await self.reader_client.search_by_label("memory", limit=20)
        self.assertTrue(any(node.id == node_id for node in labeled.nodes))

        frames = []
        async for frame in self.reader_client.query_stream({"GetNodeById": {"node_id": node_id}}):
            frames.append(frame)

        self.assertEqual(frames[0].frame_type, "meta")
        self.assertTrue(any(frame.node and frame.node.id == node_id for frame in frames[1:-1]))
