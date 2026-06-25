from __future__ import annotations

import os
import uuid

from undr9 import PropertyValue, SyncUndr9Client


BASE_URL = os.environ.get("UNDR9_SDK_BASE_URL", "http://127.0.0.1:8080")
WRITER_API_KEY = os.environ.get("UNDR9_SDK_WRITER_API_KEY", "dev-writer-key-000000000001")
READER_API_KEY = os.environ.get("UNDR9_SDK_READER_API_KEY", "dev-reader-key-000000000001")


def main() -> None:
    node_id = f"sdk_sync_demo_{uuid.uuid4().hex[:8]}"
    unique_key = f"{node_id}_key"

    with SyncUndr9Client(
        BASE_URL,
        api_key=WRITER_API_KEY,
        user_agent="undr9-python-sdk-example/sync-writer",
        headers={"x-demo-flow": "sync-end-to-end"},
        max_retries=2,
        retry_backoff_seconds=0.1,
        max_connections=20,
        max_keepalive_connections=10,
    ) as writer, SyncUndr9Client(
        BASE_URL,
        api_key=READER_API_KEY,
        user_agent="undr9-python-sdk-example/sync-reader",
        headers={"x-demo-flow": "sync-end-to-end"},
        max_retries=2,
        retry_backoff_seconds=0.1,
        max_connections=20,
        max_keepalive_connections=10,
    ) as reader:
        writer.create_node(
            node_id=node_id,
            node_type="memory",
            properties={
                "unique_key": PropertyValue.string(unique_key),
                # Optional built-in retrieval properties. Adding them helps
                # ranked_retrieval() combine recency, importance, and confidence
                # with semantic and graph signals.
                "timestamp": PropertyValue.integer(1_717_171_717_000),
                "importance": PropertyValue.float(0.9),
                "confidence": PropertyValue.float(0.85),
                "score": PropertyValue.integer(99),
            },
            vectors={
                "default": [1.0, 0.0],
                "title": [0.7, 0.3],
            },
        )

        filtered = reader.filter_nodes(
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
        print("filtered nodes:", [node.id for node in filtered.nodes])

        vector_results = reader.vector_search(
            [1.0, 0.0],
            limit=5,
            node_type="memory",
            vector_name="default",
            top_k=25,
        )
        print("vector hits:", [result.node.id for result in vector_results.ranked_results])

        frames = list(reader.query_stream({"GetNodeById": {"node_id": node_id}}))
        print("stream frames:", [frame.frame_type for frame in frames])

        writer.delete_node(node_id)


if __name__ == "__main__":
    main()
