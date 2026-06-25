from __future__ import annotations

import asyncio
import os
import uuid

from undr9 import AsyncUndr9Client, PropertyValue


BASE_URL = os.environ.get("UNDR9_SDK_BASE_URL", "http://127.0.0.1:8080")
WRITER_API_KEY = os.environ.get("UNDR9_SDK_WRITER_API_KEY", "dev-writer-key-000000000001")
READER_API_KEY = os.environ.get("UNDR9_SDK_READER_API_KEY", "dev-reader-key-000000000001")


async def main() -> None:
    node_id = f"sdk_async_demo_{uuid.uuid4().hex[:8]}"

    async with AsyncUndr9Client(
        BASE_URL,
        api_key=WRITER_API_KEY,
        user_agent="undr9-python-sdk-example/async-writer",
        headers={"x-demo-flow": "async-end-to-end"},
        max_retries=2,
        retry_backoff_seconds=0.1,
        max_connections=20,
        max_keepalive_connections=10,
    ) as writer, AsyncUndr9Client(
        BASE_URL,
        api_key=READER_API_KEY,
        user_agent="undr9-python-sdk-example/async-reader",
        headers={"x-demo-flow": "async-end-to-end"},
        max_retries=2,
        retry_backoff_seconds=0.1,
        max_connections=20,
        max_keepalive_connections=10,
    ) as reader:
        await writer.create_node(
            node_id=node_id,
            node_type="memory",
            properties={
                "unique_key": PropertyValue.string(node_id),
                # Optional built-in retrieval properties. Adding them helps
                # ranked_retrieval() combine recency, importance, and confidence
                # with semantic and graph signals.
                "timestamp": PropertyValue.integer(1_818_181_818_000),
                "importance": PropertyValue.float(0.8),
                "confidence": PropertyValue.float(0.9),
            },
            vectors={"default": [0.0, 1.0]},
        )

        labeled = await reader.search_by_label("memory", limit=10)
        print("label hits:", [node.id for node in labeled.nodes])

        frames = []
        async for frame in reader.query_stream({"GetNodeById": {"node_id": node_id}}):
            frames.append(frame.frame_type)
        print("stream frames:", frames)

        await writer.delete_node(node_id)


if __name__ == "__main__":
    asyncio.run(main())
