# UNDR9 Python SDK

The official Python client for UNDR9, a graph-native memory database for AI agents, provides sync and async APIs for:

- node CRUD
- edge CRUD
- graph query execution
- typed property filtering
- vector search
- temporal search
- ranked retrieval
- typed query streaming
- transaction lifecycle and staged transactional queries
- admin maintenance helpers
- replication and cluster helpers
- observability helpers for health, readiness, and metrics

The async client uses a real async HTTP transport and supports async context management for clean
connection shutdown.

## Install

```bash
pip install undr9
```

The package is published on PyPI as [`undr9`](https://pypi.org/project/undr9/).
For release steps and Trusted Publishing setup, see [docs/releasing.md](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/undr9-python/docs/releasing.md).

## Retrieval Philosophy

UNDR9 is built for memory retrieval, not only record storage. The core idea is that useful memory
in applications usually depends on more than one signal at a time:

- semantic similarity tells you what is related
- graph structure tells you what is connected
- recency tells you what is still fresh
- importance tells you what should stay salient
- confidence tells you how much the system should trust what was stored

This is influenced by practical memory systems and by how human memory is often described in
applications: recall is shaped by association, recency, salience, and certainty rather than by a
single keyword match. UNDR9 does not try to be a neuroscience model. It turns those ideas into a
simple, explicit retrieval model that applications can reason about.

Why this matters:

- agents usually need more than vector similarity to recover the right memory
- recent but low-value memories should not always outrank older critical ones
- highly connected memories often matter because they sit near the current context
- uncertain memories should not be treated the same as well-grounded ones

### Ranked Retrieval Formula

The default ranked retrieval profile in UNDR9 is `v1-default`. It computes a weighted score from
five normalized components:

```text
score =
  0.30 * structural +
  0.30 * semantic +
  0.15 * temporal +
  0.15 * importance +
  0.10 * confidence
```

Where:

- `structural`: graph-distance score from a reference node when one is provided
- `semantic`: cosine similarity over the selected named vector space
- `temporal`: recency score derived from the node `timestamp`
- `importance`: normalized node `importance` signal
- `confidence`: normalized node `confidence` signal

In the current implementation:

- semantic similarity is normalized cosine similarity
- temporal recency uses a seven-day half-life style decay
- missing `importance` or `confidence` default to a neutral midpoint instead of zero
- ranked retrieval can union semantic candidates with structural candidates before reranking

This is why the optional built-in node properties `timestamp`, `importance`, and `confidence` are
worth storing whenever you have them. They give the ranking model more useful memory signals than
vector similarity alone.

## Quick Start

```python
import os

from undr9 import AsyncUndr9Client, PropertyValue, SyncUndr9Client

base_url = os.environ.get("UNDR9_SDK_BASE_URL", "http://127.0.0.1:8080")

writer = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_WRITER_API_KEY"],
    user_agent="undr9-python-sdk-example/writer",
    headers={"x-demo-flow": "quick-start"},
    timeout=10.0,
    max_retries=2,
    retry_backoff_seconds=0.25,
    max_connections=20,
    max_keepalive_connections=10,
)

reader = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_READER_API_KEY"],
    user_agent="undr9-python-sdk-example/reader",
    headers={"x-demo-flow": "quick-start"},
    timeout=10.0,
    max_retries=2,
    retry_backoff_seconds=0.25,
    max_connections=20,
    max_keepalive_connections=10,
)

node = writer.create_node(
    node_id="node_a",
    node_type="memory",
    properties={
        "unique_key": PropertyValue.string("alpha"),
        # Optional built-in retrieval properties. If you add them,
        # ranked_retrieval() can use them together with vectors and graph signals.
        "timestamp": PropertyValue.integer(1000),
        "importance": PropertyValue.float(0.9),
        "confidence": PropertyValue.float(0.85),
        "score": PropertyValue.integer(98),
    },
    vectors={
        "default": [1.0, 0.0],
        "title": [0.8, 0.2],
    },
)

filtered = reader.filter_nodes(
    label="memory",
    where={
        "op": "gt",
        "field": "score",
        "value": {"kind": "Integer", "value": 90},
    },
    limit=10,
)
print(len(filtered.nodes))
```

Built-in retrieval properties:

- `timestamp`: optional node property in epoch milliseconds. Ranked retrieval uses it for recency.
- `importance`: optional node property as `Float` or `Integer`. Ranked retrieval uses it to boost higher-value memories.
- `confidence`: optional node property as `Float` or `Integer`. Ranked retrieval uses it to down-rank uncertain memories.

These properties are optional, but it is better to add them on nodes when you have the data
because `ranked_retrieval()` combines them with semantic and graph signals.

Node lookup accepts either `id=` or `key=`:

- `client.get_node(id="node_a")` performs exact lookup by node ID.
- `client.get_node(key="alpha")` resolves a node through the `unique_key` query helper.

## Namespace Concept

UNDR9 does not require every node ID to include a namespace. The Python SDK accepts both plain IDs
such as `node_a` and namespaced IDs such as `tenant_a:node_1`.

A namespace is a logical prefix embedded in the node ID before the first `:`:

- `node_a` -> no namespace
- `tenant_a:node_1` -> namespace `tenant_a`
- `customer_42:invoice_9` -> namespace `customer_42`

This is a logical partitioning convention rather than a separate required field in the node
schema. It is useful when you want to group data by tenant, workspace, or application domain while
still talking to one UNDR9 deployment.

Important behavior:

- Node IDs do not need a namespace prefix.
- If you use namespaces, keep connected nodes in the same namespace.
- Edges cannot cross namespaces, so an edge between `tenant_a:x` and `tenant_b:y` is rejected.
- Edges between plain IDs such as `node_a` and `node_b` are valid.

Example:

```python
writer.create_node(node_id="node_a", node_type="memory")
writer.create_node(node_id="tenant_a:node_1", node_type="memory")
writer.create_node(node_id="tenant_a:node_2", node_type="memory")
writer.create_edge(
    edge_id="tenant_a:edge_1",
    source_node_id="tenant_a:node_1",
    target_node_id="tenant_a:node_2",
    edge_type="relates_to",
)
```

Query helpers:

Shared setup:

```python
from undr9 import PropertyValue, SyncUndr9Client

writer = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_WRITER_API_KEY"],
)
reader = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_READER_API_KEY"],
)

writer.create_node(
    node_id="memory_alpha",
    node_type="memory",
    properties={
        "unique_key": PropertyValue.string("alpha"),
        "timestamp": PropertyValue.integer(1_717_171_717_000),
        "score": PropertyValue.integer(98),
    },
)
writer.create_node(
    node_id="memory_beta",
    node_type="memory",
    properties={
        "unique_key": PropertyValue.string("beta"),
        "timestamp": PropertyValue.integer(1_717_171_817_000),
        "score": PropertyValue.integer(87),
    },
)
writer.create_edge(
    edge_id="edge_alpha_beta",
    source="memory_alpha",
    target="memory_beta",
    edge_type="relates_to",
)
```

Exact lookup by unique key:

```python
by_key = reader.get_node_by_unique_key("alpha")
print(by_key.nodes[0].id)
```

Search by label:

```python
by_label = reader.search_by_label("memory", limit=10)
print(len(by_label.nodes))
```

Temporal range query:

```python
recent = reader.time_range(
    from_epoch_ms=1_717_171_700_000,
    to_epoch_ms=1_717_171_900_000,
    limit=10,
)
print([node.id for node in recent.nodes])
```

Graph traversal:

```python
walk = reader.traverse(
    start_node_id="memory_alpha",
    edge_type=None,
    direction="Both",
    max_hops=2,
    limit=40,
    timeout_ms=5_000,
    edge_types=[],
    node_labels=[],
)
print([node.id for node in walk.nodes])
```

`traverse()` accepts `direction="Outgoing"`, `direction="Incoming"`, or
`direction="Both"`. Use `edge_type=None` together with `edge_types=[]` when you want
to traverse all edge types without an extra filter.

Property filtering:

```python
filtered = reader.filter_nodes(
    label="memory",
    where={
        "op": "gt",
        "field": "score",
        "value": {"kind": "Integer", "value": 90},
    },
    limit=10,
)
print([node.id for node in filtered.nodes])

These helpers map to the same HTTP query endpoint:


- `get_node_by_unique_key()` -> `GetNodeByUniqueKey`
- `search_by_label()` -> `SearchByLabel`
- `filter_nodes()` -> `FilterNodes`
- `time_range()` -> `TimeRange`
- `vector_search()` -> `VectorSearch`
- `traverse()` -> `Traverse`
- `ranked_retrieval()` -> `RankedRetrieval`

Vector search:

```python
import time

from undr9 import PropertyValue, SyncUndr9Client

writer = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_WRITER_API_KEY"],
)
reader = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_READER_API_KEY"],
)

# Store embeddings in the vectors map.
writer.create_node(
    node_id="memory_alpha",
    node_type="memory",
    properties={
        "unique_key": PropertyValue.string("alpha"),
        "title": PropertyValue.string("Graph memory basics"),
        # Optional built-in retrieval properties. These are especially useful
        # when you later call ranked_retrieval().
        "timestamp": PropertyValue.integer(1_717_171_717_000),
        "importance": PropertyValue.float(0.9),
        "confidence": PropertyValue.float(0.85),
    },
    vectors={
        "default": [1.0, 0.0],
        "title_embedding": [0.9, 0.1],
    },
)
writer.create_node(
    node_id="memory_beta",
    node_type="memory",
    properties={
        "unique_key": PropertyValue.string("beta"),
        "title": PropertyValue.string("Cluster replication notes"),
        "timestamp": PropertyValue.integer(1_717_171_817_000),
        "importance": PropertyValue.float(0.6),
        "confidence": PropertyValue.float(0.7),
    },
    vectors={
        "default": [0.2, 0.9],
        "title_embedding": [0.1, 0.95],
    },
)

# Search one named vector space and inspect the ranked results.
results = reader.vector_search(
    [1.0, 0.0],
    limit=3,
    node_type="memory",
    vector_name="default",
    top_k=25,
)

best_match = results.ranked_results[0]
print(best_match.node.id)
print(best_match.score)

# Ranked retrieval combines semantic similarity with other retrieval signals.
# If the nodes include timestamp, importance, and confidence, those optional
# built-in properties are also used here.
retrieval = reader.ranked_retrieval(
    query_vector=[1.0, 0.0],
    vector_name="default",
    limit=3,
    top_k=25,
    now_epoch_ms=int(time.time() * 1000),
)
print(retrieval.ranked_results[0].node.id)

# Clean up sample data if you are running this example repeatedly.
writer.delete_node("memory_alpha")
writer.delete_node("memory_beta")
```

Use `vector_name="default"` for your primary embedding space. Use a more specific vector such as
`title_embedding` when you want retrieval against one slice of a node, for example title-only
similarity instead of whole-document similarity.

Updating vectors on an existing node:

```python
from undr9 import SyncUndr9Client

client = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_WRITER_API_KEY"],
)

node = client.get_node(id="node_a")

# Add a new named vector.
node.vectors["title_embedding"] = [0.9, 0.1, 0.3]

# Replace the primary vector.
node.vectors["default"] = [1.0, 0.2, 0.4]

# Remove a vector you no longer want to keep.
node.vectors.pop("old_embedding", None)

# Send the full node back to persist the updated vectors map.
updated = client.update_node(node)
print(updated.vectors)
```

Node CRUD:

```python
from undr9 import PropertyValue, SyncUndr9Client

client = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_WRITER_API_KEY"],
)

# Create a new node.
node = client.create_node(
    node_id="node_a",
    node_type="memory",
    properties={
        "unique_key": PropertyValue.string("alpha"),
        "title": PropertyValue.string("First memory"),
        # Optional built-in node properties. Adding them helps ranked retrieval.
        "timestamp": PropertyValue.integer(1_717_171_717_000),
        "importance": PropertyValue.float(0.8),
        "confidence": PropertyValue.float(0.9),
    },
)

# Read the node back by id.
node = client.get_node(id="node_a")
print(node.properties["title"].value)

# Read the same node back by unique key.
same_node = client.get_node(key="alpha")
print(same_node.id)

# Update properties by sending the full node again.
node.properties["title"] = PropertyValue.string("Updated memory")
node.properties["importance"] = PropertyValue.float(0.95)
node = client.update_node(node)
print(node.properties["title"].value)

# Delete the node when you no longer need it.
client.delete_node("node_a")
```

Edge CRUD:

```python
from undr9 import Edge, PropertyValue, SyncUndr9Client

client = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_WRITER_API_KEY"],
)

# Create the two nodes first. The built-in retrieval properties are optional,
# but they help ranked_retrieval() if you add them.
client.create_node(
    node_id="node_a",
    node_type="memory",
    properties={
        "unique_key": PropertyValue.string("alpha"),
        "timestamp": PropertyValue.integer(1_717_171_717_000),
        "importance": PropertyValue.float(0.9),
        "confidence": PropertyValue.float(0.85),
    },
)
client.create_node(
    node_id="node_b",
    node_type="memory",
    properties={
        "unique_key": PropertyValue.string("beta"),
        "timestamp": PropertyValue.integer(1_717_171_817_000),
        "importance": PropertyValue.float(0.7),
        "confidence": PropertyValue.float(0.8),
    },
)

# Create the edge between the two nodes. Edge properties are normal application
# metadata; ranked retrieval reads the built-in properties from nodes.
edge = client.create_edge(
    edge_id="edge_a",
    source="node_a",
    target="node_b",
    edge_type="related_to",
    properties={
        "timestamp": PropertyValue.integer(1_717_171_900_000),
        "weight": PropertyValue.float(0.75),
    },
)

# Read the edge by id.
edge = client.get_edge("edge_a")
print(edge.edge_type)

# Update properties by resubmitting the edge.
edge.properties["weight"] = PropertyValue.float(0.9)
edge = client.update_edge(edge)
print(edge.properties["weight"].value)

# Delete the edge when the relationship is no longer needed.
client.delete_edge("edge_a")
client.delete_node("node_a")
client.delete_node("node_b")
```

Async usage:

```python
async with AsyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_READER_API_KEY"],
    timeout=10.0,
    max_retries=2,
    user_agent="undr9-python-sdk-example/async-reader",
    headers={"x-demo-flow": "async-quick-start"},
    max_connections=20,
    max_keepalive_connections=10,
) as client:
    response = await client.vector_search(
        [1.0, 0.0],
        limit=3,
        vector_name="default",
        top_k=25,
    )
    print(response.plan_kind)
```

Stream usage:

```python
for frame in client.query_stream({"GetNodeById": {"node_id": "node_a"}}):
    if frame.frame_type == "meta":
        print(frame.plan_kind)
    elif frame.frame_type == "node":
        print(frame.node.id)
    elif frame.frame_type == "end":
        print(frame.item_count)
```

Transaction usage:

```python
tx = client.begin_transaction()

tx.upsert_node(
    node,
)

snapshot = tx.query({"GetNodeById": {"node_id": "node_a"}})
print(snapshot.plan_kind)

for frame in tx.query_stream({"GetNodeById": {"node_id": "node_a"}}):
    print(frame.frame_type)

commit = tx.commit()
print(commit.committed_lsn)
```

Admin and replication usage:

```python
status = client.admin_maintenance_status()
print(status.last_operation)

integrity = client.admin_integrity()
print(integrity.manifest_present)

replication = client.replication_status()
print(replication.status.mode)

topology = client.cluster_topology()
print(topology.leader_node_id)
```

Observability usage:

```python
health = client.health()
ready = client.readiness()
metrics = client.metrics()

print(health.status)
print(ready.status)
print("undr9_requests_total" in metrics)
```

Advanced transport tuning:

```python
client = SyncUndr9Client(
    base_url,
    api_key=os.environ["UNDR9_SDK_READER_API_KEY"],
    user_agent="undr9-python-sdk-prod/1.0",
    headers={"x-request-source": "worker-a"},
    http2=True,
    follow_redirects=False,
    verify=True,
    max_connections=50,
    max_keepalive_connections=20,
    keepalive_expiry=30.0,
    connect_timeout=2.0,
    read_timeout=10.0,
    write_timeout=10.0,
    pool_timeout=5.0,
    max_retries=2,
    retry_backoff_seconds=0.25,
)
```

Examples shipped in the repository:

- `examples/sync_end_to_end.py`
- `examples/async_end_to_end.py`
- `examples/operational_workflows.py`

Live contract test entrypoint:

```bash
export UNDR9_SDK_LIVE_TESTS=1
export UNDR9_SDK_BASE_URL=http://127.0.0.1:8080
export UNDR9_SDK_ADMIN_API_KEY=dev-admin-key-000000000001
export UNDR9_SDK_WRITER_API_KEY=dev-writer-key-000000000001
export UNDR9_SDK_READER_API_KEY=dev-reader-key-000000000001

python -m unittest -q tests.test_live_contract
```

Packaging verification:

```bash
python -m build
python -m twine check dist/*
python scripts/verify_dist.py
```

## Notes

- Store embeddings only in the node `vectors` map. Do not send `properties.embedding`.
- Use `timeout` on the client constructor to bound blocking HTTP calls.
- Use `max_retries` and `retry_backoff_seconds` to enable bounded retry/backoff behavior.
- Use `connect_timeout`, `read_timeout`, `write_timeout`, and `pool_timeout` when you need per-phase timeout control instead of one shared timeout.
- Use `max_connections`, `max_keepalive_connections`, and `keepalive_expiry` to tune HTTP connection pooling for long-lived services.
- Use `headers`, `user_agent`, `http2`, `follow_redirects`, and `verify` when you need proxy, TLS, or observability-oriented transport customization.
- Retries are disabled by default and only apply to safe requests plus query POSTs unless `retry_non_idempotent_requests=True` is explicitly set.
- `AsyncUndr9Client` also accepts `timeout` and should be used as an async context manager when possible.
- Use separate reader, writer, and admin clients when you want your SDK usage to mirror the server's API-key roles.
- Use `query_stream()` when you want typed NDJSON stream frames from `/v1/query/stream`.
- Use `begin_transaction()` for snapshot transactions, then stage writes with `upsert_node()`, `upsert_edge()`, `delete_node()`, or `delete_edge()`.
- Use `transaction_query()` and `transaction_query_stream()` if you prefer direct transaction-id based helpers instead of the wrapper object.
- Use `admin_compact()`, `admin_backup()`, `admin_restore()`, `admin_repair()`, `admin_rebuild_indexes()`, `admin_integrity()`, and `admin_maintenance_status()` for maintenance and integrity workflows.
- Use `replication_status()`, `replication_history()`, `configure_as_leader()`, `configure_as_follower()`, `acknowledge_replica()`, and `apply_replication_records()` for replication workflows.
- Use `cluster_topology()`, `register_cluster_node()`, `mark_cluster_node_health()`, and `promote_cluster_node()` for cluster-topology operations.
- Use `health()`, `readiness()`, and `metrics()` for runtime observability checks.
- `SyncUndr9Client` can now be used as a context manager and closes its reusable HTTP client on exit.
- Use `get_node_by_unique_key()` for exact lookup through the `unique_key` property when you do not already know the node ID.
- Use `search_by_label()` to fetch nodes by `node_type`.
- Use `time_range()` for temporal filtering over a timestamp-like property.
- Use `traverse()` for bounded graph walks from a starting node.
- Use `vector_name` to target a named vector space for `vector_search()` and `ranked_retrieval()`.
- Use `top_k` to override the semantic candidate budget when needed.
- Use `filter_nodes()` for database-side property predicates such as `eq`, `gt`, `gte`, `lt`, `lte`, `and`, and `or`.
