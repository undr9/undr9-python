# Python SDK Audit Report

## Scope

This report reviews the checked-in Python SDK under `sdk/python/` for:

- speed
- performance
- integration readiness
- scalability
- developer easiness
- missing features

The audit is based on the current SDK implementation and the current UNDR9 HTTP API contract.

## Executive Summary

The Python SDK is a useful early wrapper around the UNDR9 HTTP API, but it is not yet a
production-grade official client.

The strongest parts today are:

- a small and readable codebase
- simple typed dataclass models
- basic sync CRUD and query support
- a real async transport with async client lifecycle support
- packaging metadata for installation

The biggest current issues are:

- remaining niche API helpers may still require raw `query()`
- transport tuning remains intentionally selective rather than exhaustive
- release automation now exists, but publishing policy is still repository-driven

## Files Reviewed

- [client.py](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/sdk/python/src/undr9/client.py)
- [_transport.py](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/sdk/python/src/undr9/_transport.py)
- [models.py](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/sdk/python/src/undr9/models.py)
- [errors.py](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/sdk/python/src/undr9/errors.py)
- [__init__.py](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/sdk/python/src/undr9/__init__.py)
- [test_sdk.py](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/sdk/python/tests/test_sdk.py)
- [README.md](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/sdk/python/README.md)
- [pyproject.toml](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/sdk/python/pyproject.toml)

Relevant server contract references:

- [http.md](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/docs/api/http.md)
- [README.md](file:///Users/mdinjemamulirshad/Documents/projects/undr9-memorydb/README.md#L184-L221)

## Findings

### 1. Speed

Assessment: acceptable for scripts, weak for sustained concurrent workloads

Positive signals:

- the implementation is small and does little work beyond JSON serialization
- the models are lightweight dataclasses with `slots=True`
- the SDK adds very little client-side abstraction overhead

Limitations:

- the transport is based on blocking `urllib.request`
- the sync path still has a fallback `urllib` mode for lightweight environments
- the async path is improved, and the packaged sync path now has reusable-client support

Bottom line:

- request overhead is probably fine for low-volume tools and scripts
- the async path is materially stronger for concurrent use, but the SDK still lacks the resilience
  and richer transport controls expected from a production client

### 2. Performance

Assessment: simple, but not performance-oriented

Positive signals:

- model serialization is straightforward and predictable
- the client mostly mirrors the API contract and avoids hidden work
- timeout and bounded retry/backoff controls now exist
- the packaged sync path now supports reusable HTTP client behavior

Concerns:

- no batching or paging helpers at the SDK layer
- connection tuning is now materially better, but proxy and custom-auth transport hooks remain minimal

Bottom line:

- the SDK is not heavy, but it also does not yet provide the core transport behaviors expected from
  a fully production-hardened client

### 3. Integration Readiness

Assessment: moderate to good for the covered surface

Strengths:

- `Node`, `Edge`, `PropertyValue`, and `QueryResponse` map cleanly to the current API shapes
- there is a generic `query()` escape hatch for unsupported query variants
- packaging metadata exists and the module exports are clean
- vector writes now align with the server's `vectors`-only contract
- current convenience helpers now cover `FilterNodes`, `vector_name`, and `top_k`
- live contract coverage now exists for sync and async flows against a real server
- CI now verifies unit tests, byte-compilation, package builds, installability, and live contracts

Server contract:

- vectors live in `vectors`
- `properties.embedding` is no longer accepted

Bottom line:

- the SDK is now aligned with the current vector write contract and now covers query streaming,
  transactions, admin maintenance, replication, and cluster workflows, and it now has live contract
  validation for core sync and async paths

### 4. Scalability

Assessment: improved, but still incomplete

Missing scalability features:

- proxy configuration and custom auth injection beyond headers
- SDK-level batching or paging helpers for very large result sets

Bottom line:

- the async client now has the right basic architecture for long-lived services, and the packaged
  sync path is improved, and the SDK now has live contract coverage plus broader production
  hardening in CI

### 5. Easiness

Assessment: decent for core workflows, still incomplete for operational features

What is easy:

- basic node CRUD
- basic edge CRUD
- generic query submission
- typed query streaming
- transaction begin/stage/query/query-stream/commit/rollback helpers
- admin maintenance and integrity helpers
- replication and cluster topology helpers
- health, readiness, and metrics helpers
- convenience wrappers for vector, time-range, traversal, shortest-path, and ranked retrieval

What is hard:

- relying on the SDK for all public API capabilities without falling back to raw `query()`

Bottom line:

- the SDK is approachable for simple cases, but it does not yet make the full product easy to use

## Missing Features

The most notable missing features today are:

- proxy-oriented transport controls
- SDK-level batching or pagination helpers
- automated publish policy beyond GitHub release asset generation

## Quality Of The Current Code

### Strengths

- easy to read
- small API surface
- clean dataclass models
- no unnecessary dependencies
- clear custom error types

### Weaknesses

- transport implementation is too minimal for an official SDK
- tests are heavily mock-based and narrow
- convenience methods still lag the full public API surface
- live contract coverage currently focuses on high-value core flows rather than every admin endpoint

## Recommendations

Recommended immediate fixes:

1. extend live contract coverage to additional admin and replication scenarios as those stabilize
2. keep packaging verification in CI and release workflows
3. expand the end-to-end example set as new high-level helpers are added
4. consider whether batching/pagination should be first-class SDK features

Recommended next wave:

1. expand first-class coverage for any remaining niche admin or observability APIs
2. decide whether to publish to PyPI or keep GitHub-release-only distribution artifacts
3. add richer integration examples for replication and maintenance workflows
4. consider proxy and custom-auth transport hooks if downstream users request them
5. consider SDK-level batching or pagination helpers

## Final Verdict

The Python SDK is currently best described as:

- a credible early official SDK
- suitable for real integrations across sync and async workflows
- moving toward production-readiness, with the biggest remaining gaps now centered on breadth and
  long-tail operational polish

Its biggest recent correctness risk was vector-contract drift, which is now corrected in the SDK
surface, README, and tests.

Its biggest medium-term risk is now breadth and polish: the SDK has live contract coverage, CI and
packaging verification, richer examples, and stronger transport controls, but it still needs
continued expansion of advanced operational helpers and long-tail transport features to become a
fully production-ready flagship client.
