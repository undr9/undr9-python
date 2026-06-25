from __future__ import annotations

import os

from undr9 import SyncUndr9Client


BASE_URL = os.environ.get("UNDR9_SDK_BASE_URL", "http://127.0.0.1:8080")
ADMIN_API_KEY = os.environ.get("UNDR9_SDK_ADMIN_API_KEY", "dev-admin-key-000000000001")


def main() -> None:
    with SyncUndr9Client(
        BASE_URL,
        api_key=ADMIN_API_KEY,
        user_agent="undr9-python-sdk-example/ops",
        headers={"x-demo-flow": "operational-workflows"},
        max_retries=2,
        retry_backoff_seconds=0.1,
        max_connections=10,
        max_keepalive_connections=5,
    ) as admin:
        health = admin.health()
        readiness = admin.readiness()
        metrics = admin.metrics()
        integrity = admin.admin_integrity()
        topology = admin.cluster_topology()

        print("health:", health.status)
        print("readiness:", readiness.status)
        print("integrity:", integrity.manifest_present)
        print("leader:", topology.leader_node_id)
        print("metrics available:", "undr9_" in metrics)


if __name__ == "__main__":
    main()
