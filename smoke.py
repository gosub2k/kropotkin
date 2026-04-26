#!/usr/bin/env python3
"""End-to-end smoke test for the kropotkin gossip cluster.

Exercises the live docker-compose cluster across these phases:
  1. Cluster healthy — all 5 nodes mutually visible as ALIVE
  2. Write propagation — values written to one node reach all nodes
  3. Failure detection — stopped container is marked DEAD on survivors
  4. Writes during outage — survivors still converge among themselves
  5. Restart and catch-up — restarted node rejoins and pulls missed state
  6. Multiple simultaneous failures
  7. Full recovery

Run against an already-running cluster:
    docker compose up -d
    python smoke.py
"""

import json
import subprocess
import sys
import time
import urllib.error
import urllib.request

NODES = [0, 1, 2, 3, 4]
BASE_PORT = 8000

GREEN = "\033[32m"
RED = "\033[31m"
YELLOW = "\033[33m"
DIM = "\033[2m"
RESET = "\033[0m"


def url(node_id: int, path: str) -> str:
    return f"http://localhost:{BASE_PORT + node_id}{path}"


def http_get(node_id: int, path: str) -> dict | None:
    try:
        with urllib.request.urlopen(url(node_id, path), timeout=3.0) as r:
            return json.load(r)
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        return None


def http_put(node_id: int, key: str, value) -> bool:
    req = urllib.request.Request(
        url(node_id, f"/state/{key}"),
        data=json.dumps({"value": value}).encode(),
        method="PUT",
        headers={"Content-Type": "application/json"},
    )
    try:
        with urllib.request.urlopen(req, timeout=3.0) as r:
            return r.status == 204
    except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError):
        return False


def docker(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], check=True, capture_output=True)


def view(node_id: int) -> dict[int, str]:
    s = http_get(node_id, "/status")
    if s is None:
        return {}
    return {int(k): v["status"] for k, v in s["members"].items()}


def wait_for(predicate, timeout: float = 15.0, poll: float = 0.4):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(poll)
    return False


def section(title: str) -> None:
    print(f"\n{YELLOW}━━━ {title} ━━━{RESET}")


def passed(msg: str) -> None:
    print(f"  {GREEN}✔{RESET} {msg}")


def failed(msg: str) -> None:
    print(f"  {RED}✘{RESET} {msg}")


def info(msg: str) -> None:
    print(f"  {DIM}{msg}{RESET}")


def assert_status(observers: list[int], target: int, expected: str, timeout: float = 30) -> bool:
    ok = wait_for(
        lambda: all(view(o).get(target) == expected for o in observers),
        timeout=timeout,
    )
    if ok:
        passed(f"nodes {observers} see node{target}={expected}")
        return True
    for o in observers:
        info(f"node{o} sees: {view(o)}")
    failed(f"nodes {observers} did not all see node{target}={expected}")
    return False


def assert_state(observers: list[int], key: str, expected, timeout: float = 20) -> bool:
    def check() -> bool:
        for o in observers:
            r = http_get(o, f"/state/{key}")
            if r is None or r.get("value") != expected:
                return False
        return True
    ok = wait_for(check, timeout=timeout)
    if ok:
        passed(f"nodes {observers} all have {key}={expected!r}")
        return True
    for o in observers:
        r = http_get(o, f"/state/{key}")
        info(f"node{o}: {key}={r}")
    failed(f"state {key}={expected!r} did not reach all of {observers}")
    return False


def preflight() -> bool:
    """Verify cluster is reachable before testing."""
    for n in NODES:
        if http_get(n, "/status") is None:
            failed(f"node{n} unreachable on port {BASE_PORT + n} — is `docker compose up -d` running?")
            return False
    return True


def main() -> int:
    if not preflight():
        return 2

    failures = 0

    section("Phase 1: cluster healthy")
    if not assert_status(NODES, 0, "ALIVE"): failures += 1
    if not assert_status(NODES, 4, "ALIVE"): failures += 1

    section("Phase 2: write propagation from any node")
    info("PUT color=blue → node0")
    http_put(0, "color", "blue")
    info("PUT shape=circle → node3")
    http_put(3, "shape", "circle")
    if not assert_state(NODES, "color", "blue"):   failures += 1
    if not assert_state(NODES, "shape", "circle"): failures += 1

    section("Phase 3: failure detection (stop node2)")
    docker("stop", "node2")
    survivors = [0, 1, 3, 4]
    if not assert_status(survivors, 2, "DEAD"): failures += 1

    section("Phase 4: writes during outage reach survivors")
    info("PUT mood=happy → node1 (while node2 is down)")
    http_put(1, "mood", "happy")
    if not assert_state(survivors, "mood", "happy"): failures += 1

    section("Phase 5: restart and catch-up")
    docker("start", "node2")
    if not assert_status(NODES, 2, "ALIVE"):       failures += 1
    if not assert_state([2], "color", "blue"):     failures += 1
    if not assert_state([2], "shape", "circle"):   failures += 1
    if not assert_state([2], "mood", "happy"):     failures += 1

    section("Phase 6: simultaneous failures (stop node1, node4)")
    docker("stop", "node1", "node4")
    survivors = [0, 2, 3]
    if not assert_status(survivors, 1, "DEAD"): failures += 1
    if not assert_status(survivors, 4, "DEAD"): failures += 1

    section("Phase 7: full recovery")
    docker("start", "node1", "node4")
    if not assert_status(NODES, 1, "ALIVE"): failures += 1
    if not assert_status(NODES, 4, "ALIVE"): failures += 1
    if not assert_state(NODES, "mood", "happy"): failures += 1  # state after full recovery

    print()
    if failures == 0:
        print(f"{GREEN}all phases passed{RESET}")
        return 0
    print(f"{RED}{failures} assertion(s) failed{RESET}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
