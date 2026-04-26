"""FastAPI wrapper around a single gossip Node.

Local (in-process mock transport, no UDP):
    NODE_ID=0 SEEDS=   uvicorn server:app --port 8000

UDP between real processes / containers:
    NODE_ID=0 SEEDS=1,2 USE_UDP=1 NODE_HOSTS=node0,node1,node2 uvicorn server:app

NODE_HOSTS is a comma-separated list of hostnames indexed by NODE_ID.
When omitted, defaults to 127.0.0.1 with port UDP_BASE+NODE_ID.
"""

import asyncio
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

from fastapi.responses import PlainTextResponse

from gossip import CommLog, MockTransport, Node, UDPTransport

# ---------------------------------------------------------------------------
# Config from environment
# ---------------------------------------------------------------------------
NODE_ID = int(os.environ.get("NODE_ID", "0"))
UDP_BASE = int(os.environ.get("UDP_BASE", "9000"))

_seed_env = os.environ.get("SEEDS", "")
SEEDS = [int(s) for s in _seed_env.split(",") if s.strip()]

USE_UDP = os.environ.get("USE_UDP", "").lower() in ("1", "true", "yes")

# Comma-separated hostnames indexed by node id, e.g. "node0,node1,node2".
# When set all nodes share the same UDP_BASE port; each host is a distinct container.
_hosts_env = os.environ.get("NODE_HOSTS", "")
NODE_HOSTS = [h.strip() for h in _hosts_env.split(",") if h.strip()]

GOSSIP_INTERVAL = float(os.environ.get("GOSSIP_INTERVAL", "0.5"))
PROBE_INTERVAL = float(os.environ.get("PROBE_INTERVAL", "0.5"))
PING_TIMEOUT = float(os.environ.get("PING_TIMEOUT", "1.0"))
SUSPICION_TIMEOUT = float(os.environ.get("SUSPICION_TIMEOUT", "3.0"))

# ---------------------------------------------------------------------------
# Globals set during lifespan
# ---------------------------------------------------------------------------
_node: Node | None = None
_stop: asyncio.Event | None = None
# Mirror events to stderr by default so they appear in `docker logs`.
# Set COMM_LOG_STDERR=0 to silence stderr (ring buffer at /debug/log still works).
_mirror = os.environ.get("COMM_LOG_STDERR", "1").lower() not in ("0", "false", "no")
_comm_log = CommLog(capacity=2000, stream=sys.stderr if _mirror else None)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global _node, _stop

    if USE_UDP:
        if NODE_HOSTS:
            # node_map addresses every host; SEEDS only bootstraps initial
            # members. Lets us later learn other peers via gossip and still
            # be able to address them.
            node_map = {i: (host, UDP_BASE) for i, host in enumerate(NODE_HOSTS)}
        else:
            all_ids = sorted({NODE_ID} | set(SEEDS))
            node_map = {i: ("127.0.0.1", UDP_BASE + i) for i in all_ids}
        transport = UDPTransport(node_map, comm_log=_comm_log)
    else:
        transport = MockTransport()

    _stop = asyncio.Event()
    _node = Node(NODE_ID, SEEDS, transport,
                 gossip_interval=GOSSIP_INTERVAL,
                 probe_interval=PROBE_INTERVAL,
                 ping_timeout=PING_TIMEOUT,
                 suspicion_timeout=SUSPICION_TIMEOUT,
                 comm_log=_comm_log)

    task = asyncio.create_task(_node.run(_stop))

    def _surface(t: asyncio.Task):
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            _comm_log.event(f"run() CRASHED: {exc!r}")

    task.add_done_callback(_surface)
    yield
    _stop.set()
    await task


app = FastAPI(title="Gossip Node", lifespan=lifespan)


def node() -> Node:
    if _node is None:
        raise HTTPException(503, "Node not ready")
    return _node


# ---------------------------------------------------------------------------
# Routes
# ---------------------------------------------------------------------------
@app.get("/status")
def status():
    import time
    n = node()
    t = time.monotonic()
    return {
        "node_id": n.node_id,
        "incarnation": n.incarnation,
        "members": {
            m.node_id: {"status": m.status.name, "incarnation": m.incarnation}
            for m in sorted(n.members.values(), key=lambda m: m.node_id)
        },
        "state": {k: {"value": e.value, "version": e.version} for k, e in n.state.items()},
        "pending_pings": list(n.pending_pings.keys()),
        "suspect_since": {nid: round(t - since, 1) for nid, since in n.suspect_since.items()},
        "loops_age_secs": {name: round(t - ts, 1) for name, ts in n.loop_last_ran.items()},
        "transport": n.transport.debug_info(),
    }


@app.get("/members")
def members():
    return {
        m.node_id: m.status.name
        for m in node().members.values()
    }


@app.get("/state")
def state_all():
    return {k: e.value for k, e in node().state.items()}


@app.get("/state/{key}")
def state_get(key: str):
    value = node().get(key)
    if value is None:
        raise HTTPException(404, f"key {key!r} not found")
    return {"key": key, "value": value}


class PutBody(BaseModel):
    value: object


@app.put("/state/{key}", status_code=204)
def state_put(key: str, body: PutBody):
    node().put(key, body.value)


@app.get("/debug/log", response_class=PlainTextResponse)
def debug_log(n: int = 200):
    """Tail the per-node communication log. Separate from uvicorn stdio so
    you can `curl -N` it without interleaving HTTP access lines."""
    return "\n".join(_comm_log.tail(n))
