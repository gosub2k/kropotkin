"""FastAPI wrapper around a single gossip Node.

Local (in-process mock transport, no UDP):
    NODE_ID=0 SEEDS=   uvicorn server:app --port 8000

UDP between real processes / containers:
    NODE_ID=0 SEEDS=1,2 USE_UDP=1 NODE_HOSTS=node0,node1,node2 uvicorn server:app

NODE_HOSTS is a comma-separated list of hostnames indexed by NODE_ID.
When omitted, defaults to 127.0.0.1 with port UDP_BASE+NODE_ID.
"""

import asyncio
import html as html_lib
import math
import os
import sys
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse, PlainTextResponse
from pydantic import BaseModel

from gossip import CommLog, MockTransport, Node, Status, UDPTransport
from mapping_algorithm import mapping

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
    claims: dict[int, int] = {}
    for k, e in n.state.items():
        if k.startswith("n:"):
            try:
                claims[int(k[2:])] = int(e.value)
            except (ValueError, TypeError):
                pass
    return {
        "node_id": n.node_id,
        "incarnation": n.incarnation,
        "claimed_n": claims.get(n.node_id),
        "members": {
            m.node_id: {
                "status": m.status.name,
                "incarnation": m.incarnation,
                "n": claims.get(m.node_id),
            }
            for m in sorted(n.members.values(), key=lambda m: m.node_id)
        },
        "claims": claims,
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


# ---------------------------------------------------------------------------
# Mapping / ring views (use mapping_algorithm.mapping; never modify it)
# ---------------------------------------------------------------------------

# Color and shape counts are coprime so a simple cross-product cycle yields
# all len(_COLORS) × len(_SHAPES) distinct pairs before any pair repeats.
# The first 8 indices are kept identical to the original fixed palette.
_COLORS = [
    "#e74c3c", "#3498db", "#2ecc71", "#f39c12",
    "#9b59b6", "#1abc9c", "#e67e22", "#16a085",
]  # 8
_SHAPES = [
    "circle", "square", "triangle", "diamond",
    "star", "pentagon", "hexagon", "cross",
    "triangle-down", "octagon", "x",
]  # 11 — gcd(8, 11) == 1 → 88 unique pairs


def _style(i: int) -> tuple[str, str]:
    return (_COLORS[i % len(_COLORS)], _SHAPES[i % len(_SHAPES)])


def _shape_open(kind: str, cx: float, cy: float, size: float) -> str:
    """Return an unclosed SVG element for `kind` centered at (cx, cy).
    Caller appends fill/data attrs and `/>`."""
    if kind == "square":
        return f'<rect x="{cx-size:.1f}" y="{cy-size:.1f}" width="{size*2}" height="{size*2}"'
    if kind == "triangle":
        return (f'<polygon points="{cx:.1f},{cy-size:.1f} '
                f'{cx-size*0.866:.1f},{cy+size/2:.1f} '
                f'{cx+size*0.866:.1f},{cy+size/2:.1f}"')
    if kind == "triangle-down":
        return (f'<polygon points="{cx:.1f},{cy+size:.1f} '
                f'{cx-size*0.866:.1f},{cy-size/2:.1f} '
                f'{cx+size*0.866:.1f},{cy-size/2:.1f}"')
    if kind == "octagon":
        pts = []
        for i in range(8):
            a = math.pi / 8 + i * math.pi / 4
            pts.append(f"{cx + size*math.cos(a):.1f},{cy + size*math.sin(a):.1f}")
        return f'<polygon points="{" ".join(pts)}"'
    if kind == "x":
        s, s2 = size, size / 2.5
        c = math.cos(math.pi / 4)
        # Rotated `cross` polygon — saltire / X shape.
        pts_raw = [
            (-s2, -s), (s2, -s), (s2, -s2), (s, -s2),
            (s, s2), (s2, s2), (s2, s), (-s2, s),
            (-s2, s2), (-s, s2), (-s, -s2), (-s2, -s2),
        ]
        pts = [(cx + c*(x - y), cy + c*(x + y)) for x, y in pts_raw]
        return f'<polygon points="{" ".join(f"{x:.1f},{y:.1f}" for x, y in pts)}"'
    if kind == "diamond":
        return (f'<polygon points="{cx:.1f},{cy-size:.1f} {cx+size:.1f},{cy:.1f} '
                f'{cx:.1f},{cy+size:.1f} {cx-size:.1f},{cy:.1f}"')
    if kind == "star":
        pts = []
        for i in range(10):
            a = -math.pi / 2 + i * math.pi / 5
            r = size if i % 2 == 0 else size * 0.4
            pts.append(f"{cx + r*math.cos(a):.1f},{cy + r*math.sin(a):.1f}")
        return f'<polygon points="{" ".join(pts)}"'
    if kind == "pentagon":
        pts = []
        for i in range(5):
            a = -math.pi / 2 + i * 2 * math.pi / 5
            pts.append(f"{cx + size*math.cos(a):.1f},{cy + size*math.sin(a):.1f}")
        return f'<polygon points="{" ".join(pts)}"'
    if kind == "hexagon":
        pts = []
        for i in range(6):
            a = i * math.pi / 3
            pts.append(f"{cx + size*math.cos(a):.1f},{cy + size*math.sin(a):.1f}")
        return f'<polygon points="{" ".join(pts)}"'
    if kind == "cross":
        s, s2 = size, size / 2.5
        pts = [
            (cx-s2, cy-s), (cx+s2, cy-s), (cx+s2, cy-s2), (cx+s, cy-s2),
            (cx+s, cy+s2), (cx+s2, cy+s2), (cx+s2, cy+s), (cx-s2, cy+s),
            (cx-s2, cy+s2), (cx-s, cy+s2), (cx-s, cy-s2), (cx-s2, cy-s2),
        ]
        return f'<polygon points="{" ".join(f"{x:.1f},{y:.1f}" for x, y in pts)}"'
    # default: circle
    return f'<circle cx="{cx:.1f}" cy="{cy:.1f}" r="{size}"'


def _claims_from_state(state: dict) -> dict[int, int]:
    """Decode {node_id: claimed_n} from gossiped `n:*` state entries (raw)."""
    out: dict[int, int] = {}
    for k, e in state.items():
        if k.startswith("n:"):
            try:
                out[int(k[2:])] = int(e.value)
            except (ValueError, TypeError):
                continue
    return out


def _live_claims(n: Node) -> dict[int, int]:
    """Like _claims_from_state, but drops entries whose member is DEAD or
    not in our membership view — i.e. the mapping reflects only live nodes."""
    out: dict[int, int] = {}
    for nid, val in _claims_from_state(n.state).items():
        m = n.members.get(nid)
        if m is None or m.status == Status.DEAD:
            continue
        out[nid] = val
    return out


@app.get("/mapping", response_class=HTMLResponse)
def mapping_view():
    n = node()
    claims = _live_claims(n)
    if not claims:
        return HTMLResponse("<pre>no live claims yet</pre>")
    ns = sorted(set(claims.values()))
    m = mapping(n=ns)
    own_n = claims.get(n.node_id)

    # Bold the line for our own n in mapping.__str__()'s output.
    out_lines = []
    for line in str(m).split("\n"):
        escaped = html_lib.escape(line)
        if own_n is not None and line.startswith(f"{own_n}:"):
            out_lines.append(f"<b style='color:#fff'>{escaped}</b>")
        else:
            out_lines.append(escaped)
    body = "\n".join(out_lines)
    return HTMLResponse(
        "<html><head><title>mapping — node " + str(n.node_id) + "</title></head>"
        "<body style='font-family:monospace;background:#1a1a1a;color:#bbb;padding:20px;'>"
        f"<h2 style='color:#eee'>node {n.node_id}'s mapping (own n = {own_n})</h2>"
        f"<p><a href='/ring' style='color:#6cf'>/ring</a> · "
        f"<a href='/status' style='color:#6cf'>/status</a></p>"
        f"<pre style='font-size:13px;line-height:1.5'>{body}</pre>"
        "</body></html>"
    )


@app.get("/ring", response_class=HTMLResponse)
def ring_view():
    n = node()
    claims = _live_claims(n)
    if not claims:
        return HTMLResponse("<pre>no live claims yet</pre>")
    ns = sorted(set(claims.values()))
    m = mapping(n=ns)
    own_n = claims.get(n.node_id)

    # n → (color, shape), enumerated in claim order
    style = {sn: _style(i) for i, sn in enumerate(ns)}
    n_to_node = {v: k for k, v in claims.items()}

    # Legend — centered viewBox so the actual shape (not a circle) renders.
    legend_parts = []
    for sn in ns:
        color, shape = style[sn]
        own_cls = " own" if sn == own_n else ""
        marker = _shape_open(shape, 0, 0, 12) + f' fill="{color}" />'
        legend_parts.append(
            f'<div class="legend-item{own_cls}" data-n="{sn}">'
            f'<svg width="28" height="28" viewBox="-15 -15 30 30">{marker}</svg>'
            f'<span>n={sn} '
            f'<span style="color:#888">(node{n_to_node.get(sn, "?")})</span>'
            f'</span></div>'
        )
    legend_html = "".join(legend_parts)

    # Tokens around the ring
    radius = 280
    R = m.R
    tokens_parts = []
    server_tokens = m.toks()
    for srv, positions in server_tokens.items():
        color, shape = style[srv]
        size = 11 if srv == own_n else 8
        for pos in positions:
            angle = (pos / R) * 2 * math.pi - math.pi / 2  # 0 at top, clockwise
            x = radius * math.cos(angle)
            y = radius * math.sin(angle)
            tokens_parts.append(
                _shape_open(shape, x, y, size)
                + f' fill="{color}" class="token" data-n="{srv}" data-pos="{pos}" />'
            )
    tokens_svg = "\n".join(tokens_parts)

    # Per-node stat panel — show the actual shape, not a generic dot.
    shard_dict = m._server_to_shard_dict()
    stat_parts = []
    for sn in ns:
        color, shape = style[sn]
        own_cls = " own" if sn == own_n else ""
        nid = n_to_node.get(sn, "?")
        marker = _shape_open(shape, 0, 0, 8) + f' fill="{color}" />'
        stat_parts.append(
            f'<div class="stat{own_cls}" data-n="{sn}">'
            f'<svg width="20" height="20" viewBox="-10 -10 20 20" '
            f'style="vertical-align:middle">{marker}</svg> '
            f'n={sn} (node{nid}): {len(shard_dict.get(sn, []))} shards, '
            f'{len(server_tokens.get(sn, []))} tokens'
            f'</div>'
        )
    stats_html = "".join(stat_parts)
    skw, mn, mx = m.skew()

    css = """
      body { font-family: -apple-system, system-ui, sans-serif; background: #1a1a1a; color: #eee; padding: 20px; margin: 0; }
      h2 { margin: 0 0 4px 0; }
      .meta { color: #888; font-size: 13px; margin-bottom: 16px; }
      .meta a { color: #6cf; }
      .legend { display: flex; gap: 8px; flex-wrap: wrap; margin-bottom: 20px; }
      .legend-item { display: flex; align-items: center; gap: 8px; padding: 6px 12px;
                     border-radius: 6px; background: #2a2a2a; cursor: pointer; user-select: none; }
      .legend-item:hover { background: #3a3a3a; }
      .legend-item.own { outline: 2px solid #fff; }
      .legend-item.dimmed { opacity: 0.25; }
      .legend-item.selected { background: #444; }
      .container { display: flex; gap: 24px; align-items: flex-start; }
      svg.ring { background: #0e0e0e; border-radius: 50%; }
      .token { cursor: pointer; transition: opacity 0.2s; }
      .token:hover { stroke: #fff; stroke-width: 2; }
      .token.dimmed { opacity: 0.08; }
      .stats { background: #222; padding: 16px; border-radius: 8px; min-width: 260px; }
      .stats h3 { margin: 0 0 10px 0; font-size: 14px; color: #888; text-transform: uppercase; letter-spacing: 1px; }
      .stat { padding: 5px 0; font-family: monospace; font-size: 13px; }
      .stat.own { font-weight: bold; color: #fff; }
      .stat.dimmed { opacity: 0.3; }
      #tooltip { position: fixed; background: #000; border: 1px solid #555; padding: 6px 10px;
                 border-radius: 4px; font-family: monospace; font-size: 12px; pointer-events: none;
                 display: none; z-index: 100; }
    """

    js = """
      const tooltip = document.getElementById('tooltip');
      document.querySelectorAll('.token').forEach(t => {
        t.addEventListener('mousemove', (e) => {
          tooltip.style.display = 'block';
          tooltip.style.left = (e.clientX + 12) + 'px';
          tooltip.style.top = (e.clientY + 12) + 'px';
          tooltip.textContent = 'n=' + t.dataset.n + ' pos=' + t.dataset.pos;
        });
        t.addEventListener('mouseleave', () => { tooltip.style.display = 'none'; });
      });
      function focusN(targetN) {
        document.querySelectorAll('.legend-item, .stat').forEach(el => {
          el.classList.toggle('dimmed', el.dataset.n !== targetN);
        });
        document.querySelectorAll('.token').forEach(t => {
          t.classList.toggle('dimmed', t.dataset.n !== targetN);
        });
      }
      function clearFocus() {
        document.querySelectorAll('.legend-item, .stat, .token').forEach(el => {
          el.classList.remove('dimmed', 'selected');
        });
      }
      document.querySelectorAll('.legend-item').forEach(el => {
        el.addEventListener('click', () => {
          const wasSelected = el.classList.contains('selected');
          clearFocus();
          if (!wasSelected) {
            el.classList.add('selected');
            focusN(el.dataset.n);
            el.classList.remove('dimmed');
          }
        });
      });
    """

    page = (
        "<!DOCTYPE html><html><head>"
        f"<title>ring — node {n.node_id}</title>"
        f"<style>{css}</style>"
        "</head><body>"
        f"<h2>ring view — node {n.node_id} <span style='color:#888'>(own n = {own_n})</span></h2>"
        f"<div class='meta'>R = {m.R:,} · {m.T} tokens/server · {m.shards} shards · "
        f"skew = {skw} (min {mn} / max {mx}) · "
        f"<a href='/mapping'>/mapping</a> · <a href='/status'>/status</a> · "
        f"<a href='javascript:location.reload()'>refresh</a></div>"
        f"<div class='legend'>{legend_html}</div>"
        "<div class='container'>"
        '<svg class="ring" width="700" height="700" viewBox="-350 -350 700 700">'
        '<circle cx="0" cy="0" r="280" stroke="#333" fill="none" stroke-width="1" stroke-dasharray="2,4" />'
        f"{tokens_svg}"
        "</svg>"
        f"<div class='stats'><h3>tokens & shards</h3>{stats_html}</div>"
        "</div>"
        "<div id='tooltip'></div>"
        f"<script>{js}</script>"
        "</body></html>"
    )
    return HTMLResponse(page)
