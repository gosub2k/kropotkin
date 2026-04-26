"""SWIM-style gossip: membership with alive/suspect/dead + incarnation
numbers, plus push-pull anti-entropy of opaque key/value state.

Async, driven by a real monotonic clock. Each node runs as its own
coroutine. Swap MockTransport for UDPTransport to take it live.
"""

import asyncio
import pickle
import random
import socket
import time
from abc import ABC, abstractmethod
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Callable


class Status(Enum):
    UNKNOWN = 0   # seeded but not yet confirmed alive via comms
    JOINING = 1   # alive but hasn't claimed its `n` slot yet
    ALIVE = 2     # alive and `n` claimed
    SUSPECT = 3
    DEAD = 4


# Higher = more confident. ALIVE > JOINING > UNKNOWN means any later news
# about a peer wins; SUSPECT > ALIVE means a probe failure overrides ALIVE
# at the same incarnation (refutation is by incarnation bump).
_ORDER = {
    Status.UNKNOWN: 0,
    Status.JOINING: 1,
    Status.ALIVE: 2,
    Status.SUSPECT: 3,
    Status.DEAD: 4,
}


class CommLog:
    """In-memory ring buffer of node-communication events, optionally
    mirrored to a stream (typically sys.stderr) so events appear live in
    `docker logs`. The ring buffer remains queryable via /debug/log."""

    def __init__(self, capacity: int = 2000, stream=None):
        self._buf: deque[str] | None = (
            deque(maxlen=capacity) if capacity > 0 else None
        )
        self._stream = stream
        self._t0 = time.monotonic()

    def event(self, msg: str) -> None:
        t = time.monotonic() - self._t0
        line = f"{t:9.3f}s {msg}"
        if self._buf is not None:
            self._buf.append(line)
        if self._stream is not None:
            print(f"[comm] {line}", file=self._stream, flush=True)

    def tail(self, n: int = 200) -> list[str]:
        if self._buf is None:
            return []
        return list(self._buf)[-n:]


def now() -> float:
    return time.monotonic()


@dataclass
class Member:
    node_id: int
    incarnation: int = 0
    status: Status = Status.ALIVE

    def supersedes(self, other: "Member") -> bool:
        if self.incarnation != other.incarnation:
            return self.incarnation > other.incarnation
        return _ORDER[self.status] > _ORDER[other.status]


@dataclass
class Entry:
    value: Any
    version: int = 0


@dataclass
class Message:
    sender: int
    kind: str
    payload: dict = field(default_factory=dict)


class Transport(ABC):
    """Abstract transport — defines how nodes exchange Message objects."""

    @abstractmethod
    def register(self, node_id: int) -> None: ...

    @abstractmethod
    async def send(self, to: int, msg: Message) -> None: ...

    @abstractmethod
    async def recv(self, node_id: int, timeout: float) -> Message | None: ...

    @abstractmethod
    def drain(self, node_id: int) -> list[Message]: ...

    async def setup(self, node_id: int) -> None:  # noqa: ARG002
        """Optional async initialisation called by Node.run() before the loop."""

    def debug_info(self) -> dict:
        """Transport-level counters for the /status endpoint."""
        return {}


class MockTransport(Transport):
    """In-memory async mailbox with optional drop rate and exponential
    delivery latency. Each registered node has an asyncio.Queue."""

    def __init__(self, drop_rate: float = 0.0, latency: float = 0.0):
        self.queues: dict[int, asyncio.Queue[Message]] = {}
        self.drop_rate = drop_rate
        self.latency = latency
        self.dead: set[int] = set()

    def register(self, node_id: int) -> None:
        self.queues.setdefault(node_id, asyncio.Queue())

    async def send(self, to: int, msg: Message) -> None:
        if to in self.dead or msg.sender in self.dead:
            return
        if random.random() < self.drop_rate:
            return
        if self.latency > 0:
            await asyncio.sleep(random.expovariate(1.0 / self.latency))
            if to in self.dead:
                return
        await self.queues[to].put(msg)

    async def recv(self, node_id: int, timeout: float) -> Message | None:
        try:
            return await asyncio.wait_for(self.queues[node_id].get(), timeout)
        except asyncio.TimeoutError:
            return None

    def drain(self, node_id: int) -> list[Message]:
        q = self.queues[node_id]
        out: list[Message] = []
        while not q.empty():
            out.append(q.get_nowait())
        return out

    def kill(self, node_id: int) -> None:
        self.dead.add(node_id)


class _UDPProtocol(asyncio.DatagramProtocol):
    def __init__(
        self,
        queue: asyncio.Queue[Message],
        on_recv: "Callable[[Message | None, int, Exception | None], None]",
    ):
        self._queue = queue
        self._on_recv = on_recv

    def datagram_received(self, data: bytes, addr: tuple) -> None:  # noqa: ARG002
        try:
            msg = pickle.loads(data)
        except Exception as exc:
            self._on_recv(None, len(data), exc)
            return
        try:
            self._queue.put_nowait(msg)
        except Exception:
            pass
        self._on_recv(msg, len(data), None)

    def error_received(self, exc: Exception) -> None:  # noqa: ARG002
        pass

    def connection_lost(self, exc: Exception | None) -> None:  # noqa: ARG002
        pass


class UDPTransport(Transport):
    """Real UDP transport. Provide a mapping from node_id → (host, port).
    Node.run() calls setup() which resolves all hostnames and binds the
    listening socket. Hostnames are resolved once so send() never does
    blocking DNS — asyncio.DatagramTransport.sendto() with an unresolved
    hostname can call _fatal_error() and permanently close the transport."""

    def __init__(
        self,
        node_map: dict[int, tuple[str, int]],
        comm_log: CommLog | None = None,
    ):
        self._node_map = node_map
        self._queues: dict[int, asyncio.Queue[Message]] = {}
        self._sockets: dict[int, asyncio.DatagramTransport] = {}
        self._resolved: dict[int, tuple[str, int]] = {}
        self._sends = 0
        self._recvs = 0
        self._send_errors = 0
        self._comm = comm_log or CommLog(capacity=0)

    def register(self, node_id: int) -> None:
        self._queues.setdefault(node_id, asyncio.Queue())

    async def setup(self, node_id: int) -> None:
        loop = asyncio.get_running_loop()
        # Resolve every peer hostname → IP once. Peers may not yet be in DNS
        # (Docker only registers a service in DNS once its container starts),
        # so retry with backoff. Self has to resolve to bind, so that's the
        # only one we treat as fatal.
        for nid, (host, port) in self._node_map.items():
            for attempt in range(30):
                try:
                    infos = await loop.getaddrinfo(
                        host, port, family=socket.AF_INET, type=socket.SOCK_DGRAM
                    )
                    ip, port = infos[0][4][:2]
                    self._resolved[nid] = (str(ip), int(port))
                    break
                except OSError as exc:
                    if nid == node_id:
                        raise
                    if attempt == 29:
                        self._comm.event(f"setup: gave up resolving {host}: {exc}")
                    else:
                        await asyncio.sleep(1.0)
        self._comm.event(f"setup node={node_id} resolved={self._resolved}")
        queue = self._queues[node_id]
        transport, _ = await loop.create_datagram_endpoint(
            lambda: _UDPProtocol(queue, self._on_recv),
            local_addr=self._resolved[node_id],
        )
        self._sockets[node_id] = transport  # type: ignore[assignment]
        self._comm.event(f"bound {self._resolved[node_id]}")

    def _on_recv(self, msg: Message | None, size: int, exc: Exception | None) -> None:
        if exc is not None or msg is None:
            self._comm.event(f"recv malformed {size}B: {exc}")
            return
        self._recvs += 1
        self._comm.event(f"recv ← {msg.sender} {msg.kind} {size}B")

    async def send(self, to: int, msg: Message) -> None:
        data = pickle.dumps(msg)
        dest = self._resolved.get(to) or self._node_map[to]
        owned = self._sockets.get(msg.sender)
        try:
            if owned is not None:
                owned.sendto(data, dest)
            else:
                loop = asyncio.get_running_loop()
                sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
                sock.setblocking(False)
                try:
                    await loop.sock_sendto(sock, data, dest)
                finally:
                    sock.close()
            self._sends += 1
            self._comm.event(f"send → {to} {msg.kind} {len(data)}B → {dest}")
        except Exception as exc:
            self._send_errors += 1
            self._comm.event(f"send fail → {to} {msg.kind}: {exc}")

    def debug_info(self) -> dict:
        return {"sends": self._sends, "recvs": self._recvs, "send_errors": self._send_errors}

    async def recv(self, node_id: int, timeout: float) -> Message | None:
        try:
            return await asyncio.wait_for(self._queues[node_id].get(), timeout)
        except asyncio.TimeoutError:
            return None

    def drain(self, node_id: int) -> list[Message]:
        q = self._queues[node_id]
        out: list[Message] = []
        while not q.empty():
            out.append(q.get_nowait())
        return out


class Node:
    def __init__(
        self,
        node_id: int,
        seeds: list[int],
        transport: Transport,
        *,
        fanout: int = 3,
        gossip_interval: float = 0.5,
        probe_interval: float = 0.5,
        ping_timeout: float = 1.0,
        suspicion_timeout: float = 3.0,
        poll_cap: float = 0.1,
        k_indirect: int = 2,
        claim_interval: float = 0.5,
        claim_grace: float = 2.0,
        comm_log: CommLog | None = None,
    ):
        self.node_id = node_id
        self.transport = transport
        self.fanout = fanout
        self.gossip_interval = gossip_interval
        self.probe_interval = probe_interval
        self.ping_timeout = ping_timeout
        self.suspicion_timeout = suspicion_timeout
        self.poll_cap = poll_cap
        self.k_indirect = k_indirect
        self.claim_interval = claim_interval
        self.claim_grace = claim_grace
        self.comm_log = comm_log or CommLog(capacity=0)

        self.members: dict[int, Member] = {}
        self.state: dict[str, Entry] = {}
        self.pending_pings: dict[int, float] = {}
        self.suspect_since: dict[int, float] = {}
        self.incarnation: int = 0
        self.loop_last_ran: dict[str, float] = {}

        transport.register(node_id)
        # Self starts JOINING — we transition to ALIVE only once we've claimed
        # our `n` slot (default = node_id; reshuffled if already taken).
        self.members[node_id] = Member(node_id, 0, Status.JOINING)
        # Seeds start UNKNOWN — promoted only when we receive a message from
        # them. Avoids the "everything looks fine" initial view.
        for pid in seeds:
            if pid != node_id:
                self.members.setdefault(pid, Member(pid, 0, Status.UNKNOWN))
        self._claim_started = now()

    def put(self, key: str, value: Any) -> None:
        prev = self.state.get(key)
        self.state[key] = Entry(value, (prev.version if prev else 0) + 1)

    def get(self, key: str) -> Any:
        e = self.state.get(key)
        return e.value if e else None

    def live_peers(self) -> list[int]:
        return [
            m.node_id for m in self.members.values()
            if m.node_id != self.node_id and m.status != Status.DEAD
        ]

    async def run(self, stop: asyncio.Event) -> None:
        await self.transport.setup(self.node_id)
        await asyncio.gather(
            self._recv_loop(stop),
            self._probe_loop(stop),
            self._gossip_loop(stop),
            self._expire_loop(stop),
            self._claim_loop(stop),
        )

    async def _recv_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            try:
                msg = await self.transport.recv(self.node_id, self.poll_cap)
                self.loop_last_ran["recv"] = now()
                if msg is not None:
                    await self._handle(msg)
                    for extra in self.transport.drain(self.node_id):
                        await self._handle(extra)
            except asyncio.CancelledError:
                return
            except Exception as exc:
                self.comm_log.event(f"recv_loop ERROR: {exc!r}")

    async def _probe_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(self.probe_interval)
            if not stop.is_set():
                try:
                    await self._probe()
                    self.loop_last_ran["probe"] = now()
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    self.comm_log.event(f"probe_loop ERROR: {exc!r}")

    async def _gossip_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(self.gossip_interval)
            if not stop.is_set():
                try:
                    await self._gossip()
                    self.loop_last_ran["gossip"] = now()
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    self.comm_log.event(f"gossip_loop ERROR: {exc!r}")

    async def _expire_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(self.poll_cap)
            if not stop.is_set():
                try:
                    await self._expire()
                    self.loop_last_ran["expire"] = now()
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    self.comm_log.event(f"expire_loop ERROR: {exc!r}")

    async def _claim_loop(self, stop: asyncio.Event) -> None:
        while not stop.is_set():
            await asyncio.sleep(self.claim_interval)
            if not stop.is_set():
                try:
                    self._try_claim()
                    self.loop_last_ran["claim"] = now()
                except asyncio.CancelledError:
                    return
                except Exception as exc:
                    self.comm_log.event(f"claim_loop ERROR: {exc!r}")

    def _try_claim(self) -> None:
        # Wait for at least one peer to be heard from before claiming, unless
        # the grace window has elapsed (so a single-node cluster still claims).
        heard_peer = any(
            m.status != Status.UNKNOWN
            for nid, m in self.members.items()
            if nid != self.node_id
        )
        if not heard_peer and now() - self._claim_started < self.claim_grace:
            return

        # Read all `n:*` claims out of the gossiped state.
        claims: dict[int, int] = {}
        for k, e in self.state.items():
            if k.startswith("n:"):
                try:
                    claims[int(k[2:])] = int(e.value)
                except (ValueError, TypeError):
                    continue

        my_n = claims.get(self.node_id)
        others = {n: nid for nid, n in claims.items() if nid != self.node_id}

        # Conflict: another node claims the same n. Lower node_id wins.
        conflict = my_n in others and others[my_n] < self.node_id

        if my_n is not None and not conflict:
            self._become_alive()
            return

        # Pick own node_id by default; if taken, walk up to the lowest free n.
        candidate = self.node_id
        while candidate in others:
            candidate += 1
        self.put(f"n:{self.node_id}", candidate)
        if conflict and my_n is not None:
            self.comm_log.event(
                f"claim n={candidate} (was {my_n}, lost to node{others[my_n]})"
            )
        else:
            self.comm_log.event(f"claim n={candidate}")
        self._become_alive()

    def _become_alive(self) -> None:
        # Direct transition because _apply() for self only handles refutation.
        me = self.members[self.node_id]
        if me.status == Status.ALIVE:
            return
        self.incarnation = me.incarnation + 1
        self.members[self.node_id] = Member(self.node_id, self.incarnation, Status.ALIVE)
        self.comm_log.event(
            f"member {self.node_id}: {me.status.name} → ALIVE (inc {self.incarnation})"
        )

    async def _handle(self, msg: Message):
        if msg.kind == "ping":
            self._merge(msg.payload)
            await self._send(msg.sender, "ack", relay=msg.payload.get("relay"))
            return
        if msg.kind == "ack":
            self.pending_pings.pop(msg.sender, None)
            self._merge(msg.payload)
            existing = self.members.get(msg.sender)
            if existing and existing.status != Status.ALIVE:
                self._apply(Member(msg.sender, existing.incarnation + 1, Status.ALIVE))
            relay = msg.payload.get("relay")
            if relay is not None and relay != self.node_id:
                # Indirect probe: forward liveness evidence back to the requester.
                await self._send(relay, "ack")
            return
        if msg.kind == "ping_req":
            target = msg.payload["target"]
            await self._send(target, "ping", relay=msg.sender)
            return
        if msg.kind == "sync":
            self._merge(msg.payload)

    async def _probe(self):
        peers = self.live_peers()
        if not peers:
            return
        target = random.choice(peers)
        if target in self.pending_pings:
            self.comm_log.event(f"probe skip {target} (already pending)")
            return
        self.pending_pings[target] = now()
        self.comm_log.event(f"probe → {target}")
        await self._send(target, "ping")

    async def _gossip(self):
        peers = self.live_peers()
        if not peers:
            return
        for peer in random.sample(peers, min(self.fanout, len(peers))):
            await self._send(peer, "sync")

    async def _expire(self):
        t = now()
        for target, sent in list(self.pending_pings.items()):
            if t - sent < self.ping_timeout:
                continue
            self.pending_pings.pop(target, None)
            self.comm_log.event(f"ping_timeout {target} after {t - sent:.2f}s")
            helpers = [p for p in self.live_peers() if p != target]
            chosen = random.sample(helpers, min(self.k_indirect, len(helpers)))
            if chosen:
                self.comm_log.event(f"indirect_probe via {chosen} for {target}")
            for helper in chosen:
                await self.transport.send(
                    helper, Message(self.node_id, "ping_req", {"target": target})
                )
            m = self.members.get(target)
            if m and m.status == Status.ALIVE:
                self._apply(Member(target, m.incarnation, Status.SUSPECT))
                self.suspect_since[target] = t
        for nid, since in list(self.suspect_since.items()):
            if t - since < self.suspicion_timeout:
                continue
            m = self.members.get(nid)
            if m and m.status == Status.SUSPECT:
                self._apply(Member(nid, m.incarnation, Status.DEAD))
            self.suspect_since.pop(nid, None)

    async def _send(self, to: int, kind: str, relay: int | None = None):
        payload = {
            "members": [
                (m.node_id, m.incarnation, m.status.value)
                for m in self.members.values()
            ],
            "state": {k: (e.value, e.version) for k, e in self.state.items()},
        }
        if relay is not None:
            payload["relay"] = relay
        await self.transport.send(to, Message(self.node_id, kind, payload))

    def _merge(self, payload: dict):
        for nid, inc, sv in payload.get("members", []):
            self._apply(Member(nid, inc, Status(sv)))
        for k, (v, ver) in payload.get("state", {}).items():
            existing = self.state.get(k)
            if not existing or ver > existing.version:
                self.state[k] = Entry(v, ver)

    def _apply(self, update: Member):
        # Rebut SUSPECT/DEAD rumours about ourselves by bumping incarnation.
        # Ignore JOINING/UNKNOWN/ALIVE echoes — those don't threaten our view.
        if update.node_id == self.node_id:
            if (
                update.status in (Status.SUSPECT, Status.DEAD)
                and update.incarnation >= self.incarnation
            ):
                self.incarnation = update.incarnation + 1
                self.members[self.node_id] = Member(
                    self.node_id, self.incarnation, Status.ALIVE
                )
            return
        existing = self.members.get(update.node_id)
        if not existing or update.supersedes(existing):
            old = existing.status.name if existing else "NEW"
            self.comm_log.event(
                f"member {update.node_id}: {old} → {update.status.name} "
                f"(inc {update.incarnation})"
            )
            self.members[update.node_id] = update
            if update.status == Status.ALIVE:
                self.suspect_since.pop(update.node_id, None)


async def simulate(n_nodes: int = 6, duration: float = 12.0,
                   drop_rate: float = 0.05, latency: float = 0.02,
                   kill_at: dict[float, int] | None = None, seed: int = 42):
    random.seed(seed)
    transport = MockTransport(drop_rate=drop_rate, latency=latency)
    all_ids = list(range(n_nodes))
    nodes = [Node(0, all_ids, transport)]
    nodes += [Node(i, [0], transport) for i in range(1, n_nodes)]

    stop = asyncio.Event()
    tasks = [asyncio.create_task(n.run(stop)) for n in nodes]
    t0 = now()

    async def scenario():
        await asyncio.sleep(0.5)
        nodes[0].put("leader", "node-0")
        nodes[1].put("build", "v1.2.3")
        for at, victim in sorted((kill_at or {}).items()):
            delay = (t0 + at) - now()
            if delay > 0:
                await asyncio.sleep(delay)
            transport.kill(victim)
            print(f"t={now() - t0:5.2f}s: killed node {victim}")
        remaining = (t0 + duration) - now()
        if remaining > 0:
            await asyncio.sleep(remaining)
        stop.set()

    await scenario()
    await asyncio.gather(*tasks, return_exceptions=True)

    print("\n=== final view per node ===")
    for n in nodes:
        if n.node_id in transport.dead:
            print(f"node {n.node_id}: <killed>")
            continue
        members = {
            m.node_id: m.status.name
            for m in sorted(n.members.values(), key=lambda x: x.node_id)
        }
        state = {k: e.value for k, e in n.state.items()}
        print(f"node {n.node_id}: members={members} state={state}")


if __name__ == "__main__":
    asyncio.run(simulate(
        n_nodes=6, duration=12.0, drop_rate=0.05, latency=0.02,
        kill_at={3.0: 2},
    ))
