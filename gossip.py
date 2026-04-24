"""SWIM-style gossip: membership with alive/suspect/dead + incarnation
numbers, plus push-pull anti-entropy of opaque key/value state.

The transport is an in-process mailbox so the whole protocol can be run
as a simulation. Replace Transport with a UDP socket to take it live.
"""

import random
from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class Status(Enum):
    ALIVE = 1
    SUSPECT = 2
    DEAD = 3


_ORDER = {Status.ALIVE: 0, Status.SUSPECT: 1, Status.DEAD: 2}


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


class Transport:
    """In-memory mailbox. Drops messages to killed nodes and (stochastically)
    a fraction of all others to model a lossy network."""

    def __init__(self, drop_rate: float = 0.0):
        self.mailboxes: dict[int, list[Message]] = {}
        self.drop_rate = drop_rate
        self.dead: set[int] = set()

    def register(self, node_id: int):
        self.mailboxes.setdefault(node_id, [])

    def send(self, to: int, msg: Message):
        if to in self.dead or msg.sender in self.dead:
            return
        if random.random() < self.drop_rate:
            return
        self.mailboxes.setdefault(to, []).append(msg)

    def recv(self, node_id: int) -> list[Message]:
        msgs = self.mailboxes.get(node_id, [])
        self.mailboxes[node_id] = []
        return msgs

    def kill(self, node_id: int):
        self.dead.add(node_id)
        self.mailboxes[node_id] = []


@dataclass
class Node:
    node_id: int
    transport: Transport
    fanout: int = 3
    ping_timeout: int = 2
    suspicion_timeout: int = 5
    k_indirect: int = 2

    members: dict[int, Member] = field(default_factory=dict)
    state: dict[str, Entry] = field(default_factory=dict)
    pending_pings: dict[int, int] = field(default_factory=dict)
    suspect_since: dict[int, int] = field(default_factory=dict)
    tick: int = 0
    incarnation: int = 0

    def __post_init__(self):
        self.transport.register(self.node_id)
        self.members[self.node_id] = Member(self.node_id, 0, Status.ALIVE)

    def join(self, seeds: list[int]):
        for pid in seeds:
            if pid != self.node_id:
                self.members.setdefault(pid, Member(pid, 0, Status.ALIVE))

    def put(self, key: str, value: Any):
        prev = self.state.get(key)
        self.state[key] = Entry(value, (prev.version if prev else 0) + 1)

    def get(self, key: str) -> Any:
        e = self.state.get(key)
        return e.value if e else None

    def live_peers(self) -> list[int]:
        return [m.node_id for m in self.members.values()
                if m.node_id != self.node_id and m.status != Status.DEAD]

    def step(self):
        self.tick += 1
        for msg in self.transport.recv(self.node_id):
            self._handle(msg)
        self._expire()
        self._probe()
        self._gossip()

    def _handle(self, msg: Message):
        if msg.kind == "ping":
            self._merge(msg.payload)
            self._send(msg.sender, "ack", relay=msg.payload.get("relay"))
            return
        if msg.kind == "ack":
            self.pending_pings.pop(msg.sender, None)
            self._merge(msg.payload)
            existing = self.members.get(msg.sender)
            if existing and existing.status != Status.ALIVE:
                self._apply(Member(msg.sender, existing.incarnation + 1, Status.ALIVE))
            relay = msg.payload.get("relay")
            if relay is not None and relay != self.node_id:
                # indirect probe: forward liveness evidence back to requester
                self._send(relay, "ack")
            return
        if msg.kind == "ping_req":
            target = msg.payload["target"]
            self._send(target, "ping", relay=msg.sender)
            return
        if msg.kind == "sync":
            self._merge(msg.payload)

    def _probe(self):
        peers = self.live_peers()
        if not peers:
            return
        target = random.choice(peers)
        if target in self.pending_pings:
            return
        self.pending_pings[target] = self.tick
        self._send(target, "ping")

    def _gossip(self):
        peers = self.live_peers()
        if not peers:
            return
        for peer in random.sample(peers, min(self.fanout, len(peers))):
            self._send(peer, "sync")

    def _expire(self):
        for target, sent in list(self.pending_pings.items()):
            if self.tick - sent < self.ping_timeout:
                continue
            self.pending_pings.pop(target, None)
            helpers = [p for p in self.live_peers() if p != target]
            for helper in random.sample(helpers, min(self.k_indirect, len(helpers))):
                self.transport.send(helper, Message(
                    self.node_id, "ping_req", {"target": target}))
            m = self.members.get(target)
            if m and m.status == Status.ALIVE:
                self._apply(Member(target, m.incarnation, Status.SUSPECT))
                self.suspect_since[target] = self.tick
        for nid, since in list(self.suspect_since.items()):
            if self.tick - since < self.suspicion_timeout:
                continue
            m = self.members.get(nid)
            if m and m.status == Status.SUSPECT:
                self._apply(Member(nid, m.incarnation, Status.DEAD))
            self.suspect_since.pop(nid, None)

    def _send(self, to: int, kind: str, relay: int | None = None):
        payload = {
            "members": [(m.node_id, m.incarnation, m.status.value)
                        for m in self.members.values()],
            "state": {k: (e.value, e.version) for k, e in self.state.items()},
        }
        if relay is not None:
            payload["relay"] = relay
        self.transport.send(to, Message(self.node_id, kind, payload))

    def _merge(self, payload: dict):
        for nid, inc, sv in payload.get("members", []):
            self._apply(Member(nid, inc, Status(sv)))
        for k, (v, ver) in payload.get("state", {}).items():
            existing = self.state.get(k)
            if not existing or ver > existing.version:
                self.state[k] = Entry(v, ver)

    def _apply(self, update: Member):
        # Rebut any rumor that I'm not alive by bumping my own incarnation.
        if update.node_id == self.node_id:
            if update.status != Status.ALIVE and update.incarnation >= self.incarnation:
                self.incarnation = update.incarnation + 1
                self.members[self.node_id] = Member(
                    self.node_id, self.incarnation, Status.ALIVE)
            return
        existing = self.members.get(update.node_id)
        if not existing or update.supersedes(existing):
            self.members[update.node_id] = update
            if update.status == Status.ALIVE:
                self.suspect_since.pop(update.node_id, None)


def simulate(n_nodes: int = 6, ticks: int = 40, drop_rate: float = 0.05,
             kill_at: dict[int, int] | None = None, seed: int = 42):
    random.seed(seed)
    transport = Transport(drop_rate=drop_rate)
    nodes = [Node(i, transport) for i in range(n_nodes)]
    # Everyone joins through node 0; node 0 is seeded with the full roster.
    nodes[0].join(list(range(n_nodes)))
    for n in nodes[1:]:
        n.join([0])

    kill_at = kill_at or {}
    for t in range(ticks):
        if t in kill_at:
            victim = kill_at[t]
            transport.kill(victim)
            print(f"tick {t}: killed node {victim}")
        if t == 2:
            nodes[0].put("leader", "node-0")
            nodes[1].put("build", "v1.2.3")
        if t == 18:
            nodes[3].put("leader", "node-3")  # leader change post-kill
        for n in nodes:
            if n.node_id not in transport.dead:
                n.step()

    print("\n=== final view per node ===")
    for n in nodes:
        if n.node_id in transport.dead:
            print(f"node {n.node_id}: <killed>")
            continue
        members = {m.node_id: m.status.name for m in sorted(
            n.members.values(), key=lambda x: x.node_id)}
        state = {k: e.value for k, e in n.state.items()}
        print(f"node {n.node_id}: members={members} state={state}")


if __name__ == "__main__":
    simulate(n_nodes=6, ticks=35, drop_rate=0.1, kill_at={14: 2})
