# kropotkin — deterministic consistent hashing with gossip failure detection

![Ring token distribution](ring_screenshot.png)

## Mapping algorithm

A consistent-hashing token placement scheme using the [van der Corput (VdC) sequence](https://en.wikipedia.org/wiki/Van_der_Corput_sequence) instead of random positions.

The naive approach — assigning each server a consecutive block of VdC values — is pathological: because consecutive VdC blocks are exact ring-translates of one another, all of a failed server's positions share the same clockwise successor, collapsing failover load onto a single node regardless of token count.

The fix: assign tokens by residue class mod `S` (where `gcd(S, base) = 1`). Server `N` owns positions `{ vdc(N + tS) : t = 0, …, T−1 }`. This preserves the low-discrepancy property per server while breaking the translation coupling. See [vdc-consistent-hashing.md](vdc-consistent-hashing.md) for the full derivation.

Inspired by <!-- TODO: Rocksteady/VDC paper ref --> applied to consistent hash rings. The only state each node needs to communicate is its residue class mod `S`.

**Design parameters**

| param | role | constraint |
|-------|------|------------|
| `p` | VdC base | `gcd(p, S) = 1`; use 2 for cheap bit-reversal |
| `S` | stride / residue class count | `S > N_max`, `gcd(S, p) = 1` |
| `T` | tokens per server | failover imbalance ∝ `(log T)/T` |
| `N` | server identity | any free slot in `{0, …, S−1}`, claimed at join |

## Gossip protocol

Implements the SWIM-based failure detector from [Gupta et al. (2001)](#refs): direct probes with indirect probe-requests (`ping_req`) when a direct ping times out, two-phase ALIVE → SUSPECT → DEAD state machine.

## Docker compose

```bash
docker compose up --build
```

Five nodes start on ports 9000–9004. Tune failure detection via env vars in `docker-compose.yml`:

| var | default | effect |
|-----|---------|--------|
| `PING_TIMEOUT` | `1.0` | seconds before a probe is considered failed |
| `SUSPICION_TIMEOUT` | `2.0` | seconds in SUSPECT before marking DEAD |

Kill a container to watch failure detection propagate:

```bash
docker compose stop node2
```

## Refs

- <a name="refs"></a>[Gupta, Chandra, Lancia — "On Scalable and Efficient Distributed Failure Detectors" (PODC 2001)](https://dl.acm.org/doi/10.1145/383962.384020)
- [DeCandia et al. — "Dynamo: Amazon's Highly Available Key-value Store" (SOSP 2007)](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf)
- [Karger et al. — "Consistent Hashing and Random Trees" (STOC 1997)](https://dl.acm.org/doi/10.1145/258533.258660)
