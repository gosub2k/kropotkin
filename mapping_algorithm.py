import math
import random
from bisect import bisect_left
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Callable, Dict, List, Optional, Tuple

import numpy as np

MAX_SHARD_TO_PRINT = 16


def vdc(i: int, base=2) -> float:
    right = 1.0 / float(base)
    v = 0.0

    while i:
        md = i % base
        if md != 0:
            v += right * float(md)
        i //= base
        right /= float(base)

    return v


@dataclass
class mapping:

    R: int = 1 << 63
    shards: int = 32
    T: int = 3
    B: int = 2
    S: int = 100069
    seq: Callable[[int, int], float] = vdc
    n: List[int] = field(default_factory=list)

    def remove_random_server(self):
        ln = len(self.n)
        x = random.randint(0, ln - 1)
        self.n = self.n[0:x] + self.n[x + 1 :]

    def add_server_at_end(self):
        self.n = self.n + [max(self.n) + 1] if self.n else [0]

    def add_server_in_hole(self) -> bool:
        self.n = sorted(self.n)
        for i in range(len(self.n)):
            if not self.n.count(i) > 0:
                self.n.append(i)
                return True
        return False

    def _lens(self) -> List[int]:
        dct = self._server_to_shard_dict()
        shardlists = dct.values()
        lens = map(len, shardlists)
        lens = list(lens)
        return lens

    def skew(self) -> Tuple[int, int, int]:
        lens = self._lens()
        mn, mx = min(lens), max(lens)
        return mx - mn, mn, mx

    def stddev(self) -> float:
        lens = self._lens()
        return float(np.std(lens))

    def stderr(self) -> float:
        lens = self._lens()
        mu = float(np.mean(lens))
        return float(np.std(lens)) / mu if mu else 0.0

    def toks(self) -> Dict[int, List[int]]:
        rt = defaultdict(list)
        mp = self._token_to_server_list()
        for tok, srv in mp:
            rt[srv].append(tok)
        return {k: rt[k] for k in sorted(list(rt.keys()))}

    def ring(self) -> List[int]:
        rng = map(lambda p: p[1], self._token_to_server_list())
        return list(rng)

    def _token_to_server_list(self) -> List[Tuple[int, int]]:
        mapping: List[Tuple[int, int]] = list()
        for srvr in self.n:
            for t in range(self.T):
                pos = int(self.seq(srvr + self.S * t, self.B) * self.R)
                mapping.append((pos, srvr))
        return sorted(mapping)

    def _shard_to_server_list(self) -> List[Tuple[int, int]]:
        shard_len = self.R // self.shards
        ownership: List[Tuple[int, int]] = list()
        mapping = sorted(self._token_to_server_list())
        for i in range(self.shards):
            startpos = i * shard_len
            ind = bisect_left(mapping, (startpos, -1))
            if ind == len(mapping):
                ind = 0
            ownership.append((i, mapping[ind][1]))
        return ownership

    def _server_to_shard_dict(self) -> Dict[int, List[int]]:
        mapping = defaultdict(list)
        for shrd_no, srvr_no in self._shard_to_server_list():
            mapping[srvr_no].append(shrd_no)
        return mapping

    def __str__(self):
        mystr = ""
        mystr += f"ring size: {self.R}\n"
        mystr += f"stride: {self.S}\n"
        mystr += (
            f"{len(self.n)} servers, {self.T} tokens/server, {self.shards} shards\n"
        )
        mystr += f"shard size: {self.R//self.shards}\n"
        for srvr, shards in sorted(self._server_to_shard_dict().items()):
            mystr += f"{srvr}: {shards[0:MAX_SHARD_TO_PRINT]}"
            if len(shards) > MAX_SHARD_TO_PRINT:
                mystr += " ... "
            mystr += "\n"
        skw, mn, mx = self.skew()
        mystr += f"skew: {skw} min shards/srvr: {mn} max shards/srvr: {mx}\n"
        mystr += f"stddev: {self.stddev():0.3f} stderr (sigma/mu): {self.stderr():0.4f}\n"
        return mystr


if __name__ == "__main__":
    m = mapping()
    for i in range(10):
        m.add_server_at_end()
    print(m)
    print(m.ring())
    m.remove_random_server()
    print(m.ring())
    m.remove_random_server()
    print(m.ring())
    m.remove_random_server()
    print(m.ring())
    m.add_server_at_end()
    print(m.ring())
