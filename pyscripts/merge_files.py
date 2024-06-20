import gzip
import json
import multiprocessing as mp
from functools import partial
from itertools import groupby
from pathlib import Path

import brotli

from .common import oa_root

merged_builds = Path(oa_root, "merged-cache", "qc-builds")

merge_levels = ["r", "q", "f"]
# merge_levels = ["r", "f", "q"]

skey = "size"
pkey = "fp"
max_size = 250_000

last_idx = len(merge_levels) - 1

cmodule = brotli
cmodule = gzip

suffix, compressor = cmodule.__name__, cmodule.compress


def merge(l, level=0):
    subtree_size = sum(e[skey] for e in l)
    if (max_size < subtree_size) and (level <= last_idx):
        fun = partial(merge, level=level + 1)
        targets = []
        for _, gg in groupby(l, lambda e: e[merge_levels[level]]):
            targets.append(list(gg))
        mapper = para_map if (level == 0) else sync_map
        mapper(fun, targets)
    else:
        merged_dic = recs_to_dic(l, level)
        keys = tuple(l[0][k] for k in merge_levels[:level])
        for rec in l[1:]:
            assert keys == tuple(rec[k] for k in merge_levels[:level])
        opath = Path(merged_builds, *keys).with_suffix(f".json.{suffix}")
        opath.parent.mkdir(exist_ok=True, parents=True)
        opath.write_bytes(compressor(json.dumps(merged_dic).encode()))


def recs_to_dic(recs, level):
    if level == (last_idx + 1):
        assert len(recs) == 1, f"{level}, {recs}"
        return json.loads(gzip.decompress(recs[0][pkey].read_bytes()))
    else:
        return {
            gid: recs_to_dic(list(gg), level + 1)
            for gid, gg in groupby(recs, lambda e: e[merge_levels[level]])
        }


def sync_map(fun, iterable):
    for e in iterable:
        fun(e)


pool = mp.Pool(processes=mp.cpu_count() * 2)


def para_map(fun, iterable):
    pool.map(fun, iterable)
