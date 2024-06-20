import gzip
import json
from pathlib import Path

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pandas.compat import os

from .rust_gen import ComC, StowC

load_dotenv()


MAIN_NAME = "main.csv.gz"
COMPLETE_FILTER = "all"

oa_root = Path(os.environ["OA_ROOT"])
inst_root = oa_root / StowC.entity_csvs / ComC.INSTS


class Keys:
    ROOT = "root_entity_type"
    CITE = "citations"
    PAPER = "papers"
    SEM = "semantic_id"
    META = "meta"
    OA_ID_META = "oa_id"


def get_root(entity):
    return oa_root / StowC.entity_csvs / entity


def get_filter(sub):
    return np.frombuffer(
        Path(oa_root, StowC.filter_steps, sub).read_bytes(),
        dtype=np.dtype(np.uint64).newbyteorder(">"),
    )


def load_map(kind):
    blob = Path(oa_root, StowC.key_stores, kind).read_bytes()

    imap = np.frombuffer(blob, dtype=np.dtype(np.uint64).newbyteorder(">")).reshape(
        -1, 2
    )
    return pd.DataFrame(imap).set_index(0).loc[:, 1].to_dict()


def parse_id(col):
    return col.str[22:].astype(np.uint64)


def read_p_gz(path: Path):
    return read_p(path.with_suffix(".json.gz"))


def read_p(path: Path):
    return json.loads(gzip.decompress(path.read_bytes()))


insts = get_filter(f"13/{ComC.INSTS}")
