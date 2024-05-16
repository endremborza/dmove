import json
import re
from pathlib import Path
from string import ascii_lowercase

import numpy as np
import pandas as pd
from dotenv import load_dotenv
from pandas.compat import os
from unidecode import unidecode

load_dotenv()

oa_root = Path(os.environ["OA_ROOT"])

inst_root = oa_root / "entity-csvs/institutions"


def get_filter(sub):
    return np.frombuffer(
        Path(oa_root, "filter-steps", sub).read_bytes(),
        dtype=np.dtype(np.uint64).newbyteorder(">"),
    )


def load_map(kind):
    blob = Path(oa_root, "key-stores", kind).read_bytes()

    imap = np.frombuffer(blob, dtype=np.dtype(np.uint64).newbyteorder(">")).reshape(
        -1, 2
    )
    return pd.DataFrame(imap).set_index(0).loc[:, 1].to_dict()


def parse_id(col):
    return col.str[22:].astype(np.uint64)


insts = get_filter("13/institutions")

if __name__ == "__main__":
    df = (
        pd.read_csv(inst_root / "main.csv.gz")
        .assign(id=lambda df: df["id"].pipe(parse_id))
        .loc[lambda df: df["id"].isin(insts), :]
    )

    dn_cols = ["display_name_acronyms", "display_name_alternatives", "u_of", "x_u"]

    shortname_df = (
        df.set_index("id")
        .assign(
            x_u=lambda df: df["display_name"]
            .apply(re.compile("^([A-Z|a-z]+) University$").findall)
            .apply(json.dumps),
            u_of=lambda df: df["display_name"]
            .apply(re.compile("^University of ([A-Z|a-z]+)$").findall)
            .apply(json.dumps),
        )
        .pipe(
            lambda df: pd.concat(
                [df["display_name"]]
                + [df[k].map(json.loads).explode().dropna() for k in dn_cols]
            )
        )
        .rename("alt_name")
        .reset_index()
        .merge(df[["id", "display_name", "country_code", "cited_by_count"]])
        .assign(is_different=lambda df: df["display_name"] != df["alt_name"])
        .loc[
            lambda df: (df["alt_name"].str.len() > 3)
            | (df["display_name"].str.len() <= 3)
        ]
        .loc[lambda df: df["alt_name"].str.len() <= df["display_name"].str.len()]
        .assign(
            neg_cc=lambda df: -df["cited_by_count"],
            alt_name=lambda df: df["alt_name"].apply(
                lambda s: "-".join(
                    "".join(
                        [
                            (l if l in (ascii_lowercase + " ") else " ")
                            for l in unidecode(s).lower()
                        ]
                    ).split()
                )
            ),
        )
        .pipe(
            lambda df: pd.concat(
                [
                    df,
                    df.loc[df["alt_name"].duplicated(keep=False)].assign(
                        alt_name=lambda _df: _df["alt_name"]
                        + "-"
                        + df["country_code"].str.lower()
                    ),
                ]
            )
        )
        .pipe(
            lambda df: pd.concat(
                [
                    df,
                    df.loc[df["alt_name"].duplicated(keep=False)]
                    .groupby("alt_name", as_index=False)
                    .apply(
                        lambda gdf: gdf.sort_values(["is_different", "neg_cc"]).assign(
                            ind=range(1, gdf.shape[0] + 1),
                            alt_name=lambda df: df["alt_name"]
                            + "-"
                            + df["ind"].astype(str),
                        )
                    )
                    .reset_index(drop=True),
                ]
            )
        )
        .loc[lambda df: df["alt_name"].str.len() < 90]
        .sort_values(["is_different", "neg_cc"])
        .drop_duplicates(subset=["alt_name"], keep="first")
    )

    (
        shortname_df.drop_duplicates(subset=["id"])
        .set_index("id")[["alt_name"]]
        .to_csv(inst_root / "semantic-ids.csv.gz")
    )
