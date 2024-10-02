import json
import re
from string import ascii_lowercase

import pandas as pd
from unidecode import unidecode

from .common import (
    DN,
    get_filtered_main_df,
    load_map,
    oa_persistent,
    oa_root,
    read_p_gz,
)
from .rust_gen import ComC, EntC, StowC


def to_name_dic(df, k, entity_type):
    id_map = load_map(entity_type)
    return (
        df.assign(i=lambda df: [str(id_map[i]) for i in df.index])
        .set_index("i")[k]
        .to_dict()
    )


def get_author_semantic_ids():
    df = get_filtered_main_df(EntC.AUTHORS)

    return (
        df.assign(
            name=lambda df: df[DN]
            .str.replace(".", "")
            .str.replace("'", "")
            .str.strip()
            .str.replace(" ", "-")
            .str.lower()
            .apply(unidecode),
        )
        .sort_values(["name"], ascending=False)  # TODO: better sorting (e.g. citations)
        .assign(
            d=lambda df: df["name"].shift(),
            dename=lambda df: df["name"] == df["d"],
            suff=lambda df: df.groupby("name")["dename"].cumsum(),
            rname=lambda df: df["name"].where(
                df["suff"] == 0, df["name"] + "-" + (df["suff"] + 1).astype(str)
            ),
        )
        .sort_values("suff")["rname"]
        .to_dict()
    )


def get_country_semantic_ids():
    astats = read_p_gz(oa_root / StowC.cache / ComC.A_STAT_PATH)
    return {
        k: "-".join(ed["name"].lower().replace(".", " ").split())
        for k, ed in astats[ComC.COUNTRIES].items()
    }


def get_inst_semantic_ids():
    df = get_filtered_main_df(EntC.INSTITUTIONS)

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
    return shortname_df.drop_duplicates(subset=["id"]).set_index("id").loc[
        df["id"].values, :
    ].pipe(to_name_dic, "alt_name", EntC.INSTITUTIONS) | {"0": "unknown"}
