import gzip
import json
from functools import reduce

import pandas as pd
from Levenshtein import ratio
from tqdm import tqdm

from .common import (
    COMPLETE_FILTER,
    MAIN_NAME,
    Keys,
    get_filter,
    inst_root,
    insts,
    oa_root,
    parse_id,
    read_p_gz,
)
from .rust_gen import ComC, StowC
from .semantic_ids import get_country_semantic_ids, get_inst_semantic_ids, to_name_dic


def get_flag_emoji(ccode):
    return (
        bytes([240, 159, 135, 101 + ord(ccode[0])])
        + bytes([240, 159, 135, 101 + ord(ccode[1])])
    ).decode("utf-8")


MIN_D = 0.8

dn = "display_name"


if __name__ == "__main__":
    sources = get_filter("12/sources")

    sdf = (
        pd.read_csv(oa_root / StowC.entity_csvs / ComC.SOURCES / MAIN_NAME)
        .assign(id=lambda df: df["id"].pipe(parse_id))
        .loc[lambda df: df["id"].isin(sources), :]
        .set_index("id")
    )

    deslashed_names = (
        sdf.loc[lambda df: df[dn].str.contains("/"), dn]
        .str.split("/", expand=True)
        .fillna("")
        .assign(
            c=lambda df: df.apply(
                lambda row: (lambda i: row[i] if i < len(row) else "")(
                    reduce(
                        lambda r, l: r if row[0].lower() in row[r].lower() else l,
                        range(1, df.shape[1] + 1),
                    )
                ).title(),
                axis=1,
            ),
            d=lambda df: df.apply(lambda r: ratio(r[0].lower(), r[1].lower()), axis=1),
            alt=lambda df: df.loc[:, 0].str.title().where(df["d"] > MIN_D, df["c"]),
        )
        .loc[lambda df: df["alt"] != "", "alt"]
    )

    named_sdf = sdf.assign(
        dnames=deslashed_names,
        clean_name=lambda df: df["dnames"].where(lambda s: s.notna(), df[dn]),
    )

    df = (
        (
            pd.read_csv(inst_root / MAIN_NAME)
            .assign(id=lambda df: df["id"].pipe(parse_id))
            .loc[lambda df: df["id"].isin(insts), :]
        )
        .merge(
            pd.read_csv(inst_root / "geo.csv.gz", usecols=["parent_id", "city"])
            .assign(id=lambda df: df["parent_id"].pipe(parse_id))
            .drop("parent_id", axis=1),
            how="left",
        )
        .assign(
            flag=lambda df: df["country_code"].fillna("  ").apply(get_flag_emoji),
            country_ext=lambda df: "(" + df["country_code"].fillna("") + ")",
            countried_name=lambda df: df[dn].where(
                ~df[dn].duplicated(keep=False), df[dn] + " " + df["country_ext"]
            ),
            citied_name=lambda df: df["countried_name"].where(
                ~df["countried_name"].duplicated(keep=False),
                df[dn] + " (" + df["city"].fillna("") + ")",
            ),
        )
    )

    assert df.loc[lambda df: df["citied_name"].duplicated(keep=False)].empty
    assert df.loc[lambda df: df["citied_name"].str.contains("()", regex=False)].empty

    print(
        "- "
        + "\n- ".join(
            df.loc[lambda df: df["display_name"] != df["countried_name"]]
            .sort_values("citied_name")["citied_name"]
            .tolist()
        )
    )

    astats = read_p_gz(oa_root / StowC.cache / ComC.A_STAT_PATH)
    specs = read_p_gz(oa_root / StowC.pruned_cache / ComC.QC_CONF)
    r2spec = dict((v[Keys.ROOT], k) for k, v in specs.items())
    semdicts = {
        ComC.INSTS: get_inst_semantic_ids(),
        ComC.COUNTRIES: get_country_semantic_ids(),
    }

    for k, v in r2spec.items():
        for qid in tqdm(astats[k].keys(), desc=k):
            qcp = (
                oa_root
                / StowC.pruned_cache
                / ComC.BUILD_LOC
                / COMPLETE_FILTER
                / v
                / qid
            )
            try:
                qc = read_p_gz(qcp)
                cmeta = {Keys.CITE: qc["weight"], Keys.PAPER: qc["source_count"]}
            except FileNotFoundError:
                cmeta = {}

            ed = astats[k][qid]
            ed[Keys.META] = cmeta | {
                Keys.SEM: semdicts[k][qid],
            }

    for iid, oa_id in enumerate(df["id"]):
        astats[ComC.INSTS][str(iid + 1)][Keys.META][Keys.OA_ID_META] = str(oa_id)

    def mod_astats(name_dic, k):
        for ik, idic in astats[k].items():
            new_name = name_dic.get(ik, "")
            if new_name != idic["name"]:
                print(idic["name"], "==>", new_name, "\n")
                idic["name"] = new_name

    mod_astats(to_name_dic(df, "citied_name"), "institutions")
    mod_astats(to_name_dic(named_sdf, "clean_name"), "sources")

    (oa_root / StowC.pruned_cache / ComC.A_STAT_PATH).with_suffix(
        ".json.gz"
    ).write_bytes(gzip.compress(json.dumps(astats).encode()))
