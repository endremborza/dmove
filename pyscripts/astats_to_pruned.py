import gzip
import json
from functools import reduce
from pathlib import Path

import pandas as pd
from inst_str_id import get_filter, inst_root, insts, oa_root, parse_id
from Levenshtein import ratio


def get_flag_emoji(ccode):
    return (
        bytes([240, 159, 135, 101 + ord(ccode[0])])
        + bytes([240, 159, 135, 101 + ord(ccode[1])])
    ).decode("utf-8")


def to_name_dic(df, k):
    return (
        df.reset_index()
        .assign(i=lambda df: list(map(str, range(1, df.shape[0] + 1))))
        .set_index("i")[k]
        .to_dict()
    )


MIN_D = 0.8

dn = "display_name"

as_fname = "attribute-statics.json.gz"
astat_cache_path = Path(oa_root, "cache", as_fname)
astat_pruned_cache_path = Path(oa_root, "pruned-cache", as_fname)

if __name__ == "__main__":
    sources = get_filter("12/sources")

    sdf = (
        pd.read_csv(oa_root / "entity-csvs" / "sources" / "main.csv.gz")
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
            pd.read_csv(inst_root / "main.csv.gz")
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

    d = json.loads(gzip.decompress(astat_cache_path.read_bytes()))

    for iid, oa_id in enumerate(df["id"]):
        d["institutions"][str(iid + 1)]["meta"][
            "oa_id"
        ] = f"https://openalex.org/I{oa_id}"

    def mod_astats(name_dic, k):
        for ik, idic in d[k].items():
            new_name = name_dic.get(ik, "")
            if new_name != idic["name"]:
                print(idic["name"], "==>", new_name, "\n")
                idic["name"] = new_name

    mod_astats(to_name_dic(df, "citied_name"), "institutions")
    mod_astats(to_name_dic(named_sdf, "clean_name"), "sources")

    astat_pruned_cache_path.write_bytes(gzip.compress(json.dumps(d).encode()))
