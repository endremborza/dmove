import json

import pandas as pd
import polars as pl
from tqdm import tqdm

from pyscripts.rust_gen import ComC, EntC

from .common import get_last_filter, get_root, iter_dfs, parse_id


def get_best_q_by_year():
    return pl.read_csv("s3://tmp-borza-public-cyx/metascience/q-by-year.csv.gz")


puby = "publication_year"

if __name__ == "__main__":

    work_filter = get_last_filter(EntC.WORKS)
    source_filter = get_last_filter(EntC.SOURCES)
    adf = pd.read_csv(
        "s3://tmp-borza-public-cyx/metascience/areas.csv.gz"
    ).drop_duplicates()

    sodf = (
        pd.read_csv(get_root(EntC.SOURCES) / "ids.csv.gz")
        .assign(id=lambda df: df["openalex"].pipe(parse_id))
        .loc[lambda df: df["id"].isin(source_filter), :]
        .set_index("id")
    )

    _isc = "issn"
    _issns = pd.concat(
        [
            sodf[_isc].dropna().apply(json.loads).explode().reset_index(),
            sodf["issn_l"].dropna().rename(_isc).reset_index(),
        ]
    ).drop_duplicates()

    _issns.merge(adf).drop(_isc, axis=1).drop_duplicates().assign(
        id=lambda df: ComC.ID_PREFIX + "S" + df["id"].astype(str)
    ).to_csv(get_root(EntC.SOURCES) / f"{ComC.AREA_FIELDS}.csv.gz", index=False)
    q_matched_df = (
        get_best_q_by_year()
        .select(
            [
                pl.col(_isc),
                pl.col("year").cast(pl.UInt16).alias(puby),
                pl.col("best_q").str.slice(1, None).cast(pl.UInt8),
            ]
        )
        .join(pl.from_pandas(_issns).select(["id", pl.col(_isc)]), on=_isc)
        .drop(_isc)
        .unique()
    )

    w_dfs = []
    for wdf in tqdm(iter_dfs(EntC.WORKS, cols=["id", puby])):
        w_dfs.append(
            pl.from_pandas(
                wdf.dropna()
                .assign(id=lambda df: df["id"].pipe(parse_id))
                .loc[lambda df: df["id"].isin(work_filter)],
                schema_overrides={puby: pl.UInt16},
            )
        )

    full_ywdf = pl.concat(w_dfs).sort("id")

    lodfs = []

    wlp = get_root(EntC.WORKS) / "locations.csv.gz"
    for lodfr in tqdm(
        pd.read_csv(wlp, chunksize=500_000, usecols=["parent_id", "source"])
    ):

        lodfs.append(
            pl.from_pandas(lodfr.dropna().apply(parse_id))
            .rename({"parent_id": "id"})
            .join(full_ywdf, on="id")
            .unique()
            .join(
                q_matched_df.rename({"id": "source"}), how="left", on=["source", puby]
            )
            .fill_null(5)
        )

    (
        pl.concat(lodfs)
        .drop(puby)
        .with_columns(pl.col("best_q").replace(0, 5))
        .sort("best_q")
        .unique("id", keep="first")
        .to_pandas()
        .to_csv(get_root(EntC.WORKS) / f"{ComC.QS}.csv.gz", index=False)
    )
