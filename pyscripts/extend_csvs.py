import json

import pandas as pd
import polars as pl
from ccl_science_data.common import (
    PUBY,
    ComC,
    EntC,
    get_csv_path,
    get_last_filter,
    parse_id,
)

# link_frame = "https://tmp-borza-public-cyx.s3.amazonaws.com/{}.csv.gz"
link_frame = "s3://tmp-borza-public-cyx/{}.csv.gz"


def get_best_q_by_year():
    return pl.read_csv(link_frame.format("metascience/q-by-year"))


if __name__ == "__main__":

    source_filter = get_last_filter(EntC.SOURCES)
    adf = pd.read_csv(link_frame.format("metascience/areas")).drop_duplicates()
    sodf = (
        pd.read_csv(get_csv_path(EntC.SOURCES, "ids"))
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
    ).to_csv(get_csv_path(EntC.SOURCES, EntC.AREA_FIELDS), index=False)
    q_matched_df = (
        get_best_q_by_year()
        .select(
            [
                pl.col(_isc),
                pl.col("year").cast(pl.UInt16).alias(PUBY),
                pl.col("best_q").str.slice(1, None).cast(pl.UInt8),
            ]
        )
        .join(pl.from_pandas(_issns).select(["id", pl.col(_isc)]), on=_isc)
        .drop(_isc)
        .unique()
    )
    q_matched_df.to_pandas().to_csv(get_csv_path(EntC.SOURCES, EntC.QS), index=False)
