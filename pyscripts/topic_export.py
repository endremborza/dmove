import csv
import gzip
import json
from pathlib import Path

from tqdm import tqdm

dn = "display_name"
snapshot_root = "openalex-snapshot-2024-05/data"

cols = ["authors", "title", "journal", "year", "doi", "cited_count", "id"]

out_dir = Path("topic-export")
out_dir.mkdir(exist_ok=True)


if __name__ == "__main__":
    tset = set(json.loads(Path("topics.json").read_text()))

    with gzip.open(out_dir / "works.csv.gz", "wt") as gzfp, gzip.open(
        out_dir / "references.csv.gz", "wt"
    ) as ref_gzfp, gzip.open(out_dir / "work-topics.csv.gz", "wt") as wt_gzfp:
        csvw = csv.DictWriter(gzfp, fieldnames=cols)
        csvw.writeheader()

        ref_csvw = csv.DictWriter(
            ref_gzfp, fieldnames=["referenced_work_id", "work_id"]
        )
        ref_csvw.writeheader()

        wt_csvw = csv.DictWriter(wt_gzfp, fieldnames=["topic_id", "work_id"])
        wt_csvw.writeheader()

        for wdp in tqdm(list(Path(snapshot_root, "works").iterdir())):
            if not wdp.is_dir():
                continue
            for wp in wdp.iterdir():
                with gzip.open(wp, "rb") as gz_wfile:
                    wrapper = (
                        (lambda x: tqdm(x, f"file {wp.parts[-2:]}"))
                        if (wp.stat().st_size > 1e7)
                        else lambda x: x
                    )
                    for l in wrapper(gz_wfile):
                        if not l:
                            continue
                        d = json.loads(l)
                        ccount = d.get("cited_by_count", 0)
                        if ccount < 2:
                            continue
                        wid = d.get("id")
                        if wid is None:
                            continue
                        found = False
                        for td in d.get("topics", []):
                            tid = td.get("id")
                            if (tid in tset) and (td.get("score", 0) > 0.7):
                                found = True
                                wt_csvw.writerow({"work_id": wid, "topic_id": tid})
                        if not found:
                            continue
                        csvw.writerow(
                            {
                                "id": wid,
                                "cited_count": ccount,
                                "authors": ";".join(
                                    ad.get("author", {}).get(dn, "")
                                    for ad in d.get("authorships", [])
                                ),
                                "title": d.get(dn, ""),
                                "doi": d.get("doi", ""),
                                "year": d.get("publication_year", ""),
                                "journal": (
                                    (d.get("primary_location") or {}).get("source")
                                    or {}
                                ).get(dn),
                            }
                        )
                        ref_csvw.writerows(
                            [
                                {"work_id": wid, "referenced_work_id": refid}
                                for refid in d.get("referenced_works", [])
                            ]
                        )
