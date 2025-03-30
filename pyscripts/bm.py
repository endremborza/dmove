import datetime as dt
import re
import subprocess
import time
from pathlib import Path

import pandas as pd
import requests
from ccl_science_data.common import oa_root
from ccl_science_data.gen import EntC
from tqdm import tqdm

test_sites = {
    EntC.AUTHORS: ["cesar-a-hidalgo"],
    EntC.SUBFIELDS: ["information-systems"],
    EntC.COUNTRIES: ["hungary"],
    EntC.SOURCES: ["american-economic-review"],
    EntC.INSTITUTIONS: ["budapesti-corvinus-egyetem"],  # "upenn"],
}

do_sites = test_sites

bm_root = "/tmp/dmove-bm"

if __name__ == "__main__":
    rev = subprocess.check_output(["git", "rev-parse", "HEAD"]).decode().strip()[:6]
    dst_cache = Path(f"{bm_root}/bm-caches/{rev}")
    assert not dst_cache.exists()
    subprocess.Popen(["make", "clean-cache"]).wait()
    subprocess.Popen(["cargo", "build", "--release"]).wait()

    p = subprocess.Popen(
        ["target/release/rankless-server", oa_root.as_posix()],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
    )

    be_url = "http://127.0.0.1:3038/v1"

    time.sleep(30)
    while True:
        try:
            specs = requests.get(be_url + "/specs")
        except:
            time.sleep(3)
            assert p.poll() is None
            continue
        if specs.ok:
            break
        time.sleep(3)

    rcounts = {k: len(v) for k, v in specs.json()["specs"].items()}

    totals = {k: c * len(do_sites[k]) for k, c in rcounts.items()}

    for rt, counts in rcounts.items():
        pbar = tqdm(desc=rt, total=totals[rt])
        for sem_id in do_sites[rt]:
            # dm_id = requests.get(be_url + f"/views/{rt}/{el}").json()["dmId"]
            for tid in range(counts):
                requests.get(f"{be_url}/trees/{rt}/{sem_id}?tid={tid}")
                pbar.update()
        pbar.close()

    p.kill()

    out, err = p.communicate()

    setup = re.compile(r"loaded and set-up in (\d+)").findall(out.decode())[0]
    speed_recs = re.compile(r"([a-z]+)\((\d+)\:(\d+)/.*\)\: (.*) in (\d+)").findall(
        out.decode()
    )
    pd.DataFrame(speed_recs, columns=["et", "eid", "tid", "proc", "dur"]).assign(
        rev=rev,
        recorded=dt.datetime.now().isoformat(),
        setup=setup,
    ).to_csv(f"{bm_root}/bm-csvs/{rev}.csv.gz", index=False)
    subprocess.check_output(
        ["mv", (oa_root / "cache").as_posix(), dst_cache.as_posix()]
    )
