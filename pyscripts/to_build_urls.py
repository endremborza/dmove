import json
import os
from pathlib import Path

import pandas as pd
from inst_str_id import inst_root, load_map

app_root = Path(os.environ["RANKLESS_APP_ROOT"], "src/lib/assets/data")

intro_ids = [
    241749,
    17974374,
    45129253,
    63966007,
    74801974,
    76130692,
    78577930,
    97018004,
    145311948,
    162148367,
    163245316,
    193662353,
    202697423,
    4210092408,
]


if __name__ == "__main__":
    df = pd.read_csv(inst_root / "semantic-ids.csv.gz")
    out = app_root / "insts.json"
    intro_jsp = app_root / "intro-inst-ids.json"

    out.write_text(json.dumps(df["alt_name"].tolist()))

    vdic = load_map("institutions")
    intro_jsp.write_text(json.dumps([vdic[i] for i in intro_ids]))
