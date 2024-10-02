import json

from pyscripts.rust_gen import ComC, EntC, StowC

from .common import Keys, app_root, load_map, oa_root, read_p_gz

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


def main():
    out = app_root / "roots.json"
    astats = read_p_gz(oa_root / StowC.pruned_cache / ComC.A_STAT_PATH)
    specs = read_p_gz(oa_root / StowC.pruned_cache / ComC.QC_CONF)
    root_types = set(v[Keys.ROOT] for v in specs.values())

    root_confs = [
        {
            "entity_type": ComC.COUNTRIES,
            "prefix": "🌍",
            "default_tree": f"qc-{ComC.COUNTRIES}-3",
            "start_sentence": "Scholars in",
        },
        {
            "entity_type": EntC.INSTITUTIONS,
            "prefix": "🏛",
            "default_tree": f"qc-{EntC.INSTITUTIONS}-3",
            "start_sentence": "Scholars at",
        },
        {
            "entity_type": EntC.AUTHORS,
            "prefix": "👤",
            "default_tree": f"qc-{EntC.AUTHORS}-2",
            "start_sentence": "Papers of",
        },
    ]
    root_list = []
    for rt in root_types:
        for estats in astats[rt].values():
            rid = estats[Keys.META][Keys.SEM]
            if rid:
                root_list.append({"rootType": rt, "rootId": rid})

    out.write_text(json.dumps(root_list))

    (app_root / "qc-specs.json").write_text(json.dumps(specs))
    (app_root / "root-basics.json").write_text(json.dumps(root_confs))

    vdic = load_map(EntC.INSTITUTIONS)
    intro_jsp = app_root / "intro-inst-ids.json"
    intro_jsp.write_text(json.dumps([str(vdic[i]) for i in intro_ids]))
