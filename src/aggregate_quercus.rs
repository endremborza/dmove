use core::panic;
use hashbrown::HashMap;
use std::{
    fs::{create_dir_all, read_dir, DirEntry},
    io,
    ops::AddAssign,
    sync::Arc,
    thread::{self, JoinHandle},
    usize,
};
use tqdm::Iter;

use crate::{
    common::{
        read_cache, read_js_path, write_gz, Stowage, A_STAT_PATH, BUILD_LOC, COUNTRIES, INSTS,
        QC_CONF,
    },
    oa_fix_atts::{names, read_fix_att},
    oa_var_atts::{
        read_var_att, vnames, CountryId, InstId, MappContainer, MappedAttributes, MidId, SmolId,
        WorkId,
    },
    prune_quercus::prune_qc,
    quercus::{AttributeStaticMap, FullJsSpec, JsBifurcation, JsQcSpec, Quercus},
};

const MAX_DEPTH: usize = 4;

struct Counts {
    weight: usize,
    sources: usize,
    possible_children: usize,
}

impl Counts {
    fn new() -> Self {
        Self {
            weight: 0,
            sources: 0,
            possible_children: 0,
        }
    }
}

pub fn aggregate(stowage_owned: Stowage) -> io::Result<()> {
    let stowage_arc = Arc::new(stowage_owned);
    let stowage = &stowage_arc;

    let mut qc_confs = read_cache::<FullJsSpec>(stowage, QC_CONF);
    let mut astats = read_cache::<AttributeStaticMap>(stowage, A_STAT_PATH);

    let child_to_parent: Arc<[u8]> =
        Arc::from(read_fix_att(&stowage, names::I2C).into_boxed_slice());
    let parent_entity_type = COUNTRIES;
    let child_entity_type = INSTS;
    let agg_key = get_agg_desc(child_entity_type, parent_entity_type);

    let mut qc_extend = HashMap::new();
    for (k, v) in &qc_confs {
        let mut bifurcations: Vec<JsBifurcation> = v
            .bifurcations
            .iter()
            .take(MAX_DEPTH - 1)
            .map(|e| e.clone())
            .collect();
        bifurcations.insert(
            0,
            JsBifurcation {
                attribute_kind: child_entity_type.to_owned(),
                resolver_id: agg_key.to_owned(),
                description: agg_key.to_owned(),
                source_side: true,
            },
        );
        let new_qcid = get_new_name(&k);
        qc_extend.insert(
            new_qcid,
            JsQcSpec {
                bifurcations,
                root_entity_type: parent_entity_type.to_string(),
            },
        );
    }
    qc_confs.extend(qc_extend);
    let conf_arc = Arc::new(qc_confs);

    let full_clist: Arc<[Box<[WorkId]>]> = read_var_att(stowage, vnames::TO_CITING).into();
    let country_mapp = MappContainer::from_name::<CountryId, InstId>(stowage, vnames::COUNTRY_H);

    let mut parent_counts = HashMap::new();

    for cid in child_to_parent.iter() {
        parent_counts
            .entry(*cid as SmolId)
            .or_insert_with(Counts::new)
            .possible_children
            .add_assign(1);
    }

    for (wid, barr) in full_clist.iter().enumerate() {
        if let Some(MappedAttributes::Map(l1_map)) = country_mapp.get(&(wid as MidId)) {
            for (cid, _) in l1_map.iter() {
                let pv = parent_counts.get_mut(cid).unwrap();
                pv.sources.add_assign(1);
                pv.weight.add_assign(barr.len());
            }
        } else {
            panic!("cant get country thing {}", wid);
        }
    }

    for (cid, pid) in child_to_parent.iter().enumerate() {
        let pic = &parent_counts[&(*pid as SmolId)];
        astats
            .get_mut(child_entity_type)
            .unwrap()
            .get_mut(&(cid as SmolId))
            .unwrap()
            .spec_baselines
            .insert(
                agg_key.clone(),
                1.0 / f64::from(pic.possible_children as u32),
            );
    }

    let parent_arc = Arc::new(parent_counts);
    let astats_arc = Arc::new(astats);

    stowage
        .iter_pruned_qc_locs()
        .filter(|(_, qc_dir)| !is_new_dir(qc_dir))
        .map(|(filter_name, qc_kind_dir)| {
            let stclone = Arc::clone(&stowage);
            let pclone = Arc::clone(&parent_arc);
            let cclone = Arc::clone(&child_to_parent);
            let asclone = Arc::clone(&astats_arc);
            let confclone = Arc::clone(&conf_arc);
            thread::spawn(move || {
                write_aggs(
                    stclone,
                    pclone,
                    cclone,
                    asclone,
                    confclone,
                    qc_kind_dir,
                    filter_name,
                )
            })
        })
        .collect::<Vec<JoinHandle<()>>>()
        .into_iter()
        .map(|t| t.join().unwrap())
        .for_each(drop);

    write_gz(
        &stowage.pruned_cache.join(format!("{}.json", QC_CONF)),
        &conf_arc,
    )?;
    Ok(())
}

fn write_aggs(
    stowage: Arc<Stowage>,
    parent_map: Arc<HashMap<SmolId, Counts>>, // TODO struct (source, weight)
    child_map: Arc<[u8]>,
    astats: Arc<AttributeStaticMap>,
    full_conf: Arc<FullJsSpec>,
    qc_kind_dir: DirEntry,
    filter_kind_name: String,
) {
    let qc_kind_name = qc_kind_dir.file_name().to_owned().into_string().unwrap();
    let new_name = get_new_name(&qc_kind_name);

    let mut country_qcs = HashMap::new();
    for (cid, counts) in parent_map.iter() {
        let mut qc = Quercus::new();
        qc.source_count = counts.sources;
        qc.weight = counts.weight as u32;
        country_qcs.insert(cid, qc);
    }

    for qc_file in read_dir(qc_kind_dir.path())
        .unwrap()
        .tqdm()
        .desc(Some(qc_kind_name.clone()))
    {
        let qcp = qc_file.unwrap().path();
        let mut qc: Quercus = read_js_path(&qcp.to_str().unwrap()).unwrap();
        qc.chop(MAX_DEPTH - 2);

        let qc_iid = qcp
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .split(".")
            .next()
            .unwrap();

        let iid = SmolId::from_str_radix(&qc_iid, 10).unwrap();
        let cid = child_map[iid as usize] as SmolId;

        country_qcs
            .get_mut(&cid)
            .expect(&format!("country: {}", cid))
            .children
            .insert(iid, qc);
    }
    let qc_desc = &full_conf
        .get(&new_name)
        .expect(&format!("no conf {}", new_name));

    for (cid, mut cqc) in country_qcs {
        let pruned_path = stowage
            .pruned_cache
            .join(BUILD_LOC)
            .join(&filter_kind_name)
            .join(&new_name)
            .join(cid.to_string());
        create_dir_all(pruned_path.parent().unwrap()).unwrap();
        prune_qc(&mut cqc, &astats, 0, qc_desc);

        write_gz(&pruned_path, &cqc).unwrap();
    }
}

fn get_agg_desc(etype_child: &str, etype_parent: &str) -> String {
    format!("agged-{}-to-{}", etype_child, etype_parent)
}

fn get_new_name(old_name: &str) -> String {
    format!("{}c", old_name)
}

fn is_new_dir(qc_dir: &DirEntry) -> bool {
    qc_dir.file_name().to_str().iter().last().unwrap().eq(&"c")
}
