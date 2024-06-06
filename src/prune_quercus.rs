use core::f64;
use std::{
    fs::{self, create_dir_all, DirEntry},
    io,
    sync::Arc,
    thread, usize,
};

use hashbrown::HashMap;
use serde::de::DeserializeOwned;
use std::io::prelude::*;
use tqdm::Iter;

use crate::{
    common::{get_gz_buf, write_gz, Stowage},
    oa_var_atts::SmolId,
    quercus::{
        get_bd_description, get_qc_spec_bases, AttributeStaticMap, BreakdownHierarchy, JsQcSpec,
        Quercus, A_STAT_PATH, BUILD_LOC, QC_CONF,
    },
};

const MAX_SIBLINGS: usize = 16;

pub fn prune(stowage_owned: Stowage) -> io::Result<()> {
    let stowage_arc = Arc::new(stowage_owned);
    let stowage = &stowage_arc;
    let bases = Arc::new(get_qc_spec_bases());
    let astats: Arc<AttributeStaticMap> = Arc::new(read_cache(stowage, A_STAT_PATH));
    let mut spawned_threads = Vec::new();
    for filter_dir in fs::read_dir(stowage.cache.join(BUILD_LOC)).unwrap() {
        let filter_dir = filter_dir.unwrap();
        for qc_kind_dir in fs::read_dir(filter_dir.path()).unwrap() {
            let qc_kind_dir = qc_kind_dir.unwrap();
            let filter_name = filter_dir.file_name().to_str().unwrap().to_string();
            let stclone = Arc::clone(&stowage);
            let bclone = Arc::clone(&bases);
            let aclone = Arc::clone(&astats);
            spawned_threads.push(thread::spawn(move || {
                write_prunes(stclone, aclone, bclone, qc_kind_dir, filter_name)
            }))
        }
    }
    for done_thread in spawned_threads {
        done_thread.join().unwrap();
    }
    write_gz(
        &stowage.pruned_cache.join(format!("{}.json", QC_CONF)),
        &read_cache::<HashMap<String, JsQcSpec>>(stowage, QC_CONF),
    )?;

    Ok(())
}

fn write_prunes(
    stowage: Arc<Stowage>,
    astats: Arc<AttributeStaticMap>,
    bases: Arc<Vec<Vec<BreakdownHierarchy>>>,
    qc_kind_dir: DirEntry,
    filter_kind_name: String,
) {
    let qc_kind_name = qc_kind_dir.file_name().to_owned().into_string().unwrap();

    let qc_desc = qc_kind_name
        .split("-")
        .last()
        .map(|i| &bases[i.parse::<usize>().unwrap().checked_sub(1).unwrap()])
        .unwrap();
    for qc_file in fs::read_dir(qc_kind_dir.path())
        .unwrap()
        .tqdm()
        .desc(Some(qc_kind_name))
    {
        let qcp = qc_file.unwrap().path();
        let mut qc: Quercus = read_path(&qcp.to_str().unwrap()).unwrap();
        prune_qc(&mut qc, &astats, 0, &qc_desc);

        let pruned_path = stowage
            .pruned_cache
            .join(BUILD_LOC)
            .join(&filter_kind_name)
            .join(qc_kind_dir.file_name())
            .join(
                qcp.file_name()
                    .unwrap()
                    .to_str()
                    .unwrap()
                    .split(".")
                    .next()
                    .unwrap(),
            );
        create_dir_all(pruned_path.parent().unwrap()).unwrap();
        write_gz(&pruned_path, &qc).unwrap();
    }
}

fn prune_qc(
    qc: &mut Quercus,
    astats: &AttributeStaticMap,
    depth: usize,
    qc_desc: &Vec<BreakdownHierarchy>,
) {
    let mut top_weights: Vec<(&SmolId, u32)> = Vec::new();
    let mut top_specs: Vec<(&SmolId, f64)> = Vec::new();

    let mut h_ind = 0;
    let mut l_ind = 0;
    let mut hier = &qc_desc[0];
    let mut entity_type = &hier.entity_types[hier.levels[l_ind]];
    for _ in 0..depth {
        l_ind += 1;
        if l_ind >= hier.levels.len() {
            l_ind = 0;
            h_ind += 1;
        }
        if h_ind == qc_desc.len() {
            return;
        }
        hier = &qc_desc[h_ind];
        entity_type = &hier.entity_types[hier.levels[l_ind]];
    }
    let bd_desc = get_bd_description(hier, &hier.levels[l_ind]);

    for (k, child) in &qc.children {
        top_weights.push((k, child.weight));
        let spec_base = astats[entity_type]
            .get(k)
            .expect(&format!("{}: {} at {}", entity_type, k, depth))
            .spec_baselines
            .get(&bd_desc)
            .expect(&format!(
                "missing from {}, {} at {}, etypes: {:?}, lind: {}",
                entity_type, bd_desc, depth, &hier.entity_types, l_ind
            ));
        let child_spec = f64::from(child.weight) / f64::from(qc.weight) / spec_base;
        top_specs.push((k, child_spec));
    }
    top_weights.sort_by(|l, r| r.1.partial_cmp(&l.1).unwrap()); // this is reverse sorting
    top_specs.sort_by(|l, r| r.1.partial_cmp(&l.1).unwrap());

    let mut to_keep: Vec<SmolId> = top_weights
        .iter()
        .take(MAX_SIBLINGS)
        .map(|e| e.0.clone())
        .collect();
    to_keep.extend(top_specs.iter().take(MAX_SIBLINGS).map(|e| e.0.clone()));
    keep_keys(&mut qc.children, &to_keep);
    for mut child in &mut qc.children.values_mut() {
        prune_qc(&mut child, astats, depth + 1, qc_desc);
    }
}

fn keep_keys<K, V>(map: &mut HashMap<K, V>, to_keep: &Vec<K>)
where
    K: std::cmp::Eq + std::hash::Hash + Copy,
{
    let mut to_dump = Vec::new();
    for k in map.keys() {
        if !to_keep.contains(k) {
            to_dump.push(*k);
        }
    }
    for k in to_dump {
        map.remove(&k);
    }
}

fn read_cache<T: DeserializeOwned>(stowage: &Stowage, fname: &str) -> T {
    read_path(
        stowage
            .cache
            .join(format!("{}.json.gz", fname))
            .to_str()
            .unwrap(),
    )
    .expect(&format!("tried reading {}", fname))
}

fn read_path<T: DeserializeOwned>(fp: &str) -> Result<T, serde_json::Error> {
    let mut js_str = String::new();
    get_gz_buf(fp).read_to_string(&mut js_str).unwrap();
    serde_json::from_str(&js_str)
}
