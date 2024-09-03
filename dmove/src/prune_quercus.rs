use std::{
    fs::{self, create_dir_all, DirEntry},
    io,
    sync::Arc,
    thread, usize,
};

use hashbrown::HashMap;
use tqdm::Iter;

use crate::{
    common::{read_buf_path, read_cache, write_gz, Stowage, A_STAT_PATH, BUILD_LOC, QC_CONF},
    oa_var_atts::SmolId,
    quercus::{AttributeStaticMap, FullJsSpec, JsQcSpec, Quercus},
    quercus_packet::QP4,
};

const MAX_SIBLINGS: usize = 16;

pub fn prune(stowage_owned: Stowage) -> io::Result<()> {
    let stowage_arc = Arc::new(stowage_owned);
    let stowage = &stowage_arc;
    let qc_confs = Arc::new(read_cache::<FullJsSpec>(stowage, QC_CONF));
    let astats: Arc<AttributeStaticMap> = Arc::new(read_cache(stowage, A_STAT_PATH));
    // let mut spawned_threads = Vec::new();
    for (filter_name, qc_kind_dir) in stowage.iter_cached_qc_locs() {
        let stclone = stowage_arc.clone();
        let qcclone = qc_confs.clone();
        let aclone = astats.clone();
        let pthread =
            thread::spawn(move || write_prunes(stclone, aclone, qcclone, qc_kind_dir, filter_name));
        pthread.join().unwrap();
        // spawned_threads.push(pthread);
    }
    // for done_thread in spawned_threads {
    // done_thread.join().unwrap();
    // }

    Ok(())
}

fn write_prunes(
    stowage: Arc<Stowage>,
    astats: Arc<AttributeStaticMap>,
    full_specs: Arc<FullJsSpec>,
    qc_kind_dir: DirEntry,
    filter_kind_name: String,
) {
    let qc_kind_name = qc_kind_dir.file_name().to_owned().into_string().unwrap();

    let qc_desc = full_specs
        .get(&qc_kind_name)
        .expect(&format!("qc spec {}", qc_kind_name));
    for qc_file in fs::read_dir(qc_kind_dir.path())
        .unwrap()
        .tqdm()
        .desc(Some(format!("{}/{}", qc_kind_name, filter_kind_name)))
    {
        let qcp = qc_file.unwrap().path();
        let qc_pack: QP4 = read_buf_path(&qcp.to_str().unwrap()).unwrap();
        let mut qc = qc_pack.to_qc();
        prune_qc(&mut qc, &astats, 0, qc_desc);

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

pub fn prune_qc(qc: &mut Quercus, astats: &AttributeStaticMap, depth: usize, qc_desc: &JsQcSpec) {
    if depth == qc_desc.bifurcations.len() {
        return;
    }

    let mut top_weights: Vec<(&SmolId, u32)> = Vec::new();
    let mut top_specs: Vec<(&SmolId, f64)> = Vec::new();

    let bif = &qc_desc.bifurcations[depth];
    let entity_type = bif.attribute_kind.clone();
    for (k, child) in &qc.children {
        top_weights.push((k, child.weight));
        let child_spec = match astats[&entity_type]
            .get(k)
            .expect(&format!("{}: {} at {}", entity_type, k, depth))
            .spec_baselines
            .get(&bif.description)
        {
            Some(spec_base) => f64::from(child.weight) / f64::from(qc.weight) / spec_base,
            None => -1.0,
        };
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
