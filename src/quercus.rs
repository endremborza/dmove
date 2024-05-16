use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::{io, thread};
use tqdm::*;

use crate::common::{Stowage, COUNTRIES, FIELDS, INSTS, QS, SOURCES, SUB_FIELDS};
use crate::ingest_entity::{get_idmap, IdMap};
use crate::oa_filters::START_YEAR;
use crate::oa_fix_atts::{names, read_fix_att};
use crate::oa_var_atts::{
    get_attribute_resolver_map, get_mapped_atts, get_name_name, read_var_att, vnames,
    AttributeResolverMap, MappedAttributes, MidId, SmolId, WeightedEdge, WorkId,
};

pub const BUILD_LOC: &str = "qc-builds";
pub const A_STAT_PATH: &str = "attribute-statics";
pub const QC_CONF: &str = "qc-specs";

type GraphPath = Vec<MidId>;
pub type AttributeStaticMap = HashMap<String, HashMap<SmolId, AttributeStatic>>;

pub struct BreakdownHierarchy {
    pub levels: Vec<usize>,
    pub side: usize,
    pub resolver_id: String,
    pub entity_types: Vec<String>,
}

impl BreakdownHierarchy {
    fn new(resolver_id: &str, levels: Vec<usize>, side: usize) -> Self {
        let entity_types: Vec<String> = get_mapped_atts(resolver_id);
        Self {
            levels,
            side,
            resolver_id: resolver_id.to_owned(),
            entity_types,
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Quercus {
    //TODO: at one point include "stats" or "meta"
    pub weight: u32,
    #[serde(skip_serializing_if = "HashMap::is_empty", default = "HashMap::new")]
    pub children: HashMap<SmolId, Quercus>,
}

struct QuercusRoller<'a> {
    aresolver_map: &'a AttributeResolverMap,
    all_bifhs: &'a [BreakdownHierarchy],
    current_entity_indices: GraphPath,
    current_hier_index: usize,
    current_index_within_hier_levels: usize,
    current_hier_depth: usize,
}

#[derive(Serialize, Deserialize)]
struct JsBifurcation {
    attribute_kind: String,
    resolver_id: String,
    description: String,
}

#[derive(Serialize, Deserialize)]
pub struct JsQcSpec {
    bifurcations: Vec<JsBifurcation>,
    pub root_entity_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct AttributeStatic {
    name: String,
    pub spec_baselines: HashMap<String, f64>,
    #[serde(default = "HashMap::new")]
    meta: HashMap<String, String>,
}

impl Quercus {
    fn new() -> Self {
        Self {
            weight: 0,
            children: HashMap::new(),
        }
    }

    fn get_and_add(&mut self, k: &SmolId) -> &mut Quercus {
        let entry = self.children.entry(*k);
        let child = entry.or_insert_with(Quercus::new);
        child.weight += 1;
        child
    }

    fn prune(&mut self, level: usize) -> usize {
        if level == 0 {
            return self.children.len();
        }

        let mut dropks = Vec::new();
        for (k, child) in self.children.iter_mut() {
            if child.prune(level - 1) == 0 {
                dropks.push(*k);
            }
        }
        for k in dropks {
            self.children.remove(&k);
        }

        return self.children.len();
    }

    fn absorb(&mut self, other: &Quercus) {
        self.weight += other.weight;
        for (k, v) in &other.children {
            let child = self.children.entry(*k).or_insert_with(Quercus::new);
            child.absorb(v);
        }
    }
}

impl<'a> QuercusRoller<'a> {
    fn new(all_bifhs: &'a [BreakdownHierarchy], aresolver_map: &'a AttributeResolverMap) -> Self {
        Self {
            aresolver_map,
            all_bifhs,
            current_entity_indices: vec![0; all_bifhs.len()],
            current_hier_index: 0,
            current_index_within_hier_levels: 0,
            current_hier_depth: 0,
        }
    }

    fn set(&mut self, graph_path: GraphPath) {
        for (i, v) in self.all_bifhs.iter().enumerate() {
            self.current_entity_indices[i] = graph_path[v.side];
        }
    }

    fn roll_hier(&mut self, current_quercus: &mut Quercus) -> Option<()> {
        let bifh = self.all_bifhs.get(self.current_hier_index)?;
        self.current_index_within_hier_levels = 0;
        self.current_hier_depth = 0;
        let resolver = &self.aresolver_map[&bifh.resolver_id];
        let entity_ind = self.current_entity_indices[self.current_hier_index];
        // let mapped_attributes = &resolver[entity_ind as usize];
        let mapped_attributes = resolver.get(&entity_ind)?;
        self.roll_setup(Some(mapped_attributes), current_quercus);
        None
    }

    fn roll_setup(
        &mut self,
        mapped_attributes: Option<&MappedAttributes>,
        current_quercus: &mut Quercus,
    ) {
        use MappedAttributes::{List, Map};
        if let Some(target_depth) = self.all_bifhs[self.current_hier_index]
            .levels
            .get(self.current_index_within_hier_levels)
        {
            if self.current_hier_depth < *target_depth {
                // not really using this level, move on to next
                // <= and not < because of the += 1
                for (_, v) in mapped_attributes.unwrap().iter_inner() {
                    self.current_hier_depth += 1;
                    self.roll_setup(Some(v), current_quercus);
                    self.current_hier_depth -= 1;
                }
            } else {
                let cq = current_quercus;
                match mapped_attributes.unwrap() {
                    List(vec_data) => {
                        for v in vec_data.iter() {
                            self.hierarchy_ender(cq.get_and_add(v));
                        }
                    }
                    Map(map_data) => {
                        for (k, v) in map_data.iter() {
                            self.current_index_within_hier_levels += 1;
                            self.current_hier_depth += 1;
                            self.roll_setup(Some(v), cq.get_and_add(&k));
                            self.current_index_within_hier_levels -= 1;
                            self.current_hier_depth -= 1;
                        }
                    }
                }
            }
        } else {
            self.hierarchy_ender(current_quercus);
        }
    }

    fn hierarchy_ender(&mut self, qc: &mut Quercus) {
        let old_ends = (
            self.current_hier_depth,
            self.current_index_within_hier_levels,
        );
        self.current_hier_index += 1;
        self.roll_hier(qc);
        self.current_hier_index -= 1;
        (
            self.current_hier_depth,
            self.current_index_within_hier_levels,
        ) = old_ends;
    }
}

struct SpecPrepType(HashMap<String, HashMap<SmolId, HashMap<String, Vec<f64>>>>);

impl SpecPrepType {
    fn add(&mut self, bif: &JsBifurcation, entity_id: &SmolId, value: f64) {
        self.0
            .entry(bif.attribute_kind.clone())
            .or_insert_with(HashMap::new)
            .entry(*entity_id)
            .or_insert_with(HashMap::new)
            .entry(bif.description.clone())
            .or_insert_with(Vec::new)
            .push(value);
    }

    fn absorb(&mut self, other: &Self) {
        for (entity_type, v) in &other.0 {
            let item_map = self
                .0
                .entry(entity_type.to_string())
                .or_insert_with(HashMap::new);
            for (k2, v2) in v {
                let res_map = item_map.entry(*k2).or_insert_with(HashMap::new);
                for (k3, v3) in v2 {
                    res_map
                        .entry(k3.to_string())
                        .or_insert_with(Vec::new)
                        .extend(v3.iter());
                }
            }
        }
    }
}

struct FilterSet {
    year_atts: Vec<u8>,
    years: Vec<u16>,
    filter_keys: Vec<String>,
}

impl FilterSet {
    fn new(stowage: &Stowage) -> Self {
        let year_atts = read_fix_att(stowage, names::WORK_YEAR);
        let years = (2019..2024).collect();
        let mut filter_keys = vec!["all".to_string()];
        for y in &years {
            filter_keys.push(format!("y-{}", y).to_owned());
        }
        Self {
            year_atts,
            years,
            filter_keys,
        }
    }

    fn filter(&self, i: &usize, wid: &MidId) -> bool {
        if *i == 0 {
            return false;
        }
        let work_year = self.year_atts[*wid as usize] as u16 + START_YEAR;
        return work_year >= self.years[i - 1];
    }

    fn get_qc_ind(&self, wid: &MidId) -> usize {
        let mut i = 0;
        let work_year = self.year_atts[*wid as usize] as u16 + START_YEAR;
        for y in &self.years {
            if work_year < *y {
                break;
            }
            i = i + 1;
        }
        i
    }
}

pub fn dump_all_cache(stowage_owned: Stowage) -> io::Result<()> {
    let mut attribute_statics: AttributeStaticMap = HashMap::new();
    let stowage_arc = Arc::new(stowage_owned);
    let stowage = &stowage_arc;

    println!("getting ares map");
    let ares_map = get_attribute_resolver_map(stowage);

    println!("getting var atts");
    let full_clist: Arc<[Box<[WorkId]>]> = read_var_att(stowage, vnames::TO_CITING).into();
    let semantic_ids: Box<[String]> = read_var_att(stowage, vnames::INST_SEM_IDS).into();
    let works_of_inst: Arc<[Vec<WeightedEdge<WorkId>>]> = read_var_att(stowage, vnames::I2W).into();

    //preps for specs and metas
    let mut inst_cite_counts = vec![0; works_of_inst.len()];
    let mut spec_bases_vecs = SpecPrepType(HashMap::new());

    let filter_set = FilterSet::new(stowage);
    let mut spawned_threads = Vec::new();
    let mut js_qc_specs = HashMap::new();
    let entity_type = INSTS;
    let id_map = get_idmap(stowage, entity_type);

    let arc_idm = Arc::new(id_map);
    let arc_arm = Arc::new(ares_map);
    let arc_fset = Arc::new(filter_set);

    for (i, bd_hiers) in get_qc_spec_bases().into_iter().enumerate() {
        let mut bifurcations = Vec::new();
        for bdh in &bd_hiers {
            let resolver_id = &bdh.resolver_id;

            for i in &bdh.levels {
                bifurcations.push(JsBifurcation {
                    attribute_kind: bdh.entity_types[*i].clone(),
                    description: get_bd_description(&bdh, i),
                    resolver_id: resolver_id.clone(),
                })
            }
        }
        let qc_key = format!("qc-{}", i + 1);
        let idmap_clone = Arc::clone(&arc_idm);
        let arm_clone = Arc::clone(&arc_arm);
        let fset_clone = Arc::clone(&arc_fset);
        let clist_clone = Arc::clone(&full_clist);
        let winst_clone = Arc::clone(&works_of_inst);

        let stowage_cloned = Arc::clone(&stowage_arc);

        spawned_threads.push(thread::spawn(move || {
            make_qcs(
                stowage_cloned,
                qc_key,
                idmap_clone,
                arm_clone,
                bd_hiers,
                fset_clone,
                winst_clone,
                clist_clone,
                bifurcations,
            )
        }))
    }

    for done_thread in spawned_threads {
        let (thread_counts, thread_spec_bases, bifurcations, qc_key) = done_thread.join().unwrap();
        for (i, c) in thread_counts.iter().enumerate() {
            inst_cite_counts[i] += c;
        }
        spec_bases_vecs.absorb(&thread_spec_bases);

        let js_spec = JsQcSpec {
            bifurcations,
            root_entity_type: entity_type.to_string(),
        };
        js_qc_specs.insert(qc_key.clone(), js_spec);
    }

    for entity in [INSTS, COUNTRIES, SOURCES, QS, FIELDS, SUB_FIELDS] {
        let mut entity_statics = HashMap::new();
        let names: Vec<String> = read_var_att(stowage, &get_name_name(entity));
        for eid in get_idmap(stowage, entity).iter_ids(true) {
            let mut meta = HashMap::new();
            if entity == INSTS {
                meta.insert(
                    "papers".to_string(),
                    works_of_inst[eid as usize].len().to_string(),
                );
                meta.insert(
                    "citations".to_string(),
                    inst_cite_counts[eid as usize].to_string(),
                );
                meta.insert(
                    "semantic_id".to_string(),
                    semantic_ids[eid as usize].clone(),
                );
            }

            let e_map = AttributeStatic {
                name: names[eid as usize].clone(),
                spec_baselines: HashMap::new(),
                meta,
            };
            entity_statics.insert(eid as SmolId, e_map);
        }
        attribute_statics.insert(entity.to_string(), entity_statics);
    }

    for (ek, recs) in spec_bases_vecs.0 {
        for (k, sb_hm) in recs {
            let astats = attribute_statics.get_mut(&ek).unwrap().get_mut(&k).unwrap();
            for (res_id, sb_v) in sb_hm {
                astats.spec_baselines.insert(
                    res_id,
                    sb_v.iter().sum::<f64>() / f64::from(sb_v.len() as u32),
                );
            }
        }
    }

    stowage.write_cache(&attribute_statics, A_STAT_PATH)?;
    stowage.write_cache(&js_qc_specs, QC_CONF)?;

    Ok(())
}

fn make_qcs(
    stowage: Arc<Stowage>,
    qc_key: String,
    id_map: Arc<IdMap>,
    ares_map: Arc<AttributeResolverMap>,
    bd_hiers: Vec<BreakdownHierarchy>,
    filter_set: Arc<FilterSet>,
    l1_var_atts: Arc<[Vec<WeightedEdge<WorkId>>]>,
    l2_var_atts: Arc<[Box<[WorkId]>]>,
    bifurcations: Vec<JsBifurcation>,
) -> (Vec<u32>, SpecPrepType, Vec<JsBifurcation>, String) {
    let mut qcr = QuercusRoller::new(&bd_hiers, &ares_map);
    let mut full_counts = vec![0; id_map.current_total as usize];
    let mut spec_bases_vecs = SpecPrepType(HashMap::new());

    for iid in id_map.iter_ids(false).tqdm().desc(Some(&qc_key)) {
        let mut qcs: Box<[Quercus]> = filter_set
            .filter_keys
            .iter()
            .map(|_| Quercus::new())
            .collect();
        for wid in l1_var_atts[iid as usize].iter() {
            let qc_ind = filter_set.get_qc_ind(&wid.id);
            let mut qc = &mut qcs[qc_ind];

            for citing_wid in l2_var_atts[wid.id as usize].iter() {
                let ref_path = vec![wid.id, *citing_wid];
                qcr.set(ref_path);
                qc.weight += 1;
                qcr.current_hier_index = 0;
                qcr.roll_hier(&mut qc);
            }
        }
        let mut i = qcs.len();
        let mut qc = Quercus::new();
        for to_abs in qcs.iter().rev() {
            i = i - 1;
            full_counts[iid as usize] = to_abs.weight;
            qc.absorb(to_abs);
            qc.prune(bifurcations.len() - 1);
            let filter_key = &filter_set.filter_keys[i];
            stowage
                .write_cache(
                    &qc,
                    &format!("{}/{}/{}/{}", BUILD_LOC, filter_key, qc_key, iid),
                )
                .unwrap();
            //calc spec bases for first 2 levels only
            let root_denom = f64::from(qc.weight);
            for (child_id, child_qc) in &qc.children {
                let rate = f64::from(child_qc.weight) / root_denom;
                spec_bases_vecs.add(&bifurcations[0], &child_id, rate);
                if bifurcations[0].resolver_id == bifurcations[1].resolver_id {
                    for (sub_child_id, sub_child_qc) in &child_qc.children {
                        let rate = f64::from(sub_child_qc.weight) / root_denom;
                        spec_bases_vecs.add(&bifurcations[1], &sub_child_id, rate);
                    }
                }
            }
        }
    }
    (full_counts, spec_bases_vecs, bifurcations, qc_key)
}

pub fn get_bd_description(bdh: &BreakdownHierarchy, i: &usize) -> String {
    format!("{}-{}-{}", bdh.side, bdh.resolver_id, i)
}

pub fn get_qc_spec_bases() -> Vec<Vec<BreakdownHierarchy>> {
    vec![
        vec![
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0], 0),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0, 1], 1),
        ],
        vec![
            BreakdownHierarchy::new(vnames::W2QS, vec![0, 1], 0),
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0], 1),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0, 1], 1),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0, 1], 1),
        ],
        vec![
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0], 0),
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0, 1], 1),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0, 1], 1),
            BreakdownHierarchy::new(vnames::W2QS, vec![1], 1),
        ],
        vec![
            BreakdownHierarchy::new(vnames::W2QS, vec![1], 1),
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0], 1),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0, 1], 0),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0, 1], 0),
            // BreakdownHierarchy::new(vnames::COUNTRY_H, vec![1], 0), //TODO tricky!!!
        ],
        vec![
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![1], 0),
            BreakdownHierarchy::new(vnames::W2QS, vec![0, 1], 0),
        ],
    ]
}
