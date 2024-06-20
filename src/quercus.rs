use hashbrown::HashMap;
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::collections::VecDeque;
use std::ops::AddAssign;
use std::sync::{Arc, Mutex};
use std::{io, thread};
use tqdm::pbar;

use crate::common::{
    BigId, Stowage, A_STAT_PATH, BUILD_LOC, COUNTRIES, FIELDS, INSTS, QC_CONF, QS, SOURCES,
    SUB_FIELDS, WORKS,
};
use crate::ingest_entity::get_idmap;
use crate::oa_filters::START_YEAR;
use crate::oa_fix_atts::{names, read_fix_att};
use crate::oa_var_atts::{
    get_attribute_resolver_map, get_mapped_atts, get_name_name, read_var_att, vnames,
    AttributeResolverMap, MappedAttributes, MidId, SmolId, WeightedEdge, WorkId,
};

type GraphPath = Vec<MidId>;
pub type AttributeStaticMap = HashMap<String, HashMap<SmolId, AttributeStatic>>;
pub type FullJsSpec = HashMap<String, JsQcSpec>;

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

pub struct BdHierarcyList {
    hiers: Vec<BreakdownHierarchy>,
    qc_id: String,
}

impl BdHierarcyList {
    pub fn to_jsbifs(&self) -> Vec<JsBifurcation> {
        let mut out = Vec::new();
        for bdh in &self.hiers {
            let resolver_id = &bdh.resolver_id;
            for i in &bdh.levels {
                out.push(JsBifurcation {
                    attribute_kind: bdh.entity_types[*i].clone(),
                    description: get_bd_description(&bdh, i),
                    resolver_id: resolver_id.clone(),
                    source_side: bdh.side == 0,
                })
            }
        }
        out
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Quercus {
    pub weight: u32,
    pub source_count: usize,
    pub top_source: (BigId, u32),
    #[serde(skip_serializing, default = "HashMap::new")]
    pub sources: HashMap<MidId, u32>,
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
    current_source: MidId,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct JsBifurcation {
    pub attribute_kind: String,
    pub resolver_id: String,
    pub description: String,
    pub source_side: bool,
}

#[derive(Serialize, Deserialize)]
pub struct JsQcSpec {
    pub bifurcations: Vec<JsBifurcation>,
    pub root_entity_type: String,
}

#[derive(Serialize, Deserialize)]
pub struct AttributeStatic {
    name: String,
    pub spec_baselines: HashMap<String, f64>,
}

impl Quercus {
    pub fn new() -> Self {
        Self {
            weight: 0,
            source_count: 0,
            top_source: (0, 0),
            sources: HashMap::new(),
            children: HashMap::new(),
        }
    }

    fn get_and_add(&mut self, k: &SmolId, source: &MidId) -> &mut Self {
        self.children
            .entry(*k)
            .or_insert_with(Quercus::new)
            .bump(source)
    }

    fn bump(&mut self, source: &MidId) -> &mut Self {
        self.sources.entry(*source).or_insert(0).add_assign(1);
        self.weight += 1;
        self
    }

    fn finalize(&mut self, level: usize, source_ids: &Arc<[BigId]>) -> usize {
        //prune, resolve source count, select top source
        self.source_count = self.sources.len();
        let max_source = self
            .sources
            .iter()
            .reduce(|l, r| if l.1 > r.1 { l } else { r })
            .unwrap_or((&0, &0));
        self.top_source = (source_ids[*max_source.0 as usize], *max_source.1);

        let mut dropks = Vec::new();
        for (k, child) in self.children.iter_mut() {
            let grandchildren = child.finalize(level - 1, source_ids);
            if (grandchildren == 0) && (level > 1) {
                dropks.push(*k);
            }
        }
        for k in dropks {
            self.children.remove(&k);
        }

        return self.children.len();
    }

    pub fn absorb(&mut self, other: Quercus) {
        self.weight += other.weight;

        for (sid, sc) in &other.sources {
            self.sources.entry(*sid).or_insert(0).add_assign(*sc);
        }
        for (k, v) in other.children {
            let child = self.children.entry(k).or_insert_with(Quercus::new);
            child.absorb(v);
        }
    }

    pub fn chop(&mut self, depth: usize) {
        if depth <= 0 {
            self.children = HashMap::new();
            self.sources = HashMap::new();
        } else {
            for qc in self.children.values_mut() {
                qc.chop(depth - 1)
            }
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
            current_source: 0,
        }
    }

    fn set(&mut self, graph_path: GraphPath) {
        self.current_source = graph_path[0];
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
                            self.hierarchy_ender(cq.get_and_add(v, &self.current_source));
                        }
                    }
                    Map(map_data) => {
                        for (k, v) in map_data.iter() {
                            self.current_index_within_hier_levels += 1;
                            self.current_hier_depth += 1;
                            self.roll_setup(Some(v), cq.get_and_add(&k, &self.current_source));
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

struct SpecPrepType(HashMap<String, HashMap<SmolId, HashMap<String, (f64, usize)>>>);

impl SpecPrepType {
    fn add(&mut self, bif: &JsBifurcation, entity_id: &SmolId, value: f64) {
        let parent = self
            .0
            .entry(bif.attribute_kind.clone())
            .or_insert_with(HashMap::new)
            .entry(*entity_id)
            .or_insert_with(HashMap::new);
        let entry = parent.entry(bif.description.clone()).or_insert((0.0, 0));
        update_tentry(entry, &(value, 1));
    }
}

fn update_tentry(entry: &mut (f64, usize), other: &(f64, usize)) {
    entry.1 += other.1;
    entry.0 = (entry.0 * ((entry.1 - 1) as f64) + other.0) / (entry.1 as f64);
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

struct QcInput {
    bd_hiers: Arc<BdHierarcyList>,
    iid: usize,
}

enum QueIn {
    Go(QcInput),
    Poison,
}

macro_rules! clone_thread_push {
    ($thread_vec: ident, $para_fun: ident, $($arg: ident,)*) => {
        {
        $(let $arg = Arc::clone(&$arg);)*

        $thread_vec.push(thread::spawn(move || {
            $para_fun(
                $($arg,)*
        )
        }))
        }
    };
}

pub fn dump_all_cache(stowage_owned: Stowage) -> io::Result<()> {
    let mut attribute_statics: AttributeStaticMap = HashMap::new();
    let stowage_arc = Arc::new(stowage_owned);
    let stowage = &stowage_arc;

    println!("getting ares map");
    let ares_map = get_attribute_resolver_map(stowage);

    println!("getting var atts");
    let full_clist: Arc<[Box<[WorkId]>]> = read_var_att(stowage, vnames::TO_CITING).into();
    let works_of_inst: Arc<[Box<[WeightedEdge<WorkId>]>]> = read_var_att(stowage, vnames::I2W)
        .into_iter()
        .map(|e: Vec<WeightedEdge<WorkId>>| e.into_boxed_slice())
        .collect();

    println!("getting work ids");

    let work_idmap = get_idmap(stowage, WORKS);

    let mut source_ids_mut: Vec<BigId> = vec![0; work_idmap.current_total as usize + 1];
    for (original_id, num_id) in &work_idmap.to_map() {
        source_ids_mut[*num_id as usize] = *original_id;
    }
    let source_ids = source_ids_mut.into();

    //preps for specs and metas
    let spec_bases_vecs = SpecPrepType(HashMap::new());

    let filter_set = FilterSet::new(stowage);
    let mut spawned_threads = Vec::new();
    let mut js_qc_specs = HashMap::new();

    let arm_arc = Arc::new(ares_map);
    let fset_arc = Arc::new(filter_set);

    let n_threads: usize = std::thread::available_parallelism().unwrap().into();
    let qc_in_q = Mutex::new(VecDeque::with_capacity(n_threads * 5));
    let in_arc = Arc::new(qc_in_q);

    let specs_arc = Arc::new(Mutex::new(spec_bases_vecs));

    for _ in 0..(n_threads / 2) {
        clone_thread_push!(
            spawned_threads,
            make_qcs,
            stowage_arc,
            in_arc,
            specs_arc,
            arm_arc,
            fset_arc,
            works_of_inst,
            full_clist,
            source_ids,
        );
    }
    let mut v = Vec::new();

    let entity_type = INSTS;
    let id_map = get_idmap(stowage, entity_type);

    for bdhl in get_hier_lists() {
        js_qc_specs.insert(
            bdhl.qc_id.clone(),
            JsQcSpec {
                bifurcations: bdhl.to_jsbifs(),
                root_entity_type: entity_type.to_string(),
            },
        );
        let hiers_arc = Arc::new(bdhl);
        for iid in id_map.iter_ids(false) {
            v.push(QueIn::Go(QcInput {
                iid: iid.clone() as usize,
                bd_hiers: Arc::clone(&hiers_arc),
            }));
        }
    }
    let mut rng = rand::thread_rng();
    v.shuffle(&mut rng);
    for e in v {
        in_arc.lock().unwrap().push_front(e)
    }
    for _ in &spawned_threads {
        in_arc.lock().unwrap().push_front(QueIn::Poison);
    }

    for done_thread in spawned_threads {
        done_thread.join().unwrap();
    }

    for entity in [INSTS, COUNTRIES, SOURCES, QS, FIELDS, SUB_FIELDS] {
        let mut entity_statics = HashMap::new();
        let names: Vec<String> = read_var_att(stowage, &get_name_name(entity));
        for eid in get_idmap(stowage, entity).iter_ids(true) {
            let e_map = AttributeStatic {
                name: names[eid as usize].clone(),
                spec_baselines: HashMap::new(),
            };
            entity_statics.insert(eid as SmolId, e_map);
        }
        attribute_statics.insert(entity.to_string(), entity_statics);
    }

    for (ek, recs) in &specs_arc.lock().unwrap().0 {
        for (k, sb_hm) in recs {
            let astats = attribute_statics.get_mut(ek).unwrap().get_mut(k).unwrap();
            for (res_id, sb_v) in sb_hm {
                astats.spec_baselines.insert(res_id.to_string(), sb_v.0);
            }
        }
    }

    stowage.write_cache(&attribute_statics, A_STAT_PATH)?;
    stowage.write_cache(&js_qc_specs, QC_CONF)?;

    Ok(())
}

fn make_qcs(
    stowage: Arc<Stowage>,
    in_queue: Arc<Mutex<VecDeque<QueIn>>>,
    spec_bases_vecs: Arc<Mutex<SpecPrepType>>,
    ares_map: Arc<AttributeResolverMap>,
    filter_set: Arc<FilterSet>,
    l1_var_atts: Arc<[Box<[WeightedEdge<WorkId>]>]>,
    l2_var_atts: Arc<[Box<[WorkId]>]>,
    source_ids: Arc<[BigId]>,
) {
    let mut pbar = pbar(None);

    loop {
        let queue_in = match in_queue.lock().unwrap().pop_back() {
            Some(q) => q,
            None => continue,
        };
        if let QueIn::Go(qc_in) = queue_in {
            pbar.update(1).unwrap();
            let qc_key = &qc_in.bd_hiers.qc_id;
            let bifurcations = qc_in.bd_hiers.to_jsbifs();
            let mut qcr = QuercusRoller::new(&qc_in.bd_hiers.hiers, &ares_map);
            let mut qcs: Vec<Quercus> = filter_set
                .filter_keys
                .iter()
                .map(|_| Quercus::new())
                .collect();
            for wid in l1_var_atts[qc_in.iid as usize].iter() {
                let qc_ind = filter_set.get_qc_ind(&wid.id);
                let mut qc = &mut qcs[qc_ind];

                for citing_wid in l2_var_atts[wid.id as usize].iter() {
                    let ref_path = vec![wid.id, *citing_wid];
                    qcr.set(ref_path);
                    qc.bump(&wid.id);
                    qcr.current_hier_index = 0;
                    qcr.roll_hier(&mut qc);
                }
            }
            let mut i = qcs.len();
            let mut qc = Quercus::new();
            for to_abs in qcs.into_iter().rev().into_iter() {
                i = i - 1;
                qc.absorb(to_abs);
                qc.finalize(bifurcations.len(), &source_ids);
                let filter_key = &filter_set.filter_keys[i];
                stowage
                    .write_cache(
                        &qc,
                        &format!("{}/{}/{}/{}", BUILD_LOC, filter_key, qc_key, qc_in.iid),
                    )
                    .unwrap();
                //calc spec bases for first 2 levels only
                let root_denom = f64::from(qc.weight);
                for (child_id, child_qc) in &qc.children {
                    let rate = f64::from(child_qc.weight) / root_denom;
                    spec_bases_vecs
                        .lock()
                        .unwrap()
                        .add(&bifurcations[0], &child_id, rate);
                    if bifurcations[0].resolver_id == bifurcations[1].resolver_id {
                        for (sub_child_id, sub_child_qc) in &child_qc.children {
                            let rate = f64::from(sub_child_qc.weight) / root_denom;
                            spec_bases_vecs.lock().unwrap().add(
                                &bifurcations[1],
                                &sub_child_id,
                                rate,
                            );
                        }
                    }
                }
            }
        } else {
            break;
        };
    }
}

pub fn get_bd_description(bdh: &BreakdownHierarchy, i: &usize) -> String {
    format!("{}-{}-{}", bdh.side, bdh.resolver_id, i)
}

pub fn get_hier_lists() -> Vec<BdHierarcyList> {
    get_qc_spec_bases()
        .into_iter()
        .enumerate()
        .map(|(i, hiers)| BdHierarcyList {
            hiers,
            qc_id: format!("qc-{}", i + 1),
        })
        .collect()
}

fn get_qc_spec_bases() -> Vec<Vec<BreakdownHierarchy>> {
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
