use hashbrown::{HashMap, HashSet};
use rand::seq::SliceRandom;
use serde::{Deserialize, Serialize};
use std::io;
use std::ops::AddAssign;
use std::sync::{Arc, Mutex};
use tqdm::Iter;

use crate::common::{BigId, Stowage, A_STAT_PATH, BUILD_LOC, QC_CONF};
use crate::oa_csv_writers::{authors, institutions, works};
use crate::oa_filters::START_YEAR;
use crate::oa_fix_atts::{names, read_fix_att};
use crate::oa_var_atts::{
    get_attribute_resolver_map, get_mapped_atts, get_name_name, read_var_att, vnames,
    AttributeResolverMap, MappedAttributes, MidId, SmolId, WeightedEdge, WorkId,
};
use crate::para::Worker;
use crate::quercus_packet::QP4;

pub type AttributeStaticMap = HashMap<String, HashMap<SmolId, AttributeStatic>>;
pub type FullJsSpec = HashMap<String, JsQcSpec>;
type GraphPath = Vec<MidId>;

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

pub struct BreakdownHierarchy {
    pub levels: Vec<usize>,
    pub side: usize,
    pub resolver_id: String,
    pub entity_types: Vec<String>,
}

pub struct BdHierarcyList {
    pub hiers: Vec<BreakdownHierarchy>,
    pub qc_id: String,
    pub root_type: String,
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

struct QuercusRoller<'a> {
    aresolver_map: &'a AttributeResolverMap,
    all_bifhs: &'a [BreakdownHierarchy],
    current_entity_indices: GraphPath,
    current_hier_index: usize,
    current_index_within_hier_levels: usize,
    current_hier_depth: usize,
    current_source: MidId,
}

struct SpecPrepType(HashMap<String, HashMap<SmolId, HashMap<String, (f64, usize)>>>);

struct FilterSet {
    year_atts: Vec<u8>,
    years: Vec<u16>,
    filter_keys: Vec<String>,
}

struct QcInput {
    bd_hiers: Arc<BdHierarcyList>,
    iid: usize,
}

struct QcMaker {
    stowage: Arc<Stowage>,
    spec_bases_vecs: Arc<Mutex<SpecPrepType>>,
    ares_map: Arc<AttributeResolverMap>,
    filter_set: Arc<FilterSet>,
    l1_var_atts_map: Arc<HashMap<String, Box<[Box<[WorkId]>]>>>,
    l2_var_atts: Arc<[Box<[WorkId]>]>,
    source_ids: Arc<[BigId]>,
}

impl BreakdownHierarchy {
    pub fn new(resolver_id: &str, levels: Vec<usize>, side: usize) -> Self {
        let entity_types: Vec<String> = get_mapped_atts(resolver_id);
        Self {
            levels,
            side,
            resolver_id: resolver_id.to_owned(),
            entity_types,
        }
    }
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

impl Worker<QcInput> for QcMaker {
    fn proc(&self, qc_in: QcInput) {
        let qc_key = &qc_in.bd_hiers.qc_id;
        let bifurcations = qc_in.bd_hiers.to_jsbifs();
        let mut qcr = QuercusRoller::new(&qc_in.bd_hiers.hiers, &self.ares_map);
        let mut qcs: Vec<Quercus> = self
            .filter_set
            .filter_keys
            .iter()
            .map(|_| Quercus::new())
            .collect();
        let l1_var_atts = &self.l1_var_atts_map[&qc_in.bd_hiers.root_type];
        for wid in l1_var_atts[qc_in.iid as usize].iter() {
            let qc_ind = self.filter_set.get_qc_ind(&wid);
            let mut qc = &mut qcs[qc_ind];

            for citing_wid in self.l2_var_atts[*wid as usize].iter() {
                let ref_path = vec![*wid, *citing_wid];
                qcr.set(ref_path);
                qc.bump(&wid);
                qcr.current_hier_index = 0;
                qcr.roll_hier(&mut qc);
            }
        }
        let mut i = qcs.len();
        let mut qc = Quercus::new();
        for to_abs in qcs.into_iter().rev().into_iter() {
            i = i - 1;
            qc.absorb(to_abs);
            qc.finalize(bifurcations.len(), &self.source_ids);
            let filter_key = &self.filter_set.filter_keys[i];
            //calc spec bases for first 2 levels only
            let root_denom = f64::from(qc.weight);
            for (child_id, child_qc) in &qc.children {
                let rate = f64::from(child_qc.weight) / root_denom;
                self.spec_bases_vecs
                    .lock()
                    .unwrap()
                    .add(&bifurcations[0], &child_id, rate);
                if bifurcations[0].resolver_id == bifurcations[1].resolver_id {
                    for (sub_child_id, sub_child_qc) in &child_qc.children {
                        let rate = f64::from(sub_child_qc.weight) / root_denom;
                        self.spec_bases_vecs.lock().unwrap().add(
                            &bifurcations[1],
                            &sub_child_id,
                            rate,
                        );
                    }
                }
            }
            self.stowage
                .write_cache_buf(
                    &QP4::from_qc(&qc, 0),
                    &format!("{}/{}/{}/{}", BUILD_LOC, filter_key, qc_key, qc_in.iid),
                )
                .unwrap();
        }
    }
}

pub fn dump_all_cache(stowage_owned: Stowage) -> io::Result<()> {
    let stowage_arc = Arc::new(stowage_owned);
    let stowage = &stowage_arc;

    println!("getting ares map");
    let ares_map = get_attribute_resolver_map(stowage);

    println!("getting var atts");
    let full_clist: Arc<[Box<[WorkId]>]> = read_var_att(stowage, vnames::TO_CITING).into();

    let mut root_works_map: HashMap<String, Box<[Box<[WorkId]>]>> = HashMap::new();

    let works_of_inst = read_var_att(stowage, vnames::I2W)
        .into_iter()
        .map(|e: Vec<WeightedEdge<WorkId>>| {
            e.iter()
                .map(|e| e.id)
                .collect::<Vec<WorkId>>()
                .into_boxed_slice()
        })
        .collect();
    root_works_map.insert(institutions::C.to_string(), works_of_inst);
    root_works_map.insert(
        authors::C.to_string(),
        read_var_att(stowage, vnames::A2W).into(),
    );
    let root_works_map = Arc::new(root_works_map);

    println!("getting work ids");

    let work_idmap = stowage.get_idmap(works::C);

    let mut source_ids_mut: Vec<BigId> = vec![0; work_idmap.current_total as usize + 1];
    for (original_id, num_id) in &work_idmap.to_map() {
        source_ids_mut[*num_id as usize] = *original_id;
    }
    let source_ids = source_ids_mut.into();

    //preps for specs and metas
    let spec_bases_vecs = SpecPrepType(HashMap::new());

    let filter_set = FilterSet::new(stowage);
    let mut js_qc_specs = HashMap::new();

    let specs_arc = Arc::new(Mutex::new(spec_bases_vecs));

    let maker = QcMaker {
        stowage: stowage_arc.clone(),
        spec_bases_vecs: specs_arc.clone(),
        ares_map: Arc::new(ares_map),
        filter_set: Arc::new(filter_set),
        l1_var_atts_map: root_works_map,
        l2_var_atts: full_clist,
        source_ids,
    };

    let mut v = Vec::new();

    let mut id_cache: HashMap<String, Vec<BigId>> = HashMap::new();
    let mut all_bd_entities: HashMap<String, HashSet<String>> = HashMap::new();
    for bdhl in get_hier_lists() {
        for bdh in &bdhl.hiers {
            for l in &bdh.levels {
                all_bd_entities
                    .entry(bdh.entity_types[*l].clone())
                    .or_insert_with(HashSet::new)
                    .insert(get_bd_description(bdh, l));
            }
        }
        js_qc_specs.insert(
            bdhl.qc_id.clone(),
            JsQcSpec {
                bifurcations: bdhl.to_jsbifs(),
                root_entity_type: bdhl.root_type.clone(),
            },
        );
        let ids = id_cache
            .entry(bdhl.root_type.clone())
            .or_insert_with(|| stowage.get_idmap(&bdhl.root_type).iter_ids(false).collect());
        let hiers_arc = Arc::new(bdhl);
        for iid in ids {
            v.push(QcInput {
                iid: *iid as usize,
                bd_hiers: hiers_arc.clone(),
            });
        }
    }
    let mut rng = rand::thread_rng();
    v.shuffle(&mut rng);

    let n_threads: usize = std::thread::available_parallelism().unwrap().into();
    maker.para_n(v.into_iter().tqdm(), n_threads / 2);

    let locked_spec_base = &specs_arc.lock().unwrap().0;
    let mut attribute_statics: AttributeStaticMap = HashMap::new();

    for (entity, bd_descriptions) in all_bd_entities {
        //TODO: check general availibility and add it to jsconfig
        let mut entity_statics = HashMap::new();
        let names: Vec<String> = read_var_att(stowage, &get_name_name(&entity));
        let spec_base_recs = locked_spec_base
            .get(&entity)
            .expect(&format!("no spec baseilines for {}", entity));
        for eid in stowage.get_idmap(&entity).iter_ids(true) {
            let mut spec_baselines = HashMap::new();
            for res_id in &bd_descriptions {
                //-1 if not found
                let specbase_value = match spec_base_recs.get(&(eid as SmolId)) {
                    Some(res_map) => match res_map.get(res_id) {
                        Some(sv) => sv.0,
                        None => -1.0,
                    },
                    None => -1.0,
                };
                spec_baselines.insert(res_id.to_string(), specbase_value);
            }

            let e_map = AttributeStatic {
                name: names[eid as usize].clone(),
                spec_baselines,
            };
            entity_statics.insert(eid as SmolId, e_map);
        }
        attribute_statics.insert(entity.to_string(), entity_statics);
    }

    stowage.write_cache(&attribute_statics, A_STAT_PATH)?;
    stowage.write_cache(&js_qc_specs, QC_CONF)
}

fn get_bd_description(bdh: &BreakdownHierarchy, i: &usize) -> String {
    format!("{}-{}-{}", bdh.side, bdh.resolver_id, i)
}

fn update_tentry(entry: &mut (f64, usize), other: &(f64, usize)) {
    entry.1 += other.1;
    entry.0 = (entry.0 * ((entry.1 - 1) as f64) + other.0) / (entry.1 as f64);
}
