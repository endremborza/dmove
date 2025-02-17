use core::panic;
use std::{
    collections::VecDeque,
    fmt::{Debug, Display},
    marker::PhantomData,
    path::PathBuf,
    str::FromStr,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
    u8, vec,
};

use dmove_macro::impl_subs;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use rankless_rs::{
    env_consts::START_YEAR,
    gen::a1_entity_mapping::Works,
    steps::{
        a1_entity_mapping::{POSSIBLE_YEAR_FILTERS, YBT},
        derive_links1::WorkPeriods,
    },
};

use dmove::{BigId, Entity, InitEmpty, ET};

use crate::{ids::get_atts, instances::TreeGetter, interfacing::Getters, AttributeLabelUnion};

pub type WT = ET<Works>;
pub type WorkCiteT = u32;

pub type TreeSpecMap = HashMap<String, Vec<TreeSpec>>;
pub type AttributeLabels = HashMap<String, HashMap<usize, AttributeLabelOut>>;
pub type CollapsedNode = CollapsedNodeGen<WT>;
pub type CollapsedNodeJson = CollapsedNodeGen<Option<BigId>>;
pub type CacheMap = HashMap<CacheKey, CacheValue>;

pub type ResCvp = Arc<(Mutex<Option<TreeResponse>>, Condvar)>;
pub type BoolCvp = Arc<(Mutex<bool>, Condvar)>;
type BasisQuElem = (Option<FullTreeQuery>, ResCvp);
type BasisCvp = Arc<(Mutex<VecDeque<BasisQuElem>>, Condvar)>;

pub struct TreeBasisState {
    pub gets: Getters,
    pub att_union: AttributeLabelUnion,
    pub im_cache: Mutex<CacheMap>,
}

pub struct TreeRunManager<T> {
    state: Arc<TreeBasisState>,
    pub specs: TreeSpecs,
    semantic_id_maps: HashMap<String, HashMap<String, usize>>,
    thread_pool: Vec<JoinHandle<()>>,
    cv_pair: BasisCvp,
    p: PhantomData<T>,
}

#[derive(Eq, Hash, PartialEq, Clone)]
pub struct CacheKey {
    pub etype: u8,
    pub eid: usize,
    pub tid: u8,
}

pub struct FullTreeQuery {
    pub q: TreeQ,
    pub ck: CacheKey,
    pub period: u8,
    pub name: String,
}

#[derive(Clone, Copy)]
pub struct WorkWInd(pub WT, pub WorkCiteT);

#[derive(Serialize, Clone)]
pub struct AttributeLabel {
    pub name: String,
    #[serde(rename = "specBaseline")]
    pub spec_baseline: f64,
}

#[derive(Serialize, Clone)]
pub struct AttributeLabelOut {
    pub name: String,
    #[serde(rename = "specBaseline")]
    pub spec_baseline: f64,
    #[serde(skip_serializing_if = "Option::is_none", rename = "oaId")]
    pub oa_id: Option<BigId>,
}

#[derive(Deserialize, Clone)]
pub struct TreeQ {
    pub year: Option<u16>,
    pub tid: Option<u8>,
    pub connections: Option<String>,
    pub big: Option<bool>,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct CollapsedNodeGen<T> {
    #[serde(rename = "linkCount")]
    pub link_count: u32,
    #[serde(rename = "sourceCount")]
    pub source_count: u32,
    #[serde(rename = "topSourceId")]
    pub top_source: T,
    #[serde(rename = "topSourceLinks")]
    pub top_cite_count: u32,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct BufSerTree {
    pub node: CollapsedNode,
    pub children: Box<BufSerChildren>,
}

#[derive(Serialize, Clone)]
pub struct JsSerTree {
    #[serde(flatten)]
    pub node: CollapsedNodeJson,
    pub children: Box<JsSerChildren>,
}

#[derive(Serialize)]
pub struct TreeResponse {
    pub tree: JsSerTree,
    pub atts: AttributeLabels,
}

#[derive(Serialize)]
pub struct TreeSpecs {
    #[serde(skip_serializing)]
    root_types: Vec<String>,
    specs: TreeSpecMap,
    #[serde(rename = "yearBreaks")]
    year_breaks: YBT,
}

#[derive(Serialize)]
pub struct TreeSpec {
    #[serde(rename = "rootType")]
    pub root_type: String,
    pub breakdowns: Vec<BreakdownSpec>,
    #[serde(rename = "defaultIsSpec")]
    pub is_spec: bool,
    #[serde(rename = "allowSpec")]
    pub allow_spec: bool,
    #[serde(rename = "defaultYear")]
    pub default_partition: u16,
}

#[derive(Serialize)]
pub struct BreakdownSpec {
    #[serde(rename = "attributeType")]
    pub attribute_type: String,
    #[serde(rename = "specDenomInd")] //this is to know how deep to go back for spec calculation
    //e.g a country->inst is the same resolver
    pub spec_denom_ind: u8,
    // description: String, // used to be for spec calculation -> separate for different kinds of
    // breakdowns
    #[serde(rename = "sourceSide")]
    pub source_side: bool,
}

pub struct SCIter<'a> {
    children: &'a BufSerChildren,
    key_iter: vec::IntoIter<&'a u32>,
}

pub enum CacheValue {
    InProgress(BoolCvp),
    Done(Vec<u8>),
}

#[derive(Serialize, Deserialize, Clone)]
pub enum BufSerChildren {
    Leaves(HashMap<u32, CollapsedNode>),
    Nodes(HashMap<u32, BufSerTree>),
}

#[derive(Serialize, Clone)]
#[serde(untagged)]
pub enum JsSerChildren {
    Leaves(HashMap<u32, CollapsedNodeJson>),
    Nodes(HashMap<u32, JsSerTree>),
}

impl PartialEq for WorkWInd {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd for WorkWInd {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl<'a> Iterator for SCIter<'a> {
    type Item = (&'a u32, &'a CollapsedNode);
    fn next(&mut self) -> Option<Self::Item> {
        match self.key_iter.next() {
            Some(k) => {
                let v = match self.children {
                    BufSerChildren::Nodes(nodes) => &nodes[k].node,
                    BufSerChildren::Leaves(leaves) => &leaves[k],
                };
                Some((k, v))
            }
            None => None,
        }
    }
}

impl Display for FullTreeQuery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}({}:{}/{:?})",
            self.name, self.ck.eid, self.ck.tid, self.q.year
        )
    }
}

impl TreeResponse {
    pub fn from_pruned(
        pruned_tree: BufSerTree,
        fq: &FullTreeQuery,
        bds: &Vec<BreakdownSpec>,
        state: &TreeBasisState,
    ) -> Self {
        let now = std::time::Instant::now();
        let atts = get_atts(&pruned_tree, &bds, state, fq);
        println!("{fq}: got atts in {}", now.elapsed().as_millis());

        let now = std::time::Instant::now();
        let tree = JsSerTree::from_buf(pruned_tree, &state.gets);
        println!("{fq}: converted in {}", now.elapsed().as_millis());
        Self { tree, atts }
    }
}

impl TreeSpecs {
    pub fn new(spec_kvs: Vec<(String, Vec<TreeSpec>)>) -> Self {
        let root_types = spec_kvs.iter().map(|e| e.0.clone()).collect();
        let specs = HashMap::from_iter(spec_kvs.into_iter());
        Self {
            root_types,
            specs,
            year_breaks: POSSIBLE_YEAR_FILTERS,
        }
    }

    pub fn to_eid(&self, name: &String) -> Option<u8> {
        for (i, e) in self.root_types.iter().enumerate() {
            if name == e {
                return Some(i as u8);
            }
        }
        None
    }
}

impl CollapsedNode {
    pub fn ingest_disjunct(&mut self, o: &Self) {
        if o.top_cite_count > self.top_cite_count {
            self.top_source = o.top_source;
            self.top_cite_count = o.top_cite_count;
        }
        self.link_count += o.link_count;
        self.source_count += o.source_count;
    }

    pub fn update_with_wt(&mut self, wwind: &WorkWInd) {
        let ul = wwind.1;
        if ul > self.top_cite_count {
            self.top_source = wwind.0;
            self.top_cite_count = ul;
        }
        self.link_count += ul;
        self.source_count += 1;
    }
}

impl BufSerTree {
    pub fn ingest_disjunct(&mut self, other: Self) {
        use BufSerChildren::*;
        self.node.ingest_disjunct(&other.node);
        match self.children.as_mut() {
            Nodes(nodes) => match *other.children {
                Nodes(other_nodes) => {
                    for (ok, ov) in other_nodes {
                        match nodes.get_mut(&ok) {
                            Some(my_v) => my_v.ingest_disjunct(ov),
                            None => {
                                nodes.insert(ok, ov);
                            }
                        }
                    }
                }
                Leaves(_) => panic!("non matching trees"),
            },
            Leaves(leaves) => match *other.children {
                Leaves(other_leaves) => {
                    for (ok, ov) in other_leaves {
                        match leaves.get_mut(&ok) {
                            Some(my_v) => my_v.ingest_disjunct(&ov),
                            None => {
                                leaves.insert(ok, ov);
                            }
                        }
                    }
                }
                Nodes(_) => panic!("non matching trees"),
            },
        }
    }
}

impl BufSerChildren {
    pub fn iter_items<'a>(&'a self) -> SCIter<'a> {
        let key_vec: Vec<&'a u32> = match self {
            Self::Nodes(nodes) => nodes.keys().collect(),
            Self::Leaves(ls) => ls.keys().collect(),
        };
        SCIter {
            children: self,
            key_iter: key_vec.into_iter(),
        }
    }
}

impl JsSerTree {
    pub fn from_buf(value: BufSerTree, gets: &Getters) -> Self {
        let children = JsSerChildren::from_buf(*value.children, gets);
        Self {
            node: oaify(value.node, gets),
            children: Box::new(children),
        }
    }
}

impl JsSerChildren {
    fn from_buf(value: BufSerChildren, gets: &Getters) -> Self {
        //TODO: this is wasteful
        match value {
            BufSerChildren::Nodes(nodes) => Self::Nodes(HashMap::from_iter(
                nodes
                    .into_iter()
                    .map(|(k, v)| (k, JsSerTree::from_buf(v, gets))),
            )),
            BufSerChildren::Leaves(leaves) => Self::Leaves(HashMap::from_iter(
                leaves.into_iter().map(|(k, v)| (k, oaify(v, gets))),
            )),
        }
    }
}

pub trait RunManagerSub {
    fn fill_res_cvp(state: &TreeBasisState, fq: FullTreeQuery, res_cvp: ResCvp);
    fn get_specs() -> TreeSpecs;
    fn make_fq(
        q: TreeQ,
        eid: usize,
        root_type: &String,
        specs: &TreeSpecs,
    ) -> Option<FullTreeQuery> {
        let etype = specs.to_eid(root_type)?;
        let ck = CacheKey {
            etype,
            tid: q.tid.unwrap_or(0),
            eid,
        };
        let period = WorkPeriods::from_year(q.year.unwrap_or(START_YEAR));
        let fq = FullTreeQuery {
            ck,
            q,
            period,
            name: root_type.to_string(),
        };
        Some(fq)
    }
}

// make this a derive trait for some struct
impl_subs!(5);
impl_subs!(2);

impl<T> TreeRunManager<T>
where
    T: RunManagerSub,
{
    pub fn new(
        gets: Arc<Getters>,
        atts: Arc<Mutex<AttributeLabelUnion>>,
        maps: HashMap<String, HashMap<String, usize>>,
        n: usize,
    ) -> Arc<Self> {
        let specs = T::get_specs();
        let att_union = Arc::into_inner(atts).unwrap().into_inner().unwrap();
        let thread_pool = Vec::new();
        let mut state =
            TreeBasisState::new(Arc::into_inner(gets).expect("gets for state"), att_union);
        state.fill_cache(&specs);

        Arc::new(
            Self {
                state: Arc::new(state),
                thread_pool,
                specs,
                semantic_id_maps: maps,
                cv_pair: BasisCvp::init_empty(),
                p: PhantomData,
            }
            .fill_thread_pool(n),
        )
    }

    pub fn get_resp(
        &self,
        q: TreeQ,
        root_type: &String,
        semantic_id: &String,
    ) -> Option<TreeResponse> {
        let res_cvp = ResCvp::init_empty();
        let eid = self.semantic_id_maps.get(root_type)?.get(semantic_id)?;
        let fq = T::make_fq(q, *eid, root_type, &self.specs)?;
        self.add_to_queue(Some(fq), res_cvp.clone());
        {
            let (lock, cvar) = &*res_cvp;
            let mut out = lock.lock().unwrap();
            while out.is_none() {
                out = cvar.wait(out).unwrap();
            }
            return std::mem::replace(&mut out, None);
        }
    }

    pub fn join(self) {
        for _ in 0..self.thread_pool.len() {
            self.add_to_queue(None, ResCvp::init_empty());
        }
        for t in self.thread_pool.into_iter() {
            t.join().unwrap();
        }
    }

    pub fn fake() -> Arc<Self> {
        let gets = Getters::fake();
        let atts = Mutex::new(HashMap::new());
        let tm = HashMap::from_iter(vec![("0".to_string(), 0)].into_iter());
        let maps = HashMap::from_iter(vec![("test".to_string(), tm)].into_iter());
        Self::new(Arc::new(gets), atts.into(), maps, 2)
    }

    fn add_to_queue(&self, fq: Option<FullTreeQuery>, res_cvp: ResCvp) {
        let (lock, cvar) = &*self.cv_pair;
        let mut data = lock.lock().unwrap();
        data.push_back((fq, res_cvp));
        cvar.notify_all();
    }

    fn fill_thread_pool(mut self, n: usize) -> Self {
        for _ in 0..n {
            let shared_cvp = Arc::clone(&self.cv_pair);
            let shared_state = self.state.clone();
            let thread = std::thread::spawn(move || loop {
                let (fqo, res_cvp) = Self::get_q_cvp(shared_cvp.clone());
                match fqo {
                    Some(fq) => T::fill_res_cvp(&shared_state, fq, res_cvp),
                    None => break,
                }
            });
            self.thread_pool.push(thread);
        }
        self
    }

    fn get_q_cvp(shared_cvp: BasisCvp) -> BasisQuElem {
        let (lock, cvar) = &*shared_cvp;
        let mut data = lock.lock().unwrap();
        while data.len() == 0 {
            data = cvar.wait(data).unwrap();
        }
        return data.pop_back().unwrap();
    }
}

impl TreeBasisState {
    pub fn new(gets: Getters, att_union: AttributeLabelUnion) -> Self {
        let im_map = HashMap::new();
        Self {
            gets,
            att_union,
            im_cache: Mutex::new(im_map),
        }
    }

    pub fn full_cache_file(&self, fq: &FullTreeQuery) -> PathBuf {
        self.full_cache_file_period(fq, fq.period)
    }

    pub fn full_cache_file_period(&self, fq: &FullTreeQuery, period: u8) -> PathBuf {
        self.cache_dir(fq).join(format!("{}.gz", period))
    }

    pub fn pruned_cache_file(&self, fq: &FullTreeQuery) -> PathBuf {
        self.pruned_cache_file_period(fq, fq.period)
    }
    pub fn pruned_cache_file_period(&self, fq: &FullTreeQuery, period: u8) -> PathBuf {
        self.cache_dir(fq).join(format!("pruned-{}.gz", period))
    }
    pub fn fake() -> Self {
        Self {
            im_cache: Mutex::new(HashMap::new()),
            gets: Getters::fake(),
            att_union: HashMap::new(),
        }
    }

    fn cache_dir(&self, fq: &FullTreeQuery) -> PathBuf {
        self.rt_cache_dir(&fq.name)
            .join(fq.ck.eid.to_string())
            .join(fq.ck.tid.to_string())
    }

    fn rt_cache_dir(&self, rt: &str) -> PathBuf {
        self.gets.stowage.paths.cache.join(rt)
    }

    fn fill_cache(&mut self, specs: &TreeSpecs) {
        let mut cmap = self.im_cache.lock().unwrap();
        for (k, _) in &specs.specs {
            let rt_cdir = self.rt_cache_dir(&k);
            let etype = match specs.to_eid(k) {
                None => continue,
                Some(e) => e,
            };
            if rt_cdir.exists() {
                for eid_entry in std::fs::read_dir(rt_cdir).unwrap() {
                    let eid_path = eid_entry.unwrap().path();
                    for tid_entry in std::fs::read_dir(&eid_path).unwrap() {
                        let tid_path = tid_entry.unwrap().path();
                        let ck = CacheKey {
                            eid: fpparse(&eid_path),
                            tid: fpparse(&tid_path),
                            etype,
                        };
                        let mut v = Vec::new();
                        for pid_entry in std::fs::read_dir(&tid_path).unwrap() {
                            let pid_path = pid_entry.unwrap().path();
                            if pid_path
                                .file_stem()
                                .unwrap()
                                .to_str()
                                .unwrap()
                                .starts_with("pru")
                            {
                                continue;
                            }

                            v.push(fpparse(&pid_path));
                        }
                        cmap.insert(ck, CacheValue::Done(v));
                    }
                }
            }
        }
    }
}

fn fpparse<T: FromStr>(p: &PathBuf) -> T
where
    <T as FromStr>::Err: Debug,
{
    p.file_stem().unwrap().to_str().unwrap().parse().unwrap()
}

fn oaify(node: CollapsedNode, gets: &Getters) -> CollapsedNodeJson {
    CollapsedNodeGen {
        top_source: gets.work_oa.get(node.top_source as usize).copied(),
        link_count: node.link_count,
        source_count: node.source_count,
        top_cite_count: node.top_cite_count,
    }
}
