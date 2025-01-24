use std::fs::create_dir_all;

use dmove_macro::derive_tree_getter;

use crate::{
    ids::{get_atts, AttributeLabelUnion},
    interfacing::{Getters, NumberedEntity, NET},
    io::{BreakdownSpec, BufSerChildren, BufSerTree, CollapsedNode, TreeQ, TreeResponse, TreeSpec},
    prune::prune,
};
use rankless_rs::{
    agg_tree::{
        ordered_calls, sorted_iters_to_arr, AggTreeBase, ExtendableArr, FoldingStackConsumer,
        HeapIterator, MinHeap, OrderedMapper, ReinstateFrom, SortedRecord, Updater,
    },
    common::{read_buf_path, write_buf_path, InitEmpty},
    env_consts::START_YEAR,
    gen::{
        a1_entity_mapping::{
            Authors, Countries, Institutions, Qs, Sources, Subfields, Topics, Works,
        },
        a2_init_atts::WorksNames,
    },
    steps::derive_links1::WorkPeriods,
};

use dmove::{Entity, NamespacedEntity, UnsignedNumber, VattReadingRefMap, ET};
use dmove_macro::derive_tree_maker;
use hashbrown::HashMap;

const UNKNOWN_ID: usize = 0;

type FrTm<TM> = <<TM as TreeMaker>::SortedRec as SortedRecord>::FlatRecord;
type CollT<T> = <T as Collapsing>::Collapsed;

type WT = ET<Works>;
type WorkCiteT = u32;

#[derive(PartialOrd, PartialEq)]
struct WorkTree(AggTreeBase<WT, (), WT>);

struct IntXTree<E: NumberedEntity, C: Collapsing>(AggTreeBase<NET<E>, PrepNode, CollT<C>>);
struct DisJTree<E: NumberedEntity, C: Collapsing>(AggTreeBase<NET<E>, CollapsedNode, CollT<C>>);

pub struct DisJ<E: Entity, const N: usize, const S: bool>(E::T);
pub struct IntX<E: Entity, const N: usize, const S: bool>(E::T);

struct IddCollNode<E: NumberedEntity> {
    id: NET<E>,
    node: CollapsedNode,
}

#[derive(Clone, Copy)]
struct WorkWInd(WT, WorkCiteT);

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

const SOURCE_BUF_SIZE: usize = 0x200;
const TARGET_BUF_SIZE: usize = 0x500;

struct WVecPair {
    // pub sources: StackWExtension<SOURCE_BUF_SIZE, WorkWInd>,
    // pub targets: StackWExtension<TARGET_BUF_SIZE, WT>,
    pub sources: Vec<WorkWInd>,
    pub targets: Vec<WT>,
}

struct PrepNode {
    pub merge_into: WVecPair,
    pub merge_from: WVecPair,
}

struct WVecMerger<'a> {
    left_i: usize,
    left_from: &'a WVecPair,
    right_i: usize,
    right_from: &'a WVecPair,
    wv_into: &'a mut WVecPair,
    node: CollapsedNode,
}

struct StackWExtension<const L: usize, T> {
    buf: [T; L],
    blen: usize,
    vec: Vec<T>,
}

pub trait TreeGetter: NumberedEntity {
    #[allow(unused_variables)]
    fn get_tree(gets: &Getters, att_union: &AttributeLabelUnion, q: TreeQ) -> Option<TreeResponse> {
        None
    }

    fn get_specs() -> Vec<TreeSpec>;
}

trait Collapsing {
    type Collapsed;
    //can maybe be merged with reinstate with
    fn collapse(&mut self) -> Self::Collapsed;
}

trait TreeMaker {
    type StackBasis;
    type SortedRec: SortedRecord;
    type Root: NumberedEntity;
    type Stack;
    type RootTree: Into<BufSerTree>;

    fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>>;

    fn get_root_tree<I>(id: NET<Self::Root>, it: I) -> Self::RootTree
    where
        I: Iterator<Item = Self::SortedRec>;

    fn get_spec() -> TreeSpec;

    fn tree_resp(q: TreeQ, gets: &Getters, stats: &AttributeLabelUnion) -> TreeResponse
    where
        <Self::SortedRec as SortedRecord>::FlatRecord: Ord + Clone,
    {
        let tid = q.tid.unwrap_or(0);
        let eid = <NET<Self::Root> as UnsignedNumber>::from_usize(q.eid.to_usize());
        let req_id = format!("{}({}:{})", Self::Root::NAME, eid, tid);
        println!("requested entity: {req_id}");
        let _year = q.year.unwrap_or(START_YEAR);
        let year = "X"; //TODO
        let tree_path = gets
            .stowage
            .paths
            .cache
            .join(Self::Root::NAME)
            .join(q.eid.to_string())
            .join(tid.to_string())
            .join(year.to_string());
        let mut full_tree: BufSerTree = if !tree_path.exists() {
            let now = std::time::Instant::now();
            let heap = Self::get_heap(eid, gets);
            println!("{req_id}: got heap in {}", now.elapsed().as_millis());
            let hither: HeapIterator<Self::SortedRec> = heap.into();

            let now = std::time::Instant::now();
            let root = Self::get_root_tree(eid, hither);
            println!("{req_id}: got root in {}", now.elapsed().as_millis());

            let now = std::time::Instant::now();
            let ser_tree: BufSerTree = root.into();
            println!("{req_id}: converted tree in {}", now.elapsed().as_millis());

            let year_parent = tree_path.parent().unwrap();
            create_dir_all(year_parent).unwrap();
            for _period_id in 0..WorkPeriods::N {
                let _obj = &ser_tree;
                // write_buf_path(obj, year_parent.join(year16.to_string())).unwrap();
            }

            let now = std::time::Instant::now();
            write_buf_path(&ser_tree, year_parent.join(year.to_string())).unwrap();
            println!("{req_id}: wrote tree in {}", now.elapsed().as_millis());
            ser_tree
        } else {
            read_buf_path(tree_path).unwrap()
        };

        let now = std::time::Instant::now();
        let bds = Self::get_spec().breakdowns;
        prune(&mut full_tree, stats, &bds);
        println!("{req_id}: pruned in {}", now.elapsed().as_millis());

        let parent = gets
            .stowage
            .path_from_ns(<WorksNames as NamespacedEntity>::NS);
        let mut work_name_basis =
            VattReadingRefMap::<WorksNames>::from_locator(&gets.wn_locators, &parent);

        let now = std::time::Instant::now();
        let atts = get_atts(&full_tree, &bds, stats, &mut work_name_basis);
        println!("{req_id}: got atts in {}", now.elapsed().as_millis());

        let now = std::time::Instant::now();
        let tree = full_tree.into();
        println!("{req_id}: converted in {}", now.elapsed().as_millis());
        TreeResponse { tree, atts }
    }
}

trait FoldStackBase<C> {
    type StackElement;
    type LevelEntity: Entity;
    const SPEC_DENOM_IND: usize;
    const SOURCE_SIDE: bool;
}

trait TopTree {}

impl<const L: usize, T> StackWExtension<L, T>
where
    T: InitEmpty + Copy + Clone,
{
    fn new() -> Self {
        Self {
            buf: [T::init_empty(); L],
            vec: Vec::new(),
            blen: 0,
        }
    }

    fn reset(&mut self) {
        unsafe { self.vec.set_len(0) }
        self.blen = 0;
    }

    fn get(&self, ind: usize) -> &T {
        if ind >= self.buf.len() {
            &self.vec[ind - self.blen]
        } else {
            &self.buf[ind]
        }
    }

    fn len(&self) -> usize {
        self.blen + self.vec.len()
    }
}

impl WVecPair {
    fn new() -> Self {
        Self {
            // sources: StackWExtension::new(),
            // sources: StackWExtension::new(),
            targets: Vec::new(),
            sources: Vec::new(),
        }
    }
    fn reset(&mut self) {
        unsafe {
            self.sources.set_len(0);
            self.targets.set_len(0);
        }
        // self.sources.reset();
        // self.targets.reset();
    }

    fn add(&mut self, e: &WorkWInd, other: &Self, other_tind: usize) {
        self.sources.add(*e);
        for i in 0..e.1.to_usize() {
            let ind = other_tind + i;
            // let val = other.targets.get(ind);
            let val = other.targets[ind];
            self.targets.add(val);
        }
    }
}

impl<'a> WVecMerger<'a> {
    fn new(wv_into: &'a mut WVecPair, left_from: &'a WVecPair, right_from: &'a WVecPair) -> Self {
        Self {
            left_i: 0,
            right_i: 0,
            node: CollapsedNode::init_empty(),
            wv_into,
            left_from,
            right_from,
        }
    }
}

impl CollapsedNode {
    fn ingest_disjunct(&mut self, o: &Self) {
        if o.top_cite_count > self.top_cite_count {
            self.top_source = o.top_source;
            self.top_cite_count = o.top_cite_count;
        }
        self.link_count += o.link_count;
        self.source_count += o.source_count;
    }

    fn update_with_wt(&mut self, wwind: &WorkWInd) {
        let ul = wwind.1;
        if ul > self.top_cite_count {
            self.top_source = wwind.0;
            self.top_cite_count = ul;
        }
        self.link_count += ul;
        self.source_count += 1;
    }
}

impl<T, const L: usize> ExtendableArr<T> for StackWExtension<L, T> {
    fn add(&mut self, e: T) {
        if self.blen == self.buf.len() {
            self.vec.push(e)
        } else {
            self.buf[self.blen] = e;
            self.blen += 1;
        }
    }
}

impl OrderedMapper<WorkWInd> for WVecMerger<'_> {
    type Elem = WorkWInd;
    fn common_map(&mut self, l: &Self::Elem, r: &Self::Elem) {
        self.node.update_with_wt(r);
        let last_len = self.wv_into.targets.len() as u32;
        let left_it =
            (self.left_i..(self.left_i + (l.1 as usize))).map(|i| &self.left_from.targets[i]);
        let right_it =
            (self.right_i..(self.right_i + (r.1 as usize))).map(|i| &self.right_from.targets[i]);

        self.left_i += l.1 as usize;
        self.right_i += r.1 as usize;

        sorted_iters_to_arr(&mut self.wv_into.targets, left_it, right_it);
        let total_targets = self.wv_into.targets.len() as u32 - last_len;
        self.wv_into.sources.add(WorkWInd(l.0, total_targets));
    }

    fn left_map(&mut self, e: &Self::Elem) {
        self.wv_into.add(e, self.left_from, self.left_i);
        self.left_i += e.1.to_usize();
    }

    fn right_map(&mut self, e: &Self::Elem) {
        self.node.update_with_wt(e);
        self.wv_into.add(e, self.right_from, self.right_i);
        self.right_i += e.1.to_usize();
    }
}

impl PrepNode {
    fn update_and_get_collapsed_node(&mut self, other: &mut Self) -> CollapsedNode {
        let mut merger = WVecMerger::new(&mut self.merge_into, &self.merge_from, &other.merge_from);
        //one of them to
        let left_it = (0..self.merge_from.sources.len()).map(|e| &self.merge_from.sources[e]);
        let right_it = (0..other.merge_from.sources.len()).map(|e| &other.merge_from.sources[e]);
        ordered_calls(left_it, right_it, &mut merger);
        let node = merger.node;
        std::mem::swap(&mut self.merge_into, &mut self.merge_from);
        self.merge_into.reset();
        node
    }

    fn take_new(&mut self, work_tree: &WorkTree) {
        let top = WorkWInd(work_tree.0.id, work_tree.0.children.len() as WorkCiteT);
        self.merge_from.sources.add(top);
        work_tree
            .0
            .children
            .iter()
            .for_each(|e| self.merge_from.targets.add(*e))
    }

    fn reset(&mut self) {
        self.merge_from.reset();
        self.merge_into.reset();
    }
}

//WHY????
// type WhyT = u16;
type WhyT = u32;

impl From<WhyT> for WorkTree {
    fn from(value: WhyT) -> Self {
        Self(value.into())
    }
}

impl<T, E, C> From<T> for DisJTree<E, C>
where
    T: UnsignedNumber,
    E: NumberedEntity<T = T>,
    C: Collapsing,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl<T, E, C> From<T> for IntXTree<E, C>
where
    T: UnsignedNumber,
    E: NumberedEntity<T = T>,
    C: Collapsing,
{
    fn from(value: T) -> Self {
        Self(value.into())
    }
}

impl<E, CE, GC> Into<BufSerTree> for DisJTree<E, IntXTree<CE, GC>>
where
    E: NumberedEntity,
    CE: NumberedEntity,
    GC: Collapsing + TopTree,
    DisJTree<CE, GC>: Into<BufSerTree>,
{
    fn into(self) -> BufSerTree {
        let mut map = HashMap::new();
        for child in self.0.children {
            map.insert(child.0.id.to_usize() as u32, child.into());
        }
        let children = Box::new(BufSerChildren::Nodes(map));
        BufSerTree {
            node: self.0.node,
            children,
        }
    }
}

impl<E, CE> Into<BufSerTree> for DisJTree<E, IntXTree<CE, WorkTree>>
where
    E: NumberedEntity,
    CE: NumberedEntity,
{
    fn into(self) -> BufSerTree {
        let children = Box::new(self.0.children.into());
        BufSerTree {
            node: self.0.node,
            children,
        }
    }
}

impl<E> Into<BufSerChildren> for Vec<IddCollNode<E>>
where
    E: NumberedEntity,
{
    fn into(self) -> BufSerChildren {
        let mut leaves = HashMap::new();
        for leaf in self.into_iter() {
            leaves.insert(leaf.id.to_usize() as u32, leaf.node);
        }
        BufSerChildren::Leaves(leaves)
    }
}

impl<E, C> TopTree for IntXTree<E, C>
where
    E: NumberedEntity,
    C: Collapsing,
{
}

impl<E, C> TopTree for DisJTree<E, C>
where
    E: NumberedEntity,
    C: Collapsing,
    Vec<CollT<C>>: Into<BufSerChildren>,
{
}

impl<E, C> ReinstateFrom<NET<E>> for IntXTree<E, C>
where
    E: NumberedEntity,
    C: Collapsing,
{
    fn reinstate_from(&mut self, value: NET<E>) {
        self.0.id = value;
        self.0.node.reset();
    }
}

impl<E, C> ReinstateFrom<NET<E>> for DisJTree<E, C>
where
    E: NumberedEntity,
    C: Collapsing,
{
    fn reinstate_from(&mut self, value: NET<E>) {
        self.0.id = value;
    }
}

impl ReinstateFrom<WT> for WorkTree {
    fn reinstate_from(&mut self, value: WT) {
        self.0.id = value;
        unsafe {
            self.0.children.set_len(0);
        }
    }
}

//TODO - low-prio - these could be derived
impl InitEmpty for CollapsedNode {
    fn init_empty() -> Self {
        Self {
            link_count: 0,
            source_count: 0,
            top_source: 0,
            top_cite_count: 0,
        }
    }
}

impl InitEmpty for PrepNode {
    fn init_empty() -> Self {
        Self {
            merge_from: WVecPair::new(),
            merge_into: WVecPair::new(),
        }
    }
}

impl InitEmpty for WorkWInd {
    fn init_empty() -> Self {
        Self(0, 0)
    }
}

impl<E> Updater<WorkTree> for IntXTree<E, WorkTree>
where
    E: NumberedEntity,
{
    fn update<T>(&mut self, other: &mut WorkTree, other_reinitiator: T)
    where
        WorkTree: ReinstateFrom<T>,
    {
        self.0.node.take_new(other);
        other.reinstate_from(other_reinitiator);
    }
}

impl<E, CE> Updater<IntXTree<CE, WorkTree>> for IntXTree<E, IntXTree<CE, WorkTree>>
where
    E: NumberedEntity,
    CE: NumberedEntity,
{
    fn update<T>(&mut self, other: &mut IntXTree<CE, WorkTree>, other_reinitiator: T)
    where
        IntXTree<CE, WorkTree>: ReinstateFrom<T>,
    {
        //no need to keep grancchildren here
        let node = self.0.node.update_and_get_collapsed_node(&mut other.0.node);
        let id = other.0.id;
        other.reinstate_from(other_reinitiator);
        let iddc = IddCollNode { id, node };
        self.0.children.push(iddc)
    }
}

impl<E, CE, GC> Updater<IntXTree<CE, GC>> for IntXTree<E, IntXTree<CE, GC>>
where
    E: NumberedEntity,
    CE: NumberedEntity,
    GC: Collapsing + TopTree,
{
    fn update<T>(&mut self, other: &mut IntXTree<CE, GC>, other_reinitiator: T)
    where
        IntXTree<CE, GC>: ReinstateFrom<T>,
    {
        let node = self.0.node.update_and_get_collapsed_node(&mut other.0.node);
        let id = other.0.id;
        //this does the site reduction
        let new_grandchildren = Vec::new();
        let children = std::mem::replace(&mut other.0.children, new_grandchildren);
        other.reinstate_from(other_reinitiator);
        let gc = DisJTree(AggTreeBase { id, node, children });
        self.0.children.push(gc);
    }
}

impl<CE, E> Updater<IntXTree<CE, WorkTree>> for DisJTree<E, IntXTree<CE, WorkTree>>
where
    E: NumberedEntity,
    CE: NumberedEntity,
{
    fn update<T>(&mut self, other: &mut IntXTree<CE, WorkTree>, other_reinitiator: T)
    where
        IntXTree<CE, WorkTree>: ReinstateFrom<T>,
    {
        let node = other.collapse();
        other.reinstate_from(other_reinitiator);
        self.0.node.ingest_disjunct(&node.node);
        self.0.children.push(node);
    }
}

impl<CE, E, GC> Updater<IntXTree<CE, GC>> for DisJTree<E, IntXTree<CE, GC>>
where
    E: NumberedEntity,
    CE: NumberedEntity,
    GC: Collapsing + TopTree,
{
    fn update<T>(&mut self, other: &mut IntXTree<CE, GC>, other_reinitiator: T)
    where
        IntXTree<CE, GC>: ReinstateFrom<T>,
    {
        let node = other.collapse();
        other.reinstate_from(other_reinitiator);
        self.0.node.ingest_disjunct(&node.0.node); //this little .0 is the diff
        self.0.children.push(node);
    }
}

impl Collapsing for PrepNode {
    type Collapsed = CollapsedNode;
    fn collapse(&mut self) -> Self::Collapsed {
        let mut out = CollapsedNode::init_empty();
        for i in 0..self.merge_from.sources.len() {
            out.update_with_wt(&self.merge_from.sources[i]);
        }
        out
    }
}

impl Collapsing for WorkTree {
    type Collapsed = ();
    fn collapse(&mut self) -> Self::Collapsed {}
}

impl<E> Collapsing for IntXTree<E, WorkTree>
where
    E: NumberedEntity,
{
    type Collapsed = IddCollNode<E>;
    fn collapse(&mut self) -> Self::Collapsed {
        Self::Collapsed {
            id: self.0.id,
            node: self.0.node.collapse(),
        }
    }
}

impl<E, C> Collapsing for IntXTree<E, C>
where
    E: NumberedEntity,
    C: TopTree + Collapsing,
{
    type Collapsed = DisJTree<E, C>;
    fn collapse(&mut self) -> Self::Collapsed {
        // println!(
        //     "\n\nCOLLAPSING IJ: {} len: {}, node {} {} {} {}\n\n",
        //     self.0.id,
        //     self.0.children.len(),
        //     self.0.node.merge_from.sources.len(),
        //     self.0.node.merge_from.targets.len(),
        //     self.0.node.merge_into.sources.len(),
        //     self.0.node.merge_into.targets.len(),
        // );
        let children = std::mem::replace(&mut self.0.children, Vec::new());
        DisJTree(AggTreeBase {
            id: self.0.id,
            node: self.0.node.collapse(),
            children,
        })
    }
}

impl<E, C> Collapsing for DisJTree<E, C>
where
    E: NumberedEntity,
    C: Collapsing,
{
    type Collapsed = Self;
    fn collapse(&mut self) -> Self::Collapsed {
        let children = std::mem::replace(&mut self.0.children, Vec::new());
        let node = std::mem::replace(&mut self.0.node, CollapsedNode::init_empty());
        DisJTree(AggTreeBase {
            id: self.0.id,
            node,
            children,
        })
    }
}

impl<C> FoldStackBase<C> for WorkTree {
    type StackElement = WorkTree;
    type LevelEntity = Works;
    const SOURCE_SIDE: bool = true;
    const SPEC_DENOM_IND: usize = 0;
}

impl<E, C, const N: usize, const S: bool> FoldStackBase<C> for IntX<E, N, S>
where
    E: NumberedEntity,
    C: Collapsing,
{
    type StackElement = IntXTree<E, C>;
    type LevelEntity = E;
    const SPEC_DENOM_IND: usize = N;
    const SOURCE_SIDE: bool = S;
}

impl<E, C, const N: usize, const S: bool> FoldStackBase<C> for DisJ<E, N, S>
where
    E: NumberedEntity,
    C: Collapsing,
{
    type StackElement = DisJTree<E, C>;
    type LevelEntity = E;
    const SPEC_DENOM_IND: usize = N;
    const SOURCE_SIDE: bool = S;
}

impl FoldingStackConsumer for WorkTree {
    type Consumable = WT;
    fn consume(&mut self, child: Self::Consumable) {
        self.0.children.push(child);
    }
}

fn to_bds<T>() -> BreakdownSpec
where
    T: FoldStackBase<WorkTree>,
{
    BreakdownSpec {
        attribute_type: T::LevelEntity::NAME.to_string(),
        spec_denom_ind: T::SPEC_DENOM_IND as u8,
        source_side: T::SOURCE_SIDE,
    }
}

mod iterators {

    use super::*;

    pub struct SuCoInstIter<'a> {
        // ref_sfs: &'a Box<[ET<Subfields>]>,
        ref_sfs: &'a [ET<Subfields>],
        rsfi: usize,
        // cit_wids: &'a Box<[ET<Works>]>,
        cit_wids: &'a [ET<Works>],
        cwidi: usize,
        // cit_insts: Option<&'a Box<[ET<Institutions>]>>,
        cit_insts: Option<&'a [ET<Institutions>]>,
        cinsti: usize,
        gets: &'a Getters,
    }

    impl<'a> SuCoInstIter<'a> {
        pub fn new(refed_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
            let ref_sfs = gets.subfield(refed_wid);
            let cit_wids = gets.citing(refed_wid);
            Self {
                ref_sfs,
                rsfi: 0,
                cit_wids,
                cwidi: 0,
                cit_insts: None,
                cinsti: 0,
                gets,
            }
        }
    }

    impl<'a> Iterator for SuCoInstIter<'a> {
        type Item = (ET<Subfields>, ET<Countries>, ET<Institutions>, ET<Works>);

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                if self.rsfi >= self.ref_sfs.len() {
                    return None;
                }
                if self.cwidi >= self.cit_wids.len() {
                    self.cwidi = 0;
                    self.rsfi += 1;
                    continue;
                }
                let cit_inst = if let Some(cit_insts) = self.cit_insts {
                    if self.cinsti >= cit_insts.len() {
                        self.cinsti = 0;
                        self.cwidi += 1;
                        self.cit_insts = None;
                        continue;
                    }
                    cit_insts[self.cinsti]
                } else {
                    self.cit_insts = Some(self.gets.winsts(&self.cit_wids[self.cwidi]));
                    continue;
                };
                self.cinsti += 1;
                return Some((
                    self.ref_sfs[self.rsfi],
                    self.gets.icountry(&cit_inst).lift(),
                    cit_inst,
                    self.cit_wids[self.cwidi],
                ));
            }
        }
    }
}

#[derive_tree_getter(Authors)]
mod author_trees {
    use super::*;

    impl TreeMaker for Tree1 {
        type StackBasis = (
            IntX<Works, 0, true>,
            IntX<Countries, 1, false>,
            IntX<Institutions, 1, false>,
        );

        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.aworks(&id) {
                for citing_wid in gets.citing(refed_wid) {
                    for citing_inst in gets.winsts(citing_wid) {
                        let citing_country = gets.icountry(citing_inst);
                        let record = (
                            refed_wid.lift(),
                            citing_country.lift(),
                            citing_inst.lift(),
                            refed_wid.lift(),
                            citing_wid.lift(),
                        );
                        heap.push(record);
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for Tree2 {
        type StackBasis = (
            IntX<Countries, 0, false>,
            IntX<Subfields, 1, false>,
            IntX<Topics, 1, false>,
        );

        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.aworks(&id) {
                for citing_wid in gets.citing(refed_wid) {
                    for citing_inst in gets.winsts(citing_wid) {
                        let citing_country = gets.icountry(citing_inst);
                        for citing_topic in gets.topic(citing_wid) {
                            let citing_subfield = gets.tsuf(citing_topic);
                            let record = (
                                citing_country.lift(),
                                citing_subfield.lift(),
                                citing_topic.lift(),
                                refed_wid.lift(),
                                citing_wid.lift(),
                            );
                            heap.push(record);
                        }
                    }
                }
            }
            heap
        }
    }
}

#[derive_tree_getter(Institutions)]
mod inst_trees {

    use super::*;

    impl TreeMaker for TreeSuSuTo {
        type StackBasis = (
            IntX<Subfields, 0, true>,
            IntX<Subfields, 1, false>,
            IntX<Topics, 1, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for refed_topic in gets.topic(refed_wid) {
                    let refed_sf = gets.tsuf(refed_topic);
                    for citing_wid in gets.citing(refed_wid) {
                        for citing_topic in gets.topic(citing_wid) {
                            let citing_sf = gets.tsuf(citing_topic);
                            let record = (
                                refed_sf.lift(),
                                citing_sf.lift(),
                                citing_topic.lift(),
                                refed_wid.lift(),
                                citing_wid.lift(),
                            );
                            heap.push(record);
                        }
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeSuSoTo {
        type StackBasis = (
            IntX<Subfields, 0, false>,
            IntX<Sources, 1, false>,
            IntX<Topics, 1, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for citing_wid in gets.citing(refed_wid) {
                    for citing_source in gets.sources(citing_wid) {
                        for citing_topic in gets.topic(citing_wid) {
                            let citing_sf = gets.tsuf(citing_topic);
                            let record = (
                                citing_sf.lift(),
                                citing_source.lift(),
                                citing_topic.lift(),
                                refed_wid.lift(),
                                citing_wid.lift(),
                            );
                            heap.push(record);
                        }
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeAuCoIn {
        type StackBasis = (
            IntX<Authors, 0, true>,
            IntX<Countries, 1, false>,
            IntX<Institutions, 1, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for refed_ship in gets.wships(refed_wid) {
                    let refed_author = gets.shipa(refed_ship);
                    if refed_author.to_usize() == UNKNOWN_ID {
                        continue;
                    }
                    for refed_ship_inst in gets.shipis(refed_ship) {
                        if refed_ship_inst != &id {
                            continue;
                        }
                        for citing_wid in gets.citing(refed_wid) {
                            for citing_inst in gets.winsts(citing_wid) {
                                let citing_country = gets.icountry(citing_inst);
                                let record = (
                                    refed_author.lift(),
                                    citing_country.lift(),
                                    citing_inst.lift(),
                                    refed_wid.lift(),
                                    citing_wid.lift(),
                                );
                                heap.push(record);
                            }
                        }
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeSoCuSu {
        type StackBasis = (
            IntX<Sources, 0, false>,
            IntX<Countries, 1, false>,
            IntX<Subfields, 2, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for citing_wid in gets.citing(refed_wid) {
                    for citing_source in gets.sources(citing_wid) {
                        for citing_sf in gets.subfield(citing_wid) {
                            for citing_inst in gets.winsts(citing_wid) {
                                let citing_country = gets.icountry(citing_inst);
                                let record = (
                                    citing_source.lift(),
                                    citing_country.lift(),
                                    citing_sf.lift(),
                                    refed_wid.lift(),
                                    citing_wid.lift(),
                                );
                                heap.push(record);
                            }
                        }
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeQSoSuCo {
        type StackBasis = (
            IntX<Qs, 0, true>,
            IntX<Sources, 0, true>,
            IntX<Subfields, 2, false>,
            IntX<Countries, 3, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                let refed_year = gets.year(refed_wid);
                for refed_source in gets.sources(refed_wid) {
                    let tup = (refed_source.lift(), refed_year.lift());
                    let refed_q = gets.sqy(&tup);
                    for citing_wid in gets.citing(refed_wid) {
                        for citing_inst in gets.winsts(citing_wid) {
                            let citing_country = gets.icountry(citing_inst);
                            for citing_subfield in gets.subfield(citing_wid) {
                                let record = (
                                    refed_q.lift(),
                                    refed_source.lift(),
                                    citing_subfield.lift(),
                                    citing_country.lift(),
                                    refed_wid.lift(),
                                    citing_wid.lift(),
                                );
                                heap.push(record);
                            }
                        }
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeCoInSuToo {
        type StackBasis = (
            IntX<Countries, 0, false>,
            IntX<Institutions, 0, false>,
            IntX<Subfields, 2, false>,
            IntX<Topics, 2, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for citing_wid in gets.citing(refed_wid) {
                    for citing_inst in gets.winsts(citing_wid) {
                        let citing_country = gets.icountry(citing_inst);
                        for citing_topic in gets.topic(citing_wid) {
                            let citing_sf = gets.tsuf(citing_topic);
                            let record = (
                                citing_country.lift(),
                                citing_inst.lift(),
                                citing_sf.lift(),
                                citing_topic.lift(),
                                refed_wid.lift(),
                                citing_wid.lift(),
                            );
                            heap.push(record);
                        }
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeCoSuIn {
        type StackBasis = (
            IntX<Countries, 0, true>,
            IntX<Subfields, 1, true>,
            IntX<Institutions, 0, true>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for refed_inst in gets.winsts(refed_wid) {
                    if refed_inst == &id {
                        continue;
                    }
                    let refed_country = gets.icountry(refed_inst);
                    for refed_sf in gets.subfield(refed_wid) {
                        for citing_wid in gets.citing(refed_wid) {
                            let record = (
                                refed_country.lift(),
                                refed_sf.lift(),
                                refed_inst.lift(),
                                refed_wid.lift(),
                                citing_wid.lift(),
                            );
                            heap.push(record);
                        }
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeSuCoInSu {
        type StackBasis = (
            IntX<Subfields, 0, true>,
            IntX<Countries, 1, false>,
            IntX<Institutions, 1, false>,
            IntX<Subfields, 3, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for refed_sf in gets.subfield(refed_wid) {
                    for citing_wid in gets.citing(refed_wid) {
                        for citing_inst in gets.winsts(citing_wid) {
                            let citing_country = gets.icountry(citing_inst);
                            for citing_topic in gets.topic(citing_wid) {
                                let citing_subfield = gets.tsuf(citing_topic);
                                let record = (
                                    refed_sf.lift(),
                                    citing_country.lift(),
                                    citing_inst.lift(),
                                    citing_subfield.lift(),
                                    refed_wid.lift(),
                                    citing_wid.lift(),
                                );
                                heap.push(record);
                            }
                        }
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeSuCoInSo {
        type StackBasis = (
            IntX<Subfields, 0, true>,
            IntX<Countries, 1, false>,
            IntX<Institutions, 1, false>,
            IntX<Sources, 3, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for (refed_sf, citing_country, citing_inst, citing_wid) in
                    iterators::SuCoInstIter::new(refed_wid, gets)
                {
                    for citing_source in gets.sources(&citing_wid) {
                        heap.push((
                            refed_sf.lift(),
                            citing_country.lift(),
                            citing_inst.lift(),
                            citing_source.lift(),
                            refed_wid.lift(),
                            citing_wid.lift(),
                        ))
                    }
                }
            }
            heap
        }
    }
}

#[derive_tree_getter(Countries)]
mod country_trees {

    use super::*;

    impl TreeMaker for TreeISuCoIn {
        type StackBasis = (
            IntX<Institutions, 0, true>,
            IntX<Subfields, 1, true>,
            IntX<Countries, 2, false>,
            IntX<Institutions, 2, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_inst in gets.country_insts(&id) {
                for refed_wid in gets.iworks(refed_inst) {
                    for (refed_sf, citing_country, citing_inst, citing_wid) in
                        iterators::SuCoInstIter::new(refed_wid, gets)
                    {
                        heap.push((
                            refed_inst.lift(),
                            refed_sf.lift(),
                            citing_country.lift(),
                            citing_inst.lift(),
                            refed_wid.lift(),
                            citing_wid.lift(),
                        ))
                    }
                }
            }
            heap
        }
    }

    //collaborate with country, inst in field

    impl TreeMaker for TreeCoInSu {
        type StackBasis = (
            IntX<Countries, 0, true>,
            IntX<Institutions, 0, true>,
            IntX<Subfields, 1, true>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for country_inst in gets.country_insts(&id) {
                for refed_wid in gets.iworks(country_inst) {
                    for refed_inst in gets.winsts(refed_wid) {
                        let refed_country = gets.icountry(refed_inst);
                        if *refed_country == id {
                            continue;
                        }
                        for refed_sf in gets.subfield(refed_wid) {
                            for citing_wid in gets.citing(refed_wid) {
                                heap.push((
                                    refed_country.lift(),
                                    refed_inst.lift(),
                                    refed_sf.lift(),
                                    refed_wid.lift(),
                                    citing_wid.lift(),
                                ))
                            }
                        }
                    }
                }
            }
            heap
        }
    }
}

#[derive_tree_getter(Sources)]
mod source_trees {

    use super::*;

    impl TreeMaker for TreeSuCoInSo {
        type StackBasis = (
            IntX<Subfields, 0, true>,
            IntX<Countries, 1, false>,
            IntX<Institutions, 1, false>,
            IntX<Sources, 3, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.soworks(&id) {
                for (refed_sf, citing_country, citing_inst, citing_wid) in
                    iterators::SuCoInstIter::new(refed_wid, gets)
                {
                    for citing_source in gets.sources(&citing_wid) {
                        heap.push((
                            refed_sf.lift(),
                            citing_country.lift(),
                            citing_inst.lift(),
                            citing_source.lift(),
                            refed_wid.lift(),
                            citing_wid.lift(),
                        ))
                    }
                }
            }
            heap
        }
    }

    impl TreeMaker for TreeSCISo {
        type StackBasis = (
            IntX<Countries, 0, true>,
            IntX<Institutions, 0, true>,
            IntX<Subfields, 2, true>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.soworks(&id) {
                for refed_inst in gets.winsts(refed_wid) {
                    let refed_country = gets.icountry(refed_inst);
                    for refed_sf in gets.subfield(refed_wid) {
                        for citing_wid in gets.citing(refed_wid) {
                            heap.push((
                                refed_country.lift(),
                                refed_inst.lift(),
                                refed_sf.lift(),
                                refed_wid.lift(),
                                citing_wid.lift(),
                            ))
                        }
                    }
                }
            }
            heap
        }
    }
}

#[derive_tree_getter(Subfields)]
mod subfield_trees {
    use super::*;

    impl TreeMaker for TreeSoCuSu {
        type StackBasis = (
            IntX<Sources, 0, true>,
            IntX<Countries, 1, true>,
            IntX<Institutions, 1, true>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.fieldworks(&id) {
                for refed_source in gets.sources(refed_wid) {
                    for refed_inst in gets.winsts(refed_wid) {
                        let refed_country = gets.icountry(refed_inst);
                        for citing_wid in gets.citing(refed_wid) {
                            let record = (
                                refed_source.lift(),
                                refed_country.lift(),
                                refed_inst.lift(),
                                refed_wid.lift(),
                                citing_wid.lift(),
                            );
                            heap.push(record);
                        }
                    }
                }
            }
            heap
        }
    }
}

pub mod big_test_tree {
    use super::*;
    use crate::io::AttributeLabel;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    struct BigTree;
    #[derive_tree_maker(Institutions)]
    impl TreeMaker for BigTree {
        type StackBasis = (
            IntX<Countries, 0, true>,
            IntX<Works, 0, true>,
            IntX<Institutions, 0, true>,
            IntX<Countries, 0, true>,
        );
        fn get_heap(id: NET<Self::Root>, _gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            let mut rng = StdRng::seed_from_u64(42);
            for _ in 0..2_u32.pow(id as u32) {
                let rec = (
                    rng.gen(),
                    rng.gen(),
                    rng.gen(),
                    rng.gen(),
                    rng.gen(),
                    rng.gen(),
                );
                heap.push(rec);
            }
            heap
        }
    }

    pub fn get_big_tree(n: usize) -> TreeResponse {
        let mut fake_attu = HashMap::new();
        let gatts = |i: u32, pref: &str| {
            (0..2_u32.pow(i))
                .map(|e| AttributeLabel {
                    name: format!("{pref}{e}"),
                    spec_baseline: 0.5,
                })
                .collect::<Box<[AttributeLabel]>>()
        };
        fake_attu.insert(Countries::NAME.to_string(), gatts(8, "C"));
        fake_attu.insert(Institutions::NAME.to_string(), gatts(16, "I"));
        let q = TreeQ {
            year: None,
            eid: n as u32,
            tid: None,
        };
        BigTree::tree_resp(q, &Getters::fake(), &fake_attu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::JsSerChildren;
    use std::ops::Deref;

    use serde_json::to_string_pretty;

    pub struct Tree1;

    #[derive_tree_maker(Institutions)]
    impl TreeMaker for Tree1 {
        type StackBasis = (IntX<Countries, 0, true>, IntX<Subfields, 0, true>);
        fn get_heap(_id: NET<Self::Root>, _gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            let tups = vec![
                (30, 20, 10, 0),
                (30, 20, 10, 1),
                (30, 20, 10, 2),
                (30, 21, 10, 0), //dup last2
                (30, 21, 11, 0),
                (31, 21, 11, 0), //dup last2
                (31, 20, 12, 0),
                (31, 20, 12, 0), //dup full
                (31, 21, 13, 0),
                (31, 21, 13, 1),
                (31, 21, 13, 2),
                (31, 21, 13, 3),
                (31, 21, 14, 0),
                (31, 21, 14, 1),
                (31, 21, 11, 1),
            ];
            for e in tups.into_iter() {
                heap.push(e);
            }
            heap
        }
    }

    pub struct Tree2;

    #[derive_tree_maker(Institutions)]
    impl TreeMaker for Tree2 {
        type StackBasis = IntX<Works, 0, true>;
        fn get_heap(_id: NET<Self::Root>, _gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            let tups = vec![(0, 10, 101), (1, 10, 100), (1, 11, 100)];
            for e in tups.into_iter() {
                heap.push(e);
            }
            heap
        }
    }

    pub struct Tree3;

    #[derive_tree_maker(Institutions)]
    impl TreeMaker for Tree3 {
        type StackBasis = IntX<Countries, 0, true>;
        fn get_heap(_id: NET<Self::Root>, _gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            let tups = vec![(1, 0, 1), (0, 1, 0), (0, 0, 0)];
            for e in tups.into_iter() {
                heap.push(e);
            }
            heap
        }
    }

    fn q() -> TreeQ {
        TreeQ {
            year: None,
            eid: 0,
            tid: None,
        }
    }

    #[test]
    fn to_tree1() {
        let r = Tree1::tree_resp(q(), &Getters::fake(), &HashMap::new());
        println!("{}", to_string_pretty(&r).unwrap());
        match &r.tree.children.deref() {
            JsSerChildren::Nodes(nodes) => match &nodes[&30].children.deref() {
                JsSerChildren::Leaves(leaves) => {
                    let lone = &leaves[&21];
                    assert_eq!(lone.source_count, 2);
                    assert_eq!(lone.link_count, 2);
                }
                _ => panic!("no lone"),
            },
            _ => panic!("wrong"),
        };

        match &r.tree.children.deref() {
            JsSerChildren::Nodes(nodes) => match &nodes[&31].children.deref() {
                JsSerChildren::Leaves(leaves) => {
                    let lone = &leaves[&20];
                    assert_eq!(lone.source_count, 1);
                    assert_eq!(lone.link_count, 1);
                }
                _ => panic!("no lone"),
            },
            _ => panic!("wrong"),
        };

        assert_eq!(r.tree.node.source_count, 5);
        assert_eq!(r.tree.node.link_count, 12);
        assert_eq!(r.tree.node.top_source, 13);
        assert_eq!(r.tree.node.top_cite_count, 4);
    }

    #[test]
    fn to_tree2() {
        let r = Tree2::tree_resp(q(), &Getters::fake(), &HashMap::new());
        println!("{}", to_string_pretty(&r).unwrap());
        assert_eq!(r.tree.node.source_count, 2);
        assert_eq!(r.tree.node.link_count, 3);
        assert_eq!(r.atts.keys().len(), 1);

        let r = Tree3::tree_resp(q(), &Getters::fake(), &HashMap::new());
        println!("{}", to_string_pretty(&r).unwrap());
        assert_eq!(r.tree.node.source_count, 2);
        assert_eq!(r.tree.node.link_count, 3);
    }

    #[test]
    pub fn big_tree() {
        let r = big_test_tree::get_big_tree(20);
        // assert_eq!(r.tree.node.link_count, 524288); //19
        let node = &r.tree.node;
        println!(
            "lc: {}, sc: {}, ts: {},",
            node.link_count, node.source_count, node.top_source,
        );

        assert_eq!(r.tree.node.link_count, 1048576); //20 - full
        assert_eq!(r.tree.node.source_count, 1048434); //20 - full
        assert_eq!(r.tree.node.top_source, 16735219); //20 - full

        // assert_eq!(r.tree.node.link_count, 1048448); //20 - nano
        // assert_eq!(r.tree.node.source_count, 65536); //20 - nano
        // assert_eq!(r.tree.node.top_source, 28260); //20 - nano

        // assert_eq!(r.tree.node.link_count, 2097152); //21
    }

    #[test]
    fn test_prune1() {
        //TODO !!!!!
    }
}
