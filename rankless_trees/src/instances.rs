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
        merge_sorted_vecs, merge_sorted_vecs_fun, AggTreeBase, FoldingStackConsumer, HeapIterator,
        MinHeap, SortedRecord,
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

struct PrepNode {
    merge_into: Vec<WorkTree>,
    merge_from: Vec<WorkTree>,
}

pub trait TreeGetter: NumberedEntity {
    #[allow(unused_variables)]
    fn get_tree(gets: &Getters, att_union: &AttributeLabelUnion, q: TreeQ) -> Option<TreeResponse> {
        None
    }

    fn get_specs() -> Vec<TreeSpec>;
}

pub trait Collapsing {
    type Collapsed;
    fn collapse(self) -> Self::Collapsed;
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
        let year = q.year.unwrap_or(START_YEAR);
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
            //dump all trees for all years
            let eid = <NET<Self::Root> as UnsignedNumber>::from_usize(q.eid.to_usize());

            let now = std::time::Instant::now();
            let heap = Self::get_heap(eid, gets);
            println!("got heap in {}", now.elapsed().as_secs());
            let hither: HeapIterator<Self::SortedRec> = heap.into();

            let now = std::time::Instant::now();
            let root = Self::get_root_tree(eid, hither);
            println!("got root in {}", now.elapsed().as_secs());

            let now = std::time::Instant::now();
            let ser_tree: BufSerTree = root.into();
            println!("converted tree in {}", now.elapsed().as_secs());

            let year_parent = tree_path.parent().unwrap();
            create_dir_all(year_parent).unwrap();
            for period_id in 0..WorkPeriods::N {
                let obj = &ser_tree;
                // write_buf_path(obj, year_parent.join(year16.to_string())).unwrap();
            }

            let now = std::time::Instant::now();
            write_buf_path(&ser_tree, year_parent.join(year.to_string())).unwrap();
            println!(" wrote tree in {}", now.elapsed().as_secs());
            ser_tree
        } else {
            read_buf_path(tree_path).unwrap()
        };

        let now = std::time::Instant::now();
        let bds = Self::get_spec().breakdowns;
        prune(&mut full_tree, stats, &bds);
        println!("pruned in {}", now.elapsed().as_secs());

        let now = std::time::Instant::now();
        let parent = gets
            .stowage
            .path_from_ns(<WorksNames as NamespacedEntity>::NS);
        let mut work_name_basis =
            VattReadingRefMap::<WorksNames>::from_locator(&gets.wn_locators, &parent);
        println!("loaded in {}", now.elapsed().as_secs());

        let now = std::time::Instant::now();
        let atts = get_atts(&full_tree, &bds, stats, &mut work_name_basis);
        println!("got atts in {}", now.elapsed().as_secs());

        let now = std::time::Instant::now();
        let tree = full_tree.into();
        println!("converted in {}", now.elapsed().as_secs());
        TreeResponse { tree, atts }
    }
}

trait FoldStackBase<C> {
    type StackElement;
    type LevelEntity: Entity;
    const SPEC_DENOM_IND: usize;
    const SOURCE_SIDE: bool;
}

trait Updater<C>
where
    C: Collapsing,
{
    fn update(&mut self, other: C) -> C::Collapsed;
}

trait TopTree {}

impl CollapsedNode {
    fn ingest_disjunct(&mut self, o: &Self) {
        if o.top_cite_count > self.top_cite_count {
            self.top_source = o.top_source;
            self.top_cite_count = o.top_cite_count;
        }
        self.link_count += o.link_count;
        self.source_count += o.source_count;
    }

    fn update_with_wt(&mut self, other: &WorkTree) {
        let ul = other.0.children.len() as u32;
        if ul > self.top_cite_count {
            self.top_source = other.0.id;
            self.top_cite_count = ul;
        }
        self.link_count += ul;
        self.source_count += 1;
    }
}

impl PrepNode {
    fn update_and_get_collapsed_node(&mut self, mut other: Self) -> CollapsedNode {
        let mut node = CollapsedNode::init_empty();
        let merger_fun = |l: WorkTree, r: WorkTree| {
            WorkTree(AggTreeBase {
                id: l.0.id,
                node: (),
                children: merge_sorted_vecs(l.0.children, r.0.children),
            })
            // l.0.children.append(&mut r.0.children);
            // l
        };
        // let left = std::mem::replace(&mut self.merge_from, Vec::new());
        unsafe {
            other.merge_into.set_len(0);
            self.merge_into.set_len(0);
        }
        let left = std::mem::replace(&mut self.merge_from, other.merge_into);
        merge_sorted_vecs_fun(
            &mut self.merge_into,
            left,
            other.merge_from,
            merger_fun,
            |wt| node.update_with_wt(wt),
        );
        std::mem::swap(&mut self.merge_into, &mut self.merge_from);
        node
    }
}

//WHY????
type WhyT = u16;

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
            merge_from: Vec::new(),
            merge_into: Vec::new(),
        }
    }
}

impl Updater<WorkTree> for PrepNode {
    fn update(&mut self, other: WorkTree) -> <WorkTree as Collapsing>::Collapsed {
        self.merge_from.push(other);
    }
}

impl<E> Updater<IntXTree<E, WorkTree>> for PrepNode
where
    E: NumberedEntity,
{
    fn update(&mut self, other: IntXTree<E, WorkTree>) -> CollT<IntXTree<E, WorkTree>> {
        let node = self.update_and_get_collapsed_node(other.0.node);
        IddCollNode {
            id: other.0.id,
            node,
        }
    }
}

impl<E, C> Updater<IntXTree<E, C>> for PrepNode
where
    E: NumberedEntity,
    C: Collapsing + TopTree,
{
    fn update(&mut self, other: IntXTree<E, C>) -> CollT<IntXTree<E, C>> {
        let node = self.update_and_get_collapsed_node(other.0.node);
        DisJTree(AggTreeBase {
            id: other.0.id,
            node,
            children: other.0.children,
        })
    }
}

impl<E, C> Updater<DisJTree<E, C>> for CollapsedNode
where
    E: NumberedEntity,
    C: Collapsing,
{
    fn update(&mut self, other: DisJTree<E, C>) -> CollT<DisJTree<E, C>> {
        self.ingest_disjunct(&other.0.node);
        other
    }
}

impl<E> Updater<IntXTree<E, WorkTree>> for CollapsedNode
where
    E: NumberedEntity,
{
    fn update(&mut self, other: IntXTree<E, WorkTree>) -> CollT<IntXTree<E, WorkTree>> {
        let node = other.collapse();
        self.ingest_disjunct(&node.node);
        node
    }
}

impl<E, C> Updater<IntXTree<E, C>> for CollapsedNode
where
    E: NumberedEntity,
    C: TopTree + Collapsing,
{
    fn update(&mut self, other: IntXTree<E, C>) -> DisJTree<E, C> {
        <Self as Updater<DisJTree<E, C>>>::update(self, other.collapse())
    }
}

impl Updater<WorkTree> for CollapsedNode {
    fn update(&mut self, other: WorkTree) -> CollT<WorkTree> {
        self.update_with_wt(&other);
    }
}

impl Collapsing for PrepNode {
    type Collapsed = CollapsedNode;
    fn collapse(self) -> Self::Collapsed {
        let mut out = CollapsedNode::init_empty();
        self.merge_from.into_iter().for_each(|e| {
            out.update(e);
        });
        out
    }
}

impl Collapsing for WorkTree {
    type Collapsed = ();
    fn collapse(self) -> Self::Collapsed {}
}

impl<E> Collapsing for IntXTree<E, WorkTree>
where
    E: NumberedEntity,
{
    type Collapsed = IddCollNode<E>;
    fn collapse(self) -> Self::Collapsed {
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
    fn collapse(self) -> Self::Collapsed {
        DisJTree(AggTreeBase {
            id: self.0.id,
            node: self.0.node.collapse(),
            children: self.0.children,
        })
    }
}

impl<E, C> Collapsing for DisJTree<E, C>
where
    E: NumberedEntity,
    C: Collapsing,
{
    type Collapsed = Self;
    fn collapse(self) -> Self::Collapsed {
        self
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

impl<E, C> FoldingStackConsumer for DisJTree<E, C>
where
    CollapsedNode: Updater<C>,
    E: NumberedEntity,
    C: Collapsing,
{
    type Consumable = C;
    fn consume(&mut self, child: Self::Consumable) {
        self.0.children.push(self.0.node.update(child));
    }
}

impl<E, C> FoldingStackConsumer for IntXTree<E, C>
where
    PrepNode: Updater<C>,
    E: NumberedEntity,
    C: Collapsing,
{
    type Consumable = C;
    fn consume(&mut self, child: Self::Consumable) {
        self.0.children.push(self.0.node.update(child));
    }
}

impl FoldingStackConsumer for WorkTree {
    type Consumable = WT;
    fn consume(&mut self, child: Self::Consumable) {
        self.0.children.push(child);
        // self.0.children.insert(child);
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
        ref_sfs: &'a Box<[ET<Subfields>]>,
        rsfi: usize,
        cit_wids: &'a Box<[ET<Works>]>,
        cwidi: usize,
        cit_insts: Option<&'a Box<[ET<Institutions>]>>,
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

    pub fn fuill_heap_sci() {}
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
            for refed_inst in gets.cinsts(&id) {
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
            for country_inst in gets.cinsts(&id) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{AttributeLabel, JsSerChildren};
    use std::ops::Deref;

    use rand::{rngs::StdRng, Rng, SeedableRng};
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
            let tups = vec![(0, 0, 1), (1, 0, 0), (1, 1, 0)];
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

    struct BigTree;
    #[derive_tree_maker(Institutions)]
    impl TreeMaker for BigTree {
        type StackBasis = (
            IntX<Countries, 0, true>,
            IntX<Works, 0, true>,
            IntX<Institutions, 0, true>,
            IntX<Countries, 0, true>,
        );
        fn get_heap(_id: NET<Self::Root>, _gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            let mut rng = StdRng::seed_from_u64(42);
            for _ in 0..2_u32.pow(20) {
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

    #[test]
    pub fn big_tree() {
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
        fake_attu.insert(Institutions::NAME.to_string(), gatts(16, "C"));
        let r = BigTree::tree_resp(q(), &Getters::fake(), &fake_attu);
        // assert_eq!(r.tree.node.link_count, 524288); //19
        assert_eq!(r.tree.node.link_count, 1048448); //20 - nano

        // assert_eq!(r.tree.node.link_count, 2097152); //21
    }

    #[test]
    fn test_prune1() {
        //TODO !!!!!
    }
}
