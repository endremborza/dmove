use core::slice::Iter;
use std::{fs::create_dir_all, vec::IntoIter};

use crate::{
    ids::{get_atts, AttributeLabelUnion},
    interfacing::{Getters, NumberedEntity, NET},
    io::{
        BreakdownSpec, BufSerChildren, BufSerTree, CollapsedNode, TreeQ, TreeResponse, TreeSpec,
        WorkCiteT, WorkWInd, WT,
    },
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
    steps::{a1_entity_mapping::N_PERS, derive_links1::WorkPeriods},
};

use dmove::{Entity, NamespacedEntity, UnsignedNumber, VattReadingRefMap, ET};
use dmove_macro::derive_tree_getter;
use hashbrown::HashMap;

const UNKNOWN_ID: usize = 0;
const MAX_PARTITIONS: usize = 16;

type PartitionId = u8;
type StackFr<S> = <<S as StackBasis>::SortedRec as SortedRecord>::FlatRecord;
type CollT<T> = <T as Collapsing>::Collapsed;

#[derive(PartialOrd, PartialEq)]
pub struct WorkTree(AggTreeBase<WT, (), WT>);

pub struct IntXTree<E: NumberedEntity, C: Collapsing>(AggTreeBase<NET<E>, PrepNode, CollT<C>>);
pub struct DisJTree<E: NumberedEntity, C: Collapsing>(AggTreeBase<NET<E>, CollapsedNode, CollT<C>>);

pub struct DisJ<E: Entity, const N: usize, const S: bool>(E::T);
pub struct IntX<E: Entity, const N: usize, const S: bool>(E::T);

pub struct IddCollNode<E: NumberedEntity> {
    id: NET<E>,
    node: CollapsedNode,
}

struct WVecPair {
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

pub trait TreeGetter: NumberedEntity {
    #[allow(unused_variables)]
    fn get_tree(gets: &Getters, att_union: &AttributeLabelUnion, q: TreeQ) -> Option<TreeResponse> {
        None
    }

    fn get_specs() -> Vec<TreeSpec>;
}

pub trait Collapsing {
    type Collapsed;
    //can maybe be merged with reinstate with
    fn collapse(&mut self) -> Self::Collapsed;
}

pub trait StackBasis {
    type Stack;
    type SortedRec: SortedRecord;
    type TopTree: Collapsing;

    fn get_bds() -> Vec<BreakdownSpec>;

    fn fold_into<R, I>(root: &mut R, iter: I)
    where
        I: Iterator<Item = Self::SortedRec>,
        R: Updater<Self::TopTree>;
}

trait PartitioningIterator<'a, S>: Iterator<Item = (PartitionId, StackFr<S>)> + Sized
where
    S: StackBasis,
{
    type Root: NumberedEntity;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self;
}

trait TreeMaker<'a> {
    type StackBasis: StackBasis;
    type Iterator: PartitioningIterator<'a, Self::StackBasis>;
    const PARTITIONS: usize;

    fn get_spec() -> TreeSpec {
        let breakdowns = Self::StackBasis::get_bds();
        let root_type = Self::entity_name();
        TreeSpec {
            root_type,
            breakdowns,
        }
    }

    fn entity_name() -> String {
        <Self::Iterator as PartitioningIterator<Self::StackBasis>>::Root::NAME.to_string()
    }

    fn tree_resp<RE, CT, SR, FR>(
        q: TreeQ,
        gets: &'a Getters,
        stats: &AttributeLabelUnion,
    ) -> TreeResponse
    where
        RE: NumberedEntity,
        Self::StackBasis: StackBasis<TopTree = CT, SortedRec = SR>,
        SR: SortedRecord<FlatRecord = FR>,
        FR: Ord + Clone,
        CT: Collapsing + TopTree,
        DisJTree<RE, CT>: Into<BufSerTree>,
        IntXTree<RE, CT>: Updater<CT>,
        Self::Iterator: PartitioningIterator<'a, Self::StackBasis, Root = RE>,
    {
        let tid = q.tid.unwrap_or(0);
        let eid = q.eid.to_usize();
        let req_id = format!("{}({}:{})", Self::entity_name(), eid, tid);
        println!("requested entity: {req_id}");
        let period = WorkPeriods::from_year(q.year.unwrap_or(START_YEAR));
        let tree_cache_dir = gets
            .stowage
            .paths
            .cache
            .join(Self::entity_name())
            .join(q.eid.to_string())
            .join(tid.to_string());
        let get_path = |pid: usize| tree_cache_dir.join(format!("{pid}.gz"));
        let mut full_tree: BufSerTree = if !tree_cache_dir.exists() {
            create_dir_all(tree_cache_dir.clone()).unwrap();
            let mut heaps = [(); MAX_PARTITIONS].map(|_| MinHeap::<FR>::new());
            let et_id = NET::<RE>::from_usize(eid);
            let now = std::time::Instant::now();
            let maker = Self::Iterator::new(et_id, &gets);
            for (pid, rec) in maker {
                heaps[pid as usize].push(rec)
            }
            println!("{req_id}: got heaps in {}", now.elapsed().as_millis());
            let now = std::time::Instant::now();
            let mut roots = Vec::new();
            heaps.into_iter().take(Self::PARTITIONS).for_each(|heap| {
                let hither_o: Option<HeapIterator<<Self::StackBasis as StackBasis>::SortedRec>> =
                    heap.into();
                let mut part_root: IntXTree<RE, CT> = et_id.into();
                if let Some(hither) = hither_o {
                    Self::StackBasis::fold_into(&mut part_root, hither);
                } else {
                    println!("nothing in a partition")
                }
                roots.push(part_root.collapse());
            });
            println!("{req_id}: got roots in {}", now.elapsed().as_millis());

            let now = std::time::Instant::now();
            let mut root_it = roots.into_iter().enumerate().rev();
            let (pid, root_n) = root_it.next().unwrap();
            let mut ser_tree: BufSerTree = root_n.into();
            write_buf_path(&ser_tree, get_path(pid)).unwrap();
            for (pid, part_root) in root_it {
                let part_ser: BufSerTree = part_root.into();
                ser_tree.ingest_disjunct(part_ser);

                write_buf_path(&ser_tree, get_path(pid)).unwrap();
            }
            println!(
                "{req_id}: converted and wrote trees in {}",
                now.elapsed().as_millis()
            );
            ser_tree
        } else {
            //TODO WARN possible race condition! if multithreaded thing, one starts writing,
            //created the directory, but did not write all the files yet, this can start reading
            //shit
            //need ot fix it with some lock store like thing
            read_buf_path(get_path(period as usize)).unwrap()
        };

        let now = std::time::Instant::now();
        let bds = Self::get_spec().breakdowns;
        prune(&mut full_tree, stats, &bds);
        println!("{req_id}: pruned in {}", now.elapsed().as_millis());
        //cache pruned response, use it if no connections are requested

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

pub trait FoldStackBase<C> {
    type StackElement;
    type LevelEntity: Entity;
    const SPEC_DENOM_IND: usize;
    const SOURCE_SIDE: bool;
}

trait TopTree {}

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

impl OrderedMapper<WorkWInd> for WVecMerger<'_> {
    type Elem = WorkWInd;
    fn common_map(&mut self, l: &Self::Elem, r: &Self::Elem) {
        self.node.update_with_wt(r);
        let last_len = self.wv_into.targets.len() as u32;
        let left_it = self.left_from.targets[self.left_i..(self.left_i + (l.1 as usize))].iter();
        let right_it =
            self.right_from.targets[self.right_i..(self.right_i + (r.1 as usize))].iter();

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
        let left_it = self.merge_from.sources.iter();
        let right_it = other.merge_from.sources.iter();
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
        self.merge_from.sources.iter().for_each(|e| {
            out.update_with_wt(e);
        });
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

    use std::{iter::Peekable, marker::PhantomData};

    use dmove_macro::StackBasis;

    use crate::interfacing::WorksFromMemory;

    use super::*;

    pub trait RefWorkBasedIter<'a> {
        fn new(refed_wid: &'a WT, gets: &'a Getters) -> Self;
    }

    pub struct SuCoInstIter<'a> {
        ref_wid: &'a ET<Works>,
        ref_sfs: Peekable<Iter<'a, ET<Subfields>>>,
        cit_wids: Peekable<Iter<'a, ET<Works>>>,
        cit_insts: Option<Iter<'a, ET<Institutions>>>,
        gets: &'a Getters,
    }

    impl<'a> RefWorkBasedIter<'a> for SuCoInstIter<'a> {
        fn new(refed_wid: &'a WT, gets: &'a Getters) -> Self {
            let ref_sfs = gets.wsubfields(refed_wid).iter().peekable();
            let cit_wids = gets.citing(*refed_wid).iter().peekable();
            Self {
                ref_wid: refed_wid,
                ref_sfs,
                cit_wids,
                cit_insts: None,
                gets,
            }
        }
    }

    impl<'a> Iterator for SuCoInstIter<'a> {
        type Item = (ET<Subfields>, ET<Countries>, ET<Institutions>, ET<Works>);

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                let ref_sf = match self.ref_sfs.peek() {
                    Some(v) => *v,
                    None => return None,
                };
                let cite_wid = match self.cit_wids.peek() {
                    Some(v) => *v,
                    None => {
                        self.cit_wids = self.gets.citing(*self.ref_wid).iter().peekable();
                        self.ref_sfs.next();
                        continue;
                    }
                };
                let cit_inst = match &mut self.cit_insts {
                    Some(cit_insts) => match cit_insts.next() {
                        Some(iid) => iid,
                        None => {
                            self.cit_wids.next();
                            self.cit_insts = None;
                            continue;
                        }
                    },
                    None => {
                        self.cit_insts = Some(self.gets.winsts(cite_wid).iter());
                        continue;
                    }
                };
                return Some((
                    ref_sf.lift(),
                    self.gets.icountry(cit_inst).lift(),
                    cit_inst.lift(),
                    cite_wid.lift(),
                ));
            }
        }
    }

    pub struct CitingCoSuToIter<'a> {
        cit_wids: Peekable<Iter<'a, ET<Works>>>,
        cit_tops: Option<Peekable<Iter<'a, ET<Topics>>>>,
        cit_insts: Option<Iter<'a, ET<Institutions>>>,
        gets: &'a Getters,
    }

    impl<'a> RefWorkBasedIter<'a> for CitingCoSuToIter<'a> {
        fn new(refed_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
            let cit_wids = gets.citing(*refed_wid).iter().peekable();
            Self {
                cit_wids,
                cit_tops: None,
                cit_insts: None,
                gets,
            }
        }
    }

    impl<'a> Iterator for CitingCoSuToIter<'a> {
        type Item = (ET<Countries>, ET<Subfields>, ET<Topics>, ET<Works>);

        fn next(&mut self) -> Option<Self::Item> {
            loop {
                let citing_wid = match self.cit_wids.peek() {
                    Some(v) => *v,
                    None => return None,
                };
                let citing_topic = match &mut self.cit_tops {
                    Some(citing_topics) => match citing_topics.peek() {
                        Some(tid) => *tid,
                        None => {
                            self.cit_wids.next();
                            self.cit_insts = None;
                            continue;
                        }
                    },
                    None => {
                        self.cit_tops = Some(self.gets.wtopics(citing_wid).iter().peekable());
                        continue;
                    }
                };
                let citing_inst = match &mut self.cit_insts {
                    Some(citing_insts) => match citing_insts.next() {
                        Some(iid) => iid,
                        None => {
                            self.cit_tops.as_mut().unwrap().next();
                            self.cit_insts = None;
                            continue;
                        }
                    },
                    None => {
                        self.cit_insts = Some(self.gets.winsts(citing_wid).iter());
                        continue;
                    }
                };
                return Some((
                    self.gets.icountry(citing_inst).lift(),
                    self.gets.tsuf(citing_topic).lift(),
                    citing_topic.lift(),
                    citing_wid.lift(),
                ));
            }
        }
    }

    #[derive(StackBasis)]
    pub struct CitingCoSuTo(
        IntX<Countries, 0, false>,
        IntX<Subfields, 1, false>,
        IntX<Topics, 1, false>,
    );

    pub struct FinalIterWrap<'a, E, S, I, CE1, CE2, CE3>
    where
        E: NumberedEntity,
    {
        id: NET<E>,
        it: Option<I>,
        gets: &'a Getters,
        refs_it: Peekable<Iter<'a, WT>>,
        p: PhantomData<(S, CE1, CE2, CE3)>,
    }

    pub type CitingCoSuToForTM<'a, E> =
        FinalIterWrap<'a, E, CitingCoSuTo, CitingCoSuToIter<'a>, Countries, Subfields, Topics>;

    impl<'a, E, S, I, CE1, CE2, CE3> PartitioningIterator<'a, S>
        for FinalIterWrap<'a, E, S, I, CE1, CE2, CE3>
    where
        E: NumberedEntity + WorksFromMemory,
        S: StackBasis,
        S::SortedRec: SortedRecord<FlatRecord = (ET<CE1>, ET<CE2>, ET<CE3>, WT, WT)>,
        I: Iterator<Item = (ET<CE1>, ET<CE2>, ET<CE3>, WT)> + RefWorkBasedIter<'a>,
        CE1: Entity,
        CE2: Entity,
        CE3: Entity,
    {
        type Root = E;
        fn new(id: NET<E>, gets: &'a Getters) -> Self {
            let refs_it = E::works_from_ram(&gets, id.lift()).iter().peekable();
            Self {
                id,
                gets,
                refs_it,
                it: None,
                p: PhantomData,
            }
        }
    }

    impl<'a, E, S, I, CE1, CE2, CE3> Iterator for FinalIterWrap<'a, E, S, I, CE1, CE2, CE3>
    where
        E: NumberedEntity + WorksFromMemory,
        S: StackBasis,
        S::SortedRec: SortedRecord<FlatRecord = (ET<CE1>, ET<CE2>, ET<CE3>, WT, WT)>,
        I: Iterator<Item = (ET<CE1>, ET<CE2>, ET<CE3>, WT)> + RefWorkBasedIter<'a>,
        CE1: Entity,
        CE2: Entity,
        CE3: Entity,
    {
        type Item = (PartitionId, StackFr<S>);
        fn next(&mut self) -> Option<Self::Item> {
            loop {
                let ref_wid = match self.refs_it.peek() {
                    Some(v) => *v,
                    None => return None,
                };
                let ref_per = self.gets.wperiod(ref_wid);
                match &mut self.it {
                    Some(it) => match it.next() {
                        Some((c1, c2, c3, cw)) => {
                            return Some((*ref_per, (c1, c2, c3, *ref_wid, cw)));
                        }
                        None => {
                            self.it = None;
                            self.refs_it.next();
                            continue;
                        }
                    },
                    None => self.it = Some(I::new(&ref_wid, &self.gets)),
                }
            }
        }
    }
}

#[derive_tree_getter(Authors)]
mod author_trees {
    use super::*;

    impl<'a> TreeMaker<'a> for Tree1 {
        type StackBasis = iterators::CitingCoSuTo;
        type Iterator = iterators::CitingCoSuToForTM<'a, Authors>;
        const PARTITIONS: usize = N_PERS;
    }

    // impl TreeMaker for Tree1 {
    //     type StackBasis = (
    //         IntX<Works, 0, true>,
    //         IntX<Countries, 1, false>,
    //         IntX<Institutions, 1, false>,
    //     );
    //
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.aworks(&id) {
    //             for citing_wid in gets.citing(refed_wid) {
    //                 for citing_inst in gets.winsts(citing_wid) {
    //                     let citing_country = gets.icountry(citing_inst);
    //                     let record = (
    //                         refed_wid.lift(),
    //                         citing_country.lift(),
    //                         citing_inst.lift(),
    //                         refed_wid.lift(),
    //                         citing_wid.lift(),
    //                     );
    //                     heap.push(record);
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for Tree2 {
    //     type StackBasis = (
    //         IntX<Countries, 0, false>,
    //         IntX<Subfields, 1, false>,
    //         IntX<Topics, 1, false>,
    //     );
    //
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.aworks(&id) {
    //             for citing_wid in gets.citing(refed_wid) {
    //                 for citing_inst in gets.winsts(citing_wid) {
    //                     let citing_country = gets.icountry(citing_inst);
    //                     for citing_topic in gets.topic(citing_wid) {
    //                         let citing_subfield = gets.tsuf(citing_topic);
    //                         let record = (
    //                             citing_country.lift(),
    //                             citing_subfield.lift(),
    //                             citing_topic.lift(),
    //                             refed_wid.lift(),
    //                             citing_wid.lift(),
    //                         );
    //                         heap.push(record);
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
}

#[derive_tree_getter(Institutions)]
mod inst_trees {
    //
    // use super::*;
    //
    // impl TreeMaker for TreeSuSuTo {
    //     type StackBasis = (
    //         IntX<Subfields, 0, true>,
    //         IntX<Subfields, 1, false>,
    //         IntX<Topics, 1, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             for refed_topic in gets.topic(refed_wid) {
    //                 let refed_sf = gets.tsuf(refed_topic);
    //                 for citing_wid in gets.citing(refed_wid) {
    //                     for citing_topic in gets.topic(citing_wid) {
    //                         let citing_sf = gets.tsuf(citing_topic);
    //                         let record = (
    //                             refed_sf.lift(),
    //                             citing_sf.lift(),
    //                             citing_topic.lift(),
    //                             refed_wid.lift(),
    //                             citing_wid.lift(),
    //                         );
    //                         heap.push(record);
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeSuSoTo {
    //     type StackBasis = (
    //         IntX<Subfields, 0, false>,
    //         IntX<Sources, 1, false>,
    //         IntX<Topics, 1, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             for citing_wid in gets.citing(refed_wid) {
    //                 for citing_source in gets.sources(citing_wid) {
    //                     for citing_topic in gets.topic(citing_wid) {
    //                         let citing_sf = gets.tsuf(citing_topic);
    //                         let record = (
    //                             citing_sf.lift(),
    //                             citing_source.lift(),
    //                             citing_topic.lift(),
    //                             refed_wid.lift(),
    //                             citing_wid.lift(),
    //                         );
    //                         heap.push(record);
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeAuCoIn {
    //     type StackBasis = (
    //         IntX<Authors, 0, true>,
    //         IntX<Countries, 1, false>,
    //         IntX<Institutions, 1, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             for refed_ship in gets.wships(refed_wid) {
    //                 let refed_author = gets.shipa(refed_ship);
    //                 if refed_author.to_usize() == UNKNOWN_ID {
    //                     continue;
    //                 }
    //                 for refed_ship_inst in gets.shipis(refed_ship) {
    //                     if refed_ship_inst != &id {
    //                         continue;
    //                     }
    //                     for citing_wid in gets.citing(refed_wid) {
    //                         for citing_inst in gets.winsts(citing_wid) {
    //                             let citing_country = gets.icountry(citing_inst);
    //                             let record = (
    //                                 refed_author.lift(),
    //                                 citing_country.lift(),
    //                                 citing_inst.lift(),
    //                                 refed_wid.lift(),
    //                                 citing_wid.lift(),
    //                             );
    //                             heap.push(record);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeSoCuSu {
    //     type StackBasis = (
    //         IntX<Sources, 0, false>,
    //         IntX<Countries, 1, false>,
    //         IntX<Subfields, 2, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             for citing_wid in gets.citing(refed_wid) {
    //                 for citing_source in gets.sources(citing_wid) {
    //                     for citing_sf in gets.subfield(citing_wid) {
    //                         for citing_inst in gets.winsts(citing_wid) {
    //                             let citing_country = gets.icountry(citing_inst);
    //                             let record = (
    //                                 citing_source.lift(),
    //                                 citing_country.lift(),
    //                                 citing_sf.lift(),
    //                                 refed_wid.lift(),
    //                                 citing_wid.lift(),
    //                             );
    //                             heap.push(record);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeQSoSuCo {
    //     type StackBasis = (
    //         IntX<Qs, 0, true>,
    //         IntX<Sources, 0, true>,
    //         IntX<Subfields, 2, false>,
    //         IntX<Countries, 3, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             let refed_year = gets.year(refed_wid);
    //             for refed_source in gets.sources(refed_wid) {
    //                 let tup = (refed_source.lift(), refed_year.lift());
    //                 let refed_q = gets.sqy(&tup);
    //                 for citing_wid in gets.citing(refed_wid) {
    //                     for citing_inst in gets.winsts(citing_wid) {
    //                         let citing_country = gets.icountry(citing_inst);
    //                         for citing_subfield in gets.subfield(citing_wid) {
    //                             let record = (
    //                                 refed_q.lift(),
    //                                 refed_source.lift(),
    //                                 citing_subfield.lift(),
    //                                 citing_country.lift(),
    //                                 refed_wid.lift(),
    //                                 citing_wid.lift(),
    //                             );
    //                             heap.push(record);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeCoInSuToo {
    //     type StackBasis = (
    //         IntX<Countries, 0, false>,
    //         IntX<Institutions, 0, false>,
    //         IntX<Subfields, 2, false>,
    //         IntX<Topics, 2, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             for citing_wid in gets.citing(refed_wid) {
    //                 for citing_inst in gets.winsts(citing_wid) {
    //                     let citing_country = gets.icountry(citing_inst);
    //                     for citing_topic in gets.topic(citing_wid) {
    //                         let citing_sf = gets.tsuf(citing_topic);
    //                         let record = (
    //                             citing_country.lift(),
    //                             citing_inst.lift(),
    //                             citing_sf.lift(),
    //                             citing_topic.lift(),
    //                             refed_wid.lift(),
    //                             citing_wid.lift(),
    //                         );
    //                         heap.push(record);
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeCoSuIn {
    //     type StackBasis = (
    //         IntX<Countries, 0, true>,
    //         IntX<Subfields, 1, true>,
    //         IntX<Institutions, 0, true>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             for refed_inst in gets.winsts(refed_wid) {
    //                 if refed_inst == &id {
    //                     continue;
    //                 }
    //                 let refed_country = gets.icountry(refed_inst);
    //                 for refed_sf in gets.subfield(refed_wid) {
    //                     for citing_wid in gets.citing(refed_wid) {
    //                         let record = (
    //                             refed_country.lift(),
    //                             refed_sf.lift(),
    //                             refed_inst.lift(),
    //                             refed_wid.lift(),
    //                             citing_wid.lift(),
    //                         );
    //                         heap.push(record);
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeSuCoInSu {
    //     type StackBasis = (
    //         IntX<Subfields, 0, true>,
    //         IntX<Countries, 1, false>,
    //         IntX<Institutions, 1, false>,
    //         IntX<Subfields, 3, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             for refed_sf in gets.subfield(refed_wid) {
    //                 for citing_wid in gets.citing(refed_wid) {
    //                     for citing_inst in gets.winsts(citing_wid) {
    //                         let citing_country = gets.icountry(citing_inst);
    //                         for citing_topic in gets.topic(citing_wid) {
    //                             let citing_subfield = gets.tsuf(citing_topic);
    //                             let record = (
    //                                 refed_sf.lift(),
    //                                 citing_country.lift(),
    //                                 citing_inst.lift(),
    //                                 citing_subfield.lift(),
    //                                 refed_wid.lift(),
    //                                 citing_wid.lift(),
    //                             );
    //                             heap.push(record);
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeSuCoInSo {
    //     type StackBasis = (
    //         IntX<Subfields, 0, true>,
    //         IntX<Countries, 1, false>,
    //         IntX<Institutions, 1, false>,
    //         IntX<Sources, 3, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.iworks(&id) {
    //             for (refed_sf, citing_country, citing_inst, citing_wid) in
    //                 iterators::SuCoInstIter::new(refed_wid, gets)
    //             {
    //                 for citing_source in gets.sources(&citing_wid) {
    //                     heap.push((
    //                         refed_sf.lift(),
    //                         citing_country.lift(),
    //                         citing_inst.lift(),
    //                         citing_source.lift(),
    //                         refed_wid.lift(),
    //                         citing_wid.lift(),
    //                     ))
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
}

#[derive_tree_getter(Countries)]
mod country_trees {

    // use super::*;
    //
    // impl TreeMaker for TreeISuCoIn {
    //     type StackBasis = (
    //         IntX<Institutions, 0, true>,
    //         IntX<Subfields, 1, true>,
    //         IntX<Countries, 2, false>,
    //         IntX<Institutions, 2, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_inst in gets.country_insts(&id) {
    //             for refed_wid in gets.iworks(refed_inst) {
    //                 for (refed_sf, citing_country, citing_inst, citing_wid) in
    //                     iterators::SuCoInstIter::new(refed_wid, gets)
    //                 {
    //                     heap.push((
    //                         refed_inst.lift(),
    //                         refed_sf.lift(),
    //                         citing_country.lift(),
    //                         citing_inst.lift(),
    //                         refed_wid.lift(),
    //                         citing_wid.lift(),
    //                     ))
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeCoInSu {
    //     type StackBasis = (
    //         IntX<Countries, 0, true>,
    //         IntX<Institutions, 0, true>,
    //         IntX<Subfields, 1, true>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for country_inst in gets.country_insts(&id) {
    //             for refed_wid in gets.iworks(country_inst) {
    //                 for refed_inst in gets.winsts(refed_wid) {
    //                     let refed_country = gets.icountry(refed_inst);
    //                     if *refed_country == id {
    //                         continue;
    //                     }
    //                     for refed_sf in gets.subfield(refed_wid) {
    //                         for citing_wid in gets.citing(refed_wid) {
    //                             heap.push((
    //                                 refed_country.lift(),
    //                                 refed_inst.lift(),
    //                                 refed_sf.lift(),
    //                                 refed_wid.lift(),
    //                                 citing_wid.lift(),
    //                             ))
    //                         }
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
}

#[derive_tree_getter(Sources)]
mod source_trees {
    //
    // use super::*;
    //
    // impl TreeMaker for TreeSuCoInSo {
    //     type StackBasis = (
    //         IntX<Subfields, 0, true>,
    //         IntX<Countries, 1, false>,
    //         IntX<Institutions, 1, false>,
    //         IntX<Sources, 3, false>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.soworks(&id) {
    //             for (refed_sf, citing_country, citing_inst, citing_wid) in
    //                 iterators::SuCoInstIter::new(refed_wid, gets)
    //             {
    //                 for citing_source in gets.sources(&citing_wid) {
    //                     heap.push((
    //                         refed_sf.lift(),
    //                         citing_country.lift(),
    //                         citing_inst.lift(),
    //                         citing_source.lift(),
    //                         refed_wid.lift(),
    //                         citing_wid.lift(),
    //                     ))
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
    //
    // impl TreeMaker for TreeSCISo {
    //     type StackBasis = (
    //         IntX<Countries, 0, true>,
    //         IntX<Institutions, 0, true>,
    //         IntX<Subfields, 2, true>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.soworks(&id) {
    //             for refed_inst in gets.winsts(refed_wid) {
    //                 let refed_country = gets.icountry(refed_inst);
    //                 for refed_sf in gets.subfield(refed_wid) {
    //                     for citing_wid in gets.citing(refed_wid) {
    //                         heap.push((
    //                             refed_country.lift(),
    //                             refed_inst.lift(),
    //                             refed_sf.lift(),
    //                             refed_wid.lift(),
    //                             citing_wid.lift(),
    //                         ))
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
}

#[derive_tree_getter(Subfields)]
mod subfield_trees {
    use super::*;

    // impl TreeMaker for TreeSoCuSu {
    //     type StackBasis = (
    //         IntX<Sources, 0, true>,
    //         IntX<Countries, 1, true>,
    //         IntX<Institutions, 1, true>,
    //     );
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.fieldworks(&id) {
    //             for refed_source in gets.sources(refed_wid) {
    //                 for refed_inst in gets.winsts(refed_wid) {
    //                     let refed_country = gets.icountry(refed_inst);
    //                     for citing_wid in gets.citing(refed_wid) {
    //                         let record = (
    //                             refed_source.lift(),
    //                             refed_country.lift(),
    //                             refed_inst.lift(),
    //                             refed_wid.lift(),
    //                             citing_wid.lift(),
    //                         );
    //                         heap.push(record);
    //                     }
    //                 }
    //             }
    //         }
    //         heap
    //     }
    // }
}

pub mod test_tools {
    use super::*;

    pub trait TestSB: StackBasis {
        fn get_vec() -> Vec<StackFr<Self>>;
    }

    pub struct Tither<SB>
    where
        SB: TestSB,
    {
        viter: IntoIter<StackFr<SB>>,
    }

    impl<T> PartitioningIterator<'_, T> for Tither<T>
    where
        T: TestSB,
    {
        type Root = Institutions;
        fn new(_id: ET<Institutions>, _gets: &Getters) -> Self {
            let viter = T::get_vec().into_iter();
            Self { viter }
        }
    }

    impl<T> Iterator for Tither<T>
    where
        T: TestSB,
    {
        type Item = (PartitionId, StackFr<T>);

        fn next(&mut self) -> Option<Self::Item> {
            match self.viter.next() {
                Some(v) => Some((0, v)),
                None => None,
            }
        }
    }
}

pub mod big_test_tree {
    use super::*;
    use crate::io::AttributeLabel;
    use dmove_macro::StackBasis;
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use test_tools::*;

    #[derive(StackBasis)]
    struct BigStack(
        IntX<Countries, 0, true>,
        IntX<Works, 0, true>,
        IntX<Institutions, 0, true>,
        IntX<Countries, 0, true>,
    );

    impl TestSB for BigStack {
        fn get_vec() -> Vec<StackFr<Self>> {
            let id = 20;
            let mut vec = Vec::new();
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
                vec.push(rec);
            }
            vec
        }
    }

    struct BigTree;

    impl TreeMaker<'_> for BigTree {
        type StackBasis = BigStack;
        const PARTITIONS: usize = 1;
        type Iterator = Tither<BigStack>;
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
            connections: None,
        };
        BigTree::tree_resp(q, &Getters::fake(), &fake_attu)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::JsSerChildren;
    use std::ops::Deref;
    use test_tools::{TestSB, Tither};

    use dmove_macro::StackBasis;
    use serde_json::to_string_pretty;

    #[derive(StackBasis)]
    struct SimpleStackBasis(IntX<Works, 0, true>);

    #[derive(StackBasis)]
    struct SimpleStackBasisL2(IntX<Countries, 0, true>, IntX<Subfields, 0, true>);

    #[derive(StackBasis)]
    struct SimpleStackBasis3(IntX<Works, 0, true>);

    impl TestSB for SimpleStackBasis {
        fn get_vec() -> Vec<StackFr<Self>> {
            vec![(0, 10, 101), (1, 10, 100), (1, 11, 100)]
        }
    }

    impl TestSB for SimpleStackBasis3 {
        fn get_vec() -> Vec<StackFr<Self>> {
            vec![(1, 0, 1), (0, 1, 0), (0, 0, 0)]
        }
    }

    impl TestSB for SimpleStackBasisL2 {
        fn get_vec() -> Vec<StackFr<Self>> {
            vec![
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
            ]
        }
    }

    struct Tree1;

    impl TreeMaker<'_> for Tree1 {
        type StackBasis = SimpleStackBasisL2;
        const PARTITIONS: usize = 2;
        type Iterator = Tither<Self::StackBasis>;
    }

    struct Tree2;

    impl TreeMaker<'_> for Tree2 {
        type StackBasis = SimpleStackBasis;
        const PARTITIONS: usize = 2;
        type Iterator = Tither<Self::StackBasis>;
    }

    struct Tree3;

    impl TreeMaker<'_> for Tree3 {
        type StackBasis = SimpleStackBasis3;
        const PARTITIONS: usize = 2;
        type Iterator = Tither<Self::StackBasis>;
    }

    fn q() -> TreeQ {
        TreeQ {
            eid: 0,
            year: None,
            tid: None,
            connections: None,
        }
    }

    #[test]
    fn to_tree1() {
        let g = Getters::fake();
        let r = Tree1::tree_resp(q(), &g, &HashMap::new());
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
        let g = Getters::fake();
        let r = Tree2::tree_resp(q(), &g, &HashMap::new());
        println!("{}", to_string_pretty(&r).unwrap());
        assert_eq!(r.tree.node.source_count, 2);
        assert_eq!(r.tree.node.link_count, 3);
        assert_eq!(r.atts.keys().len(), 1);

        let r = Tree3::tree_resp(q(), &g, &HashMap::new());
        println!("{}", to_string_pretty(&r).unwrap());
        assert_eq!(r.tree.node.source_count, 2);
        assert_eq!(r.tree.node.link_count, 3);
    }

    #[test]
    pub fn big_tree() {
        let r = big_test_tree::get_big_tree(20);
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
        //
        // assert_eq!(r.tree.node.link_count, 2097152); //21
        // assert_eq!(r.tree.node.link_count, 524288); //19
    }
}
