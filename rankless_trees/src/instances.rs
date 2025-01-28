use std::vec::IntoIter;

use crate::{
    components::{IntX, PartitioningIterator, StackBasis, StackFr},
    ids::AttributeLabelUnion,
    interfacing::{Getters, NumberedEntity, NET},
    io::{
        BufSerChildren, BufSerTree, CollapsedNode, TreeQ, TreeResponse, TreeSpec, WorkCiteT,
        WorkWInd, WT,
    },
};
use rankless_rs::{
    agg_tree::{
        ordered_calls, sorted_iters_to_arr, AggTreeBase, ExtendableArr, OrderedMapper,
        ReinstateFrom, Updater,
    },
    gen::a1_entity_mapping::{Authors, Countries, Institutions, Sources, Subfields, Works},
};

use dmove::{Entity, InitEmpty, UnsignedNumber, ET};
use dmove_macro::derive_tree_getter;
use hashbrown::HashMap;

type CollT<T> = <T as Collapsing>::Collapsed;

#[derive(PartialOrd, PartialEq)]
pub struct WorkTree(pub AggTreeBase<WT, (), WT>);

pub struct IntXTree<E: NumberedEntity, C: Collapsing>(AggTreeBase<NET<E>, PrepNode, CollT<C>>);
pub struct DisJTree<E: NumberedEntity, C: Collapsing>(AggTreeBase<NET<E>, CollapsedNode, CollT<C>>);

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

pub trait FoldStackBase<C> {
    type StackElement;
    type LevelEntity: Entity;
    const SPEC_DENOM_IND: usize;
    const SOURCE_SIDE: bool;
}

pub trait TopTree {}

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

#[derive_tree_getter(Authors)]
mod author_trees {
    use super::*;
    use crate::components::{CitingCoSuToByRef, PostRefIterWrap, WCoIByRef};

    pub type Tree1<'a> = PostRefIterWrap<'a, Authors, CitingCoSuToByRef<'a>>;
    pub type Tree2<'a> = PostRefIterWrap<'a, Authors, WCoIByRef<'a>>;
}

#[derive_tree_getter(Institutions)]
mod inst_trees {
    use super::*;

    use crate::components::{
        CitingCoInstSuToByRef, CitingSourceCoSuByRef, InstBesties, PostRefIterWrap, QedInf,
        RefSubCiSubTByRef, RefSubSourceTop, SubfieldCountryInstSourceByRef,
        SubfieldCountryInstSubfieldByRef, WorkingAuthors,
    };

    pub type Tree1<'a> = PostRefIterWrap<'a, Institutions, RefSubCiSubTByRef<'a>>;
    pub type Tree2<'a> = PostRefIterWrap<'a, Institutions, RefSubSourceTop<'a>>;
    pub type Tree3<'a> = WorkingAuthors<'a>;
    pub type Tree4<'a> = PostRefIterWrap<'a, Institutions, CitingSourceCoSuByRef<'a>>;
    pub type Tree5<'a> = PostRefIterWrap<'a, Institutions, QedInf<'a>>;
    pub type Tree6<'a> = PostRefIterWrap<'a, Institutions, CitingCoInstSuToByRef<'a>>;
    pub type Tree7<'a> = InstBesties<'a>;
    pub type Tree8<'a> = PostRefIterWrap<'a, Sources, SubfieldCountryInstSubfieldByRef<'a>>;
    pub type Tree9<'a> = PostRefIterWrap<'a, Sources, SubfieldCountryInstSourceByRef<'a>>;

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

    use crate::components::{CountryBesties, CountryInstsPost, SubfieldCountryInstByRef};

    use super::*;

    pub type Tree1<'a> = CountryInstsPost<
        'a,
        SubfieldCountryInstByRef<'a>,
        (
            IntX<Institutions, 0, true>,
            IntX<Subfields, 1, true>,
            IntX<Countries, 2, false>,
            IntX<Institutions, 2, false>,
        ),
    >;
    pub type Tree2<'a> = CountryBesties<'a>;
}

#[derive_tree_getter(Sources)]
mod source_trees {
    use crate::components::{
        FullRefCountryInstSubfieldByRef, PostRefIterWrap, SubfieldCountryInstSourceByRef,
    };

    use super::*;

    pub type Tree1<'a> = PostRefIterWrap<'a, Sources, SubfieldCountryInstSourceByRef<'a>>;
    pub type Tree2<'a> = PostRefIterWrap<'a, Sources, FullRefCountryInstSubfieldByRef<'a>>;
}

#[derive_tree_getter(Subfields)]
mod subfield_trees {
    use crate::components::{FullRefSourceCountryInstByRef, PostRefIterWrap};

    use super::*;

    pub type Tree1<'a> = PostRefIterWrap<'a, Subfields, FullRefSourceCountryInstByRef<'a>>;
}

pub mod test_tools {
    use crate::components::PartitionId;

    use super::*;

    pub trait TestSB {
        type SB: StackBasis;
        fn get_vec() -> Vec<StackFr<Self::SB>>;
    }

    pub struct Tither<TSB>
    where
        TSB: TestSB,
    {
        viter: IntoIter<StackFr<TSB::SB>>,
    }

    impl<TSB> PartitioningIterator<'_> for Tither<TSB>
    where
        TSB: TestSB,
        TSB::SB: StackBasis,
    {
        type Root = Institutions;
        type StackBasis = TSB::SB;
        const PARTITIONS: usize = 2;
        fn new(_id: ET<Institutions>, _gets: &Getters) -> Self {
            let viter = TSB::get_vec().into_iter();
            Self { viter }
        }
    }

    impl<TSB> Iterator for Tither<TSB>
    where
        TSB: TestSB,
    {
        type Item = (PartitionId, StackFr<TSB::SB>);

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
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use test_tools::*;

    type BigStackFR = (
        IntX<Countries, 0, true>,
        IntX<Works, 0, true>,
        IntX<Institutions, 0, true>,
        IntX<Countries, 0, true>,
    );

    struct BigStack;

    impl TestSB for BigStack {
        type SB = BigStackFR;
        fn get_vec() -> Vec<StackFr<Self::SB>> {
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

    type BigTree = Tither<BigStack>;

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

    use serde_json::to_string_pretty;

    type SimpleStack = IntX<Works, 0, true>;
    type L2Stack = (IntX<Countries, 0, true>, IntX<Subfields, 0, true>);

    struct SimpleStackBasis;

    struct SimpleStackBasisL2;

    struct SimpleStackBasis3;

    impl TestSB for SimpleStackBasis {
        type SB = SimpleStack;
        fn get_vec() -> Vec<StackFr<Self::SB>> {
            vec![(0, 10, 101), (1, 10, 100), (1, 11, 100)]
        }
    }

    impl TestSB for SimpleStackBasis3 {
        type SB = SimpleStack;
        fn get_vec() -> Vec<StackFr<Self::SB>> {
            vec![(1, 0, 1), (0, 1, 0), (0, 0, 0)]
        }
    }

    impl TestSB for SimpleStackBasisL2 {
        type SB = L2Stack;
        fn get_vec() -> Vec<StackFr<Self::SB>> {
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

    type Tree1 = Tither<SimpleStackBasisL2>;

    type Tree2 = Tither<SimpleStackBasis>;

    type Tree3 = Tither<SimpleStackBasis3>;

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
