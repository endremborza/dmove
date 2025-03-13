use std::vec::IntoIter;

use crate::{
    components::{
        AuthorBestiePapers, AuthorBesties, CiteSubSourceTop, CitingCoInstSuToByRef,
        CitingCoSuToByRef, CitingSourceCoSuByRef, CountryBesties, CountryInstsPost,
        FullRefCountryInstSubfieldByRef, InstBesties, IntX, PostRefIterWrap, QedInf,
        RefSubCiSubTByRef, SourceSubfieldCiCoByRef, SourceWCoiByRef, StackBasis, StackFr,
        SubfieldCountryInstByRef, SubfieldCountryInstSourceByRef, SubfieldCountryInstSubfieldByRef,
        SubfieldRefTopicCountryInst, SubfieldWCoiByRef, WorkingAuthors,
    },
    interfacing::Getters,
    io::{
        BufSerChildren, BufSerTree, CollapsedNode, FullTreeQuery, ResCvp, TreeBasisState, TreeQ,
        TreeResponse, TreeSpec, WorkCiteT, WorkWInd, WT,
    },
    part_iterator::PartitioningIterator,
};
use muwo_search::{ordered_calls, sorted_iters_to_arr, ExtendableArr, OrderedMapper};
use rankless_rs::{
    agg_tree::{AggTreeBase, ReinstateFrom, Updater},
    common::{NumberedEntity, NET},
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
    fn set_tree(state: &TreeBasisState, fq: FullTreeQuery, res_cvp: ResCvp);

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

//TODO/clarity - low-prio - these could be derived
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
    pub type Tree1<'a> = PostRefIterWrap<'a, Authors, SourceWCoiByRef<'a>>;
    pub type Tree2<'a> = PostRefIterWrap<'a, Authors, CitingCoSuToByRef<'a>>;
    pub type Tree3<'a> = PostRefIterWrap<'a, Authors, SubfieldCountryInstSubfieldByRef<'a>>;
    pub type Tree4<'a> = PostRefIterWrap<'a, Authors, SubfieldWCoiByRef<'a>>;
    pub type Tree5<'a> = PostRefIterWrap<'a, Authors, RefSubCiSubTByRef<'a>>;
    pub type Tree6<'a> = PostRefIterWrap<'a, Authors, CitingCoInstSuToByRef<'a>>;
    pub type Tree7<'a> = PostRefIterWrap<'a, Authors, SourceSubfieldCiCoByRef<'a>>;
    pub type Tree8<'a> = AuthorBestiePapers<'a>;
    pub type Tree9<'a> = AuthorBesties<'a>;
}

#[derive_tree_getter(Institutions)]
mod inst_trees {
    use super::*;

    pub type Tree1<'a> = PostRefIterWrap<'a, Institutions, RefSubCiSubTByRef<'a>>;
    pub type Tree2<'a> = PostRefIterWrap<'a, Institutions, CiteSubSourceTop<'a>>;
    pub type Tree3<'a> = WorkingAuthors<'a>;
    pub type Tree4<'a> = PostRefIterWrap<'a, Institutions, CitingSourceCoSuByRef<'a>>;
    pub type Tree5<'a> = PostRefIterWrap<'a, Institutions, QedInf<'a>>;
    pub type Tree6<'a> = PostRefIterWrap<'a, Institutions, CitingCoInstSuToByRef<'a>>;
    pub type Tree7<'a> = InstBesties<'a>;
    pub type Tree8<'a> = PostRefIterWrap<'a, Institutions, SubfieldCountryInstSubfieldByRef<'a>>;
    pub type Tree9<'a> = PostRefIterWrap<'a, Institutions, SubfieldCountryInstSourceByRef<'a>>;
}

#[derive_tree_getter(Countries)]
mod country_trees {

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
    pub type Tree3<'a> = CountryInstsPost<
        'a,
        SourceSubfieldCiCoByRef<'a>,
        (
            IntX<Institutions, 0, true>,
            IntX<Sources, 1, true>,
            IntX<Subfields, 2, true>,
            IntX<Countries, 3, false>,
        ),
    >;
}

#[derive_tree_getter(Sources)]
mod source_trees {

    use super::*;

    pub type Tree1<'a> = PostRefIterWrap<'a, Sources, SubfieldCountryInstSourceByRef<'a>>;
    pub type Tree2<'a> = PostRefIterWrap<'a, Sources, FullRefCountryInstSubfieldByRef<'a>>;
    pub type Tree3<'a> = PostRefIterWrap<'a, Sources, CitingSourceCoSuByRef<'a>>;
}

#[derive_tree_getter(Subfields)]
mod subfield_trees {
    use crate::components::{FullRefSourceCountryInstByRef, PostRefIterWrap};

    use super::*;

    pub type Tree1<'a> = SubfieldRefTopicCountryInst<'a>;
    pub type Tree2<'a> = PostRefIterWrap<'a, Subfields, FullRefSourceCountryInstByRef<'a>>;
}

pub mod test_tools {
    use rankless_rs::steps::a1_entity_mapping::N_PERS;

    use crate::components::PartitionId;

    use super::*;

    pub trait TestSB {
        type SB: StackBasis;
        fn get_vec() -> Vec<StackFr<Self::SB>>;
        fn get_pid(_v: &StackFr<Self::SB>) -> PartitionId {
            0
        }
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
        const PARTITIONS: usize = N_PERS;
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
                Some(v) => Some((TSB::get_pid(&v), v)),
                None => None,
            }
        }
    }
}

pub mod big_test_tree {
    use std::{sync::Arc, thread, time::Duration};

    use super::*;
    use crate::io::{AttributeLabel, TreeRunManager};
    use dmove::{BigId, MappableEntity, NamespacedEntity};
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use test_tools::*;

    type BigStackFR = (
        IntX<Countries, 0, true>,
        IntX<Works, 0, true>,
        IntX<Institutions, 0, true>,
        IntX<Countries, 0, true>,
    );

    pub struct BigTestEntity;

    impl Entity for BigTestEntity {
        const N: usize = 0;
        const NAME: &str = "test";
        type T = u8;
    }

    impl NamespacedEntity for BigTestEntity {
        const NS: &str = "test";
    }

    impl MappableEntity for BigTestEntity {
        type KeyType = BigId;
    }

    #[derive_tree_getter(BigTestEntity)]
    mod submod {
        use super::*;
        pub type Tree1 = Tither<BigStack>;
    }

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

    pub fn get_big_tree(_n: usize) -> TreeResponse {
        let mut fake_attu = HashMap::new();
        let gatts = |i: u32, pref: &str| {
            (0..2_u32.pow(i))
                .map(|e| AttributeLabel {
                    name: format!("{pref}{e}"),
                    semantic_id: format!("{pref}{e}"),
                    spec_baseline: 0.5,
                })
                .collect::<Box<[AttributeLabel]>>()
        };
        fake_attu.insert(Countries::NAME.to_string(), gatts(8, "C"));
        fake_attu.insert(Institutions::NAME.to_string(), gatts(16, "I"));
        let q = TreeQ {
            year: None,
            tid: None,
            connections: None,
            big_prep: None,
            big_read: None,
        };

        let tstate = TreeRunManager::<(BigTestEntity, BigTestEntity)>::fake();
        let name = BigTestEntity::NAME.to_string();
        let id = "0".to_string();

        let ts1 = tstate.clone();
        let (q1, n1, i1) = (q.clone(), name.clone(), id.clone());
        let t = thread::spawn(move || ts1.get_resp(q1, &n1, &i1).unwrap());
        thread::sleep(Duration::from_millis(100));
        let ts2 = tstate.clone();
        let t2 = thread::spawn(move || ts2.get_resp(q, &name, &id).unwrap());

        let r = t.join().unwrap();
        let r2 = t2.join().unwrap();
        assert_eq!(r2.tree.node.top_source, r.tree.node.top_source);

        Arc::into_inner(tstate).unwrap().join();
        r
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::io::{JsSerChildren, TreeRunManager};
    use dmove::{BigId, MappableEntity, NamespacedEntity};
    use rand::{rngs::StdRng, Rng, SeedableRng};
    use rankless_rs::steps::{
        a1_entity_mapping::{RawYear, YearInterface, Years, POSSIBLE_YEAR_FILTERS},
        derive_links1::WorkPeriods,
    };
    use std::{ops::Deref, sync::Arc};
    use test_tools::{TestSB, Tither};

    use serde_json::to_string_pretty;

    type SimpleStack = IntX<Works, 0, true>;
    type L2Stack = (IntX<Countries, 0, true>, IntX<Subfields, 0, true>);
    type PartedStackFR = (
        IntX<Countries, 0, true>,
        IntX<Countries, 0, true>,
        IntX<Works, 0, true>,
    );
    struct PartedStack;

    struct TestEntity;

    impl Entity for TestEntity {
        const N: usize = 0;
        const NAME: &str = "test";
        type T = u8;
    }

    impl NamespacedEntity for TestEntity {
        const NS: &str = "test";
    }

    impl MappableEntity for TestEntity {
        type KeyType = BigId;
    }

    #[derive_tree_getter(TestEntity)]
    mod submod {
        use super::*;
        pub type Tree1 = Tither<SimpleStackBasisL2>;
        pub type Tree2 = Tither<SimpleStackBasis>;
        pub type Tree3 = Tither<SimpleStackBasis3>;
        pub type Tree4 = Tither<PartedStack>;
    }

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

    impl TestSB for PartedStack {
        type SB = PartedStackFR;
        fn get_vec() -> Vec<StackFr<Self::SB>> {
            let mut vec = Vec::new();
            let mut rng = StdRng::seed_from_u64(742);
            for _ in 0..2_u32.pow(16) {
                let rec = (
                    rng.gen(),
                    rng.gen(),
                    rng.gen(),
                    rng.gen::<u32>() % (Years::N as u32), //TODO: year test hack quickfix
                    rng.gen(),
                );
                vec.push(rec);
            }
            vec
        }
        fn get_pid(v: &StackFr<Self::SB>) -> crate::components::PartitionId {
            let y8 = (v.3 as usize % Years::N) as u8;
            let y16 = YearInterface::reverse(y8);
            WorkPeriods::from_year(y16)
        }
    }

    fn q(i: u8) -> TreeQ {
        TreeQ {
            year: None,
            tid: Some(i),
            connections: None,
            big_prep: None,
            big_read: None,
        }
    }

    #[test]
    fn to_tree1() {
        let tstate = TreeRunManager::<(TestEntity, TestEntity)>::fake();
        let id = "0".to_string();

        let r = tstate
            .get_resp(q(0), &TestEntity::NAME.to_string(), &id)
            .unwrap();
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
        assert_eq!(r.tree.node.top_source, Some(13));
        assert_eq!(r.tree.node.top_cite_count, 4);
        Arc::into_inner(tstate).unwrap().join();
    }

    #[test]
    fn to_tree2() {
        let tstate = TreeRunManager::<(TestEntity, TestEntity)>::fake();
        let name = TestEntity::NAME.to_string();
        let id = "0".to_string();
        let r = tstate.get_resp(q(1), &name, &id).unwrap();
        println!("{}", to_string_pretty(&r).unwrap());
        val_res2(&r);
        let rcached = tstate.get_resp(q(1), &name, &id).unwrap();
        val_res2(&rcached);

        let r = tstate.get_resp(q(2), &name, &id).unwrap();
        println!("{}", to_string_pretty(&r).unwrap());
        assert_eq!(r.tree.node.source_count, 2);
        assert_eq!(r.tree.node.link_count, 3);

        Arc::into_inner(tstate).unwrap().join();
    }

    fn val_res2(r: &TreeResponse) {
        assert_eq!(r.tree.node.source_count, 2);
        assert_eq!(r.tree.node.link_count, 3);
        assert_eq!(r.atts.keys().len(), 2); //added the key of root entity
    }

    #[test]
    pub fn big_tree() {
        let r = big_test_tree::get_big_tree(20);
        let node = &r.tree.node;
        println!(
            "lc: {}, sc: {}, ts: {:?},",
            node.link_count, node.source_count, node.top_source,
        );

        assert_eq!(r.tree.node.link_count, 1048576); //20 - full
        assert_eq!(r.tree.node.source_count, 1048434); //20 - full
        assert_eq!(r.tree.node.top_source, Some(16735219)); //20 - full
    }

    #[test]
    pub fn big_parted_tree() {
        let tstate = TreeRunManager::<(TestEntity, TestEntity)>::fake();
        let name = TestEntity::NAME.to_string();
        let q = TreeQ {
            year: None,
            tid: Some(3),
            connections: None,
            big_prep: Some(true),
            big_read: None,
        };
        let id = "0".to_string();
        let resp = tstate.get_resp(q, &name, &id).unwrap();
        assert_eq!(resp.tree.node.top_cite_count, 0);
        let mut years: Vec<Option<RawYear>> =
            POSSIBLE_YEAR_FILTERS.iter().map(|e| Some(*e)).collect();
        years.insert(0, None);
        for year in years.into_iter() {
            let q_read = TreeQ {
                year,
                tid: Some(3),
                connections: None,
                big_prep: None,
                big_read: Some(true),
            };
            let resp2 = tstate.get_resp(q_read, &name, &id).unwrap();
            assert_eq!(resp.tree.node.top_cite_count, 0);
            let q_recalc = TreeQ {
                year,
                tid: Some(3),
                connections: None,
                big_prep: None,
                big_read: None,
            };
            let id2 = "1".to_string();
            let resp3 = tstate.get_resp(q_recalc, &name, &id2).unwrap();
            assert_eq!(resp2.tree.node, resp3.tree.node);
        }
    }

    // assert_eq!(r.tree.node.link_count, 1048448); //20 - nano
    // assert_eq!(r.tree.node.source_count, 65536); //20 - nano
    // assert_eq!(r.tree.node.top_source, 28260); //20 - nano
    //
    // assert_eq!(r.tree.node.link_count, 2097152); //21
    // assert_eq!(r.tree.node.link_count, 524288); //19
}
