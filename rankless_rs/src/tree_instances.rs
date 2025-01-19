use dmove_macro::derive_tree_getter;
use std::collections::BTreeSet;

use crate::{
    agg_tree::{
        merge_sorted_vecs, merge_sorted_vecs_fun, AggTreeBase, FoldStackBase, FoldingStackConsumer,
        HeapIterator, MinHeap, SortedRecord,
    },
    common::{read_buf_path, InitEmpty},
    env_consts::START_YEAR,
    gen::a1_entity_mapping::{Authors, Countries, Institutions, Sources, Subfields, Topics, Works},
    interfacing::{Getters, NumberedEntity, ET, NET},
    tree_ids::{AttributeLabelUnion, AttributeLabels, LabelFiller, TreeIdSet},
};

use dmove::{Entity, UnsignedNumber};
use dmove_macro::derive_tree_maker;
use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

const POSSIBLE_YEAR_FILTERS: YBT = [START_YEAR, 2010, 2015, 2020, 2021, 2022, 2023, 2024];

pub type TreeSpecMap = HashMap<String, Vec<TreeSpec>>;
type CollT<T> = <T as Collapsing>::Collapsed;
type WT = ET<Works>;
type YBT = [u16; 8];

type FrTm<TM> = <<TM as TreeMaker>::SortedRec as SortedRecord>::FlatRecord;

#[derive(Deserialize, Clone)]
pub struct TreeQ {
    year: Option<u16>,
    eid: u16,
    tid: Option<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct CollapsedNode {
    #[serde(rename = "linkCount")]
    pub link_count: u32,
    #[serde(rename = "sourceCount")]
    source_count: u32,
    #[serde(rename = "topSourceId")]
    top_source: WT,
    #[serde(rename = "topSourceLinks")]
    top_cite_count: u32,
}

#[derive(Serialize, Deserialize)]
pub struct SerTree {
    #[serde(flatten)]
    pub node: CollapsedNode,
    pub children: Box<HashMap<u32, SerChild>>,
}

#[derive(Serialize)]
pub struct TreeResponse {
    tree: SerTree,
    atts: AttributeLabels,
}

#[derive(Serialize)]
pub struct TreeSpecs {
    specs: TreeSpecMap,
    #[serde(rename = "yearBreaks")]
    year_breaks: YBT,
}

#[derive(Serialize)]
pub struct TreeSpec {
    #[serde(rename = "rootType")]
    root_type: String,
    pub breakdowns: Vec<BreakdownSpec>,
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

#[derive(Deserialize, Serialize)]
struct IddCollNode<E: NumberedEntity> {
    id: NET<E>,
    node: CollapsedNode,
}

struct IddSerChild {
    id: usize,
    node: SerChild,
}

struct PrepNode(Vec<WorkTree>);
#[derive(PartialOrd, PartialEq)]
struct WorkTree(AggTreeBase<WT, (), WT>);
struct IntXTree<E: NumberedEntity, C: Collapsing>(AggTreeBase<NET<E>, PrepNode, CollT<C>>);
struct DisJTree<E: NumberedEntity, C: Collapsing>(AggTreeBase<NET<E>, CollapsedNode, CollT<C>>);

struct DisJ<E: Entity, const N: usize, const S: bool>(E::T);
struct IntX<E: Entity, const N: usize, const S: bool>(E::T);

#[derive(Serialize, Deserialize)]
#[serde(untagged)]
pub enum SerChild {
    Leaf(CollapsedNode),
    Node(SerTree),
}

pub trait TreeGetter: NumberedEntity {
    #[allow(unused_variables)]
    fn get_tree(
        gets: &Getters,
        stat_union: &AttributeLabelUnion,
        q: TreeQ,
    ) -> Option<TreeResponse> {
        None
    }

    fn get_specs() -> Vec<TreeSpec>;

    fn get_q(
        tree_q: TreeQ,
        gets: &Getters,
        att_union: &AttributeLabelUnion,
    ) -> Option<TreeResponse> {
        todo!();
        None
    }
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
    type RootTree: Into<SerTree>;
    type NestedIds: LabelFiller;

    fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>>;

    fn get_root_tree<I>(id: NET<Self::Root>, it: I) -> Self::RootTree
    where
        I: Iterator<Item = Self::SortedRec>;

    fn get_spec() -> TreeSpec;
}

trait MergeWith<T> {
    fn merge_into(&self, other: &mut T);
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
    fn update_and_get_collapsed_node(&mut self, v: Vec<WorkTree>) -> CollapsedNode {
        //collapsed version of v parameter
        let mut node = CollapsedNode::init_empty();
        let merger_fun = |l: WorkTree, r: WorkTree| {
            WorkTree(AggTreeBase {
                id: l.0.id,
                node: (),
                children: merge_sorted_vecs(l.0.children, r.0.children),
            })
        };
        let left = std::mem::replace(&mut self.0, Vec::new());
        merge_sorted_vecs_fun(&mut self.0, left, v, merger_fun, |wt| {
            node.update_with_wt(wt)
        });
        node
    }
}

impl SerChild {
    pub fn weight(&self) -> u32 {
        match self {
            Self::Leaf(cn) => cn.link_count,
            Self::Node(n) => n.node.link_count,
        }
    }
}

impl TreeSpecs {
    pub fn new(specs: TreeSpecMap) -> Self {
        Self {
            specs,
            year_breaks: POSSIBLE_YEAR_FILTERS,
        }
    }
}

impl From<WT> for WorkTree {
    fn from(value: WT) -> Self {
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

impl<E, C> From<DisJTree<E, C>> for SerTree
where
    E: NumberedEntity,
    C: Collapsing,
    CollT<C>: Into<IddSerChild>,
{
    fn from(value: DisJTree<E, C>) -> Self {
        let mut children = HashMap::new();
        for child in value.0.children.into_iter() {
            let id_child: IddSerChild = child.into();
            children.insert(id_child.id as u32, id_child.node);
        }
        Self {
            node: value.0.node,
            children: children.into(),
        }
    }
}

impl<E, C> From<DisJTree<E, C>> for IddSerChild
where
    E: NumberedEntity,
    C: Collapsing,
    CollT<C>: Into<IddSerChild>,
{
    fn from(value: DisJTree<E, C>) -> Self {
        let mut children = HashMap::new();
        for child in value.0.children.into_iter() {
            let id_child: IddSerChild = child.into();
            children.insert(id_child.id as u32, id_child.node);
        }
        let tree = SerTree {
            node: value.0.node,
            children: children.into(),
        };
        IddSerChild {
            id: value.0.id.to_usize(),
            node: SerChild::Node(tree),
        }
    }
}

impl<E, C, TE, TC> Into<TreeIdSet<E, C>> for &DisJTree<TE, TC>
where
    C: InitEmpty,
    E: NumberedEntity,
    TE: NumberedEntity,
    TC: Collapsing,
    CollT<TC>: Into<IddSerChild>,
    Vec<TC::Collapsed>: MergeWith<TreeIdSet<E, C>>,
{
    fn into(self) -> TreeIdSet<E, C> {
        let mut o = TreeIdSet::init_empty();
        self.0.children.merge_into(&mut o);
        o
    }
}

impl<E> Into<IddSerChild> for IddCollNode<E>
where
    E: NumberedEntity,
{
    fn into(self) -> IddSerChild {
        IddSerChild {
            id: self.id.to_usize(),
            node: SerChild::Leaf(self.node),
        }
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
    CollT<C>: Into<IddSerChild>,
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
        Self(Vec::new())
    }
}

impl<E, C> InitEmpty for TreeIdSet<E, C>
where
    C: InitEmpty,
    E: NumberedEntity,
{
    fn init_empty() -> Self {
        Self {
            level: BTreeSet::new(),
            child: C::init_empty(),
        }
    }
}

impl Updater<WorkTree> for PrepNode {
    fn update(&mut self, other: WorkTree) -> <WorkTree as Collapsing>::Collapsed {
        self.0.push(other);
    }
}

impl<E> Updater<IntXTree<E, WorkTree>> for PrepNode
where
    E: NumberedEntity,
{
    fn update(&mut self, other: IntXTree<E, WorkTree>) -> CollT<IntXTree<E, WorkTree>> {
        let node = self.update_and_get_collapsed_node(other.0.node.0);
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
        let node = self.update_and_get_collapsed_node(other.0.node.0);
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
        self.0.into_iter().for_each(|e| {
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

impl<PE, E> MergeWith<TreeIdSet<PE, BTreeSet<NET<E>>>> for DisJTree<PE, IntXTree<E, WorkTree>>
where
    E: NumberedEntity,
    PE: NumberedEntity,
{
    fn merge_into(&self, other: &mut TreeIdSet<PE, BTreeSet<NET<E>>>) {
        let mut level_ext = BTreeSet::from_iter(self.0.children.iter().map(|e| e.id.lift()));
        other.level.insert(self.0.id.lift());
        other.child.append(&mut level_ext);
    }
}

impl<E> MergeWith<TreeIdSet<E, ()>> for Vec<IddCollNode<E>>
where
    E: NumberedEntity,
{
    fn merge_into(&self, other: &mut TreeIdSet<E, ()>) {
        let mut level_ext = BTreeSet::from_iter(self.iter().map(|e| e.id.lift()));
        other.level.append(&mut level_ext);
    }
}

impl<E, C, IDCC> MergeWith<TreeIdSet<E, IDCC>> for Vec<DisJTree<E, C>>
where
    E: NumberedEntity,
    C: Collapsing + TopTree,
    Vec<C::Collapsed>: MergeWith<IDCC>,
{
    fn merge_into(&self, ids: &mut TreeIdSet<E, IDCC>) {
        let mut level_ext = BTreeSet::from_iter(self.iter().map(|e| {
            e.0.children.merge_into(&mut ids.child);
            e.0.id.lift()
        }));
        ids.level.append(&mut level_ext);
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
    }
}

pub fn to_bds<T, C>() -> BreakdownSpec
where
    T: FoldStackBase<C>,
{
    BreakdownSpec {
        attribute_type: T::LevelEntity::NAME.to_string(),
        spec_denom_ind: T::SPEC_DENOM_IND as u8,
        source_side: T::SOURCE_SIDE,
    }
}

fn tree_resp<T>(q: TreeQ, gets: &Getters, stats: &AttributeLabelUnion) -> TreeResponse
where
    T: TreeMaker,
    for<'a> &'a T::RootTree: Into<T::NestedIds>,
    <T::SortedRec as SortedRecord>::FlatRecord: Ord + Clone,
{
    let tid = q.tid.unwrap_or(0);
    let year = q.year.unwrap_or(START_YEAR);
    let tree_path = std::path::Path::new("cache")
        .join(T::Root::NAME)
        .join(q.eid.to_string())
        .join(tid.to_string())
        .join(year.to_string());
    if !tree_path.exists() {
        //dump all trees for all years
        let eid = <NET<T::Root> as UnsignedNumber>::from_usize(q.eid.into());
        let heap = T::get_heap(eid, gets);
        let hither: HeapIterator<T::SortedRec> = heap.into();
        let root = T::get_root_tree(eid, hither);
        let ser_tree: SerTree = root.into();
        for year16 in POSSIBLE_YEAR_FILTERS.iter() {
            println!("y");
        }
    }
    let full_tree: SerTree = read_buf_path(tree_path).unwrap();

    let bds = T::get_spec().breakdowns;
    let tree_ids: T::NestedIds = (&root).into();
    let mut atts = HashMap::new();
    tree_ids.fill(&mut atts, stats);

    let resp = TreeResponse {
        tree: root.into(),
        atts,
    };
    return resp;
}

#[derive_tree_getter(Authors)]
mod author_trees {
    use super::*;

    // pub struct Tree1;
    //
    // #[derive_tree_maker]
    // impl TreeMaker for Tree1 {
    //     type StackBasis = (
    //         DisJ<Authors>,
    //         IntX<Years>,
    //         IntX<Subfields>,
    //         IntX<Subfields>,
    //         IntX<Topics>,
    //     );
    //
    //     fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
    //         let mut heap = MinHeap::new();
    //         for refed_wid in gets.aworks(&id) {
    //             let refed_year = gets.year(refed_wid);
    //             for refed_subfield in gets.subfield(refed_wid) {
    //                 for citing_wid in gets.citing(refed_wid) {
    //                     for citing_topic in gets.topic(citing_wid) {
    //                         let citing_subfield = gets.tsuf(citing_topic);
    //                         let record = (
    //                             refed_year.lift(),
    //                             refed_subfield.lift(),
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

    pub struct Tree2;

    #[derive_tree_maker]
    impl TreeMaker for Tree2 {
        type StackBasis = (IntX<Countries, 0, false>, IntX<Subfields, 1, false>);

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

    use crate::gen::a1_entity_mapping::Qs;

    use super::*;

    pub struct TreeSST;

    impl TreeMaker for TreeSST {
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

    pub struct TreeSSoT;

    impl TreeMaker for TreeSSoT {
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

    pub struct TreeACI;

    impl TreeMaker for TreeACI {
        type StackBasis = (
            IntX<Authors, 0, true>,
            IntX<Countries, 1, false>,
            IntX<Institutions, 1, false>,
        );
        fn get_heap(id: NET<Self::Root>, gets: &Getters) -> MinHeap<FrTm<Self>> {
            let mut heap = MinHeap::new();
            for refed_wid in gets.iworks(&id) {
                for refed_ship in gets.wships(refed_wid) {
                    for refed_ship_inst in gets.shipis(refed_ship) {
                        if refed_ship_inst != &id {
                            continue;
                        }
                        let refed_author = gets.shipa(refed_ship);
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

    pub struct TreeSCS;

    impl TreeMaker for TreeSCS {
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

    pub struct TreeQSSC;

    impl TreeMaker for TreeQSSC {
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

    pub struct TreeCIST;

    impl TreeMaker for TreeCIST {
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

    pub struct TreeCSI;

    impl TreeMaker for TreeCSI {
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

    pub struct TreeSCIS;

    #[derive_tree_maker]
    impl TreeMaker for TreeSCIS {
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
}

#[derive_tree_getter(Sources)]
mod source_trees {}

#[derive_tree_getter(Countries)]
mod country_trees {}

#[derive_tree_getter(Subfields)]
mod subfield_trees {}

#[cfg(test)]
mod tests {

    use core::panic;

    use super::*;

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
        type StackBasis = IntX<Countries, 0, true>;
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
        let r = tree_resp::<Tree1>(q(), &Getters::fake(), &HashMap::new());
        println!("{}", to_string_pretty(&r).unwrap());

        match &r.tree.children[&30] {
            SerChild::Node(st) => match &st.children[&21] {
                SerChild::Leaf(lone) => {
                    assert_eq!(lone.source_count, 2);
                    assert_eq!(lone.link_count, 2);
                }
                _ => panic!("no lone"),
            },
            _ => panic!("wrong"),
        };

        match &r.tree.children[&31] {
            SerChild::Node(st) => match &st.children[&20] {
                SerChild::Leaf(lone) => {
                    assert_eq!(lone.source_count, 1);
                    assert_eq!(lone.top_source, 12);
                }
                _ => panic!("no lone"),
            },
            _ => panic!("wrong"),
        };
        assert_eq!(r.tree.node.source_count, 5);
        assert_eq!(r.tree.node.link_count, 12);
        assert_eq!(r.tree.node.top_source, 13);
        assert_eq!(r.tree.node.top_cite_count, 4);
        // assert!(false);
    }

    #[test]
    fn to_tree2() {
        let r = tree_resp::<Tree2>(q(), &Getters::fake(), &HashMap::new());
        println!("{}", to_string_pretty(&r).unwrap());
        assert_eq!(r.tree.node.source_count, 2);
        assert_eq!(r.tree.node.link_count, 3);

        let r = tree_resp::<Tree3>(q(), &Getters::fake(), &HashMap::new());
        println!("{}", to_string_pretty(&r).unwrap());
        assert_eq!(r.tree.node.source_count, 2);
        assert_eq!(r.tree.node.link_count, 3);
    }
}
