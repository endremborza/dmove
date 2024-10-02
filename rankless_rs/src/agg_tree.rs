use core::panic;
use std::collections::BinaryHeap;

use dmove::{Entity, EntityImmutableRefMapperBackend, MappableEntity, UnsignedNumber};
use hashbrown::HashMap;

use crate::common::{BackendSelector, IterCompactElement, QuickestBox, QuickestVBox, Stowage};
use crate::gen_derived_links1::{WorkInstitutions, WorkSubfields, WorksCiting};
use crate::gen_derived_links2::AuthorWorks;
use crate::gen_entity_mapping::{Authors, Subfields, Topics, Works};
use crate::gen_init_links::{TopicSubfields, WorkTopics, WorkYears};

struct AggTreePrep<PrepT, ChildPrepT> {
    pub id: usize,
    pub prep: PrepT,
    pub children: Vec<AggTreePrep<ChildPrepT, ChildPrepT>>,
}

struct ManualInstTree1 {
    int_paper_interface: <QuickestVBox as BackendSelector<WorkInstitutions>>::BE,
    citing_interface: <QuickestVBox as BackendSelector<WorksCiting>>::BE,
}

struct ManualAuthorTree1 {
    authorwork_interface: <QuickestVBox as BackendSelector<AuthorWorks>>::BE,
    year_interface: <QuickestBox as BackendSelector<WorkYears>>::BE,
    citing_interface: <QuickestVBox as BackendSelector<WorksCiting>>::BE,
    topic_interface: <QuickestVBox as BackendSelector<WorkTopics>>::BE,
    subfield_interface: <QuickestVBox as BackendSelector<WorkSubfields>>::BE,
    t2sf_interface: <QuickestBox as BackendSelector<TopicSubfields>>::BE,
}

impl ManualAuthorTree1 {
    fn new(stowage: &Stowage) -> Self {
        Self {
            authorwork_interface: stowage.get_entity_interface::<AuthorWorks, QuickestVBox>(),
            year_interface: stowage.get_entity_interface::<WorkYears, QuickestBox>(),
            citing_interface: stowage.get_entity_interface::<WorksCiting, QuickestVBox>(),
            topic_interface: stowage.get_entity_interface::<WorkTopics, QuickestVBox>(),
            subfield_interface: stowage.get_entity_interface::<WorkSubfields, QuickestVBox>(),
            t2sf_interface: stowage.get_entity_interface::<TopicSubfields, QuickestBox>(),
        }
    }

    fn build_from_root(&mut self, aid: <Authors as Entity>::T) {
        let mut heap = BinaryHeap::new();
        for refed_wid in map_ref::<AuthorWorks, _, _>(&self.authorwork_interface, &aid).into_iter()
        {
            let refed_year = map_ref::<WorkYears, _, _>(&self.year_interface, refed_wid);
            for citing_wid in
                map_ref::<WorksCiting, _, _>(&self.citing_interface, refed_wid).into_iter()
            {
                for refed_subfield in
                    map_ref::<WorkSubfields, _, _>(&self.subfield_interface, citing_wid).into_iter()
                {
                    for citing_topic in
                        map_ref::<WorkTopics, _, _>(&self.topic_interface, citing_wid)
                    {
                        let citing_subfield =
                            map_ref::<TopicSubfields, _, _>(&self.t2sf_interface, citing_topic);
                        let record = (
                            refed_year.lift(),
                            refed_subfield.lift(),
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
    }
}

type R0T = (u8, u8, u8, u16, u16, u16);
type R1T = (u8, u8, u16, u16, u16);
type R2T = (u8, u16, u16, u16);
type R3T = (u16, u16, u16);
type R4T = (u16, u16);
type R5T = u16;

enum LeftRightOrderedTableBlock {
    New1(R0T),
    New2(R1T),
    New3(R2T),
    New4(R3T),
    New5(R4T),
    New6(R5T),
}

struct LeftRightOrderedTable(Box<[LeftRightOrderedTableBlock]>);

trait InitEmpty {
    fn init_empty() -> Self;
}

impl<K, V> InitEmpty for HashMap<K, V> {
    fn init_empty() -> Self {
        Self::new()
    }
}

impl<T> InitEmpty for Vec<T> {
    fn init_empty() -> Self {
        Self::new()
    }
}

struct LevelAgg {
    link_count: u32,
    top_source: <Works as Entity>::T,
    top_cite_count: u32,
}

impl LevelAgg {
    fn ingest(&mut self, e: PreCollapseBlock) -> PreCollapseBlock {
        let lc = e.1.len() as u32;
        if lc > self.top_cite_count {
            self.top_cite_count = lc;
            self.top_source = e.0;
        }
        self.link_count += lc;
        e
    }
}

impl InitEmpty for LevelAgg {
    fn init_empty() -> Self {
        Self {
            link_count: 0,
            top_source: 0,
            top_cite_count: 0,
        }
    }
}

impl<T, CT> AggTreePrep<T, CT>
where
    T: InitEmpty,
{
    fn new(id: usize) -> Self {
        Self {
            id,
            children: Vec::new(),
            prep: T::init_empty(),
        }
    }
}

impl<T, CT> AggTreePrep<T, CT> {
    fn new_prepped(id: usize, prep: T) -> Self {
        Self {
            id,
            children: Vec::new(),
            prep,
        }
    }
}

type PreCollapseBlock = (<Works as Entity>::T, Vec<<Works as Entity>::T>);
type PreCollapsePrep = Vec<PreCollapseBlock>;

type PostCollapsePrep = LevelAgg;

type PostCollapseAggTree = AggTreePrep<PostCollapsePrep, PostCollapsePrep>;
type PreCollapseAggTree = AggTreePrep<PreCollapsePrep, PostCollapsePrep>;

type PrepStack = (
    PostCollapseAggTree,
    PreCollapseAggTree,
    PreCollapseAggTree,
    PreCollapseAggTree,
    PreCollapseAggTree,
);

impl LeftRightOrderedTable {
    fn from_heap(mut heap: BinaryHeap<R0T>) -> Self {
        use LeftRightOrderedTableBlock::*;
        let mut last_rec = heap.pop().unwrap();
        let mut v = vec![LeftRightOrderedTableBlock::New1(last_rec.clone())];

        while let Some(rec) = heap.pop() {
            let ne = if rec.0 != last_rec.0 {
                //only _breaking_ level - creates definitely disjunct subsets of aggregable IDS
                //therefore _collapse_ here
                New1(rec)
            } else if rec.1 != last_rec.1 {
                New2((rec.1, rec.2, rec.3, rec.4, rec.5))
            } else if rec.2 != last_rec.2 {
                New3((rec.2, rec.3, rec.4, rec.5))
            } else if rec.3 != last_rec.3 {
                New4((rec.3, rec.4, rec.5))
            } else if rec.4 != last_rec.4 {
                New5((rec.4, rec.5))
            } else {
                New6(rec.5)
            };
            v.push(ne);
            last_rec = rec;
        }
        Self(v.into_boxed_slice())
    }

    fn to_agg_tree_prep(self, id: usize) -> PostCollapseAggTree {
        use LeftRightOrderedTableBlock::*;
        //this is post because year is toOne on referenced
        let mut self_iterator = self.0.into_vec().into_iter();
        let mut prep_stack =
            if let LeftRightOrderedTableBlock::New1(rec) = self_iterator.next().unwrap() {
                (
                    PostCollapseAggTree::new(id),
                    PreCollapseAggTree::new(rec.0.to_usize()),
                    PreCollapseAggTree::new(rec.1.to_usize()),
                    PreCollapseAggTree::new(rec.2.to_usize()),
                    PreCollapseAggTree::new_prepped(rec.3.to_usize(), vec![(rec.4, vec![rec.5])]),
                )
            } else {
                panic!("bad first row");
            };
        for block in self_iterator {
            match block {
                New1(rec) => {
                    consume_full_rec(&mut prep_stack, rec);
                    //pack up all stacks to one
                    //if breakdown level is toOne kind from either refed or citing
                    //that is why root is post thing and that is why we can just push
                    //but push should also increase counts and change post prep things
                    //so post should ingest the same way
                }
                New2(rec) => {
                    let last = std::mem::replace(
                        &mut prep_stack.4,
                        PreCollapseAggTree::new_prepped(
                            rec.2.to_usize(),
                            vec![(rec.3, vec![rec.4])],
                        ),
                    );
                    consume_pre_collapse(&mut prep_stack.3, last);

                    let plast = std::mem::replace(
                        &mut prep_stack.3,
                        PreCollapseAggTree::new(rec.1.to_usize()),
                    );
                    consume_pre_collapse(&mut prep_stack.2, plast);

                    let pplast = std::mem::replace(
                        &mut prep_stack.2,
                        PreCollapseAggTree::new(rec.0.to_usize()),
                    );
                    consume_pre_collapse(&mut prep_stack.1, pplast);
                }
                New3(rec) => {
                    let last = std::mem::replace(
                        &mut prep_stack.4,
                        PreCollapseAggTree::new_prepped(
                            rec.1.to_usize(),
                            vec![(rec.2, vec![rec.3])],
                        ),
                    );
                    consume_pre_collapse(&mut prep_stack.3, last);

                    let plast = std::mem::replace(
                        &mut prep_stack.3,
                        PreCollapseAggTree::new(rec.0.to_usize()),
                    );
                    consume_pre_collapse(&mut prep_stack.2, plast);
                }
                New4(rec) => {
                    let last = std::mem::replace(
                        &mut prep_stack.4,
                        PreCollapseAggTree::new_prepped(
                            rec.0.to_usize(),
                            vec![(rec.1, vec![rec.2])],
                        ),
                    );
                    consume_pre_collapse(&mut prep_stack.3, last);
                }
                New5(rec) => {
                    add_rec(&mut prep_stack.3.prep, rec);
                }
                New6(rec) => {
                    let last_idx = prep_stack.3.prep.len() - 1;
                    prep_stack.3.prep[last_idx].1.push(rec);
                }
            };
        }
        consume_full_rec(&mut prep_stack, (0, 0, 0, 0, 0, 0));
        prep_stack.0
    }
}

fn consume_full_rec(prep_stack: &mut PrepStack, rec: R0T) {
    let last = std::mem::replace(
        &mut prep_stack.4,
        PreCollapseAggTree::new_prepped(rec.3.to_usize(), vec![(rec.4, vec![rec.5])]),
    );
    consume_pre_collapse(&mut prep_stack.3, last);

    let plast = std::mem::replace(&mut prep_stack.3, PreCollapseAggTree::new(rec.2.to_usize()));
    consume_pre_collapse(&mut prep_stack.2, plast);

    let pplast = std::mem::replace(&mut prep_stack.2, PreCollapseAggTree::new(rec.1.to_usize()));
    consume_pre_collapse(&mut prep_stack.1, pplast);

    let ppplast = std::mem::replace(&mut prep_stack.1, PreCollapseAggTree::new(rec.0.to_usize()));
    consume_post_collapse(&mut prep_stack.0, ppplast);
}

fn add_rec(prep: &mut PreCollapsePrep, rec: R4T) {
    prep.push((rec.0, vec![rec.1]))
}

fn consume_post_collapse(tree: &mut PostCollapseAggTree, child: PreCollapseAggTree) {
    tree.children.push(collapse(child));
}

fn consume_pre_collapse(tree: &mut PreCollapseAggTree, child: PreCollapseAggTree) {
    let mut child_prep = PostCollapsePrep::init_empty();
    let child_iter = child.prep.into_iter().map(|e| child_prep.ingest(e));
    merge_sorted_iter_into_vec(&mut tree.prep, child_iter, |l, r| {
        merge_sorted_vecs(&mut l.1, r.1);
    });

    tree.children.push(PostCollapseAggTree {
        id: child.id,
        children: child.children,
        prep: child_prep,
    });
}

fn collapse(tree: PreCollapseAggTree) -> PostCollapseAggTree {
    let mut prep = PostCollapsePrep::init_empty();
    tree.prep.into_iter().for_each(|e| {
        prep.ingest(e);
    });
    PostCollapseAggTree {
        id: tree.id,
        children: tree.children,
        prep,
    }
}

fn merge_sorted_iter_into_vec<RI, T, F>(left_vec: &mut Vec<T>, mut right_iter: RI, merging_fun: F)
where
    T: PartialOrd,
    F: Fn(&mut T, T),
    RI: Iterator<Item = T>,
{
    //TODO: this needs testing
    let mut i = 0;
    let n = left_vec.len();
    'outer: while let Some(mut right_elem) = right_iter.next() {
        if i >= n {
            break 'outer;
        }
        let mut left_elem = &mut left_vec[i];
        if left_elem == &right_elem {
            merging_fun(left_elem, right_elem);
            i += 1;
            continue;
        }
        while left_elem < &mut right_elem {
            i += 1;
            if i >= n {
                break 'outer;
            }
            left_elem = &mut left_vec[i];
        }
        if &right_elem < left_elem {
            left_vec.insert(i, right_elem);
        }
        i += 1;
    }
    for e in right_iter {
        left_vec.push(e)
    }
}

fn merge_sorted_vecs<T>(left: &mut Vec<T>, right: Vec<T>)
where
    T: PartialOrd,
{
    merge_sorted_iter_into_vec(left, right.into_iter(), |_l, _r| {})
}

fn map_ref<'a, E, IF, K>(interface: &'a IF, key: &K) -> &'a E::T
where
    E: Entity + MappableEntity<E, KeyType = usize>,
    IF: EntityImmutableRefMapperBackend<E, E>,
    K: UnsignedNumber,
{
    interface.get_ref_via_immut(&key.to_usize()).unwrap()
}

fn main(stowage: Stowage) {
    let mut tree_builder = ManualAuthorTree1::new(&stowage);
    let root_interface = stowage.get_entity_interface::<Authors, IterCompactElement>();
    for aid in root_interface {
        tree_builder.build_from_root(aid)
    }
}
