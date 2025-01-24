use crate::common::InitEmpty;
use dmove_macro::def_srecs;
use serde::{Deserialize, Serialize};
use std::{cmp::Reverse, collections::BinaryHeap as MaxHeap};

def_srecs!();

#[derive(Deserialize, Serialize, Debug)]
pub struct AggTreeBase<IdType, Node, Child> {
    pub id: IdType,
    pub node: Node,
    pub children: Vec<Child>,
}

pub struct MinHeap<T>(MaxHeap<Reverse<T>>);

impl<T> MinHeap<T>
where
    T: Ord,
{
    pub fn new() -> Self {
        Self(MaxHeap::new())
    }

    pub fn pop(&mut self) -> Option<T> {
        if let Some(e) = self.0.pop() {
            Some(e.0)
        } else {
            None
        }
    }

    pub fn push(&mut self, e: T) {
        self.0.push(Reverse(e))
    }
}

pub struct HeapIterator<SR: SortedRecord>
where
    SR::FlatRecord: Ord,
{
    heap: MinHeap<SR::FlatRecord>,
    last_rec: SR::FlatRecord,
    next_srec: Option<SR>,
}

pub trait FoldingStackConsumer {
    type Consumable;

    fn consume(&mut self, child: Self::Consumable);
}

pub trait SortedRecord: Sized {
    type FlatRecord: Into<Self>;

    fn from_cmp(last_rec: &Self::FlatRecord, next_rec: Self::FlatRecord) -> Option<Self>;
}

pub trait Updater<C> {
    //+initiator replacing other
    //+?
    fn update<T>(&mut self, other: &mut C, other_reinitiator: T)
    where
        C: ReinstateFrom<T>;
}

pub trait ReinstateFrom<T> {
    fn reinstate_from(&mut self, value: T);
}

impl<SR> From<MinHeap<SR::FlatRecord>> for HeapIterator<SR>
where
    SR: SortedRecord,
    SR::FlatRecord: Ord + Clone,
{
    fn from(mut heap: MinHeap<SR::FlatRecord>) -> Self {
        let last_rec = heap.pop().unwrap();
        let rec = last_rec.clone();
        let next_srec = Some(rec.into());
        Self {
            heap,
            last_rec,
            next_srec,
        }
    }
}

impl<SR> Iterator for HeapIterator<SR>
where
    SR: SortedRecord,
    SR::FlatRecord: Ord + Clone,
{
    type Item = SR;

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_srec.is_none() {
            return None;
        }
        let mut next_srec = None;
        while let Some(rec) = self.heap.pop() {
            let rclone = rec.clone();
            if let Some(srec) = SR::from_cmp(&self.last_rec, rec) {
                self.last_rec = rclone;
                next_srec = Some(srec);
                break;
            };
        }
        std::mem::replace(&mut self.next_srec, next_srec)
    }
}

impl<IT, T, CT> From<IT> for AggTreeBase<IT, T, CT>
where
    T: InitEmpty,
{
    fn from(id: IT) -> Self {
        Self {
            id,
            children: Vec::new(),
            node: T::init_empty(),
        }
    }
}

impl<I, N, C> PartialOrd for AggTreeBase<I, N, C>
where
    I: PartialOrd,
{
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.id.partial_cmp(&other.id)
    }
}

impl<I: PartialEq, N, C> PartialEq for AggTreeBase<I, N, C> {
    fn eq(&self, other: &Self) -> bool {
        self.id.eq(&other.id)
    }
}

pub struct VecExtender<'a, T> {
    v: &'a mut Vec<T>,
}

pub trait OrderedMapper {
    type Elem;
    fn left_map(&mut self, e: &Self::Elem);
    fn right_map(&mut self, e: &Self::Elem);
    fn common_map(&mut self, l: &Self::Elem, r: &Self::Elem);
}

impl<'a, T> OrderedMapper for VecExtender<'a, T>
where
    T: Clone,
{
    type Elem = T;
    fn left_map(&mut self, e: &Self::Elem) {
        self.v.push(e.clone())
    }
    fn right_map(&mut self, e: &Self::Elem) {
        self.v.push(e.clone())
    }
    fn common_map(&mut self, l: &Self::Elem, _r: &Self::Elem) {
        self.v.push(l.clone())
    }
}

pub fn merge_sorted_vecs<T>(left_vec: Vec<T>, right_vec: Vec<T>) -> Vec<T>
where
    T: PartialOrd + Clone,
{
    let mut v = Vec::new();
    sorted_iters_to_vec(&mut v, left_vec.iter(), right_vec.iter());
    v
}

pub fn sorted_iters_to_vec<'a, 'b, T, IL, IR>(out: &mut Vec<T>, left_it: IL, right_it: IR)
where
    T: PartialOrd + Clone + 'a + 'b,
    IL: Iterator<Item = &'a T>,
    IR: Iterator<Item = &'b T>,
{
    let mut vadd = VecExtender { v: out };
    ordered_calls(left_it, right_it, &mut vadd);
}

pub fn ordered_calls<'a, 'b, T, IL, IR, M>(mut left_it: IL, mut right_it: IR, merger: &mut M)
where
    T: PartialOrd + 'a + 'b,
    IL: Iterator<Item = &'a T>,
    IR: Iterator<Item = &'b T>,
    M: OrderedMapper<Elem = T> + ?Sized,
{
    let mut left_elem = match left_it.next() {
        Some(v) => v,
        None => return right_it.for_each(|e| merger.right_map(e)),
    };

    let mut right_elem = match right_it.next() {
        Some(v) => v,
        None => return left_it.for_each(|e| merger.left_map(e)),
    };

    loop {
        if left_elem == right_elem {
            merger.common_map(left_elem, right_elem);
            match right_it.next() {
                Some(el) => right_elem = el,
                None => break,
            }
            match left_it.next() {
                Some(el) => left_elem = el,
                None => {
                    merger.right_map(right_elem);
                    break;
                }
            }
        } else if right_elem < left_elem {
            merger.right_map(right_elem);
            match right_it.next() {
                Some(el) => right_elem = el,
                None => {
                    merger.left_map(left_elem);
                    break;
                }
            }
        } else {
            merger.left_map(left_elem);
            match left_it.next() {
                Some(el) => left_elem = el,
                None => {
                    merger.right_map(right_elem);
                    break;
                }
            }
        }
    }
    left_it.for_each(|e| merger.left_map(e));
    right_it.for_each(|e| merger.right_map(e));
}
