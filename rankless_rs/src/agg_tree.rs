use crate::common::InitEmpty;
use dmove_macro::def_srecs;
use serde::{Deserialize, Serialize};
use std::{cmp::Reverse, collections::BinaryHeap as MaxHeap};

def_srecs!();

#[derive(Deserialize, Serialize, Debug, Clone)]
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

pub fn merge_sorted_vecs_fun<T, F, F2>(
    out: &mut Vec<T>,
    left_vec: Vec<T>,
    right_vec: Vec<T>,
    merging_fun: F,
    mut right_mapper: F2,
) where
    T: PartialOrd,
    F: Fn(T, T) -> T,
    F2: FnMut(&T),
{
    if left_vec.len() == 0 {
        right_vec.iter().for_each(right_mapper);
        let _ = std::mem::replace(out, right_vec);
        return;
    }
    if right_vec.len() == 0 {
        let _ = std::mem::replace(out, left_vec);
        return;
    }
    let mut li = left_vec.into_iter();
    let mut ri = right_vec.into_iter().map(|e| {
        right_mapper(&e);
        e
    });
    let mut left_elem = li.next().unwrap();
    let mut right_elem = ri.next().unwrap();

    loop {
        if left_elem == right_elem {
            out.push(merging_fun(left_elem, right_elem));
            match ri.next() {
                Some(el) => right_elem = el,
                None => break,
            }
            match li.next() {
                Some(el) => left_elem = el,
                None => {
                    out.push(right_elem);
                    break;
                }
            }
        } else if right_elem < left_elem {
            out.push(right_elem);
            match ri.next() {
                Some(el) => right_elem = el,
                None => {
                    out.push(left_elem);
                    break;
                }
            }
        } else {
            out.push(left_elem);
            match li.next() {
                Some(el) => left_elem = el,
                None => {
                    out.push(right_elem);
                    break;
                }
            }
        }
    }
    for rem in li.chain(ri) {
        out.push(rem);
    }
}

pub fn merge_sorted_vecs<T>(left: Vec<T>, right: Vec<T>) -> Vec<T>
where
    T: PartialOrd,
{
    //needs to recycle vectors
    if left.len() == 0 {
        return right;
    }
    if right.len() == 0 {
        return left;
    }
    let mut out = Vec::with_capacity(left.len() + right.len());
    merge_sorted_vecs_fun(&mut out, left, right, |l, _r| l, |_| ());
    out
}
