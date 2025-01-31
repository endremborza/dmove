use dmove::InitEmpty;
use dmove_macro::def_srecs;
use serde::{Deserialize, Serialize};
use std::{cmp::Reverse, collections::BinaryHeap as MaxHeap};

def_srecs!(7);

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

    pub fn from_iter<I>(iter: I) -> Self
    where
        I: Iterator<Item = T>,
    {
        let mut heap = Self::new();
        iter.for_each(|e| heap.push(e));
        heap
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

impl<SR> From<MinHeap<SR::FlatRecord>> for Option<HeapIterator<SR>>
where
    SR: SortedRecord,
    SR::FlatRecord: Ord + Clone,
{
    fn from(mut heap: MinHeap<SR::FlatRecord>) -> Self {
        let last_rec = heap.pop()?;
        let rec = last_rec.clone();
        let next_srec = Some(rec.into());
        Some(HeapIterator {
            heap,
            last_rec,
            next_srec,
        })
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
