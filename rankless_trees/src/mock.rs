use dmove::{Entity, ET};
use dmove_macro::{derive_tree_getter, derive_tree_maker};
use rand::{rngs::StdRng, Rng, SeedableRng};
use rankless_rs::agg_tree::{AggTreeBase, FoldingStackConsumer, MinHeap};

use crate::{
    instances::{CollT, FrTm, TreeMaker},
    interfacing::{Getters, NET},
    io::TreeSpec,
};

type WT = ET<Big>;

struct Mid;
struct Big;

// #[derive(PartialOrd, PartialEq)]
// struct WorkTree(HeapedTree<WT>);
// struct WorkTree(AggTreeBase<WT, (), WT>);

impl Entity for Big {
    type T = u32;
    const N: usize = 0x1000;
    const NAME: &str = "big";
}

impl Entity for Mid {
    type T = u16;
    const N: usize = 0x1000;
    const NAME: &str = "mid";
}

// impl FoldingStackConsumer for WorkTree {
//     type Consumable = WT;
//     fn consume(&mut self, child: Self::Consumable) {
//         self.0.children.push(child);
//     }
// }

// impl From<WT> for WorkTree {
//     fn from(value: WT) -> Self {
//         Self((value as u16).into())
//     }
// }

// #[derive_tree_getter(Mid)]
// mod mid_trees {
//     use super::*;
//
//     impl TreeMaker for BigTree {
//         type StackBasis = (
//             IntX<Mid, 0, true>,
//             IntX<Mid, 0, true>,
//             IntX<Mid, 0, true>,
//             IntX<Mid, 0, true>,
//         );
//         fn get_heap(_id: NET<Self::Root>, _gets: &Getters) -> MinHeap<FrTm<Self>> {
//             let mut heap = MinHeap::new();
//             let mut rng = StdRng::seed_from_u64(42);
//             for _ in 0..2_u32.pow(19) {
//                 let rec = (
//                     rng.gen(),
//                     rng.gen(),
//                     rng.gen(),
//                     rng.gen(),
//                     rng.gen(),
//                     rng.gen(),
//                 );
//                 heap.push(rec);
//             }
//             heap
//         }
//     }
// }

mod ht {
    use std::collections::BTreeSet;

    struct HeapedTree<T> {
        id: T,
        children: BTreeSet<T>,
    }

    impl<T> From<T> for HeapedTree<T> {
        fn from(id: T) -> Self {
            Self {
                id,
                children: BTreeSet::new(),
            }
        }
    }

    impl<T> PartialOrd for HeapedTree<T>
    where
        T: PartialOrd,
    {
        fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
            self.id.partial_cmp(&other.id)
        }
    }

    impl<T> PartialEq for HeapedTree<T>
    where
        T: PartialEq,
    {
        fn eq(&self, other: &Self) -> bool {
            self.id.eq(&other.id)
        }
    }
}
