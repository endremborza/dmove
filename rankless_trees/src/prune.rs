use std::cmp::Reverse;

use dmove::UnsignedNumber;
use hashbrown::HashMap;
use muwo_search::FixedHeap;

use crate::{
    ids::AttributeLabelUnion,
    io::{BreakdownSpec, BufSerChildren, BufSerTree, CollapsedNode},
};

const MAX_SIBLINGS: usize = 16;
const MAX_DEPTH: usize = 8;
type IndType = u32;

pub fn prune(tree: &BufSerTree, astats: &AttributeLabelUnion, bds: &[BreakdownSpec]) -> BufSerTree {
    let mut denoms = [0; MAX_DEPTH];
    prune_tree::<MAX_SIBLINGS>(tree, astats, bds, &mut denoms, 0)
}

fn prune_tree<const SIZE: usize>(
    tree: &BufSerTree,
    astats: &AttributeLabelUnion,
    bds: &[BreakdownSpec],
    denoms: &mut [u32],
    depth: usize,
) -> BufSerTree {
    let mut top_weights: FixedHeap<Reverse<(u32, IndType)>, SIZE> = FixedHeap::new();
    let mut top_specs: FixedHeap<Reverse<(f64, IndType)>, SIZE> = FixedHeap::new();
    denoms[depth] = tree.node.link_count;
    let bd_denom = f64::from(denoms[bds[depth].spec_denom_ind as usize]);

    //TODO: skip all this if less children than SIZE
    let entity_type = &bds[depth].attribute_type;
    for (k, child) in tree.children.as_ref().iter_items() {
        let cw = child.link_count;
        let numerator = f64::from(cw);
        top_weights.push_unique(Reverse((cw, *k)));
        let baseline = match astats.get(entity_type) {
            Some(arr) => arr[k.to_usize()].spec_baseline,
            None => {
                // println!("no {entity_type} in union for prune");
                0.1
            }
        };
        let child_spec = numerator / bd_denom / baseline; //TODO: some correction here?
        top_specs.push_unique(Reverse((child_spec, *k)));
    }

    let to_keep: Vec<u32> = top_weights
        .into_iter()
        .map(|e| e.0 .1)
        .chain(top_specs.into_iter().map(|e| e.0 .1))
        .collect();
    let node = tree.node.clone();
    let children = match tree.children.as_ref() {
        BufSerChildren::Nodes(nodes) => {
            BufSerChildren::Nodes(keep_keys(&nodes, &to_keep, |e: &BufSerTree| {
                prune_tree::<SIZE>(e, astats, &bds, denoms, depth + 1)
            }))
        }
        BufSerChildren::Leaves(leaves) => {
            BufSerChildren::Leaves(keep_keys(&leaves, &to_keep, |e: &CollapsedNode| e.clone()))
        }
    };
    BufSerTree {
        node,
        children: Box::new(children),
    }
}

fn keep_keys<K, V, F>(map: &HashMap<K, V>, to_keep: &Vec<K>, mut keep_map: F) -> HashMap<K, V>
where
    K: std::cmp::Eq + std::hash::Hash + Copy,
    F: FnMut(&V) -> V,
{
    let mut out = HashMap::new();
    for k in to_keep.iter() {
        out.insert(*k, keep_map(&map[k]));
    }
    out
}
