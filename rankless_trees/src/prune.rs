use std::cmp::Reverse;

use dmove::UnsignedNumber;
use hashbrown::HashMap;
use muwo_search::FixedHeap;

use crate::{
    ids::AttributeLabelUnion,
    io::{BreakdownSpec, BufSerChildren, BufSerTree},
};

const MAX_SIBLINGS: usize = 16;
const MAX_DEPTH: usize = 8;
type IndType = u32;

pub fn prune(tree: &mut BufSerTree, astats: &AttributeLabelUnion, bds: &[BreakdownSpec]) {
    let mut denoms = [0; MAX_DEPTH];
    prune_tree::<MAX_SIBLINGS>(tree, astats, bds, &mut denoms, 0)
}

fn prune_tree<const SIZE: usize>(
    tree: &mut BufSerTree,
    astats: &AttributeLabelUnion,
    bds: &[BreakdownSpec],
    denoms: &mut [u32],
    depth: usize,
) {
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
    match tree.children.as_mut() {
        BufSerChildren::Nodes(ref mut nodes) => {
            keep_keys(nodes, &to_keep);
            for node in &mut nodes.values_mut() {
                prune_tree::<SIZE>(node, astats, &bds, denoms, depth + 1);
            }
        }
        BufSerChildren::Leaves(ref mut leaves) => {
            keep_keys(leaves, &to_keep);
        }
    }
}

fn keep_keys<K, V>(map: &mut HashMap<K, V>, to_keep: &Vec<K>)
where
    K: std::cmp::Eq + std::hash::Hash + Copy,
{
    let mut to_dump = Vec::new();
    for k in map.keys() {
        if !to_keep.contains(k) {
            to_dump.push(*k);
        }
    }
    for k in to_dump {
        map.remove(&k);
    }
}
