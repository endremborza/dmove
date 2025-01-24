use dmove::{Entity, EntityMutableMapperBackend, UnsignedNumber, VattReadingRefMap};

use hashbrown::HashMap;

use crate::io::{AttributeLabel, AttributeLabels, BreakdownSpec, BufSerChildren, BufSerTree};
use rankless_rs::gen::{a1_entity_mapping::Works, a2_init_atts::WorksNames};

pub type AttributeLabelUnion = HashMap<String, Box<[AttributeLabel]>>;

pub fn get_atts(
    tree: &BufSerTree,
    bds: &[BreakdownSpec],
    union: &AttributeLabelUnion,
    work_map: &mut VattReadingRefMap<WorksNames>,
) -> AttributeLabels {
    let mut atts = HashMap::new();
    ext_atts(&mut atts, tree, bds, union, work_map);
    atts
}

fn ext_atts(
    atts: &mut AttributeLabels,
    tree: &BufSerTree,
    bds: &[BreakdownSpec],
    union: &AttributeLabelUnion,
    work_map: &mut VattReadingRefMap<WorksNames>,
) {
    let at = &bds[0].attribute_type;
    let eatts = atts.entry(at.to_string()).or_insert(HashMap::new());
    match tree.children.as_ref() {
        BufSerChildren::Leaves(leaves) => add_leaves(leaves.keys(), eatts, union, work_map, at),
        BufSerChildren::Nodes(nodes) => {
            add_leaves(nodes.keys(), eatts, union, work_map, at);
            nodes
                .values()
                .for_each(|v| ext_atts(atts, v, &bds[1..], union, work_map))
        }
    };
}

fn add_leaves<'a, I>(
    leaves: I,
    eatts: &mut HashMap<usize, AttributeLabel>,
    union: &AttributeLabelUnion,
    work_map: &mut VattReadingRefMap<WorksNames>,
    at: &str,
) where
    I: Iterator<Item = &'a u32>,
{
    if at == Works::NAME {
        leaves.for_each(|k| {
            eatts.insert(
                k.to_usize(),
                AttributeLabel {
                    name: work_map
                        .get_via_mut(&k.to_usize())
                        .unwrap_or("Unknown".to_string()),
                    spec_baseline: 1.0,
                },
            );
        });
        return;
    }
    if let Some(u_eatts) = union.get(at) {
        leaves.for_each(|k| {
            eatts.insert(k.to_usize(), u_eatts[k.to_usize()].clone());
        });
    } else {
        println!("WARING: {at} not found in attribute union");
    }
}
