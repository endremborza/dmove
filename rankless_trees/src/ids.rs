use dmove::{
    Entity, EntityMutableMapperBackend, NamespacedEntity, UnsignedNumber, VattReadingRefMap,
};

use hashbrown::HashMap;

use crate::{
    interfacing::Getters,
    io::{
        AttributeLabel, AttributeLabelOut, AttributeLabels, BreakdownSpec, BufSerChildren,
        BufSerTree, FullTreeQuery, TreeBasisState,
    },
};
use rankless_rs::gen::{
    a1_entity_mapping::{Institutions, Works},
    a2_init_atts::WorksNames,
};

pub type AttributeLabelUnion = HashMap<String, Box<[AttributeLabel]>>;

pub fn get_atts(
    tree: &BufSerTree,
    bds: &[BreakdownSpec],
    state: &TreeBasisState,
    fq: &FullTreeQuery,
) -> AttributeLabels {
    let parent = state
        .gets
        .stowage
        .path_from_ns(<WorksNames as NamespacedEntity>::NS);
    let mut work_name_basis =
        VattReadingRefMap::<WorksNames>::from_locator(&state.gets.wn_locators, &parent);
    let eid = fq.ck.eid;
    let etype = &fq.name;

    let mut atts = HashMap::new();
    let eatts = atts.entry(etype.to_string()).or_insert(HashMap::new());
    add_leaves(
        vec![eid as u32].iter(),
        eatts,
        &mut work_name_basis,
        &etype,
        state,
    );
    ext_atts(&mut atts, tree, bds, &mut work_name_basis, state);
    atts
}

fn ext_atts(
    atts: &mut AttributeLabels,
    tree: &BufSerTree,
    bds: &[BreakdownSpec],
    work_map: &mut VattReadingRefMap<WorksNames>,
    state: &TreeBasisState,
) {
    let at = &bds[0].attribute_type;
    let eatts = atts.entry(at.to_string()).or_insert(HashMap::new());
    match tree.children.as_ref() {
        BufSerChildren::Leaves(leaves) => add_leaves(leaves.keys(), eatts, work_map, at, state),
        BufSerChildren::Nodes(nodes) => {
            add_leaves(nodes.keys(), eatts, work_map, at, state);
            nodes
                .values()
                .for_each(|v| ext_atts(atts, v, &bds[1..], work_map, state))
        }
    };
}

fn add_leaves<'a, I>(
    leaves: I,
    eatts: &mut HashMap<usize, AttributeLabelOut>,
    work_map: &mut VattReadingRefMap<WorksNames>,
    at: &str,
    state: &TreeBasisState,
) where
    I: Iterator<Item = &'a u32>,
{
    if at == Works::NAME {
        leaves.for_each(|k| {
            eatts.insert(
                k.to_usize(),
                AttributeLabelOut {
                    name: work_map
                        .get_via_mut(&k.to_usize())
                        .unwrap_or("Unknown".to_string()),
                    spec_baseline: 1.0,
                    oa_id: None,
                },
            );
        });
        return;
    }
    if let Some(u_eatts) = state.att_union.get(at) {
        if at == Institutions::NAME {
            leaves.for_each(|k| {
                let ku = k.to_usize();
                eatts.insert(ku, oaify_inst(&u_eatts[k.to_usize()], &state.gets, ku));
            })
        } else {
            leaves.for_each(|k| {
                eatts.insert(k.to_usize(), to_none_alabel(&u_eatts[k.to_usize()]));
            })
        };
    } else {
        // println!("WARNING: {at} not found in attribute union");
    }
}

fn oaify_inst(v: &AttributeLabel, gets: &Getters, id: usize) -> AttributeLabelOut {
    AttributeLabelOut {
        spec_baseline: v.spec_baseline,
        name: v.name.clone(),
        oa_id: Some(gets.inst_oa[id]),
    }
}

fn to_none_alabel(v: &AttributeLabel) -> AttributeLabelOut {
    AttributeLabelOut {
        spec_baseline: v.spec_baseline,
        name: v.name.clone(),
        oa_id: None,
    }
}
