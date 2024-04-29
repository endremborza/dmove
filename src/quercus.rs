use hashbrown::HashMap;
use serde::Serialize;
use std::io;
use tqdm::*;

use crate::common::{Stowage, CONCEPTS, COUNTRIES, INSTS, MAIN_CONCEPTS, SOURCES, SUB_CONCEPTS};
use crate::ingest_entity::get_idmap;
use crate::oa_fix_atts::{names, read_fix_att};
use crate::oa_var_atts::{
    get_attribute_resolver_map, get_mapped_atts, get_name_name, read_var_att, vnames,
    AttributeResolverMap, MappedAttributes, MidId, WeightedEdge, WorkId,
};

type GraphPath = Vec<MidId>;
type AttributeStaticMap = HashMap<String, HashMap<MidId, AttributeStatic>>;

struct BreakdownHierarchy {
    levels: Vec<usize>,
    side: usize,
    resolver_id: String,
    entity_types: Vec<String>,
}

impl BreakdownHierarchy {
    fn new(resolver_id: &str, levels: Vec<usize>, side: usize) -> Self {
        let entity_types: Vec<String> = get_mapped_atts(resolver_id);
        Self {
            levels,
            side,
            resolver_id: resolver_id.to_owned(),
            entity_types,
        }
    }
}
#[derive(Debug, Serialize)]
pub struct Quercus {
    //TODO: at one point include "stats" or "meta"
    weight: u32,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    children: HashMap<MidId, Quercus>,
}

struct QuercusRoller<'a> {
    aresolver_map: &'a AttributeResolverMap,
    all_bifhs: &'a [BreakdownHierarchy],
    current_entity_indices: GraphPath,
    current_hier_index: usize,
    current_index_within_hier_levels: usize,
    current_hier_depth: usize,
}

#[derive(Serialize)]
struct JsBifurcation {
    attribute_kind: String,
    resolver_id: String,
    description: String,
}

#[derive(Serialize)]
struct JsQcSpec {
    bifurcations: Vec<JsBifurcation>,
    root_entity_type: String,
}

#[derive(Serialize)]
struct AttributeStatic {
    name: String,
    spec_baseline: f64,
    #[serde(default = "default_meta")]
    meta: HashMap<String, u32>,
}

trait Absorbable {
    fn new() -> Self;
    fn absorb(&mut self, other: Self);
}

impl Quercus {
    fn new() -> Self {
        Self {
            weight: 0,
            children: HashMap::new(),
        }
    }

    fn get_and_add(&mut self, k: &MidId) -> &mut Quercus {
        let entry = self.children.entry(*k);
        let child = entry.or_insert_with(|| Quercus::new());
        child.weight += 1;
        child
    }
}

impl<'a> QuercusRoller<'a> {
    fn new(all_bifhs: &'a [BreakdownHierarchy], aresolver_map: &'a AttributeResolverMap) -> Self {
        Self {
            aresolver_map,
            all_bifhs,
            current_entity_indices: vec![0; all_bifhs.len()],
            current_hier_index: 0,
            current_index_within_hier_levels: 0,
            current_hier_depth: 0,
        }
    }

    fn set(&mut self, graph_path: GraphPath) {
        for (i, v) in self.all_bifhs.iter().enumerate() {
            self.current_entity_indices[i] = graph_path[v.side];
        }
    }

    fn roll_hier(&mut self, current_quercus: &mut Quercus) -> Option<()> {
        let bifh = self.all_bifhs.get(self.current_hier_index)?;
        self.current_index_within_hier_levels = 0;
        self.current_hier_depth = 0;
        let resolver = &self.aresolver_map[&bifh.resolver_id];
        let entity_ind = self.current_entity_indices[self.current_hier_index];
        let mapped_attributes = resolver.get(&entity_ind)?;
        self.roll_setup(Some(mapped_attributes), current_quercus);
        None
    }

    fn roll_setup(
        &mut self,
        mapped_attributes: Option<&MappedAttributes>,
        current_quercus: &mut Quercus,
    ) {
        use MappedAttributes::{List, Map};
        if let Some(si) = self.all_bifhs[self.current_hier_index]
            .levels
            .get(self.current_index_within_hier_levels)
        {
            if self.current_hier_depth < *si {
                // not really using this level, move on to next
                // <= and not < because of the += 1
                match mapped_attributes.unwrap() {
                    List(_) => {
                        panic!("no more levels")
                    }
                    Map(map_data) => {
                        //should flatten
                        for v in map_data.values() {
                            self.current_hier_depth += 1;
                            self.roll_setup(Some(v), current_quercus);
                            self.current_hier_depth -= 1;
                        }
                    }
                }
            } else {
                let cq = current_quercus;
                match mapped_attributes.unwrap() {
                    List(vec_data) => {
                        for v in vec_data {
                            self.hierarchy_ender(cq.get_and_add(v));
                        }
                    }
                    Map(map_data) => {
                        for (k, v) in map_data {
                            self.current_index_within_hier_levels += 1;
                            self.current_hier_depth += 1;
                            self.roll_setup(Some(v), cq.get_and_add(k));
                            self.current_index_within_hier_levels -= 1;
                            self.current_hier_depth -= 1;
                        }
                    }
                }
            }
        } else {
            self.hierarchy_ender(current_quercus);
        }
    }

    fn hierarchy_ender(&mut self, qc: &mut Quercus) {
        let old_ends = (
            self.current_hier_depth,
            self.current_index_within_hier_levels,
        );
        self.current_hier_index += 1;
        self.roll_hier(qc);
        self.current_hier_index -= 1;
        (
            self.current_hier_depth,
            self.current_index_within_hier_levels,
        ) = old_ends;
    }
}

pub fn dump_all_cache(stowage: &Stowage) -> io::Result<()> {
    let mut attribute_statics: AttributeStaticMap = HashMap::new();
    attribute_statics.insert(MAIN_CONCEPTS.to_string(), HashMap::new());
    attribute_statics.insert(SUB_CONCEPTS.to_string(), HashMap::new());

    let clevels = read_fix_att(stowage, names::CLEVEL);
    let cnames: Vec<String> = read_var_att(stowage, &get_name_name(CONCEPTS));
    for cid in get_idmap(stowage, CONCEPTS).iter_ids() {
        let k = {
            if clevels[cid as usize] == 0 {
                MAIN_CONCEPTS
            } else {
                SUB_CONCEPTS
            }
        };
        attribute_statics.get_mut(k).unwrap().insert(
            cid.try_into().unwrap(),
            AttributeStatic {
                name: cnames[cid as usize].clone(),
                spec_baseline: 0.742,
                meta: HashMap::new(),
            },
        );
    }

    println!("getting ares map");
    let ares_map = get_attribute_resolver_map(stowage);

    println!("getting var atts");
    let full_clist: Vec<Vec<WorkId>> = read_var_att(stowage, vnames::TO_CITING);
    let works_of_inst: Vec<Vec<WeightedEdge<WorkId>>> = read_var_att(stowage, vnames::I2W);
    println!("got var atts");

    //preps for specs and metas
    let mut inst_cite_counts = vec![0; works_of_inst.len()];

    let mut spec_bases_vecs: HashMap<String, HashMap<MidId, Vec<f64>>> = HashMap::new();
    for ename in [INSTS, SOURCES, COUNTRIES, SUB_CONCEPTS, MAIN_CONCEPTS] {
        spec_bases_vecs.insert(ename.to_owned(), HashMap::new());
    }

    let mut js_qc_specs = HashMap::new();
    for (i, bd_hiers) in get_qc_spec_bases().iter().enumerate() {
        let entity_type = INSTS;
        let id_map = get_idmap(stowage, entity_type);

        let mut bifurcations = Vec::new();
        for bdh in bd_hiers {
            let resolver_id = &bdh.resolver_id;

            for i in &bdh.levels {
                bifurcations.push(JsBifurcation {
                    attribute_kind: bdh.entity_types[*i].clone(),
                    description: format!("{}-{}-{}", bdh.side, resolver_id, i).clone(),
                    resolver_id: resolver_id.clone(),
                })
            }
        }
        let qc_key = format!("qc-{}", i + 1);

        for iid in id_map.iter_ids().tqdm().desc(Some(&qc_key)) {
            let mut qc = Quercus::new();
            let mut qcr = QuercusRoller::new(bd_hiers, &ares_map);
            for wid in works_of_inst[iid as usize].iter() {
                for citing_wid in full_clist[wid.id as usize].iter() {
                    let ref_path = vec![wid.id, *citing_wid];
                    qcr.set(ref_path);
                    qc.weight += 1;
                    qcr.current_hier_index = 0;
                    qcr.roll_hier(&mut qc);
                }
            }
            stowage.write_cache(&qc, &format!("qc-builds/{}/{}", qc_key, iid))?;
            inst_cite_counts[iid as usize] = qc.weight;
            //calc spec bases for first 2 levels only
            let root_denom = f64::from(qc.weight);
            for (child_id, child_qc) in qc.children {
                let rate = f64::from(child_qc.weight) / root_denom;
                spec_bases_vecs
                    .get_mut(&bifurcations[0].attribute_kind)
                    .unwrap()
                    .entry(child_id)
                    .or_insert_with(|| Vec::new())
                    .push(rate);

                if bifurcations[0].resolver_id == bifurcations[1].resolver_id {
                    for (sub_child_id, sub_child_qc) in child_qc.children {
                        let rate = f64::from(sub_child_qc.weight) / root_denom;
                        spec_bases_vecs
                            .get_mut(&bifurcations[1].attribute_kind)
                            .unwrap()
                            .entry(sub_child_id)
                            .or_insert_with(|| Vec::new())
                            .push(rate);
                    }
                }
            }
        }
        js_qc_specs.insert(
            qc_key.clone(),
            JsQcSpec {
                bifurcations,
                root_entity_type: entity_type.to_string(),
            },
        );
    }

    for entity in [INSTS, COUNTRIES, SOURCES] {
        let mut entity_statics = HashMap::new();
        let names: Vec<String> = read_var_att(stowage, &get_name_name(entity));
        for eid in get_idmap(stowage, entity).iter_ids() {
            let mut meta = HashMap::new();
            if entity == INSTS {
                meta.insert(
                    "papers".to_string(),
                    works_of_inst[eid as usize].len().try_into().unwrap(),
                );
                meta.insert("citations".to_string(), inst_cite_counts[eid as usize]);
            }

            let e_map = AttributeStatic {
                name: names[eid as usize].clone(),
                spec_baseline: 0.742,
                meta,
            };
            entity_statics.insert(eid as MidId, e_map);
        }
        attribute_statics.insert(entity.to_string(), entity_statics);
    }

    for (ek, recs) in spec_bases_vecs {
        for (k, sb_v) in recs {
            if let Some(astats) = attribute_statics.get_mut(&ek).unwrap().get_mut(&k) {
                astats.spec_baseline = sb_v.iter().sum::<f64>() / f64::from(sb_v.len() as u32);
            } else {
                println!("not found for spec base {:?} ind: {:?}", ek, k);
            }
        }
    }

    stowage.write_cache(&attribute_statics, "attribute-statics")?;
    stowage.write_cache(&js_qc_specs, "qc-specs")?;

    Ok(())
}

fn get_qc_spec_bases() -> Vec<Vec<BreakdownHierarchy>> {
    vec![
        vec![
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0, 1], 1),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0, 1], 1),
        ],
        vec![
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0], 0),
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0, 1], 1),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0], 0),
            BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0, 1], 1),
        ],
        // vec![
        //     BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0, 1], 1),
        //     BreakdownHierarchy::new(vnames::W2S, vec![0], 1),
        // ],
        // vec![
        //     BreakdownHierarchy::new(vnames::W2S, vec![0], 1),
        //     BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0], 1),
        //     BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0], 1),
        // ],
        // // vec![
        //     BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0], 0),
        //     BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0, 1], 0),
        //     BreakdownHierarchy::new(vnames::COUNTRY_H, vec![1], 0), //TODO tricky!!!
        // ],
        // vec![
        //     BreakdownHierarchy::new("qed-source", vec![0, 1], 0),
        //     BreakdownHierarchy::new(vnames::COUNTRY_H, vec![0], 1),
        //     BreakdownHierarchy::new(vnames::CONCEPT_H, vec![0], 1),
        // ],
    ]
}
