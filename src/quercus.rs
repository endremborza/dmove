use hashbrown::HashMap;
use serde::{Deserialize, Serialize};
use std::io;
use tqdm::*;

use crate::common::{Stowage, COUNTRIES, INSTS, MAIN_CONCEPTS, SOURCES, SUB_CONCEPTS};
use crate::ingest_entity::get_idmap;
use crate::oa_var_atts::{
    get_attribute_resolver_map, iter_ref_paths, AttributeResolverMap, MappedAttributes, MidId,
};

type GraphPath = Vec<MidId>;
type AttributeStaticMap = HashMap<String, HashMap<MidId, AttributeStatic>>;

struct AttributeResolver {
    entity_types: Vec<String>,
}

impl AttributeResolver {
    fn name(&self) -> String {
        return self.entity_types.join("_");
    }
}

struct BreakdownHierarchy {
    levels: Vec<usize>,
    side: usize,
    resolver_id: String,
}

impl BreakdownHierarchy {
    fn new(resolver_id: &str, levels: Vec<usize>, side: usize) -> Self {
        Self {
            levels,
            side,
            resolver_id: resolver_id.to_owned(),
        }
    }

    fn entity_types(&self) -> Vec<&str> {
        self.resolver_id.split("_").collect()
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

#[derive(Serialize, Deserialize, Debug)]
struct PathCollectionSpec {
    col_num: usize,
    root_type_id: String,
}

#[derive(Serialize)]
struct AttributeStatic {
    name: String,
    spec_baseline: f32,
    #[serde(default = "default_meta")]
    meta: String,
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

fn default_meta() -> String {
    "{}".to_string()
}

pub fn dump_all_cache(stowage: &Stowage) -> io::Result<()> {
    let ares_map = get_attribute_resolver_map(stowage);

    let mut js_qc_specs = HashMap::new();

    for (i, bd_hiers) in get_qc_spec_bases().iter().enumerate() {
        let entity_type = INSTS;
        let id_map = get_idmap(stowage, entity_type);

        let mut bifurcations = Vec::new();
        for bdh in bd_hiers {
            let res_id: Vec<&str> = bdh.entity_types();
            let resolver_id = &bdh.resolver_id;

            for i in &bdh.levels {
                bifurcations.push(JsBifurcation {
                    attribute_kind: res_id[i + 1].to_owned(),
                    description: format!("{}-{}-{}", bdh.side, resolver_id, i).clone(),
                    resolver_id: resolver_id.clone(),
                })
            }
        }
        let qc_key = format!("qc-{}", i + 1);
        js_qc_specs.insert(
            qc_key.clone(),
            JsQcSpec {
                bifurcations,
                root_entity_type: INSTS.to_string(),
            },
        );
        for iid in id_map.iter_ids().tqdm().desc(Some(&qc_key)) {
            let mut qc = Quercus::new();
            let mut qcr = QuercusRoller::new(bd_hiers, &ares_map);
            for ref_path in iter_ref_paths(stowage, iid.try_into().unwrap()) {
                qcr.set(ref_path);
                qcr.roll_hier(&mut qc);
            }
            stowage.write_cache(&qc, &format!("qc-builds/{}/{}", qc_key, iid))?;
        }
    }

    let mut attribute_statics: AttributeStaticMap = HashMap::new();
    for entity in [INSTS, MAIN_CONCEPTS, SUB_CONCEPTS, COUNTRIES, SOURCES] {
        let mut entity_statics = HashMap::new();
        for eid in get_idmap(stowage, entity).iter_ids() {
            let e_map = AttributeStatic {
                name: "Name".to_string(), //TODO
                spec_baseline: 0.5,       //TODO
                meta: default_meta(),
            };
            entity_statics.insert(eid as MidId, e_map);
            todo!();
        }
        attribute_statics.insert(entity.to_string(), entity_statics);
    }
    stowage.write_cache(&attribute_statics, "attribute-statics")?;
    stowage.write_cache(&js_qc_specs, "qc-specs")?;

    Ok(())
}

fn get_qc_spec_bases() -> Vec<Vec<BreakdownHierarchy>> {
    vec![
        vec![
            BreakdownHierarchy::new("country-inst", vec![0, 1], 1),
            BreakdownHierarchy::new("concept-hier", vec![0, 1], 1),
        ],
        vec![
            BreakdownHierarchy::new("concept-hier", vec![0], 0),
            BreakdownHierarchy::new("country-inst", vec![0, 1], 1),
            BreakdownHierarchy::new("concept-hier", vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::new("country-inst", vec![0], 0),
            BreakdownHierarchy::new("concept-hier", vec![0, 1], 1),
        ],
        vec![
            BreakdownHierarchy::new("country-inst", vec![0], 0),
            BreakdownHierarchy::new("concept-hier", vec![0, 1], 0),
            BreakdownHierarchy::new("country-inst", vec![1], 0), //TODO tricky!!!
        ],
        vec![
            BreakdownHierarchy::new("paper-source", vec![0], 1),
            BreakdownHierarchy::new("country-inst", vec![0], 1),
            BreakdownHierarchy::new("concept-hier", vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::new("concept-hier", vec![0, 1], 1),
            BreakdownHierarchy::new("paper-source", vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::new("qed-source", vec![0, 1], 0),
            BreakdownHierarchy::new("country-inst", vec![0], 1),
            BreakdownHierarchy::new("concept-hier", vec![0], 1),
        ],
    ]
}
