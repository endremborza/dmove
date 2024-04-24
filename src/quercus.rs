use core::f32;
use hashbrown::HashMap;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use std::fs::{self, File};
use std::io::{self, BufReader, Read};
use std::marker::PhantomData;
use std::path::Path;
use std::sync::{mpsc, Arc, Mutex};
use std::thread;
use tqdm::tqdm;

use crate::common::{Stowage, COUNTRIES, INSTS, MAIN_CONCEPTS, QS, SOURCES, SUB_CONCEPTS};
use crate::ingest_entity::get_idmap;

// use std::sync::{mpsc, Arc};
// use std::thread;
// const MAX_DEPTH: u8 = 6;
// TODO: define Quercus with macros
// Optimizations:
// - replace GraphPath and QuercusPath with Arrays from Vector
// - don't add to quercus set_paths on all levels, only after last breakdown
//   - merge them to n_unique at export

type EntityInd = u32;
type QuercusLevelInd = usize;
type QuercusBranch = Vec<QuercusLevelInd>;
type GraphPath = Vec<EntityInd>;

type SMap<T> = HashMap<String, T>;

type AttributeResolverMap<T: AttributeResolver> = SMap<T>;
type AttributeStaticMap = SMap<HashMap<QuercusLevelInd, AttributeStatic>>;

#[derive(Debug, Serialize)]
pub struct Quercus {
    //TODO: at one point include "stats" or "meta"
    weight: u32,
    #[serde(skip_serializing_if = "HashMap::is_empty")]
    children: HashMap<QuercusLevelInd, Quercus>,
}

struct QuercusRoller<'a, T: AttributeResolver> {
    aresolver_map: &'a AttributeResolverMap<T>,
    all_bifhs: &'a [BreakdownHierarchy<T>],
    current_entity_indices: GraphPath,
    current_hier_index: usize,
    current_setup_index: usize,
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
    fn get_and_add(&mut self, k: &usize) -> &mut Quercus {
        let entry = self.children.entry(*k);
        let child = entry.or_insert_with(|| Quercus::new());
        child.weight += 1;
        child
    }
}

impl QuercusRoller<'_, T> {
    fn set(&mut self, graph_path: GraphPath) {
        for (i, v) in self.all_bifhs.iter().enumerate() {
            self.current_entity_indices[i] = graph_path[v.entity_col_ind];
        }
    }

    fn roll_hier(&mut self, current_quercus: &mut Quercus) -> Option<()> {
        let bifh = self.all_bifhs.get(self.current_hier_index)?;
        self.current_setup_index = 0;
        self.current_hier_depth = 0;
        let resolver = &self.aresolver_map[&bifh.resolver_id];
        let entity_ind = self.current_entity_indices[self.current_hier_index];
        let mapped_attributes = resolver.att_map.get(&entity_ind)?;
        self.roll_setup(Some(mapped_attributes), current_quercus);
        None
    }

    fn roll_setup(
        &mut self,
        mapped_attributes: Option<&MappedAttributes>,
        current_quercus: &mut Quercus,
    ) {
        if let Some(si) = self.all_bifhs[self.current_hier_index]
            .levels
            .get(self.current_setup_index)
        {
            if self.current_hier_depth < *si {
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
                            //resets setup index and hier depth here
                            self.hierarchy_ender(cq.get_and_add(v));
                        }
                    }
                    Map(map_data) => {
                        for (k, v) in map_data {
                            self.current_setup_index += 1;
                            self.current_hier_depth += 1;
                            self.roll_setup(Some(v), cq.get_and_add(k));
                            //this rolls over and resets in roll_hier :(
                            self.current_setup_index -= 1;
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
        let old_ends = (self.current_hier_depth, self.current_setup_index);
        self.current_hier_index += 1;
        self.roll_hier(qc);
        self.current_hier_index -= 1;
        (self.current_hier_depth, self.current_setup_index) = old_ends;
    }
}

fn default_meta() -> String {
    "{}".to_string()
}

pub fn dump_all_cache(stowage: &Stowage) -> io::Result<()> {
    let mut js_qc_specs = HashMap::new();

    for bd_hiers in get_qc_spec_bases() {
        let mut bifurcations = Vec::new();
        for bdh in &bd_hiers {
            let res_id: Vec<&str> = bdh.entity_types();
            let resolver_id = bdh.resolver_name();

            for i in &bdh.levels {
                bifurcations.push(JsBifurcation {
                    attribute_kind: res_id[i + 1].to_owned(),
                    description: format!("{}-{}-{}", bdh.side, resolver_id, i).clone(),
                    resolver_id: resolver_id.clone(),
                })
            }
        }
        let qc_key = "xy";
        js_qc_specs.insert(
            qc_key,
            JsQcSpec {
                bifurcations,
                root_entity_type: INSTS.to_string(),
            },
        );
    }

    let mut attribute_statics = HashMap::new();
    for entity in [INSTS, MAIN_CONCEPTS, SUB_CONCEPTS, COUNTRIES, SOURCES] {
        let mut entity_statics = HashMap::new();
        for eid in get_idmap(stowage, entity).iter_ids() {
            let e_map = AttributeStatic {
                name: "Name".to_string(),
                spec_baseline: 0.5,
                meta: default_meta(),
            };
            entity_statics.insert(eid, e_map);
        }
        attribute_statics.insert(entity, entity_statics);
    }
    stowage.write_cache(&attribute_statics, "attribute-statics")?;
    stowage.write_cache(&js_qc_specs, "qc-specs")?;

    Ok(())
}

trait AttributeResolver {
    fn entity_types() -> Vec<String>;
    fn name() -> String {
        return Self::entity_types().join("-");
    }
}

struct BreakdownHierarchy<T: AttributeResolver> {
    levels: Vec<usize>,
    side: usize,
    phantom: PhantomData<T>,
}

impl<T: AttributeResolver> BreakdownHierarchy<T> {
    fn new(levels: Vec<usize>, side: usize) -> Self {
        Self {
            levels,
            side,
            phantom: PhantomData::<T>,
        }
    }

    fn entities() -> Vec<String> {
        T::entity_types()
    }

    fn resolver_name() -> Vec<String> {
        T::name()
    }
}

struct CountryInst;
struct ConceptHier;
struct PaperSource;
struct QedSource;

impl AttributeResolver for CountryInst {
    fn entity_types() -> Vec<String> {
        vec![COUNTRIES.to_string(), INSTS.to_string()]
    }
}

impl AttributeResolver for ConceptHier {
    fn entity_types() -> Vec<String> {
        vec![MAIN_CONCEPTS.to_string(), SUB_CONCEPTS.to_string()]
    }
}

impl AttributeResolver for PaperSource {
    fn entity_types() -> Vec<String> {
        vec![SOURCES.to_string()]
    }
}

impl AttributeResolver for QedSource {
    fn entity_types() -> Vec<String> {
        vec![QS.to_string(), SOURCES.to_string()]
    }
}

pub fn get_qc_spec_bases() -> Vec<Vec<BreakdownHierarchy<dyn AttributeResolver>>> {
    vec![
        vec![
            BreakdownHierarchy::<CountryInst>::new(vec![0, 1], 1),
            BreakdownHierarchy::<ConceptHier>::new(vec![0, 1], 1),
        ],
        vec![
            BreakdownHierarchy::<ConceptHier>::new(vec![0], 0),
            BreakdownHierarchy::<CountryInst>::new(vec![0, 1], 1),
            BreakdownHierarchy::<ConceptHier>::new(vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::<CountryInst>::new(vec![0], 0),
            BreakdownHierarchy::<ConceptHier>::new(vec![0, 1], 1),
        ],
        vec![
            BreakdownHierarchy::<CountryInst>::new(vec![0], 0),
            BreakdownHierarchy::<ConceptHier>::new(vec![0, 1], 0),
            BreakdownHierarchy::<CountryInst>::new(vec![1], 0), //TODO tricky!!!
        ],
        vec![
            BreakdownHierarchy::<PaperSource>::new(vec![0], 1),
            BreakdownHierarchy::<CountryInst>::new(vec![0], 1),
            BreakdownHierarchy::<ConceptHier>::new(vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::<ConceptHier>::new(vec![0, 1], 1),
            BreakdownHierarchy::<PaperSource>::new(vec![0], 1),
        ],
        vec![
            BreakdownHierarchy::<QedSource>::new(vec![0, 1], 0),
            BreakdownHierarchy::<CountryInst>::new(vec![0], 1),
            BreakdownHierarchy::<ConceptHier>::new(vec![0], 1),
        ],
    ]
}
