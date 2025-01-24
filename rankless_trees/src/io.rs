use std::vec;

use hashbrown::HashMap;
use serde::{Deserialize, Serialize};

use rankless_rs::{
    gen::a1_entity_mapping::Works,
    steps::a1_entity_mapping::{POSSIBLE_YEAR_FILTERS, YBT},
};

use dmove::ET;

pub type TreeSpecMap = HashMap<String, Vec<TreeSpec>>;
pub type AttributeLabels = HashMap<String, HashMap<usize, AttributeLabel>>;

#[derive(Serialize, Clone)]
pub struct AttributeLabel {
    pub name: String,
    #[serde(rename = "specBaseline")]
    pub spec_baseline: f64,
    // spec_baselines: HashMap<usize, f64>,
    // meta: HashMap<String, String>,
}

#[derive(Deserialize, Clone)]
pub struct TreeQ {
    pub year: Option<u16>,
    pub eid: u32,
    pub tid: Option<u8>,
}

#[derive(Serialize, Deserialize)]
pub struct CollapsedNode {
    #[serde(rename = "linkCount")]
    pub link_count: u32,
    #[serde(rename = "sourceCount")]
    pub source_count: u32,
    #[serde(rename = "topSourceId")]
    pub top_source: ET<Works>,
    #[serde(rename = "topSourceLinks")]
    pub top_cite_count: u32,
}

#[derive(Serialize, Deserialize)]
pub struct BufSerTree {
    pub node: CollapsedNode,
    pub children: Box<BufSerChildren>,
}

#[derive(Serialize)]
pub struct JsSerTree {
    #[serde(flatten)]
    pub node: CollapsedNode,
    pub children: Box<JsSerChildren>,
}

#[derive(Serialize)]
pub struct TreeResponse {
    pub tree: JsSerTree,
    pub atts: AttributeLabels,
}

#[derive(Serialize)]
pub struct TreeSpecs {
    specs: TreeSpecMap,
    #[serde(rename = "yearBreaks")]
    year_breaks: YBT,
}

#[derive(Serialize)]
pub struct TreeSpec {
    #[serde(rename = "rootType")]
    pub root_type: String,
    pub breakdowns: Vec<BreakdownSpec>,
}

#[derive(Serialize)]
pub struct BreakdownSpec {
    #[serde(rename = "attributeType")]
    pub attribute_type: String,
    #[serde(rename = "specDenomInd")] //this is to know how deep to go back for spec calculation
    //e.g a country->inst is the same resolver
    pub spec_denom_ind: u8,
    // description: String, // used to be for spec calculation -> separate for different kinds of
    // breakdowns
    #[serde(rename = "sourceSide")]
    pub source_side: bool,
}

pub struct SCIter<'a> {
    children: &'a BufSerChildren,
    key_iter: vec::IntoIter<&'a u32>,
}

#[derive(Serialize, Deserialize)]
pub enum BufSerChildren {
    Leaves(HashMap<u32, CollapsedNode>),
    Nodes(HashMap<u32, BufSerTree>),
}

#[derive(Serialize)]
#[serde(untagged)]
pub enum JsSerChildren {
    Leaves(HashMap<u32, CollapsedNode>),
    Nodes(HashMap<u32, JsSerTree>),
}

impl TreeSpecs {
    pub fn new(specs: TreeSpecMap) -> Self {
        Self {
            specs,
            year_breaks: POSSIBLE_YEAR_FILTERS,
        }
    }
}

impl BufSerChildren {
    pub fn iter_items<'a>(&'a self) -> SCIter<'a> {
        let key_vec: Vec<&'a u32> = match self {
            Self::Nodes(nodes) => nodes.keys().collect(),
            Self::Leaves(ls) => ls.keys().collect(),
        };
        SCIter {
            children: self,
            key_iter: key_vec.into_iter(),
        }
    }
}

impl<'a> Iterator for SCIter<'a> {
    type Item = (&'a u32, &'a CollapsedNode);
    fn next(&mut self) -> Option<Self::Item> {
        match self.key_iter.next() {
            Some(k) => {
                let v = match self.children {
                    BufSerChildren::Nodes(nodes) => &nodes[k].node,
                    BufSerChildren::Leaves(leaves) => &leaves[k],
                };
                Some((k, v))
            }
            None => None,
        }
    }
}

impl From<BufSerTree> for JsSerTree {
    fn from(value: BufSerTree) -> Self {
        let children = JsSerChildren::from(*value.children);
        Self {
            node: value.node,
            children: Box::new(children),
        }
    }
}

impl From<BufSerChildren> for JsSerChildren {
    fn from(value: BufSerChildren) -> Self {
        //TODO: this is wasteful
        match value {
            BufSerChildren::Nodes(nodes) => Self::Nodes(HashMap::from_iter(
                nodes.into_iter().map(|(k, v)| (k, v.into())),
            )),
            BufSerChildren::Leaves(leaves) => Self::Leaves(leaves),
        }
    }
}
