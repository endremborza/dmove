use std::any::type_name;

use dmove::{Entity, Link};

use crate::{
    common::code_path,
    gen_entity_mapping::Works,
    gen_init_links::{
        AuthorshipAuthor, AuthorshipInstitutions, SourceAreaFields, SourceYearQs, WorkAuthorships,
        WorkReferences, WorkSources, WorkYears,
    },
};

struct BreakdownBasisNetwork {
    links: Vec<BbnLink>,
    nodes: Vec<BbnNode>,
}

struct BbnNode {
    prefix: String,
    entity_type_path: String,
}

struct BbnLink {
    source_idx: usize,
    target_idx: usize,
    link_type_path: String,
}

impl BbnNode {
    fn new<E>(prefix: &str) -> Self {
        Self {
            prefix: prefix.to_string(),
            entity_type_path: e_type_name::<E>(),
        }
    }
}

impl BbnLink {
    fn new<L>(source_idx: usize, target_idx: usize) -> Self {
        Self {
            source_idx,
            target_idx,
            link_type_path: e_type_name::<L>(),
        }
    }
}

impl BreakdownBasisNetwork {
    fn new() -> Self {
        Self {
            links: Vec::new(),
            nodes: Vec::new(),
        }
    }

    fn add_root<E: Entity>(&mut self, prefix: &str) -> usize {
        let root = BbnNode::new::<E>(prefix);
        self.nodes.push(root);
        self.nodes.len() - 1
    }

    fn add<L: Link>(&mut self, source_idx: usize) -> usize {
        self.add_prefixed::<L>(source_idx, "")
    }

    fn add_fwd_prefix<L: Link>(&mut self, source_idx: usize) -> usize {
        let prefix = self.nodes[source_idx].prefix.clone();
        self.add_prefixed::<L>(source_idx, &prefix)
    }

    fn add_prefixed<L: Link>(&mut self, source_idx: usize, target_prefix: &str) -> usize {
        let target = BbnNode::new::<L::Target>(target_prefix);
        self.nodes.push(target);
        let target_idx = self.nodes.len() - 1;
        self.links.push(BbnLink::new::<L>(source_idx, target_idx));
        target_idx
    }

    fn ingest<L: Link>(&mut self, other: Self, self_source_idx: usize, other_target_idx: usize) {
        let n = other.nodes.len();
        for l in other.links.into_iter() {
            self.links.push(BbnLink {
                source_idx: l.source_idx + n,
                target_idx: l.target_idx + n,
                link_type_path: l.link_type_path,
            })
        }
        self.nodes.extend(other.nodes.into_iter());
        self.links
            .push(BbnLink::new::<L>(self_source_idx, other_target_idx + n))
    }

    fn to_file(&self, path: &str) {
        todo!();
    }
}

fn make_paper_ego(prefix: &str) -> BreakdownBasisNetwork {
    let mut bbn = BreakdownBasisNetwork::new();
    let paper = bbn.add_root::<Works>(prefix);
    let ship = bbn.add_fwd_prefix::<WorkAuthorships>(paper);
    let _author = bbn.add_fwd_prefix::<AuthorshipAuthor>(ship);
    let _instituion = bbn.add_fwd_prefix::<AuthorshipInstitutions>(ship);
    let _year = bbn.add_fwd_prefix::<WorkYears>(paper);
    let source = bbn.add_fwd_prefix::<WorkSources>(paper);
    let _area_fields = bbn.add_fwd_prefix::<SourceAreaFields>(source);
    // let q = bbn.add_fwd_prefix::<SourceYearQs>((source, year));
    bbn
}

pub fn make_bbns() {
    let mut source_paper = make_paper_ego("citing");
    let target_paper = make_paper_ego("referenced");
    source_paper.ingest::<WorkReferences>(target_paper, 0, 0);
    source_paper.to_file(&code_path("bbns"));
}

fn prefixed<T>(prefix: &str) -> String {
    format!("{}{}", prefix, type_name::<T>())
}

fn e_type_name<T>() -> String {
    type_name::<T>().to_string()
}
