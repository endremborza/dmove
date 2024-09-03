//BBNs: Breakdown Basis Networks

use std::{any::type_name, fs::File, io::Write};

use dmove::Entity;

use crate::gen_types::{Sources, Works};

struct AffiliatedAuthorship {}
struct AuthorshipLink {}

impl Link for AuthorshipLink {
    type Source = Works;
    type Target = AffiliatedAuthorship;
}

impl Entity for AffiliatedAuthorship {
    type T = u8;
    const N: usize = 10;
    type FullT = [u8; 10];
    const NAME: &'static str = "ship";
}

struct BreakdownBasisNetwork {
    links: Vec<[String; 3]>,
}

trait Link {
    type Source: Entity;
    type Target: Entity;
}

trait BbnNode {}

impl BreakdownBasisNetwork {
    fn new() -> Self {
        Self { links: Vec::new() }
    }

    fn add<L: Link>(&mut self) {
        self.add_prefixed::<L>("", "")
    }

    fn add_prefixed<L: Link>(&mut self, source_prefix: &str, target_prefix: &str) {
        self.links.push([
            prefixed::<L::Source>(source_prefix),
            type_name::<L>().to_string(),
            prefixed::<L::Target>(target_prefix),
        ])
    }

    fn to_file(&self, path: &str) {
        todo!();
    }
}

fn make_paper_ego(prefix: &str) -> BreakdownBasisNetwork {
    let mut bbn = BreakdownBasisNetwork::new();
    // bbn.add::<Works, Sources>()
    bbn.add::<AuthorshipLink>();
    bbn
}

pub fn make_bbns() {
    let source_paper = make_paper_ego("citing");
    let target_paper = make_paper_ego("referenced");
}

fn prefixed<T>(prefix: &str) -> String {
    format!("{}{}", prefix, type_name::<T>())
}
