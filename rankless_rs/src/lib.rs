#![feature(min_specialization)]
#![feature(future_join)]
use std::io;

pub mod agg_tree;
pub mod common;
mod csv_writers;
pub mod env_consts;
mod filter;
pub mod gen;
mod oa_structs;
mod semantic_ids;
pub mod steps;

pub use common::{
    CiteCountMarker, NameExtensionMarker, NameMarker, QuickestBox, QuickestNumbered, QuickestVBox,
    ReadFixIter, ReadIter, SemanticIdMarker, Stowage, WorkCountMarker,
};

macro_rules! mods_as_comms {
    ($($mod_name:ident),*) => {
        fn subrun(comm: &str, mut stowage: Stowage) -> io::Result<()>{
            $(
            let mstr = stringify!($mod_name);
            if (comm == mstr) {
                stowage.set_namespace(mstr);
                return steps::$mod_name::main(stowage);
            }
            )*
            Ok(())
        }
    };
}

pub fn runner(comm: &str, root_str: &str, in_root_o: Option<String>) -> io::Result<()> {
    let stowage = Stowage::new(root_str);
    if comm == "to-csv" {
        if let Some(in_root_str) = in_root_o {
            csv_writers::write_csvs(&in_root_str, &stowage)?;
        }
    } else if comm == "filter" {
        return filter::main(stowage);
    }
    subrun(comm, stowage)
}
mods_as_comms!(a1_entity_mapping, a2_init_atts, derive_links1, derive_links2, derive_links3, derive_links4, derive_links5);
