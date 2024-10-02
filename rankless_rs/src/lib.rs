#![feature(min_specialization)]
use std::io;

mod bbns;
mod common;
mod csv_writers;
pub mod gen_derived_links1;
pub mod gen_derived_links2;
pub mod gen_entity_mapping;
pub mod gen_init_links;
mod oa_structs;

//
mod agg_tree;

pub use common::{Quickest, QuickestBox, QuickestVBox, ReadIter, Stowage};

macro_rules! mods_as_comms {
    ($($mod_name:ident),*) => {
        $(mod $mod_name;)*

        fn subrun(comm: &str, mut stowage: Stowage) -> io::Result<()>{
            $(

            let mstr = stringify!($mod_name);
            if (comm == mstr) {
                stowage.set_namespace(mstr);
                return $mod_name::main(stowage);
            }
            )*
            Ok(())
        }
    };
}

mods_as_comms!(
    filter,
    entity_mapping,
    init_atts,
    derive_links1,
    derive_links2
);

pub fn runner(comm: &str, root_str: &str, in_root_o: Option<String>) -> io::Result<()> {
    let stowage = Stowage::new(root_str);
    if comm == "to-csv" {
        if let Some(in_root_str) = in_root_o {
            csv_writers::write_csvs(&in_root_str, &stowage)?;
        }
    }
    subrun(comm, stowage)
}
