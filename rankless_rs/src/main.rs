#![feature(min_specialization)]
use std::io;

mod bbns;
mod common;
mod csv_writers;
// mod derived_entities;
mod entity_mapping;
mod filters;
mod fix_atts;
mod gen_fix_att_structs;
mod gen_types;
mod gen_var_att_structs;
mod oa_structs;
// mod quercus_config;
mod var_atts;

use common::Stowage;
use csv_writers::write_csvs;
use entity_mapping::make_ids;
use filters::filter_setup;

pub fn runner(comm: &str, root_str: &str, in_root_o: Option<String>) -> io::Result<()> {
    let stowage = Stowage::new(root_str);
    if comm == "to-csv" {
        if let Some(in_root_str) = in_root_o {
            write_csvs(&in_root_str, &stowage.entity_csvs.to_str().unwrap())?;
        }
    } else if comm == "filter" {
        filter_setup(&stowage)?;
    } else if comm == "make-ids" {
        make_ids(stowage)?;
    } else if comm == "derive-entities" {
        // derived_entities::derive_entities(stowage)?;
    } else if comm == "fix-atts" {
        fix_atts::write_fix_atts(stowage)?;
    } else if comm == "var-atts" {
        var_atts::write_var_atts(stowage)?;
    } else if comm == "bbns" {
        bbns::make_bbns();
    } else if comm == "build-qcs" {

        // dump_all_cache(stowage)?;
    } else if comm == "prune-qcs" {
        // prune(stowage)?;
    } else if comm == "agg-qcs" {
        // aggregate(stowage)?;
    } else if comm == "packet-qcs" {
        // dump_packets(stowage)?;
    }
    Ok(())
}

fn main() -> io::Result<()> {
    let mut args = std::env::args();
    args.next();
    if let (Some(comm), Some(root_str)) = (args.next(), args.next()) {
        let in_root_str = args.next();
        runner(&comm, &root_str, in_root_str)?
    }
    Ok(())
}
