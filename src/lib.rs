use std::io;

//gen
mod common;
mod ingest_entity;
mod para;
mod quercus;
//spec
mod aggregate_quercus;
mod oa_csv_writers;
mod oa_entity_mapping;
mod oa_filters;
mod oa_fix_atts;
mod oa_structs;
mod oa_var_atts;
mod prune_quercus;
mod quercus_packet;

use aggregate_quercus::aggregate;
use common::Stowage;
use oa_csv_writers::write_csvs;
use oa_filters::filter_setup;
use oa_fix_atts::write_fix_atts;
use oa_var_atts::write_var_atts;
use prune_quercus::prune;
use quercus::dump_all_cache;
use quercus_packet::dump_packets;

pub fn runner(
    comm: &str,
    root_str: &str,
    in_root_o: Option<String>,
    n: Option<usize>,
) -> io::Result<()> {
    let stowage = Stowage::new(root_str);
    if comm == "to-csv" {
        if let Some(in_root_str) = in_root_o {
            write_csvs(&in_root_str, &stowage.entity_csvs.to_str().unwrap(), n)?;
        }
    } else if comm == "filter" {
        filter_setup(&stowage)?;
    } else if comm == "fix-atts" {
        write_fix_atts(&stowage)?;
    } else if comm == "var-atts" {
        write_var_atts(&stowage)?;
    } else if comm == "build-qcs" {
        dump_all_cache(stowage)?;
    } else if comm == "prune-qcs" {
        prune(stowage)?;
    } else if comm == "agg-qcs" {
        aggregate(stowage)?;
    } else if comm == "packet-qcs" {
        dump_packets(stowage)?;
    }
    Ok(())
}
