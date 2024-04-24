use std::io;

//gen
mod common;
mod ingest_entity;
mod quercus;
//spec
mod oa_csv_writers;
mod oa_entity_mapping;
mod oa_filters;
mod oa_fix_atts;
mod oa_structs;
mod oa_var_atts;

use common::Stowage;
use oa_csv_writers::write_csvs;
use oa_entity_mapping::make_ids;
use oa_filters::filter_setup;
use oa_fix_atts::write_fix_atts;
use oa_var_atts::write_var_atts;
use quercus::dump_all_cache;

fn main() -> io::Result<()> {
    let mut args = std::env::args();
    args.next();

    if let (Some(comm), Some(root_str)) = (args.next(), args.next()) {
        let stowage = Stowage::new(&root_str);
        if comm == "to-csv" {
            if let Some(in_root_str) = args.next() {
                let n: Option<usize> = match args.next() {
                    Some(sn) => Some(sn.parse::<usize>().unwrap()),
                    None => None,
                };
                write_csvs(&in_root_str, &stowage.entity_csvs.to_str().unwrap(), n)?;
            }
        } else if comm == "filter" {
            filter_setup(&stowage)?;
        } else if comm == "to-keys" {
            make_ids(&stowage)?;
        } else if comm == "fix-atts" {
            write_fix_atts(&stowage)?;
        } else if comm == "var-atts" {
            write_var_atts(&stowage)?;
        } else if comm == "build-qcs" {
            dump_all_cache(&stowage);
        }
    }
    Ok(())
}
