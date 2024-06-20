use serde::Serialize;
use std::io;

use crate::{
    common::{read_js_path, Stowage},
    quercus::Quercus,
};

// const LEVEL_ELEMS: usize = 16;

#[derive(Serialize)]
struct QuercusPacket {
    pub id: u16,
    pub weight: u32,
    pub source_count: u32,
    pub top_source: (u64, u32),
}

pub fn dump_packets(stowage: Stowage) -> io::Result<()> {
    stowage
        .iter_pruned_qc_locs()
        .take(20)
        .map(|(_, de)| {
            let qc = read_js_path::<Quercus>(de.path().to_str().unwrap());
        })
        .for_each(drop);

    let qp = QuercusPacket {
        id: 1,
        weight: 2,
        source_count: 3,
        top_source: (5, 6),
    };

    let buf = bincode::serialize(&qp).unwrap();

    println!("{}  -  {:?}", buf.len(), buf);

    Ok(())
}
