use std::io;

use crate::{gen::a1_entity_mapping::Countries, steps::derive_links3::work_count, Stowage};

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    work_count::<Countries>(&mut stowage);
    stowage.write_code()?;
    Ok(())
}
