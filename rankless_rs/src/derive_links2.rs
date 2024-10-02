use std::io;

use crate::{
    common::Stowage,
    derive_links1::invert_read_multi_link,
    gen_derived_links1::{WorkAuthors, WorkInstitutions},
    gen_entity_mapping::{Authors, Institutions, Works},
};

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    invert_read_multi_link::<WorkAuthors, Works, Authors>(&mut stowage, "author-works");
    invert_read_multi_link::<WorkInstitutions, Works, Institutions>(
        &mut stowage,
        "institution-works",
    );
    stowage.write_code()?;
    Ok(())
}
