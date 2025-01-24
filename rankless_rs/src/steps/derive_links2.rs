use std::io;

use dmove::DowncastingBuilder;

use crate::{
    common::Stowage,
    gen::{
        a1_entity_mapping::Works,
        a2_init_atts::InstCountries,
        derive_links1::{WorkAuthors, WorkInstitutions, WorkSubfields, WorksCiting},
    },
    steps::derive_links1::{collapse_links, invert_read_multi_link_to_work},
    CiteCountMarker, ReadIter,
};

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    let interface = stowage.get_entity_interface::<WorksCiting, ReadIter>();
    let wc_name = "work-citing-counts";
    stowage.add_iter_owned::<DowncastingBuilder, _, _>(interface.map(|e| e.len()), Some(&wc_name));
    stowage.declare::<Works, CiteCountMarker>(wc_name);

    invert_read_multi_link_to_work::<WorkAuthors>(&mut stowage, "author-works");
    invert_read_multi_link_to_work::<WorkSubfields>(&mut stowage, "subfield-works");
    invert_read_multi_link_to_work::<WorkInstitutions>(&mut stowage, "institution-works");
    collapse_links::<WorkInstitutions, InstCountries>(&mut stowage, "work-countries");
    stowage.write_code()?;
    Ok(())
}
