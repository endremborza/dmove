use std::io;

use dmove::{DowncastingBuilder, FixAttBuilder};

use crate::{
    common::{QuickMap, Stowage},
    gen::{
        a1_entity_mapping::Works,
        a2_init_atts::{InstCountries, SourceYearQs, WorkSources, WorkYears},
        derive_links1::{WorkAuthors, WorkInstitutions, WorkSubfields, WorksCiting},
    },
    steps::derive_links1::{collapse_links, invert_read_multi_link_to_work},
    CiteCountMarker, QuickestBox, ReadIter,
};

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    let interface = stowage.get_entity_interface::<WorksCiting, ReadIter>();
    let wc_name = "work-citing-counts";
    stowage.add_iter_owned::<DowncastingBuilder, _, _>(interface.map(|e| e.len()), Some(&wc_name));
    stowage.declare::<Works, CiteCountMarker>(wc_name);

    let sqy = stowage.get_entity_interface::<SourceYearQs, QuickMap>();
    let w_sources = stowage.get_entity_interface::<WorkSources, ReadIter>();
    let w_years = stowage.get_entity_interface::<WorkYears, QuickestBox>();
    let iter = w_sources.enumerate().map(|(i, sources)| {
        let wy = w_years[i];
        let mut best_q = 6;
        let mut best_s = 0;
        let mut update = |mut q, sid| {
            if q == 0 {
                q = 5
            }
            if q < best_q {
                best_s = sid;
                best_q = q;
            }
        };
        for sid in sources {
            let q = *sqy.get(&(sid, wy)).unwrap_or(&5);
            update(q, sid);
        }
        best_s
    });
    stowage.add_iter_owned::<FixAttBuilder, _, _>(iter, Some("work-top-source"));

    invert_read_multi_link_to_work::<WorkAuthors>(&mut stowage, "author-works");
    invert_read_multi_link_to_work::<WorkSubfields>(&mut stowage, "subfield-works");
    invert_read_multi_link_to_work::<WorkInstitutions>(&mut stowage, "institution-works");
    collapse_links::<WorkInstitutions, InstCountries>(&mut stowage, "work-countries");
    stowage.write_code()?;
    Ok(())
}
