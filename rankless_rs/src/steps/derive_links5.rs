use std::io;

use dmove::{
    DowncastingBuilder, Entity, MarkedAttribute, NamespacedEntity, UnsignedNumber,
    VariableSizeAttribute, ET, MAA,
};
use hashbrown::HashMap;

use crate::{
    common::{init_empty_slice, MainWorkMarker, QuickMap},
    gen::{
        a1_entity_mapping::{
            Authors, Countries, Institutions, Qs, Sources, Subfields, Topics, Works,
        },
        a2_init_atts::{SourceYearQs, WorkYears},
        derive_links2::WorkCitingCounts,
    },
    CiteCountMarker, QuickestBox, ReadIter, Stowage,
};

pub fn cite_count<E>(stowage: &mut Stowage, wif: &Box<[ET<WorkCitingCounts>]>)
where
    E: MarkedAttribute<MainWorkMarker>,
    MAA<E, MainWorkMarker>:
        Entity<T = Box<[<Works as Entity>::T]>> + NamespacedEntity + VariableSizeAttribute,
{
    let wc_interface = stowage.get_entity_interface::<MAA<E, MainWorkMarker>, ReadIter>();
    let cc_name = format!("{}-cite-count", E::NAME);

    stowage.add_iter_owned::<DowncastingBuilder, _, _>(
        wc_interface.map(|ws| ws.iter().map(|e| wif[*e as usize] as usize).sum()),
        Some(&cc_name),
    );
    stowage.declare::<E, CiteCountMarker>(&cc_name);
}

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    let wif = stowage.get_entity_interface::<WorkCitingCounts, QuickestBox>();
    // cite_count::<Sources>(&mut stowage, &wif);
    cite_count::<Institutions>(&mut stowage, &wif);
    cite_count::<Authors>(&mut stowage, &wif);
    cite_count::<Countries>(&mut stowage, &wif);
    cite_count::<Subfields>(&mut stowage, &wif);
    cite_count::<Topics>(&mut stowage, &wif);
    q_ccs(&mut stowage, &wif);
    stowage.write_code()?;
    Ok(())
}

fn q_ccs(stowage: &mut Stowage, wif: &Box<[ET<WorkCitingCounts>]>) {
    let mut q_maps = init_empty_slice::<Qs, HashMap<ET<Works>, usize>>();
    let wc_interface = stowage.get_entity_interface::<MAA<Sources, MainWorkMarker>, ReadIter>();
    let qy_map = stowage.get_entity_interface::<SourceYearQs, QuickMap>();
    let wyears = stowage.get_entity_interface::<WorkYears, QuickestBox>();
    let qc_name = format!("qs-cite-count");
    let sc_name = format!("sources-cite-count");

    stowage.add_iter_owned::<DowncastingBuilder, _, _>(
        wc_interface.enumerate().map(|(i, ws)| {
            let sid = ET::<Sources>::from_usize(i);
            ws.iter()
                .map(|e| {
                    let wind = *e as usize;
                    let year = wyears[wind];
                    let wcount = wif[wind] as usize;
                    let q = qy_map.get(&(sid, year)).unwrap_or(&0);
                    q_maps[*q as usize].insert(*e, wcount);
                    wcount
                })
                .sum()
        }),
        Some(&sc_name),
    );

    stowage.add_iter_owned::<DowncastingBuilder, _, _>(
        q_maps.iter().map(|e| e.values().sum()),
        Some(&qc_name),
    );

    stowage.declare::<Qs, CiteCountMarker>(&qc_name);
    stowage.declare::<Sources, CiteCountMarker>(&sc_name);
}
