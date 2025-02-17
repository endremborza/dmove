use dmove::{
    Data64MappedEntityBuilder, DowncastingBuilder, Entity, Link, MarkedAttribute, NamespacedEntity,
    UnsignedNumber, VariableSizeAttribute, MAA,
};
use tqdm::Iter;

use crate::{
    common::MainWorkMarker,
    gen::{
        a1_entity_mapping::{Authors, Institutions, Sources, Subfields, Topics, Works},
        derive_links2::WorkCountries,
    },
    steps::derive_links1::invert_read_multi_link_to_work,
    CiteCountMarker, ReadFixIter, ReadIter, Stowage, WorkCountMarker,
};

const MIN_FOR_HIT: usize = 1000;

pub fn work_count<E>(stowage: &mut Stowage)
where
    E: MarkedAttribute<MainWorkMarker>,
    <E as MarkedAttribute<MainWorkMarker>>::AttributeEntity: Entity<T = Box<[<Works as Entity>::T]>>
        + Link<Target = Works>
        + NamespacedEntity
        + VariableSizeAttribute,
{
    let interface = stowage.get_entity_interface::<MAA<E, MainWorkMarker>, ReadIter>();
    let wc_name = format!("{}-work-count", E::NAME);

    stowage.add_iter_owned::<DowncastingBuilder, _, _>(interface.map(|e| e.len()), Some(&wc_name));
    stowage.declare::<E, WorkCountMarker>(&wc_name)
}

pub fn main(mut stowage: Stowage) -> std::io::Result<()> {
    work_count::<Sources>(&mut stowage);
    work_count::<Institutions>(&mut stowage);
    work_count::<Authors>(&mut stowage);
    work_count::<Subfields>(&mut stowage);
    work_count::<Topics>(&mut stowage);
    invert_read_multi_link_to_work::<WorkCountries>(&mut stowage, "country-works");
    let interface = stowage.get_entity_interface::<MAA<Works, CiteCountMarker>, ReadFixIter>();
    let hit_papers = interface.tqdm().enumerate().filter_map(|(i, e)| {
        if e.to_usize() >= MIN_FOR_HIT {
            Some(i as u64)
        } else {
            None
        }
    });
    stowage.add_iter_owned::<Data64MappedEntityBuilder, _, _>(hit_papers, Some("hit-papers"));
    stowage.write_code()?;
    Ok(())
}
