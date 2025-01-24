use dmove::{
    DowncastingBuilder, Entity, Link, MarkedAttribute, NamespacedEntity, VariableSizeAttribute,
};

use crate::{
    common::MainWorkMarker,
    gen::{
        a1_entity_mapping::{Authors, Institutions, Sources, Subfields, Topics, Works},
        derive_links2::WorkCountries,
    },
    steps::derive_links1::invert_read_multi_link_to_work,
    ReadIter, Stowage, WorkCountMarker,
};

pub fn work_count<E>(stowage: &mut Stowage)
where
    E: MarkedAttribute<MainWorkMarker>,
    <E as MarkedAttribute<MainWorkMarker>>::AttributeEntity: Entity<T = Box<[<Works as Entity>::T]>>
        + Link<Target = Works>
        + NamespacedEntity
        + VariableSizeAttribute,
{
    let interface = stowage
        .get_entity_interface::<<E as MarkedAttribute<MainWorkMarker>>::AttributeEntity, ReadIter>(
        );
    let wc_name = format!("{}-work-count", E::NAME);

    stowage.add_iter_owned::<DowncastingBuilder, _, _>(interface.map(|e| e.len()), Some(&wc_name));
    stowage
        .builder
        .as_mut()
        .unwrap()
        .declare_marked_attribute::<E, WorkCountMarker>(&wc_name);
}

pub fn main(mut stowage: Stowage) -> std::io::Result<()> {
    work_count::<Sources>(&mut stowage);
    work_count::<Institutions>(&mut stowage);
    work_count::<Authors>(&mut stowage);
    work_count::<Subfields>(&mut stowage);
    work_count::<Topics>(&mut stowage);
    invert_read_multi_link_to_work::<WorkCountries>(&mut stowage, "country-works");
    stowage.write_code()?;
    Ok(())
}
