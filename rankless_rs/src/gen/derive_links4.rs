use dmove::{MarkedAttribute, Entity, MappableEntity, NamespacedEntity};

pub struct CountriesWorkCount { }

impl Entity for CountriesWorkCount { type T = u32; const N: usize = 230; const NAME: & str = "countries-work-count"; }

impl MappableEntity for CountriesWorkCount { type KeyType = usize; }

impl NamespacedEntity for CountriesWorkCount { const NS: & str = "derive_links4"; }

impl MarkedAttribute<crate::common::WorkCountMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesWorkCount; }