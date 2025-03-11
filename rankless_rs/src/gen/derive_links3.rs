use dmove::{MarkedAttribute, Entity, Link, VariableSizeAttribute, MappableEntity, NamespacedEntity};

pub struct InstitutionsWorkCount { }

pub struct AuthorsWorkCount { }

pub struct SourcesWorkCount { }

pub struct SubfieldsWorkCount { }

pub struct TopicsWorkCount { }

pub struct HitPapers { }

pub struct CountryWorks { }

impl Entity for SourcesWorkCount { type T = u32; const N: usize = 39074; const NAME: & str = "sources-work-count"; }

impl MappableEntity for SourcesWorkCount { type KeyType = usize; }

impl NamespacedEntity for SourcesWorkCount { const NS: & str = "derive_links3"; }

impl MarkedAttribute<crate::common::WorkCountMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesWorkCount; }

impl Entity for InstitutionsWorkCount { type T = u32; const N: usize = 20967; const NAME: & str = "institutions-work-count"; }

impl MappableEntity for InstitutionsWorkCount { type KeyType = usize; }

impl NamespacedEntity for InstitutionsWorkCount { const NS: & str = "derive_links3"; }

impl MarkedAttribute<crate::common::WorkCountMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsWorkCount; }

impl Entity for AuthorsWorkCount { type T = u16; const N: usize = 3882893; const NAME: & str = "authors-work-count"; }

impl MappableEntity for AuthorsWorkCount { type KeyType = usize; }

impl NamespacedEntity for AuthorsWorkCount { const NS: & str = "derive_links3"; }

impl MarkedAttribute<crate::common::WorkCountMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsWorkCount; }

impl Entity for SubfieldsWorkCount { type T = u32; const N: usize = 254; const NAME: & str = "subfields-work-count"; }

impl MappableEntity for SubfieldsWorkCount { type KeyType = usize; }

impl NamespacedEntity for SubfieldsWorkCount { const NS: & str = "derive_links3"; }

impl MarkedAttribute<crate::common::WorkCountMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsWorkCount; }

impl Entity for TopicsWorkCount { type T = u32; const N: usize = 4518; const NAME: & str = "topics-work-count"; }

impl MappableEntity for TopicsWorkCount { type KeyType = usize; }

impl NamespacedEntity for TopicsWorkCount { const NS: & str = "derive_links3"; }

impl MarkedAttribute<crate::common::WorkCountMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsWorkCount; }

impl Entity for CountryWorks { type T = Box<[u32]>; const N: usize = 230; const NAME: & str = "country-works"; }

impl MappableEntity for CountryWorks { type KeyType = usize; }

impl VariableSizeAttribute for CountryWorks { type SizeType = u32; }

impl NamespacedEntity for CountryWorks { const NS: & str = "derive_links3"; }

impl Link for CountryWorks { type Source = crate::gen::a1_entity_mapping::Countries; type Target = crate::gen::a1_entity_mapping::Works; }

impl MarkedAttribute<crate::common::MainWorkMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountryWorks; }

impl Entity for HitPapers { type T = u16; const N: usize = 53606; const NAME: & str = "hit-papers"; }

impl MappableEntity for HitPapers { type KeyType = u64; }

impl NamespacedEntity for HitPapers { const NS: & str = "derive_links3"; }