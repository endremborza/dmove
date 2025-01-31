use dmove::{MarkedAttribute, Entity, MappableEntity, NamespacedEntity};

pub struct SourcesCiteCount { }

pub struct AuthorsCiteCount { }

pub struct CountriesCiteCount { }

pub struct TopicsCiteCount { }

pub struct QsCiteCount { }

pub struct InstitutionsCiteCount { }

pub struct SubfieldsCiteCount { }

impl Entity for InstitutionsCiteCount { type T = u32; const N: usize =  17783; const NAME: & str =  "institutions-cite-count"; }

impl MappableEntity for InstitutionsCiteCount { type KeyType = usize; }

impl NamespacedEntity for InstitutionsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsCiteCount; }

impl Entity for AuthorsCiteCount { type T = u32; const N: usize =  2294555; const NAME: & str =  "authors-cite-count"; }

impl MappableEntity for AuthorsCiteCount { type KeyType = usize; }

impl NamespacedEntity for AuthorsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsCiteCount; }

impl Entity for CountriesCiteCount { type T = u32; const N: usize =  230; const NAME: & str =  "countries-cite-count"; }

impl MappableEntity for CountriesCiteCount { type KeyType = usize; }

impl NamespacedEntity for CountriesCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesCiteCount; }

impl Entity for SubfieldsCiteCount { type T = u32; const N: usize =  254; const NAME: & str =  "subfields-cite-count"; }

impl MappableEntity for SubfieldsCiteCount { type KeyType = usize; }

impl NamespacedEntity for SubfieldsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsCiteCount; }

impl Entity for TopicsCiteCount { type T = u32; const N: usize =  4518; const NAME: & str =  "topics-cite-count"; }

impl MappableEntity for TopicsCiteCount { type KeyType = usize; }

impl NamespacedEntity for TopicsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsCiteCount; }

impl Entity for SourcesCiteCount { type T = u32; const N: usize =  28671; const NAME: & str =  "sources-cite-count"; }

impl MappableEntity for SourcesCiteCount { type KeyType = usize; }

impl NamespacedEntity for SourcesCiteCount { const NS: & str =  "derive_links5"; }

impl Entity for QsCiteCount { type T = u32; const N: usize =  6; const NAME: & str =  "qs-cite-count"; }

impl MappableEntity for QsCiteCount { type KeyType = usize; }

impl NamespacedEntity for QsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Qs { type AttributeEntity = QsCiteCount; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesCiteCount; }