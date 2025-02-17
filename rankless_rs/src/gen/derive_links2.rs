use dmove::{MarkedAttribute, Entity, Link, VariableSizeAttribute, MappableEntity, NamespacedEntity};

pub struct SubfieldWorks { }

pub struct WorkCountries { }

pub struct AuthorWorks { }

pub struct WorkCitingCounts { }

pub struct WorkTopSource { }

pub struct InstitutionWorks { }

impl Entity for WorkCitingCounts { type T = u32; const N: usize =  52173052; const NAME: & str =  "work-citing-counts"; }

impl MappableEntity for WorkCitingCounts { type KeyType = usize; }

impl NamespacedEntity for WorkCitingCounts { const NS: & str =  "derive_links2"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Works { type AttributeEntity = WorkCitingCounts; }

impl Entity for WorkTopSource { type T = u16; const N: usize =  52173052; const NAME: & str =  "work-top-source"; }

impl MappableEntity for WorkTopSource { type KeyType = usize; }

impl NamespacedEntity for WorkTopSource { const NS: & str =  "derive_links2"; }

impl Entity for AuthorWorks { type T = Box<[u32]>; const N: usize =  4070683; const NAME: & str =  "author-works"; }

impl MappableEntity for AuthorWorks { type KeyType = usize; }

impl VariableSizeAttribute for AuthorWorks { type SizeType = u32; }

impl NamespacedEntity for AuthorWorks { const NS: & str =  "derive_links2"; }

impl Link for AuthorWorks { type Source = crate::gen::a1_entity_mapping::Authors; type Target = crate::gen::a1_entity_mapping::Works; }

impl MarkedAttribute<crate::common::MainWorkMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorWorks; }

impl Entity for SubfieldWorks { type T = Box<[u32]>; const N: usize =  254; const NAME: & str =  "subfield-works"; }

impl MappableEntity for SubfieldWorks { type KeyType = usize; }

impl VariableSizeAttribute for SubfieldWorks { type SizeType = u32; }

impl NamespacedEntity for SubfieldWorks { const NS: & str =  "derive_links2"; }

impl Link for SubfieldWorks { type Source = crate::gen::a1_entity_mapping::Subfields; type Target = crate::gen::a1_entity_mapping::Works; }

impl MarkedAttribute<crate::common::MainWorkMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldWorks; }

impl Entity for InstitutionWorks { type T = Box<[u32]>; const N: usize =  19833; const NAME: & str =  "institution-works"; }

impl MappableEntity for InstitutionWorks { type KeyType = usize; }

impl VariableSizeAttribute for InstitutionWorks { type SizeType = u32; }

impl NamespacedEntity for InstitutionWorks { const NS: & str =  "derive_links2"; }

impl Link for InstitutionWorks { type Source = crate::gen::a1_entity_mapping::Institutions; type Target = crate::gen::a1_entity_mapping::Works; }

impl MarkedAttribute<crate::common::MainWorkMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionWorks; }

impl Entity for WorkCountries { type T = Box<[u8]>; const N: usize =  52173052; const NAME: & str =  "work-countries"; }

impl MappableEntity for WorkCountries { type KeyType = usize; }

impl VariableSizeAttribute for WorkCountries { type SizeType = u8; }

impl NamespacedEntity for WorkCountries { const NS: & str =  "derive_links2"; }

impl Link for WorkCountries { type Source = crate::gen::a1_entity_mapping::Works; type Target = crate::gen::a1_entity_mapping::Countries; }