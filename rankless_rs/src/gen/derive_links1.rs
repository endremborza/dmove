use dmove::{MarkedAttribute, Entity, Link, VariableSizeAttribute, MappableEntity, NamespacedEntity};

pub struct SourceWorks { }

pub struct WorkAuthors { }

pub struct WorkInstitutions { }

pub struct WorksCiting { }

pub struct TopicWorks { }

pub struct WorkSubfields { }

impl Entity for WorksCiting { type T = Box<[u32]>; const N: usize =  44973353; const NAME: & str =  "works-citing"; }

impl MappableEntity for WorksCiting { type KeyType = usize; }

impl VariableSizeAttribute for WorksCiting { type SizeType = u32; }

impl NamespacedEntity for WorksCiting { const NS: & str =  "derive_links1"; }

impl Link for WorksCiting { type Source = crate::gen::a1_entity_mapping::Works; type Target = crate::gen::a1_entity_mapping::Works; }

impl MarkedAttribute<crate::common::MainWorkMarker> for crate::gen::a1_entity_mapping::Works { type AttributeEntity = WorksCiting; }

impl Entity for TopicWorks { type T = Box<[u32]>; const N: usize =  4518; const NAME: & str =  "topic-works"; }

impl MappableEntity for TopicWorks { type KeyType = usize; }

impl VariableSizeAttribute for TopicWorks { type SizeType = u32; }

impl NamespacedEntity for TopicWorks { const NS: & str =  "derive_links1"; }

impl Link for TopicWorks { type Source = crate::gen::a1_entity_mapping::Topics; type Target = crate::gen::a1_entity_mapping::Works; }

impl MarkedAttribute<crate::common::MainWorkMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicWorks; }

impl Entity for SourceWorks { type T = Box<[u32]>; const N: usize =  28671; const NAME: & str =  "source-works"; }

impl MappableEntity for SourceWorks { type KeyType = usize; }

impl VariableSizeAttribute for SourceWorks { type SizeType = u32; }

impl NamespacedEntity for SourceWorks { const NS: & str =  "derive_links1"; }

impl Link for SourceWorks { type Source = crate::gen::a1_entity_mapping::Sources; type Target = crate::gen::a1_entity_mapping::Works; }

impl MarkedAttribute<crate::common::MainWorkMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourceWorks; }

impl Entity for WorkSubfields { type T = Box<[u8]>; const N: usize =  44973353; const NAME: & str =  "work-subfields"; }

impl MappableEntity for WorkSubfields { type KeyType = usize; }

impl VariableSizeAttribute for WorkSubfields { type SizeType = u8; }

impl NamespacedEntity for WorkSubfields { const NS: & str =  "derive_links1"; }

impl Link for WorkSubfields { type Source = crate::gen::a1_entity_mapping::Works; type Target = crate::gen::a1_entity_mapping::Subfields; }

impl Entity for WorkAuthors { type T = Box<[u32]>; const N: usize =  44973353; const NAME: & str =  "work-authors"; }

impl MappableEntity for WorkAuthors { type KeyType = usize; }

impl VariableSizeAttribute for WorkAuthors { type SizeType = u8; }

impl NamespacedEntity for WorkAuthors { const NS: & str =  "derive_links1"; }

impl Link for WorkAuthors { type Source = crate::gen::a1_entity_mapping::Works; type Target = crate::gen::a1_entity_mapping::Authors; }

impl Entity for WorkInstitutions { type T = Box<[u16]>; const N: usize =  44973353; const NAME: & str =  "work-institutions"; }

impl MappableEntity for WorkInstitutions { type KeyType = usize; }

impl VariableSizeAttribute for WorkInstitutions { type SizeType = u8; }

impl NamespacedEntity for WorkInstitutions { const NS: & str =  "derive_links1"; }

impl Link for WorkInstitutions { type Source = crate::gen::a1_entity_mapping::Works; type Target = crate::gen::a1_entity_mapping::Institutions; }