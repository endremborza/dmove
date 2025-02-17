use dmove::{MarkedAttribute, Entity, MappableEntity, NamespacedEntity};

pub struct InstitutionsCitSubfields { }

pub struct SubfieldsPapersYearly { }

pub struct SourcesCitationsYearly { }

pub struct InstitutionsRefSubfields { }

pub struct AuthorsRefSubfields { }

pub struct TopicsPapersYearly { }

pub struct InstitutionsPapersYearly { }

pub struct InstitutionsCitationsYearly { }

pub struct CountriesRefSubfields { }

pub struct SourcesCiteCount { }

pub struct AuthorsCiteCount { }

pub struct CountriesCiteCount { }

pub struct TopicsCiteCount { }

pub struct AuthorsPapersYearly { }

pub struct SourcesRefSubfields { }

pub struct SourcesRelInsts { }

pub struct TopicsCitSubfields { }

pub struct SubfieldsCitSubfields { }

pub struct SourcesPapersYearly { }

pub struct SubfieldsRelInsts { }

pub struct QsCiteCount { }

pub struct CountriesPapersYearly { }

pub struct TopicsRelInsts { }

pub struct AuthorsRelInsts { }

pub struct InstitutionsRelInsts { }

pub struct InstitutionsCiteCount { }

pub struct CountriesCitationsYearly { }

pub struct AuthorsCitSubfields { }

pub struct CountriesRelInsts { }

pub struct TopicsCitationsYearly { }

pub struct AuthorsCitationsYearly { }

pub struct SubfieldsCitationsYearly { }

pub struct SourcesCitSubfields { }

pub struct TopicsRefSubfields { }

pub struct CountriesCitSubfields { }

pub struct SubfieldsCiteCount { }

pub struct SubfieldsRefSubfields { }

impl Entity for InstitutionsCiteCount { type T = u32; const N: usize =  19833; const NAME: & str =  "institutions-cite-count"; }

impl MappableEntity for InstitutionsCiteCount { type KeyType = usize; }

impl NamespacedEntity for InstitutionsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsCiteCount; }

impl Entity for InstitutionsCitSubfields { type T = [u32; 253]; const N: usize =  19833; const NAME: & str =  "institutions-cit-subfields"; }

impl MappableEntity for InstitutionsCitSubfields { type KeyType = usize; }

impl NamespacedEntity for InstitutionsCitSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsCitSubfields; }

impl Entity for InstitutionsRefSubfields { type T = [u32; 253]; const N: usize =  19833; const NAME: & str =  "institutions-ref-subfields"; }

impl MappableEntity for InstitutionsRefSubfields { type KeyType = usize; }

impl NamespacedEntity for InstitutionsRefSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsRefSubfields; }

impl Entity for InstitutionsPapersYearly { type T = [u32; 11]; const N: usize =  19833; const NAME: & str =  "institutions-papers-yearly"; }

impl MappableEntity for InstitutionsPapersYearly { type KeyType = usize; }

impl NamespacedEntity for InstitutionsPapersYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsPapersYearly; }

impl Entity for InstitutionsCitationsYearly { type T = [u32; 11]; const N: usize =  19833; const NAME: & str =  "institutions-citations-yearly"; }

impl MappableEntity for InstitutionsCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for InstitutionsCitationsYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsCitationsYearly; }

impl Entity for InstitutionsRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize =  19833; const NAME: & str =  "institutions-rel-insts"; }

impl MappableEntity for InstitutionsRelInsts { type KeyType = usize; }

impl NamespacedEntity for InstitutionsRelInsts { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsRelInsts; }

impl Entity for AuthorsCiteCount { type T = u32; const N: usize =  4070683; const NAME: & str =  "authors-cite-count"; }

impl MappableEntity for AuthorsCiteCount { type KeyType = usize; }

impl NamespacedEntity for AuthorsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsCiteCount; }

impl Entity for AuthorsCitSubfields { type T = [u32; 253]; const N: usize =  4070683; const NAME: & str =  "authors-cit-subfields"; }

impl MappableEntity for AuthorsCitSubfields { type KeyType = usize; }

impl NamespacedEntity for AuthorsCitSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsCitSubfields; }

impl Entity for AuthorsRefSubfields { type T = [u32; 253]; const N: usize =  4070683; const NAME: & str =  "authors-ref-subfields"; }

impl MappableEntity for AuthorsRefSubfields { type KeyType = usize; }

impl NamespacedEntity for AuthorsRefSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsRefSubfields; }

impl Entity for AuthorsPapersYearly { type T = [u32; 11]; const N: usize =  4070683; const NAME: & str =  "authors-papers-yearly"; }

impl MappableEntity for AuthorsPapersYearly { type KeyType = usize; }

impl NamespacedEntity for AuthorsPapersYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsPapersYearly; }

impl Entity for AuthorsCitationsYearly { type T = [u32; 11]; const N: usize =  4070683; const NAME: & str =  "authors-citations-yearly"; }

impl MappableEntity for AuthorsCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for AuthorsCitationsYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsCitationsYearly; }

impl Entity for AuthorsRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize =  4070683; const NAME: & str =  "authors-rel-insts"; }

impl MappableEntity for AuthorsRelInsts { type KeyType = usize; }

impl NamespacedEntity for AuthorsRelInsts { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsRelInsts; }

impl Entity for CountriesCiteCount { type T = u32; const N: usize =  230; const NAME: & str =  "countries-cite-count"; }

impl MappableEntity for CountriesCiteCount { type KeyType = usize; }

impl NamespacedEntity for CountriesCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesCiteCount; }

impl Entity for CountriesCitSubfields { type T = [u32; 253]; const N: usize =  230; const NAME: & str =  "countries-cit-subfields"; }

impl MappableEntity for CountriesCitSubfields { type KeyType = usize; }

impl NamespacedEntity for CountriesCitSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesCitSubfields; }

impl Entity for CountriesRefSubfields { type T = [u32; 253]; const N: usize =  230; const NAME: & str =  "countries-ref-subfields"; }

impl MappableEntity for CountriesRefSubfields { type KeyType = usize; }

impl NamespacedEntity for CountriesRefSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesRefSubfields; }

impl Entity for CountriesPapersYearly { type T = [u32; 11]; const N: usize =  230; const NAME: & str =  "countries-papers-yearly"; }

impl MappableEntity for CountriesPapersYearly { type KeyType = usize; }

impl NamespacedEntity for CountriesPapersYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesPapersYearly; }

impl Entity for CountriesCitationsYearly { type T = [u32; 11]; const N: usize =  230; const NAME: & str =  "countries-citations-yearly"; }

impl MappableEntity for CountriesCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for CountriesCitationsYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesCitationsYearly; }

impl Entity for CountriesRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize =  230; const NAME: & str =  "countries-rel-insts"; }

impl MappableEntity for CountriesRelInsts { type KeyType = usize; }

impl NamespacedEntity for CountriesRelInsts { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesRelInsts; }

impl Entity for SubfieldsCiteCount { type T = u32; const N: usize =  254; const NAME: & str =  "subfields-cite-count"; }

impl MappableEntity for SubfieldsCiteCount { type KeyType = usize; }

impl NamespacedEntity for SubfieldsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsCiteCount; }

impl Entity for SubfieldsCitSubfields { type T = [u32; 253]; const N: usize =  254; const NAME: & str =  "subfields-cit-subfields"; }

impl MappableEntity for SubfieldsCitSubfields { type KeyType = usize; }

impl NamespacedEntity for SubfieldsCitSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsCitSubfields; }

impl Entity for SubfieldsRefSubfields { type T = [u32; 253]; const N: usize =  254; const NAME: & str =  "subfields-ref-subfields"; }

impl MappableEntity for SubfieldsRefSubfields { type KeyType = usize; }

impl NamespacedEntity for SubfieldsRefSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsRefSubfields; }

impl Entity for SubfieldsPapersYearly { type T = [u32; 11]; const N: usize =  254; const NAME: & str =  "subfields-papers-yearly"; }

impl MappableEntity for SubfieldsPapersYearly { type KeyType = usize; }

impl NamespacedEntity for SubfieldsPapersYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsPapersYearly; }

impl Entity for SubfieldsCitationsYearly { type T = [u32; 11]; const N: usize =  254; const NAME: & str =  "subfields-citations-yearly"; }

impl MappableEntity for SubfieldsCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for SubfieldsCitationsYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsCitationsYearly; }

impl Entity for SubfieldsRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize =  254; const NAME: & str =  "subfields-rel-insts"; }

impl MappableEntity for SubfieldsRelInsts { type KeyType = usize; }

impl NamespacedEntity for SubfieldsRelInsts { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsRelInsts; }

impl Entity for TopicsCiteCount { type T = u32; const N: usize =  4518; const NAME: & str =  "topics-cite-count"; }

impl MappableEntity for TopicsCiteCount { type KeyType = usize; }

impl NamespacedEntity for TopicsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsCiteCount; }

impl Entity for TopicsCitSubfields { type T = [u32; 253]; const N: usize =  4518; const NAME: & str =  "topics-cit-subfields"; }

impl MappableEntity for TopicsCitSubfields { type KeyType = usize; }

impl NamespacedEntity for TopicsCitSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsCitSubfields; }

impl Entity for TopicsRefSubfields { type T = [u32; 253]; const N: usize =  4518; const NAME: & str =  "topics-ref-subfields"; }

impl MappableEntity for TopicsRefSubfields { type KeyType = usize; }

impl NamespacedEntity for TopicsRefSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsRefSubfields; }

impl Entity for TopicsPapersYearly { type T = [u32; 11]; const N: usize =  4518; const NAME: & str =  "topics-papers-yearly"; }

impl MappableEntity for TopicsPapersYearly { type KeyType = usize; }

impl NamespacedEntity for TopicsPapersYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsPapersYearly; }

impl Entity for TopicsCitationsYearly { type T = [u32; 11]; const N: usize =  4518; const NAME: & str =  "topics-citations-yearly"; }

impl MappableEntity for TopicsCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for TopicsCitationsYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsCitationsYearly; }

impl Entity for TopicsRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize =  4518; const NAME: & str =  "topics-rel-insts"; }

impl MappableEntity for TopicsRelInsts { type KeyType = usize; }

impl NamespacedEntity for TopicsRelInsts { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsRelInsts; }

impl Entity for SourcesCiteCount { type T = u32; const N: usize =  37187; const NAME: & str =  "sources-cite-count"; }

impl MappableEntity for SourcesCiteCount { type KeyType = usize; }

impl NamespacedEntity for SourcesCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesCiteCount; }

impl Entity for SourcesCitSubfields { type T = [u32; 253]; const N: usize =  37187; const NAME: & str =  "sources-cit-subfields"; }

impl MappableEntity for SourcesCitSubfields { type KeyType = usize; }

impl NamespacedEntity for SourcesCitSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesCitSubfields; }

impl Entity for SourcesRefSubfields { type T = [u32; 253]; const N: usize =  37187; const NAME: & str =  "sources-ref-subfields"; }

impl MappableEntity for SourcesRefSubfields { type KeyType = usize; }

impl NamespacedEntity for SourcesRefSubfields { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesRefSubfields; }

impl Entity for SourcesPapersYearly { type T = [u32; 11]; const N: usize =  37187; const NAME: & str =  "sources-papers-yearly"; }

impl MappableEntity for SourcesPapersYearly { type KeyType = usize; }

impl NamespacedEntity for SourcesPapersYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesPapersYearly; }

impl Entity for SourcesCitationsYearly { type T = [u32; 11]; const N: usize =  37187; const NAME: & str =  "sources-citations-yearly"; }

impl MappableEntity for SourcesCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for SourcesCitationsYearly { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesCitationsYearly; }

impl Entity for SourcesRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize =  37187; const NAME: & str =  "sources-rel-insts"; }

impl MappableEntity for SourcesRelInsts { type KeyType = usize; }

impl NamespacedEntity for SourcesRelInsts { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesRelInsts; }

impl Entity for QsCiteCount { type T = u32; const N: usize =  6; const NAME: & str =  "qs-cite-count"; }

impl MappableEntity for QsCiteCount { type KeyType = usize; }

impl NamespacedEntity for QsCiteCount { const NS: & str =  "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Qs { type AttributeEntity = QsCiteCount; }