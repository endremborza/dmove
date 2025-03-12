use dmove::{MarkedAttribute, Entity, VariableSizeAttribute, MappableEntity, NamespacedEntity};

pub struct SourceStats { }

pub struct InstitutionsSemanticIds { }

pub struct CountriesTopJournals { }

pub struct SourcesCitationsYearly { }

pub struct CountriesSemanticIds { }

pub struct InstitutionsRefSubfields { }

pub struct SubfieldsPapersYearly { }

pub struct SubfieldsTopPaperTopics { }

pub struct TopicsPapersYearly { }

pub struct AuthorsTopPaperSubfields { }

pub struct AuthorsTopCitingSubfields { }

pub struct CountriesRefSubfields { }

pub struct SourcesCiteCount { }

pub struct SourcesRefSubfields { }

pub struct InstitutionsPapersYearly { }

pub struct SubfieldsSemanticIds { }

pub struct CountriesCiteCount { }

pub struct InstitutionsCitationsYearly { }

pub struct AuthorsPapersYearly { }

pub struct AuthorsTopPaperTopics { }

pub struct SubfieldsTopCitingSubfields { }

pub struct InstitutionsTopJournals { }

pub struct AuthorsTopAffCountries { }

pub struct TopicsCitSubfields { }

pub struct SourcesPapersYearly { }

pub struct InstitutionsTopPaperTopics { }

pub struct AuthorsSemanticIds { }

pub struct TopicsTopCitingSubfields { }

pub struct InstitutionsTopCitingSubfields { }

pub struct SourcesTopPaperSubfields { }

pub struct SourcesTopJournals { }

pub struct SubfieldsTopAffCountries { }

pub struct TopicsRelInsts { }

pub struct AuthorsRelInsts { }

pub struct InstitutionsCiteCount { }

pub struct SubfieldsTopPaperAuthors { }

pub struct TopicsCitationsYearly { }

pub struct TopicsTopJournals { }

pub struct AuthorsCitationsYearly { }

pub struct AuthorsTopPaperAuthors { }

pub struct TopicsTopPaperSubfields { }

pub struct SubfieldsTopJournals { }

pub struct SourcesCitSubfields { }

pub struct AuthorsTopJournals { }

pub struct TopicsRefSubfields { }

pub struct CountriesCitSubfields { }

pub struct SubfieldsRefSubfields { }

pub struct SourcesTopCitingSubfields { }

pub struct InstitutionsCitSubfields { }

pub struct CountriesTopPaperAuthors { }

pub struct TopicsTopPaperTopics { }

pub struct AuthorsRefSubfields { }

pub struct CountriesTopPaperTopics { }

pub struct SourcesTopPaperAuthors { }

pub struct InstitutionsTopAffCountries { }

pub struct CountriesTopPaperSubfields { }

pub struct CountriesTopCitingSubfields { }

pub struct AuthorsCiteCount { }

pub struct TopicsCiteCount { }

pub struct TopicsTopAffCountries { }

pub struct SourcesRelInsts { }

pub struct SubfieldsCitSubfields { }

pub struct InstitutionsTopPaperSubfields { }

pub struct QsCiteCount { }

pub struct SubfieldsRelInsts { }

pub struct SourcePairsByPath { }

pub struct CountriesPapersYearly { }

pub struct InstitutionsTopPaperAuthors { }

pub struct SubfieldPairsByPath { }

pub struct SubfieldsTopPaperSubfields { }

pub struct SourcesSemanticIds { }

pub struct CountriesCitationsYearly { }

pub struct CountriesRelInsts { }

pub struct InstitutionsRelInsts { }

pub struct AuthorsCitSubfields { }

pub struct TopicsTopPaperAuthors { }

pub struct SourcesTopPaperTopics { }

pub struct CountriesTopAffCountries { }

pub struct SubfieldsCitationsYearly { }

pub struct SourcesTopAffCountries { }

pub struct SubfieldsCiteCount { }

impl Entity for SourcesCiteCount { type T = u32; const N: usize = 39074; const NAME: & str = "sources-cite-count"; }

impl MappableEntity for SourcesCiteCount { type KeyType = usize; }

impl NamespacedEntity for SourcesCiteCount { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesCiteCount; }

impl Entity for SourcesCitSubfields { type T = [u32; 253]; const N: usize = 39074; const NAME: & str = "sources-cit-subfields"; }

impl MappableEntity for SourcesCitSubfields { type KeyType = usize; }

impl NamespacedEntity for SourcesCitSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesCitSubfields; }

impl Entity for SourcesRefSubfields { type T = [u32; 253]; const N: usize = 39074; const NAME: & str = "sources-ref-subfields"; }

impl MappableEntity for SourcesRefSubfields { type KeyType = usize; }

impl NamespacedEntity for SourcesRefSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesRefSubfields; }

impl Entity for SourcesPapersYearly { type T = [u32; 11]; const N: usize = 39074; const NAME: & str = "sources-papers-yearly"; }

impl MappableEntity for SourcesPapersYearly { type KeyType = usize; }

impl NamespacedEntity for SourcesPapersYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesPapersYearly; }

impl Entity for SourcesCitationsYearly { type T = [u32; 11]; const N: usize = 39074; const NAME: & str = "sources-citations-yearly"; }

impl MappableEntity for SourcesCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for SourcesCitationsYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesCitationsYearly; }

impl Entity for SourcesTopPaperSubfields { type T = [(u32, u8); 3]; const N: usize = 39074; const NAME: & str = "sources-top-paper-subfields"; }

impl MappableEntity for SourcesTopPaperSubfields { type KeyType = usize; }

impl NamespacedEntity for SourcesTopPaperSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperSfMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesTopPaperSubfields; }

impl Entity for SourcesTopCitingSubfields { type T = [(u32, u8); 3]; const N: usize = 39074; const NAME: & str = "sources-top-citing-subfields"; }

impl MappableEntity for SourcesTopCitingSubfields { type KeyType = usize; }

impl NamespacedEntity for SourcesTopCitingSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3CitingSfMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesTopCitingSubfields; }

impl Entity for SourcesTopPaperTopics { type T = [(u32, u16); 3]; const N: usize = 39074; const NAME: & str = "sources-top-paper-topics"; }

impl MappableEntity for SourcesTopPaperTopics { type KeyType = usize; }

impl NamespacedEntity for SourcesTopPaperTopics { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperTopicMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesTopPaperTopics; }

impl Entity for SourcesTopPaperAuthors { type T = [(u32, u32); 5]; const N: usize = 39074; const NAME: & str = "sources-top-paper-authors"; }

impl MappableEntity for SourcesTopPaperAuthors { type KeyType = usize; }

impl NamespacedEntity for SourcesTopPaperAuthors { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AuthorMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesTopPaperAuthors; }

impl Entity for SourcesTopJournals { type T = [(u32, u16); 3]; const N: usize = 39074; const NAME: & str = "sources-top-journals"; }

impl MappableEntity for SourcesTopJournals { type KeyType = usize; }

impl NamespacedEntity for SourcesTopJournals { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3JournalMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesTopJournals; }

impl Entity for SourcesTopAffCountries { type T = [(u32, u8); 3]; const N: usize = 39074; const NAME: & str = "sources-top-aff-countries"; }

impl MappableEntity for SourcesTopAffCountries { type KeyType = usize; }

impl NamespacedEntity for SourcesTopAffCountries { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AffCountryMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesTopAffCountries; }

impl Entity for SourcesRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize = 39074; const NAME: & str = "sources-rel-insts"; }

impl MappableEntity for SourcesRelInsts { type KeyType = usize; }

impl NamespacedEntity for SourcesRelInsts { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesRelInsts; }

impl Entity for SourceStats { type T = ([u32; 2], u8); const N: usize = 39074; const NAME: & str = "source-stats"; }

impl MappableEntity for SourceStats { type KeyType = usize; }

impl NamespacedEntity for SourceStats { const NS: & str = "derive_links5"; }

impl Entity for QsCiteCount { type T = u32; const N: usize = 6; const NAME: & str = "qs-cite-count"; }

impl MappableEntity for QsCiteCount { type KeyType = usize; }

impl NamespacedEntity for QsCiteCount { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::steps::a1_entity_mapping::Qs { type AttributeEntity = QsCiteCount; }

impl Entity for AuthorsSemanticIds { type T = String; const N: usize = 3882893; const NAME: & str = "authors-semantic-ids"; }

impl MappableEntity for AuthorsSemanticIds { type KeyType = usize; }

impl VariableSizeAttribute for AuthorsSemanticIds { type SizeType = u8; }

impl NamespacedEntity for AuthorsSemanticIds { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsSemanticIds; }

impl Entity for InstitutionsSemanticIds { type T = String; const N: usize = 20967; const NAME: & str = "institutions-semantic-ids"; }

impl MappableEntity for InstitutionsSemanticIds { type KeyType = usize; }

impl VariableSizeAttribute for InstitutionsSemanticIds { type SizeType = u8; }

impl NamespacedEntity for InstitutionsSemanticIds { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsSemanticIds; }

impl Entity for SourcesSemanticIds { type T = String; const N: usize = 39074; const NAME: & str = "sources-semantic-ids"; }

impl MappableEntity for SourcesSemanticIds { type KeyType = usize; }

impl VariableSizeAttribute for SourcesSemanticIds { type SizeType = u16; }

impl NamespacedEntity for SourcesSemanticIds { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Sources { type AttributeEntity = SourcesSemanticIds; }

impl Entity for SubfieldsSemanticIds { type T = String; const N: usize = 254; const NAME: & str = "subfields-semantic-ids"; }

impl MappableEntity for SubfieldsSemanticIds { type KeyType = usize; }

impl VariableSizeAttribute for SubfieldsSemanticIds { type SizeType = u8; }

impl NamespacedEntity for SubfieldsSemanticIds { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsSemanticIds; }

impl Entity for CountriesSemanticIds { type T = String; const N: usize = 230; const NAME: & str = "countries-semantic-ids"; }

impl MappableEntity for CountriesSemanticIds { type KeyType = usize; }

impl VariableSizeAttribute for CountriesSemanticIds { type SizeType = u8; }

impl NamespacedEntity for CountriesSemanticIds { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesSemanticIds; }

impl Entity for CountriesCiteCount { type T = u32; const N: usize = 230; const NAME: & str = "countries-cite-count"; }

impl MappableEntity for CountriesCiteCount { type KeyType = usize; }

impl NamespacedEntity for CountriesCiteCount { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesCiteCount; }

impl Entity for CountriesCitSubfields { type T = [u32; 253]; const N: usize = 230; const NAME: & str = "countries-cit-subfields"; }

impl MappableEntity for CountriesCitSubfields { type KeyType = usize; }

impl NamespacedEntity for CountriesCitSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesCitSubfields; }

impl Entity for CountriesRefSubfields { type T = [u32; 253]; const N: usize = 230; const NAME: & str = "countries-ref-subfields"; }

impl MappableEntity for CountriesRefSubfields { type KeyType = usize; }

impl NamespacedEntity for CountriesRefSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesRefSubfields; }

impl Entity for CountriesPapersYearly { type T = [u32; 11]; const N: usize = 230; const NAME: & str = "countries-papers-yearly"; }

impl MappableEntity for CountriesPapersYearly { type KeyType = usize; }

impl NamespacedEntity for CountriesPapersYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesPapersYearly; }

impl Entity for CountriesCitationsYearly { type T = [u32; 11]; const N: usize = 230; const NAME: & str = "countries-citations-yearly"; }

impl MappableEntity for CountriesCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for CountriesCitationsYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesCitationsYearly; }

impl Entity for CountriesTopPaperSubfields { type T = [(u32, u8); 3]; const N: usize = 230; const NAME: & str = "countries-top-paper-subfields"; }

impl MappableEntity for CountriesTopPaperSubfields { type KeyType = usize; }

impl NamespacedEntity for CountriesTopPaperSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperSfMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesTopPaperSubfields; }

impl Entity for CountriesTopCitingSubfields { type T = [(u32, u8); 3]; const N: usize = 230; const NAME: & str = "countries-top-citing-subfields"; }

impl MappableEntity for CountriesTopCitingSubfields { type KeyType = usize; }

impl NamespacedEntity for CountriesTopCitingSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3CitingSfMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesTopCitingSubfields; }

impl Entity for CountriesTopPaperTopics { type T = [(u32, u16); 3]; const N: usize = 230; const NAME: & str = "countries-top-paper-topics"; }

impl MappableEntity for CountriesTopPaperTopics { type KeyType = usize; }

impl NamespacedEntity for CountriesTopPaperTopics { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperTopicMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesTopPaperTopics; }

impl Entity for CountriesTopPaperAuthors { type T = [(u32, u32); 5]; const N: usize = 230; const NAME: & str = "countries-top-paper-authors"; }

impl MappableEntity for CountriesTopPaperAuthors { type KeyType = usize; }

impl NamespacedEntity for CountriesTopPaperAuthors { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AuthorMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesTopPaperAuthors; }

impl Entity for CountriesTopJournals { type T = [(u32, u16); 3]; const N: usize = 230; const NAME: & str = "countries-top-journals"; }

impl MappableEntity for CountriesTopJournals { type KeyType = usize; }

impl NamespacedEntity for CountriesTopJournals { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3JournalMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesTopJournals; }

impl Entity for CountriesTopAffCountries { type T = [(u32, u8); 3]; const N: usize = 230; const NAME: & str = "countries-top-aff-countries"; }

impl MappableEntity for CountriesTopAffCountries { type KeyType = usize; }

impl NamespacedEntity for CountriesTopAffCountries { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AffCountryMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesTopAffCountries; }

impl Entity for CountriesRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize = 230; const NAME: & str = "countries-rel-insts"; }

impl MappableEntity for CountriesRelInsts { type KeyType = usize; }

impl NamespacedEntity for CountriesRelInsts { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Countries { type AttributeEntity = CountriesRelInsts; }

impl Entity for InstitutionsCiteCount { type T = u32; const N: usize = 20967; const NAME: & str = "institutions-cite-count"; }

impl MappableEntity for InstitutionsCiteCount { type KeyType = usize; }

impl NamespacedEntity for InstitutionsCiteCount { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsCiteCount; }

impl Entity for InstitutionsCitSubfields { type T = [u32; 253]; const N: usize = 20967; const NAME: & str = "institutions-cit-subfields"; }

impl MappableEntity for InstitutionsCitSubfields { type KeyType = usize; }

impl NamespacedEntity for InstitutionsCitSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsCitSubfields; }

impl Entity for InstitutionsRefSubfields { type T = [u32; 253]; const N: usize = 20967; const NAME: & str = "institutions-ref-subfields"; }

impl MappableEntity for InstitutionsRefSubfields { type KeyType = usize; }

impl NamespacedEntity for InstitutionsRefSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsRefSubfields; }

impl Entity for InstitutionsPapersYearly { type T = [u32; 11]; const N: usize = 20967; const NAME: & str = "institutions-papers-yearly"; }

impl MappableEntity for InstitutionsPapersYearly { type KeyType = usize; }

impl NamespacedEntity for InstitutionsPapersYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsPapersYearly; }

impl Entity for InstitutionsCitationsYearly { type T = [u32; 11]; const N: usize = 20967; const NAME: & str = "institutions-citations-yearly"; }

impl MappableEntity for InstitutionsCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for InstitutionsCitationsYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsCitationsYearly; }

impl Entity for InstitutionsTopPaperSubfields { type T = [(u32, u8); 3]; const N: usize = 20967; const NAME: & str = "institutions-top-paper-subfields"; }

impl MappableEntity for InstitutionsTopPaperSubfields { type KeyType = usize; }

impl NamespacedEntity for InstitutionsTopPaperSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperSfMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsTopPaperSubfields; }

impl Entity for InstitutionsTopCitingSubfields { type T = [(u32, u8); 3]; const N: usize = 20967; const NAME: & str = "institutions-top-citing-subfields"; }

impl MappableEntity for InstitutionsTopCitingSubfields { type KeyType = usize; }

impl NamespacedEntity for InstitutionsTopCitingSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3CitingSfMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsTopCitingSubfields; }

impl Entity for InstitutionsTopPaperTopics { type T = [(u32, u16); 3]; const N: usize = 20967; const NAME: & str = "institutions-top-paper-topics"; }

impl MappableEntity for InstitutionsTopPaperTopics { type KeyType = usize; }

impl NamespacedEntity for InstitutionsTopPaperTopics { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperTopicMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsTopPaperTopics; }

impl Entity for InstitutionsTopPaperAuthors { type T = [(u32, u32); 5]; const N: usize = 20967; const NAME: & str = "institutions-top-paper-authors"; }

impl MappableEntity for InstitutionsTopPaperAuthors { type KeyType = usize; }

impl NamespacedEntity for InstitutionsTopPaperAuthors { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AuthorMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsTopPaperAuthors; }

impl Entity for InstitutionsTopJournals { type T = [(u32, u16); 3]; const N: usize = 20967; const NAME: & str = "institutions-top-journals"; }

impl MappableEntity for InstitutionsTopJournals { type KeyType = usize; }

impl NamespacedEntity for InstitutionsTopJournals { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3JournalMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsTopJournals; }

impl Entity for InstitutionsTopAffCountries { type T = [(u32, u8); 3]; const N: usize = 20967; const NAME: & str = "institutions-top-aff-countries"; }

impl MappableEntity for InstitutionsTopAffCountries { type KeyType = usize; }

impl NamespacedEntity for InstitutionsTopAffCountries { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AffCountryMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsTopAffCountries; }

impl Entity for InstitutionsRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize = 20967; const NAME: & str = "institutions-rel-insts"; }

impl MappableEntity for InstitutionsRelInsts { type KeyType = usize; }

impl NamespacedEntity for InstitutionsRelInsts { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Institutions { type AttributeEntity = InstitutionsRelInsts; }

impl Entity for SubfieldsCiteCount { type T = u32; const N: usize = 254; const NAME: & str = "subfields-cite-count"; }

impl MappableEntity for SubfieldsCiteCount { type KeyType = usize; }

impl NamespacedEntity for SubfieldsCiteCount { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsCiteCount; }

impl Entity for SubfieldsCitSubfields { type T = [u32; 253]; const N: usize = 254; const NAME: & str = "subfields-cit-subfields"; }

impl MappableEntity for SubfieldsCitSubfields { type KeyType = usize; }

impl NamespacedEntity for SubfieldsCitSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsCitSubfields; }

impl Entity for SubfieldsRefSubfields { type T = [u32; 253]; const N: usize = 254; const NAME: & str = "subfields-ref-subfields"; }

impl MappableEntity for SubfieldsRefSubfields { type KeyType = usize; }

impl NamespacedEntity for SubfieldsRefSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsRefSubfields; }

impl Entity for SubfieldsPapersYearly { type T = [u32; 11]; const N: usize = 254; const NAME: & str = "subfields-papers-yearly"; }

impl MappableEntity for SubfieldsPapersYearly { type KeyType = usize; }

impl NamespacedEntity for SubfieldsPapersYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsPapersYearly; }

impl Entity for SubfieldsCitationsYearly { type T = [u32; 11]; const N: usize = 254; const NAME: & str = "subfields-citations-yearly"; }

impl MappableEntity for SubfieldsCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for SubfieldsCitationsYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsCitationsYearly; }

impl Entity for SubfieldsTopPaperSubfields { type T = [(u32, u8); 3]; const N: usize = 254; const NAME: & str = "subfields-top-paper-subfields"; }

impl MappableEntity for SubfieldsTopPaperSubfields { type KeyType = usize; }

impl NamespacedEntity for SubfieldsTopPaperSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperSfMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsTopPaperSubfields; }

impl Entity for SubfieldsTopCitingSubfields { type T = [(u32, u8); 3]; const N: usize = 254; const NAME: & str = "subfields-top-citing-subfields"; }

impl MappableEntity for SubfieldsTopCitingSubfields { type KeyType = usize; }

impl NamespacedEntity for SubfieldsTopCitingSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3CitingSfMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsTopCitingSubfields; }

impl Entity for SubfieldsTopPaperTopics { type T = [(u32, u16); 3]; const N: usize = 254; const NAME: & str = "subfields-top-paper-topics"; }

impl MappableEntity for SubfieldsTopPaperTopics { type KeyType = usize; }

impl NamespacedEntity for SubfieldsTopPaperTopics { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperTopicMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsTopPaperTopics; }

impl Entity for SubfieldsTopPaperAuthors { type T = [(u32, u32); 5]; const N: usize = 254; const NAME: & str = "subfields-top-paper-authors"; }

impl MappableEntity for SubfieldsTopPaperAuthors { type KeyType = usize; }

impl NamespacedEntity for SubfieldsTopPaperAuthors { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AuthorMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsTopPaperAuthors; }

impl Entity for SubfieldsTopJournals { type T = [(u32, u16); 3]; const N: usize = 254; const NAME: & str = "subfields-top-journals"; }

impl MappableEntity for SubfieldsTopJournals { type KeyType = usize; }

impl NamespacedEntity for SubfieldsTopJournals { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3JournalMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsTopJournals; }

impl Entity for SubfieldsTopAffCountries { type T = [(u32, u8); 3]; const N: usize = 254; const NAME: & str = "subfields-top-aff-countries"; }

impl MappableEntity for SubfieldsTopAffCountries { type KeyType = usize; }

impl NamespacedEntity for SubfieldsTopAffCountries { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AffCountryMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsTopAffCountries; }

impl Entity for SubfieldsRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize = 254; const NAME: & str = "subfields-rel-insts"; }

impl MappableEntity for SubfieldsRelInsts { type KeyType = usize; }

impl NamespacedEntity for SubfieldsRelInsts { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Subfields { type AttributeEntity = SubfieldsRelInsts; }

impl Entity for TopicsCiteCount { type T = u32; const N: usize = 4518; const NAME: & str = "topics-cite-count"; }

impl MappableEntity for TopicsCiteCount { type KeyType = usize; }

impl NamespacedEntity for TopicsCiteCount { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsCiteCount; }

impl Entity for TopicsCitSubfields { type T = [u32; 253]; const N: usize = 4518; const NAME: & str = "topics-cit-subfields"; }

impl MappableEntity for TopicsCitSubfields { type KeyType = usize; }

impl NamespacedEntity for TopicsCitSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsCitSubfields; }

impl Entity for TopicsRefSubfields { type T = [u32; 253]; const N: usize = 4518; const NAME: & str = "topics-ref-subfields"; }

impl MappableEntity for TopicsRefSubfields { type KeyType = usize; }

impl NamespacedEntity for TopicsRefSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsRefSubfields; }

impl Entity for TopicsPapersYearly { type T = [u32; 11]; const N: usize = 4518; const NAME: & str = "topics-papers-yearly"; }

impl MappableEntity for TopicsPapersYearly { type KeyType = usize; }

impl NamespacedEntity for TopicsPapersYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsPapersYearly; }

impl Entity for TopicsCitationsYearly { type T = [u32; 11]; const N: usize = 4518; const NAME: & str = "topics-citations-yearly"; }

impl MappableEntity for TopicsCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for TopicsCitationsYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsCitationsYearly; }

impl Entity for TopicsTopPaperSubfields { type T = [(u32, u8); 3]; const N: usize = 4518; const NAME: & str = "topics-top-paper-subfields"; }

impl MappableEntity for TopicsTopPaperSubfields { type KeyType = usize; }

impl NamespacedEntity for TopicsTopPaperSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperSfMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsTopPaperSubfields; }

impl Entity for TopicsTopCitingSubfields { type T = [(u32, u8); 3]; const N: usize = 4518; const NAME: & str = "topics-top-citing-subfields"; }

impl MappableEntity for TopicsTopCitingSubfields { type KeyType = usize; }

impl NamespacedEntity for TopicsTopCitingSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3CitingSfMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsTopCitingSubfields; }

impl Entity for TopicsTopPaperTopics { type T = [(u32, u16); 3]; const N: usize = 4518; const NAME: & str = "topics-top-paper-topics"; }

impl MappableEntity for TopicsTopPaperTopics { type KeyType = usize; }

impl NamespacedEntity for TopicsTopPaperTopics { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperTopicMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsTopPaperTopics; }

impl Entity for TopicsTopPaperAuthors { type T = [(u32, u32); 5]; const N: usize = 4518; const NAME: & str = "topics-top-paper-authors"; }

impl MappableEntity for TopicsTopPaperAuthors { type KeyType = usize; }

impl NamespacedEntity for TopicsTopPaperAuthors { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AuthorMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsTopPaperAuthors; }

impl Entity for TopicsTopJournals { type T = [(u32, u16); 3]; const N: usize = 4518; const NAME: & str = "topics-top-journals"; }

impl MappableEntity for TopicsTopJournals { type KeyType = usize; }

impl NamespacedEntity for TopicsTopJournals { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3JournalMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsTopJournals; }

impl Entity for TopicsTopAffCountries { type T = [(u32, u8); 3]; const N: usize = 4518; const NAME: & str = "topics-top-aff-countries"; }

impl MappableEntity for TopicsTopAffCountries { type KeyType = usize; }

impl NamespacedEntity for TopicsTopAffCountries { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AffCountryMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsTopAffCountries; }

impl Entity for TopicsRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize = 4518; const NAME: & str = "topics-rel-insts"; }

impl MappableEntity for TopicsRelInsts { type KeyType = usize; }

impl NamespacedEntity for TopicsRelInsts { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Topics { type AttributeEntity = TopicsRelInsts; }

impl Entity for AuthorsCiteCount { type T = u32; const N: usize = 3882893; const NAME: & str = "authors-cite-count"; }

impl MappableEntity for AuthorsCiteCount { type KeyType = usize; }

impl NamespacedEntity for AuthorsCiteCount { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CiteCountMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsCiteCount; }

impl Entity for AuthorsCitSubfields { type T = [u16; 253]; const N: usize = 3882893; const NAME: & str = "authors-cit-subfields"; }

impl MappableEntity for AuthorsCitSubfields { type KeyType = usize; }

impl NamespacedEntity for AuthorsCitSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::CitSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsCitSubfields; }

impl Entity for AuthorsRefSubfields { type T = [u16; 253]; const N: usize = 3882893; const NAME: & str = "authors-ref-subfields"; }

impl MappableEntity for AuthorsRefSubfields { type KeyType = usize; }

impl NamespacedEntity for AuthorsRefSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::RefSubfieldsArrayMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsRefSubfields; }

impl Entity for AuthorsPapersYearly { type T = [u32; 11]; const N: usize = 3882893; const NAME: & str = "authors-papers-yearly"; }

impl MappableEntity for AuthorsPapersYearly { type KeyType = usize; }

impl NamespacedEntity for AuthorsPapersYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyPapersMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsPapersYearly; }

impl Entity for AuthorsCitationsYearly { type T = [u32; 11]; const N: usize = 3882893; const NAME: & str = "authors-citations-yearly"; }

impl MappableEntity for AuthorsCitationsYearly { type KeyType = usize; }

impl NamespacedEntity for AuthorsCitationsYearly { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::YearlyCitationsMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsCitationsYearly; }

impl Entity for AuthorsTopPaperSubfields { type T = [(u32, u8); 3]; const N: usize = 3882893; const NAME: & str = "authors-top-paper-subfields"; }

impl MappableEntity for AuthorsTopPaperSubfields { type KeyType = usize; }

impl NamespacedEntity for AuthorsTopPaperSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperSfMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsTopPaperSubfields; }

impl Entity for AuthorsTopCitingSubfields { type T = [(u32, u8); 3]; const N: usize = 3882893; const NAME: & str = "authors-top-citing-subfields"; }

impl MappableEntity for AuthorsTopCitingSubfields { type KeyType = usize; }

impl NamespacedEntity for AuthorsTopCitingSubfields { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3CitingSfMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsTopCitingSubfields; }

impl Entity for AuthorsTopPaperTopics { type T = [(u32, u16); 3]; const N: usize = 3882893; const NAME: & str = "authors-top-paper-topics"; }

impl MappableEntity for AuthorsTopPaperTopics { type KeyType = usize; }

impl NamespacedEntity for AuthorsTopPaperTopics { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3PaperTopicMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsTopPaperTopics; }

impl Entity for AuthorsTopPaperAuthors { type T = [(u32, u32); 5]; const N: usize = 3882893; const NAME: & str = "authors-top-paper-authors"; }

impl MappableEntity for AuthorsTopPaperAuthors { type KeyType = usize; }

impl NamespacedEntity for AuthorsTopPaperAuthors { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AuthorMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsTopPaperAuthors; }

impl Entity for AuthorsTopJournals { type T = [(u32, u16); 3]; const N: usize = 3882893; const NAME: & str = "authors-top-journals"; }

impl MappableEntity for AuthorsTopJournals { type KeyType = usize; }

impl NamespacedEntity for AuthorsTopJournals { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3JournalMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsTopJournals; }

impl Entity for AuthorsTopAffCountries { type T = [(u32, u8); 3]; const N: usize = 3882893; const NAME: & str = "authors-top-aff-countries"; }

impl MappableEntity for AuthorsTopAffCountries { type KeyType = usize; }

impl NamespacedEntity for AuthorsTopAffCountries { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::Top3AffCountryMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsTopAffCountries; }

impl Entity for AuthorsRelInsts { type T = [crate::steps::derive_links5::InstRelation; 8]; const N: usize = 3882893; const NAME: & str = "authors-rel-insts"; }

impl MappableEntity for AuthorsRelInsts { type KeyType = usize; }

impl NamespacedEntity for AuthorsRelInsts { const NS: & str = "derive_links5"; }

impl MarkedAttribute<crate::common::InstRelMarker> for crate::gen::a1_entity_mapping::Authors { type AttributeEntity = AuthorsRelInsts; }

impl Entity for SourcePairsByPath { type T = ([u16; 2], u32); const N: usize = 143921979; const NAME: & str = "source-pairs-by-path"; }

impl MappableEntity for SourcePairsByPath { type KeyType = usize; }

impl NamespacedEntity for SourcePairsByPath { const NS: & str = "derive_links5"; }

impl Entity for SubfieldPairsByPath { type T = ([u8; 2], u32); const N: usize = 31614; const NAME: & str = "subfield-pairs-by-path"; }

impl MappableEntity for SubfieldPairsByPath { type KeyType = usize; }

impl NamespacedEntity for SubfieldPairsByPath { const NS: & str = "derive_links5"; }