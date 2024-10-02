use dmove::{
    Entity, FixedSizeAttribute, Link, MappableEntity, NamespacedEntity, VariableSizeAttribute,
};

pub struct CountriesNames {}

pub struct SubfieldAncestors {}

pub struct WorkSources {}

pub struct WorkReferences {}

pub struct AuthorsNames {}

pub struct InstCountries {}

pub struct TopicSubfields {}

pub struct FieldsNames {}

pub struct InstitutionsNames {}

pub struct WorkYears {}

pub struct WorkAuthorships {}

pub struct AuthorshipInstitutions {}

pub struct SubfieldsNames {}

pub struct QsNames {}

pub struct SourceYearQs {}

pub struct SourcesNames {}

pub struct AuthorshipAuthor {}

pub struct WorkTopics {}

pub struct SourceAreaFields {}

impl Entity for QsNames {
    type T = String;
    const N: usize = 5;
    const NAME: &'static str = "qs-names";
}

impl MappableEntity<Self> for QsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for QsNames {
    type SizeType = u8;
}

impl NamespacedEntity for QsNames {
    const NS: &str = "init-links";
}

impl Entity for FieldsNames {
    type T = String;
    const N: usize = 28;
    const NAME: &'static str = "fields-names";
}

impl MappableEntity<Self> for FieldsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for FieldsNames {
    type SizeType = u8;
}

impl NamespacedEntity for FieldsNames {
    const NS: &str = "init-links";
}

impl Entity for CountriesNames {
    type T = String;
    const N: usize = 92;
    const NAME: &'static str = "countries-names";
}

impl MappableEntity<Self> for CountriesNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for CountriesNames {
    type SizeType = u8;
}

impl NamespacedEntity for CountriesNames {
    const NS: &str = "init-links";
}

impl Entity for SubfieldsNames {
    type T = String;
    const N: usize = 254;
    const NAME: &'static str = "subfields-names";
}

impl MappableEntity<Self> for SubfieldsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for SubfieldsNames {
    type SizeType = u8;
}

impl NamespacedEntity for SubfieldsNames {
    const NS: &str = "init-links";
}

impl Entity for InstitutionsNames {
    type T = String;
    const N: usize = 306;
    const NAME: &'static str = "institutions-names";
}

impl MappableEntity<Self> for InstitutionsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for InstitutionsNames {
    type SizeType = u8;
}

impl NamespacedEntity for InstitutionsNames {
    const NS: &str = "init-links";
}

impl Entity for SourcesNames {
    type T = String;
    const N: usize = 178;
    const NAME: &'static str = "sources-names";
}

impl MappableEntity<Self> for SourcesNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for SourcesNames {
    type SizeType = u8;
}

impl NamespacedEntity for SourcesNames {
    const NS: &str = "init-links";
}

impl Entity for AuthorsNames {
    type T = String;
    const N: usize = 254;
    const NAME: &'static str = "authors-names";
}

impl MappableEntity<Self> for AuthorsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for AuthorsNames {
    type SizeType = u8;
}

impl NamespacedEntity for AuthorsNames {
    const NS: &str = "init-links";
}

impl Entity for InstCountries {
    type T = u8;
    const N: usize = 306;
    const NAME: &'static str = "inst-countries";
}

impl MappableEntity<Self> for InstCountries {
    type KeyType = usize;
}

impl FixedSizeAttribute for InstCountries {}

impl NamespacedEntity for InstCountries {
    const NS: &str = "init-links";
}

impl Link for InstCountries {
    type Source = crate::gen_entity_mapping::Institutions;
    type Target = crate::gen_entity_mapping::Countries;
}

impl Entity for SubfieldAncestors {
    type T = u8;
    const N: usize = 254;
    const NAME: &'static str = "subfield-ancestors";
}

impl MappableEntity<Self> for SubfieldAncestors {
    type KeyType = usize;
}

impl FixedSizeAttribute for SubfieldAncestors {}

impl NamespacedEntity for SubfieldAncestors {
    const NS: &str = "init-links";
}

impl Link for SubfieldAncestors {
    type Source = crate::gen_entity_mapping::Subfields;
    type Target = crate::gen_entity_mapping::Fields;
}

impl Entity for TopicSubfields {
    type T = u8;
    const N: usize = 4518;
    const NAME: &'static str = "topic-subfields";
}

impl MappableEntity<Self> for TopicSubfields {
    type KeyType = usize;
}

impl FixedSizeAttribute for TopicSubfields {}

impl NamespacedEntity for TopicSubfields {
    const NS: &str = "init-links";
}

impl Link for TopicSubfields {
    type Source = crate::gen_entity_mapping::Topics;
    type Target = crate::gen_entity_mapping::Subfields;
}

impl Entity for WorkYears {
    type T = u8;
    const N: usize = 10816;
    const NAME: &'static str = "work-years";
}

impl MappableEntity<Self> for WorkYears {
    type KeyType = usize;
}

impl FixedSizeAttribute for WorkYears {}

impl NamespacedEntity for WorkYears {
    const NS: &str = "init-links";
}

impl Link for WorkYears {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::entity_mapping::Years;
}

impl Entity for SourceAreaFields {
    type T = Box<[u8]>;
    const N: usize = 178;
    const NAME: &'static str = "source-area-fields";
}

impl MappableEntity<Self> for SourceAreaFields {
    type KeyType = usize;
}

impl VariableSizeAttribute for SourceAreaFields {
    type SizeType = u8;
}

impl NamespacedEntity for SourceAreaFields {
    const NS: &str = "init-links";
}

impl Link for SourceAreaFields {
    type Source = crate::gen_entity_mapping::Sources;
    type Target = crate::gen_entity_mapping::AreaFields;
}

impl Entity for WorkReferences {
    type T = Box<[u16]>;
    const N: usize = 10816;
    const NAME: &'static str = "work-references";
}

impl MappableEntity<Self> for WorkReferences {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkReferences {
    type SizeType = u8;
}

impl NamespacedEntity for WorkReferences {
    const NS: &str = "init-links";
}

impl Link for WorkReferences {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Works;
}

impl Entity for WorkSources {
    type T = Box<[u8]>;
    const N: usize = 10816;
    const NAME: &'static str = "work-sources";
}

impl MappableEntity<Self> for WorkSources {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkSources {
    type SizeType = u8;
}

impl NamespacedEntity for WorkSources {
    const NS: &str = "init-links";
}

impl Link for WorkSources {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Sources;
}

impl Entity for WorkTopics {
    type T = Box<[u16]>;
    const N: usize = 10816;
    const NAME: &'static str = "work-topics";
}

impl MappableEntity<Self> for WorkTopics {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkTopics {
    type SizeType = u8;
}

impl NamespacedEntity for WorkTopics {
    const NS: &str = "init-links";
}

impl Link for WorkTopics {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Topics;
}

impl Entity for AuthorshipAuthor {
    type T = u8;
    const N: usize = 73953;
    const NAME: &'static str = "authorship-author";
}

impl MappableEntity<Self> for AuthorshipAuthor {
    type KeyType = usize;
}

impl FixedSizeAttribute for AuthorshipAuthor {}

impl NamespacedEntity for AuthorshipAuthor {
    const NS: &str = "init-links";
}

impl Entity for AuthorshipInstitutions {
    type T = Box<[u16]>;
    const N: usize = 73953;
    const NAME: &'static str = "authorship-institutions";
}

impl MappableEntity<Self> for AuthorshipInstitutions {
    type KeyType = usize;
}

impl VariableSizeAttribute for AuthorshipInstitutions {
    type SizeType = u8;
}

impl NamespacedEntity for AuthorshipInstitutions {
    const NS: &str = "init-links";
}

impl Entity for WorkAuthorships {
    type T = Box<[u32]>;
    const N: usize = 10816;
    const NAME: &'static str = "work-authorships";
}

impl MappableEntity<Self> for WorkAuthorships {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkAuthorships {
    type SizeType = u8;
}

impl NamespacedEntity for WorkAuthorships {
    const NS: &str = "init-links";
}

impl Link for AuthorshipAuthor {
    type Source = crate::gen_entity_mapping::Authorships;
    type Target = crate::gen_entity_mapping::Authors;
}

impl Link for AuthorshipInstitutions {
    type Source = crate::gen_entity_mapping::Authorships;
    type Target = crate::gen_entity_mapping::Institutions;
}

impl Link for WorkAuthorships {
    type Source = crate::gen_entity_mapping::Works;
    type Target = crate::gen_entity_mapping::Authorships;
}

impl Entity for SourceYearQs {
    type T = u8;
    const N: usize = 1;
    const NAME: &'static str = "source-year-qs";
}

impl MappableEntity<Self> for SourceYearQs {
    type KeyType = usize;
}

impl MappableEntity<(u8, u8)> for SourceYearQs {
    type KeyType = (u8, u8);
}

impl NamespacedEntity for SourceYearQs {
    const NS: &str = "init-links";
}

