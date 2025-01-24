use dmove::{
    Entity, Link, MappableEntity, MarkedAttribute, NamespacedEntity, VariableSizeAttribute,
};

pub struct InstitutionsSemanticIds {}

pub struct WorkSources {}

pub struct CountriesSemanticIds {}

pub struct WorkReferences {}

pub struct AuthorsNames {}

pub struct TopicsNames {}

pub struct InstCountries {}

pub struct TopicSubfields {}

pub struct InstitutionsNames {}

pub struct SubfieldsSemanticIds {}

pub struct InstitutionsNameExts {}

pub struct WorkAuthorships {}

pub struct WorkTopics {}

pub struct SubfieldAncestors {}

pub struct CountriesNames {}

pub struct SourcesNameExts {}

pub struct AuthorsSemanticIds {}

pub struct SourcesSemanticIds {}

pub struct FieldsNames {}

pub struct CountriesNameExts {}

pub struct WorkYears {}

pub struct WorksNames {}

pub struct SubfieldsNameExts {}

pub struct AuthorsNameExts {}

pub struct AuthorshipInstitutions {}

pub struct SubfieldsNames {}

pub struct QsNames {}

pub struct SourceYearQs {}

pub struct SourcesNames {}

pub struct AuthorshipAuthor {}

pub struct SourceAreaFields {}

impl Entity for QsNames {
    type T = String;
    const N: usize = 5;
    const NAME: &str = "qs-names";
}

impl MappableEntity for QsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for QsNames {
    type SizeType = u8;
}

impl NamespacedEntity for QsNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Qs {
    type AttributeEntity = QsNames;
}

impl Entity for FieldsNames {
    type T = String;
    const N: usize = 28;
    const NAME: &str = "fields-names";
}

impl MappableEntity for FieldsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for FieldsNames {
    type SizeType = u8;
}

impl NamespacedEntity for FieldsNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Fields {
    type AttributeEntity = FieldsNames;
}

impl Entity for CountriesNames {
    type T = String;
    const N: usize = 230;
    const NAME: &str = "countries-names";
}

impl MappableEntity for CountriesNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for CountriesNames {
    type SizeType = u8;
}

impl NamespacedEntity for CountriesNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Countries {
    type AttributeEntity = CountriesNames;
}

impl Entity for SubfieldsNames {
    type T = String;
    const N: usize = 254;
    const NAME: &str = "subfields-names";
}

impl MappableEntity for SubfieldsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for SubfieldsNames {
    type SizeType = u8;
}

impl NamespacedEntity for SubfieldsNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Subfields {
    type AttributeEntity = SubfieldsNames;
}

impl Entity for InstitutionsNames {
    type T = String;
    const N: usize = 16371;
    const NAME: &str = "institutions-names";
}

impl MappableEntity for InstitutionsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for InstitutionsNames {
    type SizeType = u8;
}

impl NamespacedEntity for InstitutionsNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Institutions {
    type AttributeEntity = InstitutionsNames;
}

impl Entity for SourcesNames {
    type T = String;
    const N: usize = 26948;
    const NAME: &str = "sources-names";
}

impl MappableEntity for SourcesNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for SourcesNames {
    type SizeType = u16;
}

impl NamespacedEntity for SourcesNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Sources {
    type AttributeEntity = SourcesNames;
}

impl Entity for AuthorsNames {
    type T = String;
    const N: usize = 1918517;
    const NAME: &str = "authors-names";
}

impl MappableEntity for AuthorsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for AuthorsNames {
    type SizeType = u8;
}

impl NamespacedEntity for AuthorsNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Authors {
    type AttributeEntity = AuthorsNames;
}

impl Entity for TopicsNames {
    type T = String;
    const N: usize = 4518;
    const NAME: &str = "topics-names";
}

impl MappableEntity for TopicsNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for TopicsNames {
    type SizeType = u8;
}

impl NamespacedEntity for TopicsNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Topics {
    type AttributeEntity = TopicsNames;
}

impl Entity for WorksNames {
    type T = String;
    const N: usize = 42264875;
    const NAME: &str = "works-names";
}

impl MappableEntity for WorksNames {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorksNames {
    type SizeType = u16;
}

impl NamespacedEntity for WorksNames {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameMarker> for crate::gen::a1_entity_mapping::Works {
    type AttributeEntity = WorksNames;
}

impl Entity for InstitutionsNameExts {
    type T = String;
    const N: usize = 16371;
    const NAME: &str = "institutions-name-exts";
}

impl MappableEntity for InstitutionsNameExts {
    type KeyType = usize;
}

impl VariableSizeAttribute for InstitutionsNameExts {
    type SizeType = u8;
}

impl NamespacedEntity for InstitutionsNameExts {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameExtensionMarker>
    for crate::gen::a1_entity_mapping::Institutions
{
    type AttributeEntity = InstitutionsNameExts;
}

impl Entity for SourcesNameExts {
    type T = String;
    const N: usize = 26948;
    const NAME: &str = "sources-name-exts";
}

impl MappableEntity for SourcesNameExts {
    type KeyType = usize;
}

impl VariableSizeAttribute for SourcesNameExts {
    type SizeType = u16;
}

impl NamespacedEntity for SourcesNameExts {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameExtensionMarker>
    for crate::gen::a1_entity_mapping::Sources
{
    type AttributeEntity = SourcesNameExts;
}

impl Entity for AuthorsNameExts {
    type T = String;
    const N: usize = 1918516;
    const NAME: &str = "authors-name-exts";
}

impl MappableEntity for AuthorsNameExts {
    type KeyType = usize;
}

impl VariableSizeAttribute for AuthorsNameExts {
    type SizeType = u8;
}

impl NamespacedEntity for AuthorsNameExts {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameExtensionMarker>
    for crate::gen::a1_entity_mapping::Authors
{
    type AttributeEntity = AuthorsNameExts;
}

impl Entity for CountriesNameExts {
    type T = String;
    const N: usize = 229;
    const NAME: &str = "countries-name-exts";
}

impl MappableEntity for CountriesNameExts {
    type KeyType = usize;
}

impl VariableSizeAttribute for CountriesNameExts {
    type SizeType = u8;
}

impl NamespacedEntity for CountriesNameExts {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameExtensionMarker>
    for crate::gen::a1_entity_mapping::Countries
{
    type AttributeEntity = CountriesNameExts;
}

impl Entity for SubfieldsNameExts {
    type T = String;
    const N: usize = 253;
    const NAME: &str = "subfields-name-exts";
}

impl MappableEntity for SubfieldsNameExts {
    type KeyType = usize;
}

impl VariableSizeAttribute for SubfieldsNameExts {
    type SizeType = u8;
}

impl NamespacedEntity for SubfieldsNameExts {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::NameExtensionMarker>
    for crate::gen::a1_entity_mapping::Subfields
{
    type AttributeEntity = SubfieldsNameExts;
}

impl Entity for InstitutionsSemanticIds {
    type T = String;
    const N: usize = 16371;
    const NAME: &str = "institutions-semantic-ids";
}

impl MappableEntity for InstitutionsSemanticIds {
    type KeyType = usize;
}

impl VariableSizeAttribute for InstitutionsSemanticIds {
    type SizeType = u8;
}

impl NamespacedEntity for InstitutionsSemanticIds {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::SemanticIdMarker>
    for crate::gen::a1_entity_mapping::Institutions
{
    type AttributeEntity = InstitutionsSemanticIds;
}

impl Entity for SourcesSemanticIds {
    type T = String;
    const N: usize = 26948;
    const NAME: &str = "sources-semantic-ids";
}

impl MappableEntity for SourcesSemanticIds {
    type KeyType = usize;
}

impl VariableSizeAttribute for SourcesSemanticIds {
    type SizeType = u8;
}

impl NamespacedEntity for SourcesSemanticIds {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Sources {
    type AttributeEntity = SourcesSemanticIds;
}

impl Entity for AuthorsSemanticIds {
    type T = String;
    const N: usize = 1918517;
    const NAME: &str = "authors-semantic-ids";
}

impl MappableEntity for AuthorsSemanticIds {
    type KeyType = usize;
}

impl VariableSizeAttribute for AuthorsSemanticIds {
    type SizeType = u8;
}

impl NamespacedEntity for AuthorsSemanticIds {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Authors {
    type AttributeEntity = AuthorsSemanticIds;
}

impl Entity for CountriesSemanticIds {
    type T = String;
    const N: usize = 230;
    const NAME: &str = "countries-semantic-ids";
}

impl MappableEntity for CountriesSemanticIds {
    type KeyType = usize;
}

impl VariableSizeAttribute for CountriesSemanticIds {
    type SizeType = u8;
}

impl NamespacedEntity for CountriesSemanticIds {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Countries {
    type AttributeEntity = CountriesSemanticIds;
}

impl Entity for SubfieldsSemanticIds {
    type T = String;
    const N: usize = 254;
    const NAME: &str = "subfields-semantic-ids";
}

impl MappableEntity for SubfieldsSemanticIds {
    type KeyType = usize;
}

impl VariableSizeAttribute for SubfieldsSemanticIds {
    type SizeType = u8;
}

impl NamespacedEntity for SubfieldsSemanticIds {
    const NS: &str = "a2_init_atts";
}

impl MarkedAttribute<crate::common::SemanticIdMarker> for crate::gen::a1_entity_mapping::Subfields {
    type AttributeEntity = SubfieldsSemanticIds;
}

impl Entity for InstCountries {
    type T = u8;
    const N: usize = 16371;
    const NAME: &str = "inst-countries";
}

impl MappableEntity for InstCountries {
    type KeyType = usize;
}

impl NamespacedEntity for InstCountries {
    const NS: &str = "a2_init_atts";
}

impl Link for InstCountries {
    type Source = crate::gen::a1_entity_mapping::Institutions;
    type Target = crate::gen::a1_entity_mapping::Countries;
}

impl Entity for SubfieldAncestors {
    type T = u8;
    const N: usize = 254;
    const NAME: &str = "subfield-ancestors";
}

impl MappableEntity for SubfieldAncestors {
    type KeyType = usize;
}

impl NamespacedEntity for SubfieldAncestors {
    const NS: &str = "a2_init_atts";
}

impl Link for SubfieldAncestors {
    type Source = crate::gen::a1_entity_mapping::Subfields;
    type Target = crate::gen::a1_entity_mapping::Fields;
}

impl Entity for TopicSubfields {
    type T = u8;
    const N: usize = 4518;
    const NAME: &str = "topic-subfields";
}

impl MappableEntity for TopicSubfields {
    type KeyType = usize;
}

impl NamespacedEntity for TopicSubfields {
    const NS: &str = "a2_init_atts";
}

impl Link for TopicSubfields {
    type Source = crate::gen::a1_entity_mapping::Topics;
    type Target = crate::gen::a1_entity_mapping::Subfields;
}

impl Entity for WorkYears {
    type T = u8;
    const N: usize = 42264875;
    const NAME: &str = "work-years";
}

impl MappableEntity for WorkYears {
    type KeyType = usize;
}

impl NamespacedEntity for WorkYears {
    const NS: &str = "a2_init_atts";
}

impl Link for WorkYears {
    type Source = crate::gen::a1_entity_mapping::Works;
    type Target = crate::steps::a1_entity_mapping::Years;
}

impl Entity for SourceAreaFields {
    type T = Box<[u8]>;
    const N: usize = 26948;
    const NAME: &str = "source-area-fields";
}

impl MappableEntity for SourceAreaFields {
    type KeyType = usize;
}

impl VariableSizeAttribute for SourceAreaFields {
    type SizeType = u8;
}

impl NamespacedEntity for SourceAreaFields {
    const NS: &str = "a2_init_atts";
}

impl Link for SourceAreaFields {
    type Source = crate::gen::a1_entity_mapping::Sources;
    type Target = crate::gen::a1_entity_mapping::AreaFields;
}

impl Entity for WorkReferences {
    type T = Box<[u32]>;
    const N: usize = 42264875;
    const NAME: &str = "work-references";
}

impl MappableEntity for WorkReferences {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkReferences {
    type SizeType = u16;
}

impl NamespacedEntity for WorkReferences {
    const NS: &str = "a2_init_atts";
}

impl Link for WorkReferences {
    type Source = crate::gen::a1_entity_mapping::Works;
    type Target = crate::gen::a1_entity_mapping::Works;
}

impl Entity for WorkSources {
    type T = Box<[u16]>;
    const N: usize = 42264875;
    const NAME: &str = "work-sources";
}

impl MappableEntity for WorkSources {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkSources {
    type SizeType = u8;
}

impl NamespacedEntity for WorkSources {
    const NS: &str = "a2_init_atts";
}

impl Link for WorkSources {
    type Source = crate::gen::a1_entity_mapping::Works;
    type Target = crate::gen::a1_entity_mapping::Sources;
}

impl Entity for WorkTopics {
    type T = Box<[u16]>;
    const N: usize = 42264875;
    const NAME: &str = "work-topics";
}

impl MappableEntity for WorkTopics {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkTopics {
    type SizeType = u8;
}

impl NamespacedEntity for WorkTopics {
    const NS: &str = "a2_init_atts";
}

impl Link for WorkTopics {
    type Source = crate::gen::a1_entity_mapping::Works;
    type Target = crate::gen::a1_entity_mapping::Topics;
}

impl Entity for AuthorshipAuthor {
    type T = u32;
    const N: usize = 182179075;
    const NAME: &str = "authorship-author";
}

impl MappableEntity for AuthorshipAuthor {
    type KeyType = usize;
}

impl NamespacedEntity for AuthorshipAuthor {
    const NS: &str = "a2_init_atts";
}

impl Entity for AuthorshipInstitutions {
    type T = Box<[u16]>;
    const N: usize = 182179075;
    const NAME: &str = "authorship-institutions";
}

impl MappableEntity for AuthorshipInstitutions {
    type KeyType = usize;
}

impl VariableSizeAttribute for AuthorshipInstitutions {
    type SizeType = u8;
}

impl NamespacedEntity for AuthorshipInstitutions {
    const NS: &str = "a2_init_atts";
}

impl Entity for WorkAuthorships {
    type T = Box<[u32]>;
    const N: usize = 42264875;
    const NAME: &str = "work-authorships";
}

impl MappableEntity for WorkAuthorships {
    type KeyType = usize;
}

impl VariableSizeAttribute for WorkAuthorships {
    type SizeType = u8;
}

impl NamespacedEntity for WorkAuthorships {
    const NS: &str = "a2_init_atts";
}

impl Link for AuthorshipAuthor {
    type Source = crate::gen::a1_entity_mapping::Authorships;
    type Target = crate::gen::a1_entity_mapping::Authors;
}

impl Link for AuthorshipInstitutions {
    type Source = crate::gen::a1_entity_mapping::Authorships;
    type Target = crate::gen::a1_entity_mapping::Institutions;
}

impl Link for WorkAuthorships {
    type Source = crate::gen::a1_entity_mapping::Works;
    type Target = crate::gen::a1_entity_mapping::Authorships;
}

impl Entity for SourceYearQs {
    type T = u8;
    const N: usize = 1422825;
    const NAME: &str = "source-year-qs";
}

impl MappableEntity for SourceYearQs {
    type KeyType = (u16, u8);
}

impl NamespacedEntity for SourceYearQs {
    const NS: &str = "a2_init_atts";
}

