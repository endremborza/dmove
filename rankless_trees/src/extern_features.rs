use dmove::{Entity, MappableEntity, MarkedAttribute, NamespacedEntity};
use rankless_rs::gen::a1_entity_mapping::{
    Authors, Countries, Institutions, Sources, Subfields, Topics,
};

pub struct CitSubfieldsConcentrationMarker;
pub struct RefSubfieldsConcentrationMarker;

pub struct InstitutionsCitSubfieldConc;

impl Entity for InstitutionsCitSubfieldConc {
    type T = f64;
    const N: usize = 19832;
    const NAME: &str = "institutions-cit-sf-conc";
}

impl MappableEntity for InstitutionsCitSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for InstitutionsCitSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<CitSubfieldsConcentrationMarker> for Institutions {
    type AttributeEntity = InstitutionsCitSubfieldConc;
}

pub struct InstitutionsRefSubfieldConc;

impl Entity for InstitutionsRefSubfieldConc {
    type T = f64;
    const N: usize = 19832;
    const NAME: &str = "institutions-ref-sf-conc";
}

impl MappableEntity for InstitutionsRefSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for InstitutionsRefSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<RefSubfieldsConcentrationMarker> for Institutions {
    type AttributeEntity = InstitutionsRefSubfieldConc;
}

pub struct AuthorsCitSubfieldConc;

impl Entity for AuthorsCitSubfieldConc {
    type T = f64;
    const N: usize = 4070681;
    const NAME: &str = "authors-cit-sf-conc";
}

impl MappableEntity for AuthorsCitSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for AuthorsCitSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<CitSubfieldsConcentrationMarker> for Authors {
    type AttributeEntity = AuthorsCitSubfieldConc;
}

pub struct AuthorsRefSubfieldConc;

impl Entity for AuthorsRefSubfieldConc {
    type T = f64;
    const N: usize = 4070681;
    const NAME: &str = "authors-ref-sf-conc";
}

impl MappableEntity for AuthorsRefSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for AuthorsRefSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<RefSubfieldsConcentrationMarker> for Authors {
    type AttributeEntity = AuthorsRefSubfieldConc;
}

pub struct CountriesCitSubfieldConc;

impl Entity for CountriesCitSubfieldConc {
    type T = f64;
    const N: usize = 229;
    const NAME: &str = "countries-cit-sf-conc";
}

impl MappableEntity for CountriesCitSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for CountriesCitSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<CitSubfieldsConcentrationMarker> for Countries {
    type AttributeEntity = CountriesCitSubfieldConc;
}

pub struct CountriesRefSubfieldConc;

impl Entity for CountriesRefSubfieldConc {
    type T = f64;
    const N: usize = 229;
    const NAME: &str = "countries-ref-sf-conc";
}

impl MappableEntity for CountriesRefSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for CountriesRefSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<RefSubfieldsConcentrationMarker> for Countries {
    type AttributeEntity = CountriesRefSubfieldConc;
}

pub struct SubfieldsCitSubfieldConc;

impl Entity for SubfieldsCitSubfieldConc {
    type T = f64;
    const N: usize = 253;
    const NAME: &str = "subfields-cit-sf-conc";
}

impl MappableEntity for SubfieldsCitSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for SubfieldsCitSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<CitSubfieldsConcentrationMarker> for Subfields {
    type AttributeEntity = SubfieldsCitSubfieldConc;
}

pub struct SubfieldsRefSubfieldConc;

impl Entity for SubfieldsRefSubfieldConc {
    type T = f64;
    const N: usize = 253;
    const NAME: &str = "subfields-ref-sf-conc";
}

impl MappableEntity for SubfieldsRefSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for SubfieldsRefSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<RefSubfieldsConcentrationMarker> for Subfields {
    type AttributeEntity = SubfieldsRefSubfieldConc;
}

pub struct TopicsCitSubfieldConc;

impl Entity for TopicsCitSubfieldConc {
    type T = f64;
    const N: usize = 4517;
    const NAME: &str = "topics-cit-sf-conc";
}

impl MappableEntity for TopicsCitSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for TopicsCitSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<CitSubfieldsConcentrationMarker> for Topics {
    type AttributeEntity = TopicsCitSubfieldConc;
}

pub struct TopicsRefSubfieldConc;

impl Entity for TopicsRefSubfieldConc {
    type T = f64;
    const N: usize = 4517;
    const NAME: &str = "topics-ref-sf-conc";
}

impl MappableEntity for TopicsRefSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for TopicsRefSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<RefSubfieldsConcentrationMarker> for Topics {
    type AttributeEntity = TopicsRefSubfieldConc;
}

pub struct SourcesCitSubfieldConc;

impl Entity for SourcesCitSubfieldConc {
    type T = f64;
    const N: usize = 37186;
    const NAME: &str = "sources-cit-sf-conc";
}

impl MappableEntity for SourcesCitSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for SourcesCitSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<CitSubfieldsConcentrationMarker> for Sources {
    type AttributeEntity = SourcesCitSubfieldConc;
}

pub struct SourcesRefSubfieldConc;

impl Entity for SourcesRefSubfieldConc {
    type T = f64;
    const N: usize = 37186;
    const NAME: &str = "sources-ref-sf-conc";
}

impl MappableEntity for SourcesRefSubfieldConc {
    type KeyType = usize;
}

impl NamespacedEntity for SourcesRefSubfieldConc {
    const NS: &str = "extern";
}

impl MarkedAttribute<RefSubfieldsConcentrationMarker> for Sources {
    type AttributeEntity = SourcesRefSubfieldConc;
}
