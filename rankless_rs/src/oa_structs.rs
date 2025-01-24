use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::{
    add_parent_parsed_id_traits, add_parsed_id_traits, add_strict_parsed_id_traits,
    common::{field_id_parse, oa_id_parse, short_string_to_u64, ParsedId},
};

use dmove::BigId;

macro_rules! add_id_traits {
    ($($struct:ident),*) => {
        $(impl IdTrait for $struct {
            fn get_id(&self) -> String {
                self.id.clone()
            }
        })*
    };
}

#[derive(Deserialize, Debug)]
pub struct IdCountDecorated<T: IdTrait> {
    #[serde(flatten)]
    pub child: T,
    pub ids: Option<IdSet>,
    pub counts_by_year: Option<Vec<CountByYear>>,
}

pub trait IdTrait {
    fn get_id(&self) -> String;
}

pub trait Named {
    fn get_name(&self) -> String;
}

add_id_traits!(
    Author,
    Concept,
    Institution,
    Publisher,
    Source,
    Work,
    Topic,
    FieldLike,
    SubField
);

add_strict_parsed_id_traits!(Institution, Work, NamedEntity);
add_parsed_id_traits!(IdStruct);
add_parent_parsed_id_traits!(ReferencedWork, WorkTopic);

impl<T: IdTrait> IdTrait for IdCountDecorated<T> {
    fn get_id(&self) -> String {
        self.child.get_id()
    }
}

#[derive(Deserialize)]
pub struct IdStruct {
    pub id: Option<String>,
}

#[derive(Deserialize)]
pub struct NamedEntity {
    id: String,
    pub display_name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct IdSet {
    pub parent_id: Option<String>,
    openalex: Option<String>,
    ror: Option<String>,
    grid: Option<String>,
    orcid: Option<String>,
    scopus: Option<String>,
    twitter: Option<String>,
    wikipedia: Option<String>,
    wikidata: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    umls_aui: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    umls_cui: Option<String>,
    mag: Option<i64>,
    issn_l: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    issn: Option<String>,
    fatcat: Option<String>,
    doi: Option<String>,
    pmid: Option<String>,
    pmcid: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct CountByYear {
    pub parent_id: Option<String>,
    year: u16,
    works_count: Option<u32>,
    cited_by_count: u32,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Author {
    id: String,
    orcid: Option<String>,
    display_name: Option<String>,
    // display_name_alternatives: Option<String>,
    works_count: Option<u32>,
    cited_by_count: Option<u32>,
    #[serde(default, deserialize_with = "deserialize_hash_field")]
    last_known_institution: Option<String>,
    updated_date: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Concept {
    id: String,
    wikidata: Option<String>,
    display_name: Option<String>,
    level: Option<u8>,
    description: Option<String>,
    works_count: Option<u32>,
    cited_by_count: Option<u64>,
    image_url: Option<String>,
    image_thumbnail_url: Option<String>,
    updated_date: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Topic {
    pub id: String,
    pub display_name: String,
    #[serde(default, deserialize_with = "deserialize_strict_hash_field")]
    pub subfield: String,
    #[serde(default, deserialize_with = "deserialize_strict_hash_field")]
    pub field: String,
    #[serde(default, deserialize_with = "deserialize_strict_hash_field")]
    domain: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct FieldLike {
    pub id: String,
    pub display_name: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SubField {
    pub id: String,
    pub display_name: String,
    #[serde(default, deserialize_with = "deserialize_strict_hash_field")]
    pub field: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Ancestor {
    #[serde(rename = "concept_id")]
    pub parent_id: Option<String>,
    #[serde(rename = "id")]
    pub ancestor_id: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RelatedConcept {
    #[serde(rename = "concept_id")]
    pub parent_id: Option<String>,
    #[serde(rename = "id")]
    related_concept_id: String,
    score: f32,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Institution {
    id: String,
    ror: Option<String>,
    display_name: Option<String>,
    pub country_code: Option<String>,
    #[serde(rename = "type")]
    inst_type: Option<String>,
    homepage_url: Option<String>,
    image_url: Option<String>,
    image_thumbnail_url: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    display_name_acronyms: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    display_name_alternatives: Option<String>,
    works_count: Option<u32>,
    cited_by_count: Option<u64>,
    updated_date: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Geo {
    pub parent_id: Option<String>,
    city: Option<String>,
    geonames_city_id: Option<String>,
    region: Option<String>,
    pub country_code: Option<String>,
    pub country: Option<String>,
    latitude: Option<f64>,
    longitude: Option<f64>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct AssociatedInstitution {
    pub parent_id: Option<String>,
    #[serde(rename = "id")]
    associated_institution_id: Option<String>,
    relationship: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Publisher {
    id: String,
    display_name: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    alternate_titles: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    country_codes: Option<String>,
    hierarchy_level: Option<u8>,
    #[serde(default, deserialize_with = "deserialize_hash_field")]
    parent_publisher: Option<String>,
    works_count: Option<u32>,
    cited_by_count: Option<u64>,
    updated_date: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Source {
    id: String,
    issn_l: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    issn: Option<String>,
    display_name: Option<String>,
    publisher: Option<String>,
    works_count: Option<u32>,
    cited_by_count: Option<u64>,
    is_oa: Option<bool>,
    is_in_doaj: Option<bool>,
    homepage_url: Option<String>,
    updated_date: Option<String>,
    #[serde(default, deserialize_with = "deserialize_json_array")]
    alternate_titles: Option<String>,
    abbreviated_title: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Work {
    id: String,
    doi: Option<String>,
    title: Option<String>,
    display_name: Option<String>,
    pub publication_year: Option<u16>,
    publication_date: Option<String>,
    #[serde(rename = "type")]
    pub work_type: Option<String>,
    cited_by_count: Option<u64>,
    pub is_retracted: Option<bool>,
    is_paratext: Option<bool>,
    // #[serde(deserialize_with = "deserialize_list_of_strings", skip_serializing)]
    // pub related_works: Option<Vec<RelatedWork>>,
    #[serde(
        deserialize_with = "deserialize_list_of_strings",
        skip_serializing,
        default = "default_empty"
    )]
    pub referenced_works: Option<Vec<ReferencedWork>>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Location {
    pub parent_id: Option<String>,
    #[serde(deserialize_with = "deserialize_hash_field", rename = "source")]
    pub source_id: Option<String>,
    landing_page_url: Option<String>,
    pdf_url: Option<String>,
    is_oa: Option<bool>,
    version: Option<String>,
    license: Option<String>,
    tag: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Authorship {
    pub parent_id: Option<String>,
    #[serde(deserialize_with = "deserialize_hash_field", rename = "author")]
    pub author_id: Option<String>,
    #[serde(deserialize_with = "deserialize_hash_fields")]
    pub institutions: Option<String>,
    author_position: Option<String>,
    raw_affiliation_string: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct SummaryStats {
    pub parent_id: Option<String>,
    pub h_index: u32,
    pub i10_index: u32,
    pub works_count: u32,
    pub cited_by_count: u64,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Biblio {
    pub parent_id: Option<String>,
    volume: Option<String>,
    issue: Option<String>,
    first_page: Option<String>,
    last_page: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorkConcept {
    pub parent_id: Option<String>,
    #[serde(rename = "id")]
    pub concept_id: Option<String>,
    pub score: Option<f64>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct WorkTopic {
    pub parent_id: Option<String>,
    #[serde(rename = "id")]
    pub topic_id: Option<String>,
    pub score: Option<f64>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Mesh {
    pub parent_id: Option<String>,
    descriptor_ui: Option<String>,
    descriptor_name: Option<String>,
    qualifier_ui: Option<String>,
    qualifier_name: Option<String>,
    is_major_topic: Option<bool>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct OpenAccess {
    pub parent_id: Option<String>,
    is_oa: Option<bool>,
    oa_status: Option<String>,
    oa_url: Option<String>,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct ReferencedWork {
    pub parent_id: Option<String>,
    pub referenced_work_id: String,
}

#[derive(Deserialize, Serialize, Debug)]
pub struct RelatedWork {
    pub parent_id: Option<String>,
    related_work_id: String,
}

impl ParsedId for FieldLike {
    fn get_parsed_id(&self) -> BigId {
        field_id_parse(&self.id)
    }
}

impl ParsedId for Geo {
    fn get_parsed_id(&self) -> BigId {
        short_string_to_u64(&self.country_code.clone().unwrap_or("".to_string()))
    }
}

impl Named for NamedEntity {
    fn get_name(&self) -> String {
        self.display_name.clone()
    }
}

impl Named for FieldLike {
    fn get_name(&self) -> String {
        self.display_name.clone()
    }
}

impl Named for Geo {
    fn get_name(&self) -> String {
        self.country.clone().unwrap_or("".to_string())
    }
}

impl From<String> for ReferencedWork {
    fn from(value: String) -> Self {
        Self {
            referenced_work_id: value,
            parent_id: None,
        }
    }
}

impl From<String> for RelatedWork {
    fn from(value: String) -> Self {
        Self {
            related_work_id: value,
            parent_id: None,
        }
    }
}

fn default_empty<T>() -> Option<Vec<T>> {
    Some(Vec::new())
}

fn deserialize_list_of_strings<'de, T, D>(deserializer: D) -> Result<Option<Vec<T>>, D::Error>
where
    D: Deserializer<'de>,
    T: From<String>,
{
    let strings: Vec<String> = Vec::deserialize(deserializer)?;

    let result: Vec<T> = strings.into_iter().map(T::from).collect();

    Ok(Some(result))
}

fn deserialize_hash_fields<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let h_maps_o = Option::<Vec<IdStruct>>::deserialize(deserializer)?;
    if let Some(h_maps) = h_maps_o {
        let ids: Vec<String> = h_maps.iter().map(|e| e.id.clone().unwrap()).collect();
        return Ok(Some(ids.join(";")));
    }
    return Ok(None);
}

fn deserialize_strict_hash_field<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let hfid = deserialize_hash_field(deserializer)?;
    Ok(hfid.unwrap())
}

fn deserialize_hash_field<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: Deserializer<'de>,
{
    let h_map_o = Option::<IdStruct>::deserialize(deserializer)?;
    if let Some(h_map) = h_map_o {
        return Ok(h_map.id);
    }
    return Ok(None);
}

fn deserialize_json_array<'de, D>(deserializer: D) -> Result<Option<String>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let json_array_opt: Option<Vec<Value>> = Option::deserialize(deserializer)?;
    if let Some(json_array) = json_array_opt {
        let json_string = serde_json::to_string(&json_array).unwrap();
        return Ok(Some(json_string));
    }
    return Ok(None);
}

pub mod post {
    use crate::{add_parent_parsed_id_traits, add_strict_parsed_id_traits};

    use super::{oa_id_parse, BigId, Deserialize, IdTrait, ParsedId};
    use crate::common::field_id_parse;

    #[derive(Deserialize, Debug)]
    pub struct Authorship {
        pub parent_id: Option<String>,
        #[serde(rename = "author")]
        pub author_id: Option<String>,
        pub institutions: Option<String>,
    }

    #[derive(Deserialize, Debug)]
    pub struct Location {
        pub parent_id: Option<String>,
        #[serde(rename = "source")]
        pub source_id: Option<String>,
    }

    #[derive(Deserialize, Debug)]
    pub struct Author {
        id: String,
        // orcid: Option<String>,
        // display_name: Option<String>,
        pub works_count: Option<u32>,
        pub cited_by_count: Option<u32>,
    }

    #[derive(Deserialize, Debug)]
    pub struct Topic {
        pub id: String,
        // pub display_name: String,
        pub subfield: String,
        // pub field: String,
        // pub domain: String,
    }

    #[derive(Deserialize, Debug)]
    pub struct SubField {
        pub id: String,
        // pub display_name: String,
        pub field: String,
    }

    #[derive(Deserialize, Debug)]
    pub struct Institution {
        id: String,
        pub country_code: Option<String>,
        pub display_name_acronyms: Option<String>,
    }

    #[derive(Deserialize, Debug)]
    pub struct Source {
        id: String,
        pub alternate_titles: Option<String>,
        // abbreviated_title: Option<String>,
    }

    add_id_traits!(Author, Institution, Source);
    add_strict_parsed_id_traits!(Author, Topic, Institution, Source);

    impl ParsedId for SubField {
        fn get_parsed_id(&self) -> BigId {
            field_id_parse(&self.id)
        }
    }

    add_parent_parsed_id_traits!(Location, Authorship);
}
