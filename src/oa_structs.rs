use serde::{Deserialize, Deserializer, Serialize};
use serde_json::Value;

use crate::common::IdStruct;

macro_rules! add_id_traits {
    () => {};
    ($struct:ident $(, $rest:ident)*) => {
        impl IdTrait for $struct {
            fn get_id(&self) -> String {
                self.id.clone()
            }
        }
        add_id_traits!($($rest),*);
    };
}

#[derive(Deserialize, Debug)]
pub struct IdCountDecorated<T>
where
    T: IdTrait,
{
    #[serde(flatten)]
    pub child: T,
    pub ids: Option<IdSet>,
    pub counts_by_year: Option<Vec<CountByYear>>,
}

pub trait IdTrait {
    fn get_id(&self) -> String;
}

add_id_traits!(Author, Concept, Institution, Publisher, Source, Work);

impl<T> IdTrait for IdCountDecorated<T>
where
    T: IdTrait,
{
    fn get_id(&self) -> String {
        self.child.get_id()
    }
}

// STRUCTS

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
    country_code: Option<String>,
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
    country_code: Option<String>,
    country: Option<String>,
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
}

#[derive(Deserialize, Serialize, Debug)]
pub struct Work {
    id: String,
    doi: Option<String>,
    title: Option<String>,
    display_name: Option<String>,
    publication_year: Option<u16>,
    publication_date: Option<String>,
    #[serde(rename = "type")]
    work_type: Option<String>,
    cited_by_count: Option<u64>,
    is_retracted: Option<bool>,
    is_paratext: Option<bool>,
    #[serde(deserialize_with = "deserialize_list_of_strings", skip_serializing)]
    pub related_works: Option<Vec<RelatedWork>>,
    #[serde(deserialize_with = "deserialize_list_of_strings", skip_serializing)]
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
    author_id: Option<String>,
    #[serde(deserialize_with = "deserialize_hash_fields")]
    pub institutions: Option<String>,
    author_position: Option<String>,
    raw_affiliation_string: Option<String>,
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
        //TODO ugly quick dirty
        let ids: Vec<String> = h_maps.iter().map(|e| e.id.clone().unwrap()).collect();
        return Ok(Some(ids.join(";")));
    }
    return Ok(None);
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
        let json_string = serde_json::to_string(&json_array).expect("gotem");
        return Ok(Some(json_string));
    }
    return Ok(None);
}
