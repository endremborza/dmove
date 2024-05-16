use std::io;

use serde::{de::DeserializeOwned, Deserialize};
use std::collections::HashSet;

use crate::common::{
    field_id_parse, oa_id_parse, BigId, ParsedId, Stowage, CONCEPTS, COUNTRIES, FIELDS, INSTS, QS,
    SOURCES, SUB_FIELDS, TOPICS, WORKS,
};
use crate::ingest_entity::get_idmap;
use crate::oa_filters::get_last_filter;
use crate::{add_parsed_id_traits, add_strict_parsed_id_traits};

#[derive(Deserialize, Debug)]
struct SIdObj {
    id: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct SInstitution {
    id: Option<String>,
    pub country_code: Option<String>,
    #[serde(rename = "type")]
    pub ints_type: Option<String>,
    pub display_name: String,
}

#[derive(Deserialize, Debug)]
pub struct STopic {
    pub id: String,
    pub field: String,
    pub subfield: String,
}

add_parsed_id_traits!(SIdObj, SInstitution);
add_strict_parsed_id_traits!(STopic);

pub fn make_ids(stowage: &Stowage) -> io::Result<()> {
    entities_from_iter(stowage, QS, 1..6, None)?;

    for sw in vec![FIELDS, SUB_FIELDS] {
        ids_from_atts::<SIdObj, _>(stowage, sw, sw, |e| field_id_parse(&e.id.unwrap()))?;
    }

    for en in vec![WORKS, INSTS, SOURCES, CONCEPTS, TOPICS] {
        ids_from_atts::<SIdObj, _>(stowage, en, en, |e| e.get_parsed_id())?;
    }

    ids_from_atts::<SInstitution, _>(stowage, COUNTRIES, INSTS, |e| {
        short_string_to_u64(&e.country_code.unwrap_or("".to_string()))
    })?;

    Ok(())
}

fn ids_from_atts<T, F>(
    stowage: &Stowage,
    out_name: &str,
    parent_entity: &str,
    closure: F,
) -> io::Result<()>
where
    T: DeserializeOwned,
    F: Fn(T) -> BigId,
{
    entities_from_iter(
        stowage,
        out_name,
        stowage
            .read_csv_objs::<T>(parent_entity, "main")
            .map(closure),
        get_last_filter(stowage, out_name),
    )
}

fn entities_from_iter<I>(
    stowage: &Stowage,
    entity_name: &str,
    iter: I,
    filter: Option<HashSet<BigId>>,
) -> io::Result<()>
where
    I: Iterator<Item = BigId>,
{
    let mut id_map = get_idmap(stowage, entity_name);
    match &filter {
        None => println!("\n{:?} no filter\n", entity_name),
        Some(fs) => println!("\n{:?} filter of {:?}\n", entity_name, fs.len()),
    }
    id_map.set_filter(filter);

    for id in iter {
        id_map.push(id);
    }
    id_map.extend();
    println!(
        "\n entity-type: {:?} total-size: {:?}",
        entity_name, id_map.current_total
    );

    Ok(())
}

pub fn short_string_to_u64(input: &str) -> BigId {
    let mut padded_input = [0u8; 8];
    let l = input.len().min(8);
    padded_input[..l].copy_from_slice(&input.as_bytes()[..l]);
    BigId::from_le_bytes(padded_input)
}
