use std::io;

use hashbrown::HashSet;
use serde::{de::DeserializeOwned, Deserialize};

use crate::common::{
    field_id_parse, oa_id_parse, BigId, ParsedId, Stowage, AREA_FIELDS, COUNTRIES, QS,
};
use crate::ingest_entity::get_idmap;
use crate::oa_csv_writers::{
    authors, concepts, fields, institutions, sources, subfields, topics, works,
};
use crate::oa_filters::get_last_filter;
use crate::{add_parsed_id_traits, add_strict_parsed_id_traits};

#[derive(Deserialize, Debug)]
pub struct SInstitution {
    id: Option<String>,
    pub country_code: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct STopic {
    pub id: String,
    // pub field: String,
    pub subfield: String,
}

#[derive(Deserialize, Debug)]
pub struct SSource {
    id: Option<String>,
    pub area: Option<String>,
}

#[derive(Deserialize, Debug)]
struct SIdObj {
    id: Option<String>,
}

add_parsed_id_traits!(SIdObj, SInstitution, SSource);
add_strict_parsed_id_traits!(STopic);

pub fn make_ids(stowage: &Stowage) -> io::Result<()> {
    entities_from_iter(stowage, QS, 1..6, None)?;

    ids_from_atts::<SInstitution, _>(stowage, COUNTRIES, institutions::C, |e| {
        short_string_to_u64(&e.country_code.unwrap_or("".to_string()))
    })?;

    entities_from_iter(
        stowage,
        AREA_FIELDS,
        stowage
            .read_csv_objs::<SSource>(sources::C, AREA_FIELDS)
            .map(|e| short_string_to_u64(&e.area.unwrap_or("".to_string()))),
        get_last_filter(stowage, AREA_FIELDS),
    )?;

    for sw in vec![fields::C, subfields::C] {
        ids_from_atts::<SIdObj, _>(stowage, sw, sw, |e| field_id_parse(&e.id.unwrap()))?;
    }

    for en in vec![
        works::C,
        institutions::C,
        sources::C,
        concepts::C,
        topics::C,
        authors::C,
    ] {
        ids_from_atts::<SIdObj, _>(stowage, en, en, |e| e.get_parsed_id())?;
    }

    Ok(())
}

pub fn short_string_to_u64(input: &str) -> BigId {
    let mut padded_input = [0u8; 8];
    let l = input.len().min(8);
    padded_input[..l].copy_from_slice(&input.as_bytes()[..l]);
    BigId::from_le_bytes(padded_input)
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
        None => println!("\n{:?} no filter", entity_name),
        Some(fs) => println!("\n{:?} filter of {:?}", entity_name, fs.len()),
    }
    id_map.set_filter(filter);

    for id in iter {
        id_map.push(id);
    }
    id_map.extend();
    println!(
        "entity-type: {:?} total-size: {:?}",
        entity_name, id_map.current_total
    );

    Ok(())
}
