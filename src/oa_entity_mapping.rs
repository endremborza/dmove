use std::io;
use std::marker::PhantomData;

use serde::{de::DeserializeOwned, Deserialize};
use std::collections::HashSet;

use tqdm::Iter;

use crate::add_parsed_id_traits;
use crate::common::{
    oa_id_parse, BigId, ParsedId, StowReader, Stowage, CONCEPTS, COUNTRIES, INSTS, SOURCES, WORKS,
};
use crate::ingest_entity::get_idmap;
use crate::oa_filters::get_last_filter;

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

struct InstIter<T> {
    rdr: StowReader,
    phantom: PhantomData<T>,
}

impl<T> InstIter<T> {
    pub fn new(stowage: &Stowage) -> Self {
        let rdr = stowage.get_sub_reader(INSTS, "main");
        Self {
            rdr,
            phantom: PhantomData::<T>,
        }
    }
}

trait OptionGetter<T> {
    fn get(e: T) -> Option<String>;
}

struct GCountry;
struct GType;

impl OptionGetter<SInstitution> for GCountry {
    fn get(e: SInstitution) -> Option<String> {
        e.country_code
    }
}
impl OptionGetter<SInstitution> for GType {
    fn get(e: SInstitution) -> Option<String> {
        e.ints_type
    }
}

impl<T: OptionGetter<SInstitution>> Iterator for InstIter<T> {
    type Item = BigId;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let inst = self.rdr.deserialize::<SInstitution>().next();
            match inst {
                Some(i) => {
                    if let Some(cc) = T::get(i.unwrap()) {
                        return Some(short_string_to_u64(&cc));
                    }
                }
                None => return None,
            }
        }
    }
}

add_parsed_id_traits!(SIdObj, SInstitution);

macro_rules! entites_from_dir {
    ($dirname: ident, $typename: ident, $s: ident) => {
        make_entity_ids::<$typename>($s, $dirname)?;
    };
}

macro_rules! entites_from_attr {
    ($ename: ident, $typename: ident, $s: ident) => {
        entities_from_iter($s, $ename, InstIter::<$typename>::new($s), None)?;
    };
}

pub fn make_ids(stowage: &Stowage) -> io::Result<()> {
    entites_from_dir!(WORKS, SIdObj, stowage);
    entites_from_dir!(SOURCES, SIdObj, stowage);
    entites_from_dir!(INSTS, SInstitution, stowage);
    entites_from_attr!(COUNTRIES, GCountry, stowage);
    entites_from_dir!(CONCEPTS, SIdObj, stowage);
    // entites_from_attr!("inst_types", GType, stowage);
    Ok(())
}

fn make_entity_ids<T>(stowage: &Stowage, entity_name: &str) -> io::Result<()>
where
    T: DeserializeOwned + ParsedId,
{
    let mut rdr = stowage.get_sub_reader(entity_name, "main");
    entities_from_iter(
        stowage,
        entity_name,
        rdr.deserialize::<T>()
            .map(|e| e.unwrap().get_parsed_id())
            .tqdm(),
        get_last_filter(stowage, entity_name),
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
