use crate::{
    add_strict_parsed_id_traits,
    common::{field_id_parse, oa_id_parse, BigId, ParsedId, Stowage, COUNTRIES},
    ingest_entity::{get_idmap, LoadedIdMap},
    oa_csv_writers::{fields, institutions, subfields, topics, works},
    oa_entity_mapping::{short_string_to_u64, SInstitution, STopic},
    oa_filters::START_YEAR,
    para::Worker,
};
use hashbrown::HashMap;
use serde::{de::DeserializeOwned, Deserialize};
use std::{
    io::{self, Read, Write},
    sync::Mutex,
};

#[derive(Deserialize)]
pub struct SWork {
    id: String,
    publication_year: Option<u16>,
}

#[derive(Deserialize, Debug)]
struct SSubField {
    id: String,
    field: String,
}

struct Fatter<T> {
    id_map: LoadedIdMap,
    att_id_map: LoadedIdMap,
    attribute_arr: Mutex<Box<[T]>>,
}

trait ParseId {
    fn parse_id(id: &BigId) -> Self;
    fn null_value() -> Self;
    fn barr(&self) -> Vec<u8>;
}

impl ParsedId for SSubField {
    fn get_parsed_id(&self) -> BigId {
        field_id_parse(&self.id)
    }
}

macro_rules! uints {
    ($($u:ident,)*) => {
        $(impl ParseId for $u {
            fn parse_id(id: &BigId) -> Self {
                *id as Self
            }
            fn null_value() -> Self {0}
            fn barr(&self) -> Vec<u8>{
                self.to_be_bytes().into()
            }
        })*
    };
}

macro_rules! by_size {
    ($f: ident, $t:ident, $n:ident, $($a:ident,)*) => {
        if ($n >> 8) == 0 {
            $f::<u8, $t>($($a,)*);
        } else if ($n >> 16) == 0 {
            $f::<u16, $t>($($a,)*)
        } else if ($n >> 32) == 0 {
            $f::<u32, $t>($($a,)*);
        } else {
            $f::<u64, $t>($($a,)*);
        };
    };
}
uints!(u8, u16, u32, u64,);

add_strict_parsed_id_traits!(SWork);

pub fn write_fix_atts(stowage: &Stowage) -> io::Result<()> {
    //WARNING only u8 len things work now!
    sized_run::<SInstitution>(stowage, names::I2C, COUNTRIES, institutions::C);
    // sized_run::<SSource>(stowage, names::SOURCE_AREAS, AREA_FIELDS, AREA_FIELDS);
    sized_run::<SSubField>(stowage, names::ANCESTOR, fields::C, subfields::C);
    sized_run::<STopic>(stowage, names::TOPIC_SUBFIELDS, subfields::C, topics::C);
    let phantom_map = HashMap::new();
    run_fatt::<u8, SWork>(stowage, names::WORK_YEAR, works::C, &phantom_map);
    Ok(())
}

fn sized_run<T>(stowage: &Stowage, fatt_name: &str, id_base: &str, parent: &str)
where
    T: FatGetter + ParsedId + DeserializeOwned + Send,
{
    let id_map = get_idmap(stowage, id_base).to_map();
    let max_count = id_map.len();
    let id_map_arg = &id_map;
    by_size!(run_fatt, T, max_count, stowage, fatt_name, parent, id_map_arg,);
}

trait FatGetter {
    fn get_fatt(&self, att_id_map: &HashMap<BigId, BigId>) -> Option<BigId>;
}

impl FatGetter for SSubField {
    fn get_fatt(&self, field_id_map: &HashMap<BigId, BigId>) -> Option<BigId> {
        Some(*field_id_map.get(&field_id_parse(&self.field))?)
    }
}

impl FatGetter for STopic {
    fn get_fatt(&self, subfield_id_map: &HashMap<BigId, BigId>) -> Option<BigId> {
        Some(*subfield_id_map.get(&field_id_parse(&self.subfield))?)
    }
}

impl FatGetter for SWork {
    fn get_fatt(&self, _: &LoadedIdMap) -> Option<BigId> {
        if let Some(year) = &self.publication_year {
            return Some((year - START_YEAR) as BigId); //TODO: this is not really an id
        };
        None
    }
}

impl FatGetter for SInstitution {
    fn get_fatt(&self, att_id_map: &HashMap<BigId, BigId>) -> Option<BigId> {
        if let Some(cc_id) = &self.country_code {
            return Some(*att_id_map.get(&short_string_to_u64(&cc_id)).unwrap());
        }
        return None;
    }
}

impl<I, O> Worker<I> for Fatter<O>
where
    I: ParsedId + Send + FatGetter,
    O: ParseId + Send + Sized,
{
    fn proc(&self, input: I) {
        if let Some(oid) = self.id_map.get(&input.get_parsed_id()) {
            if let Some(inner_id) = input.get_fatt(&self.att_id_map) {
                let parsed_id = O::parse_id(&inner_id);
                self.attribute_arr.lock().unwrap()[*oid as usize] = parsed_id;
            }
        }
    }
}

fn run_fatt<T, R>(stowage: &Stowage, fatt_name: &str, main_type: &str, att_id_map: &LoadedIdMap)
where
    T: ParseId + std::clone::Clone + Send,
    R: DeserializeOwned + ParsedId + FatGetter + Send,
{
    let obj_id_map = get_idmap(stowage, main_type);
    let mut out_file = stowage.get_fix_writer(fatt_name);
    let attribute_arr: Vec<T> = vec![T::null_value(); (obj_id_map.current_total + 1) as usize];

    let setup = Fatter {
        id_map: obj_id_map.to_map(),
        att_id_map: att_id_map.clone(),
        attribute_arr: Mutex::new(attribute_arr.into_boxed_slice()),
    };

    for att in setup
        .para(stowage.read_csv_objs::<R>(main_type, "main"))
        .attribute_arr
        .lock()
        .unwrap()
        .iter()
    {
        out_file.write(&att.barr()).unwrap();
    }
}

pub fn read_fix_att(stowage: &Stowage, name: &str) -> Vec<u8> {
    const S: usize = 1; //TODO absolute shambles
    let mut out = Vec::new();
    let mut buf: [u8; S] = [0; S];
    let mut br = stowage.get_fix_reader(name);
    loop {
        if let Ok(_) = br.read_exact(&mut buf) {
            out.push(buf[0])
        } else {
            break;
        }
    }
    out
}

pub mod names {
    pub const I2C: &str = "inst-countries";
    pub const ANCESTOR: &str = "subfield-ancestor";
    pub const TOPIC_SUBFIELDS: &str = "topic-subfields";
    pub const WORK_YEAR: &str = "work-years";
}
