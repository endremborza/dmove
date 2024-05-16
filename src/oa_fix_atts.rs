use crate::{
    common::{
        field_id_parse, BigId, ParsedId, Stowage, COUNTRIES, FIELDS, INSTS, SUB_FIELDS, TOPICS,
        WORKS,
    },
    ingest_entity::get_idmap,
    oa_entity_mapping::{short_string_to_u64, SInstitution, STopic},
    oa_filters::{SWork, START_YEAR},
};
use hashbrown::HashMap;
use serde::{de::DeserializeOwned, Deserialize};
use std::io::{self, Read, Write};

pub mod names {
    pub const I2C: &str = "inst-countries";
    pub const ANCESTOR: &str = "subfield-ancestor";
    pub const TOPIC_SUBFIELDS: &str = "topic-subfields";
    pub const WORK_YEAR: &str = "work-years";
}

#[derive(Deserialize, Debug)]
struct SSubField {
    id: String,
    field: String,
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

pub fn write_fix_atts(stowage: &Stowage) -> io::Result<()> {
    //WARNING only u8 len things work now!
    sized_run::<SInstitution>(stowage, names::I2C, COUNTRIES, INSTS);
    sized_run::<SSubField>(stowage, names::ANCESTOR, FIELDS, SUB_FIELDS);
    sized_run::<STopic>(stowage, names::TOPIC_SUBFIELDS, SUB_FIELDS, TOPICS);
    let phantom_map = HashMap::new();
    run_fatt::<u8, SWork>(stowage, names::WORK_YEAR, WORKS, &phantom_map);
    Ok(())
}

fn sized_run<T>(stowage: &Stowage, fatt_name: &str, id_base: &str, parent: &str)
where
    T: FatGetter + ParsedId + DeserializeOwned,
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
    fn get_fatt(&self, _: &HashMap<BigId, BigId>) -> Option<BigId> {
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

fn run_fatt<T, R>(
    stowage: &Stowage,
    fatt_name: &str,
    main_type: &str,
    att_id_map: &HashMap<BigId, BigId>,
) where
    T: ParseId + std::clone::Clone,
    R: for<'de> Deserialize<'de> + ParsedId + FatGetter,
{
    let mut obj_id_map = get_idmap(stowage, main_type);
    let mut out_file = stowage.get_fix_writer(fatt_name);
    let mut attribute_arr: Vec<T> = vec![T::null_value(); (obj_id_map.current_total + 1) as usize];

    for obj in stowage.read_csv_objs::<R>(main_type, "main") {
        if let Some(oid) = obj_id_map.get(&obj.get_parsed_id()) {
            if let Some(inner_id) = obj.get_fatt(att_id_map) {
                let parsed_id = T::parse_id(&inner_id);
                attribute_arr[oid as usize] = parsed_id;
            }
        }
    }
    for att in attribute_arr {
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
