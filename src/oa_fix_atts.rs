use crate::{
    common::{BigId, ParsedId, Stowage, CONCEPTS, COUNTRIES, INSTS},
    ingest_entity::{get_idmap, IdMap},
    oa_entity_mapping::{short_string_to_u64, SInstitution},
    oa_filters::SConcept,
};
use serde::Deserialize;
use std::io::{self, Read, Write};
use tqdm::Iter;

#[derive(Deserialize, Debug)]
struct SWork {
    _id: Option<String>,
    _publication_year: Option<u16>,
}

trait ParseId {
    fn parse_id(id: &BigId) -> Self;
    fn null_value() -> Self;
    fn barr(&self) -> Vec<u8>;
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
    let mut cid_map = get_idmap(stowage, COUNTRIES);
    let mut iid_map = get_idmap(stowage, INSTS);

    let ct = cid_map.current_total;
    let iid_map_arg = &mut iid_map;
    let cid_map_arg = &mut cid_map;
    let cfatt_name = "inst-country";
    by_size!(
        run_fatt,
        SInstitution,
        ct,
        stowage,
        cfatt_name,
        INSTS,
        cid_map_arg,
    );
    let fatt_name = "concept-levels";
    let max_level = 3;
    by_size!(
        run_fatt,
        SConcept,
        max_level,
        stowage,
        fatt_name,
        CONCEPTS,
        iid_map_arg,
    );
    Ok(())
}

trait FatGetter {
    fn get_fatt(&self, att_id_map: &mut IdMap) -> Option<BigId>;
}

impl FatGetter for SConcept {
    fn get_fatt(&self, _: &mut IdMap) -> Option<BigId> {
        Some(self.level.into())
    }
}

impl FatGetter for SInstitution {
    fn get_fatt(&self, att_id_map: &mut IdMap) -> Option<BigId> {
        if let Some(cc_id) = &self.country_code {
            return Some(att_id_map.get(&short_string_to_u64(&cc_id)).unwrap());
        }
        return None;
    }
}

fn run_fatt<T, R>(stowage: &Stowage, fatt_name: &str, main_type: &str, att_id_map: &mut IdMap)
where
    T: ParseId + std::clone::Clone,
    R: for<'de> Deserialize<'de> + ParsedId + FatGetter,
{
    let mut obj_id_map = get_idmap(stowage, main_type);
    let mut out_file = stowage.get_fix_writer(fatt_name);
    let mut rdr = stowage.get_sub_reader(main_type, "main");
    let mut attribute_arr: Vec<T> = vec![T::null_value(); (obj_id_map.current_total + 1) as usize];

    println!(
        "writing {} for {} with map of {} ({})",
        fatt_name,
        main_type,
        attribute_arr.len(),
        obj_id_map.current_total
    );

    for obj_raw in rdr.deserialize::<R>().tqdm() {
        let obj = obj_raw.unwrap();
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
    const s: usize = 1; //TODO absolute shambles
    let mut out = Vec::new();
    let mut buf: [u8; s] = [0; s];
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
