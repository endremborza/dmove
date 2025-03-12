use std::{io, sync::Arc, thread};

use hashbrown::HashSet;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    add_parsed_id_traits,
    common::{
        field_id_parse, oa_id_parse, short_string_to_u64, BackendSelector, MarkedBackendLoader,
        ObjIter, ParsedId, Stowage, MAIN_NAME,
    },
    csv_writers::{authors, fields, institutions, sources, subfields, topics, works},
    env_consts::{FINAL_YEAR, START_YEAR},
    oa_structs::{
        post::{Authorship, Institution},
        IdStruct,
    },
    NameMarker, QuickestVBox,
};
use dmove::{
    BigId, Data64MappedEntityBuilder, Entity, EntityImmutableMapperBackend, MappableEntity,
    MarkedAttribute, ET,
};

pub type RawYear = u16;
pub type YBT = [RawYear; N_PERS];
pub const N_PERS: usize = 8;
pub const POSSIBLE_YEAR_FILTERS: YBT = [START_YEAR, 2010, 2015, 2020, 2021, 2022, 2023, 2024];

pub struct Years {}
pub struct YearInterface {}
pub struct Qs {}
pub struct QsNames {}

#[derive(Deserialize, Debug)]
pub struct SourceArea {
    id: Option<String>,
    pub area: Option<String>,
}

pub struct ShipIterator {
    raw_iter: ObjIter<Authorship>,
    work_filter: HashSet<BigId>,
}

add_parsed_id_traits!(SourceArea);

impl ShipIterator {
    fn new(stowage: &Stowage) -> Self {
        let raw_iter = stowage.read_csv_objs::<Authorship>(works::C, works::atts::authorships);
        let work_filter = stowage.get_last_filter(works::C).unwrap();
        Self {
            raw_iter,
            work_filter,
        }
    }
}

impl SourceArea {
    pub fn raw_area_id(&self) -> BigId {
        short_string_to_u64(&self.area.clone().unwrap_or("".to_string()))
    }
}

impl YearInterface {
    pub fn reverse(y: ET<Years>) -> RawYear {
        y as RawYear + START_YEAR
    }

    pub fn parse(raw: RawYear) -> ET<Years> {
        (raw - START_YEAR) as ET<Years>
    }

    pub fn iter() -> std::ops::Range<u8> {
        0..((FINAL_YEAR - START_YEAR + 1) as u8)
    }
}

impl Iterator for ShipIterator {
    type Item = Authorship;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(ship) = self.raw_iter.next() {
            if self.work_filter.contains(&ship.get_parsed_id()) {
                return Some(ship);
            }
        }
        None
    }
}

impl Entity for Years {
    type T = u8;
    const N: usize = (FINAL_YEAR - START_YEAR) as usize;
    const NAME: &'static str = "years";
}

impl Entity for QsNames {
    type T = String;
    const N: usize = 5;
    const NAME: &str = "qs-names";
}

impl Entity for Qs {
    type T = u8;
    const N: usize = 5;
    const NAME: &str = "qs";
}

impl MappableEntity for Qs {
    type KeyType = BigId;
}

impl MappableEntity for QsNames {
    type KeyType = usize;
}

impl MarkedAttribute<NameMarker> for Qs {
    type AttributeEntity = QsNames;
}

impl MarkedBackendLoader<QuickestVBox> for QsNames {
    type BE = <QuickestVBox as BackendSelector<QsNames>>::BE;
    fn load(_stowage: &Stowage) -> Self::BE {
        let mut q_names: Vec<String> = vec!["Uncategorized".to_owned()];
        q_names.extend((1..5).map(|i| format!("Q{}", i)));
        q_names.into()
    }
}

impl MappableEntity for Years {
    type KeyType = RawYear;
}

impl EntityImmutableMapperBackend<Years> for YearInterface {
    fn get_via_immut(&self, k: &RawYear) -> Option<ET<Years>> {
        Some(Self::parse(*k))
    }
}

pub fn main(stowage: Stowage) -> io::Result<()> {
    let mut threads = Vec::new();
    let starc = Arc::new(stowage);

    for sw in vec![fields::C, subfields::C] {
        let sc = starc.clone();
        threads.push(thread::spawn(move || {
            ids_from_atts::<IdStruct, _>(&sc, sw, sw, |e| field_id_parse(&e.id.unwrap()));
        }));
    }

    for en in vec![
        works::C,
        institutions::C,
        sources::C,
        // concepts::C,
        topics::C,
        authors::C,
    ] {
        let sc = starc.clone();
        threads.push(thread::spawn(move || {
            ids_from_atts::<IdStruct, _>(&sc, en, en, |e| e.get_parsed_id());
        }));
    }

    ids_from_atts::<SourceArea, _>(&starc, "area-fields", sources::C, |e| e.raw_area_id());

    ids_from_atts::<Institution, _>(&starc, "countries", institutions::C, |e| {
        short_string_to_u64(&e.country_code.unwrap_or("".to_string()))
    });

    //TODO: distinguish as no null value here (??)
    //fix inderect authorships
    let ship_n = iter_authorships(&starc).count();
    threads.into_iter().for_each(|h| h.join().unwrap());
    starc
        .mu_bu()
        .add_scaled_entity(works::atts::authorships, ship_n, true);
    starc.mu_bu().add_scaled_entity("qs", 5, true);
    starc.write_code()?;
    Ok(())
}

pub fn iter_authorships(stowage: &Stowage) -> ShipIterator {
    ShipIterator::new(stowage)
}

fn ids_from_atts<T, F>(stowage: &Stowage, out_name: &str, parent_entity: &str, closure: F)
where
    T: DeserializeOwned,
    F: Fn(T) -> BigId,
{
    entities_from_iter(
        stowage,
        out_name,
        stowage
            .read_csv_objs::<T>(parent_entity, MAIN_NAME)
            .map(closure),
        stowage.get_last_filter(out_name),
    )
}

fn entities_from_iter<I>(stowage: &Stowage, name: &str, iter: I, filter: Option<HashSet<BigId>>)
where
    I: Iterator<Item = BigId>,
{
    match &filter {
        None => {
            println!("\n{name} no filter");
            stowage.add_iter_owned::<Data64MappedEntityBuilder, _, _>(iter, Some(name));
        }
        Some(fs) => {
            println!("\n{name} filter of {:?}", fs.len());
            stowage.add_iter_owned::<Data64MappedEntityBuilder, _, _>(
                iter.filter(|e| fs.contains(e)),
                Some(name),
            );
        }
    };
}
