use std::io;

use hashbrown::HashSet;
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    add_parsed_id_traits,
    common::{
        field_id_parse, oa_id_parse, short_string_to_u64, ObjIter, ParsedId, Stowage, AREA_FIELDS,
        COUNTRIES,
    },
    csv_writers::{authors, fields, institutions, sources, subfields, topics, works},
    filter::{FINAL_YEAR, START_YEAR},
    oa_structs::{post::Authorship, IdStruct, Institution},
};
use dmove::{
    BigId, Data64MappedEntityBuilder, Entity, EntityImmutableMapperBackend, MappableEntity,
};

pub struct Years {}

pub struct YearInterface {}

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

impl SourceArea {
    pub fn raw_area_id(&self) -> BigId {
        short_string_to_u64(&self.area.clone().unwrap_or("".to_string()))
    }
}

impl Entity for Years {
    type T = u8;
    const N: usize = (FINAL_YEAR - START_YEAR) as usize;
    const NAME: &'static str = "years";
}

impl MappableEntity<BigId> for Years {
    type KeyType = u16;
}

impl EntityImmutableMapperBackend<Years, BigId> for YearInterface {
    fn get_via_immut(
        &self,
        k: &<Years as MappableEntity<BigId>>::KeyType,
    ) -> Option<<Years as Entity>::T> {
        Some((k - START_YEAR) as u8)
    }
}

pub fn main(mut stowage: Stowage) -> io::Result<()> {
    let areas_iterator = stowage
        .read_csv_objs::<SourceArea>(sources::C, AREA_FIELDS)
        .map(|e| e.raw_area_id());

    entities_from_iter(&mut stowage, AREA_FIELDS, areas_iterator, None);

    //TODO: MeaningfulId - is it worh it to make the usize meaningful
    //definitely for Qs
    //TODO: distinguish as no null value here
    let ship_n = iter_authorships(&stowage).count();

    let builder = &mut stowage.builder.as_mut().unwrap();
    builder.add_scaled_compact_entity(works::atts::authorships, ship_n);
    builder.add_scaled_compact_entity("qs", 5);

    ids_from_atts::<Institution, _>(&mut stowage, COUNTRIES, institutions::C, |e| {
        short_string_to_u64(&e.country_code.unwrap_or("".to_string()))
    });

    for sw in vec![fields::C, subfields::C] {
        ids_from_atts::<IdStruct, _>(&mut stowage, sw, sw, |e| field_id_parse(&e.id.unwrap()));
    }

    for en in vec![
        works::C,
        institutions::C,
        sources::C,
        // concepts::C,
        topics::C,
        authors::C,
    ] {
        ids_from_atts::<IdStruct, _>(&mut stowage, en, en, |e| e.get_parsed_id());
    }

    stowage.write_code()?;
    Ok(())
}

pub fn iter_authorships(stowage: &Stowage) -> ShipIterator {
    ShipIterator::new(stowage)
}
fn ids_from_atts<T, F>(stowage: &mut Stowage, out_name: &str, parent_entity: &str, closure: F)
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
        stowage.get_last_filter(out_name),
    )
}

fn entities_from_iter<I>(stowage: &mut Stowage, name: &str, iter: I, filter: Option<HashSet<BigId>>)
where
    I: Iterator<Item = BigId>,
{
    stowage.set_name(Some(name));
    match &filter {
        None => {
            println!("\n{:?} no filter", name);
            stowage.add_iter_owned::<Data64MappedEntityBuilder, _, _>(iter, None);
        }
        Some(fs) => {
            println!("\n{:?} filter of {:?}", name, fs.len());
            stowage.add_iter_owned::<Data64MappedEntityBuilder, _, _>(
                iter.filter(|e| fs.contains(e)),
                None,
            );
        }
    };
}
