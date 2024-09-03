use std::io;

use hashbrown::HashSet;
use serde::{de::DeserializeOwned, Deserialize};

use crate::add_parsed_id_traits;
use crate::common::{
    field_id_parse, oa_id_parse, short_string_to_u64, ParsedId, Stowage, AREA_FIELDS, COUNTRIES, QS,
};
use crate::csv_writers::{
    authors, concepts, fields, institutions, sources, subfields, topics, works,
};
use crate::filters::get_last_filter;
use crate::oa_structs::{post::Authorship, Institution};
use dmove::{BigId, EntityBuilder, IdMappedEntityBuilder, MainBuilder, MetaIntegrator};

#[derive(Deserialize, Debug)]
pub struct SourceArea {
    id: Option<String>,
    pub area: Option<String>,
}

#[derive(Deserialize, Debug)]
struct SIdObj {
    id: Option<String>,
}

pub struct EStow {
    stowage: Stowage,
    builder: MainBuilder,
    current_name: String,
}

add_parsed_id_traits!(SIdObj, SourceArea);

impl SourceArea {
    pub fn raw_area_id(&self) -> BigId {
        short_string_to_u64(&self.area.clone().unwrap_or("".to_string()))
    }
}

impl EStow {
    fn new(stowage: Stowage) -> Self {
        Self {
            builder: MainBuilder::new(&stowage.key_stores),
            stowage,
            current_name: "".to_string(),
        }
    }

    fn write_all(&mut self) -> io::Result<usize> {
        let areas_iterator = self
            .stowage
            .read_csv_objs::<SourceArea>(sources::C, AREA_FIELDS)
            .map(|e| e.raw_area_id());

        self.entities_from_iter(AREA_FIELDS, areas_iterator, None);

        //TODO: MeaningfulId - is it worh it to make the usize meaningful
        //definitely for Qs
        let authorship_iterator = self
            .stowage
            .read_csv_objs::<Authorship>(works::C, works::atts::authorships)
            .enumerate()
            .map(|(i, _ship)| i);

        EntityBuilder::add_iter_owned(
            &mut self.builder,
            authorship_iterator,
            works::atts::authorships,
        );
        EntityBuilder::add_iter_owned(&mut self.builder, 0..5, QS);

        self.ids_from_atts::<Institution, _>(COUNTRIES, institutions::C, |e| {
            short_string_to_u64(&e.country_code.unwrap_or("".to_string()))
        });

        for sw in vec![fields::C, subfields::C] {
            self.ids_from_atts::<SIdObj, _>(sw, sw, |e| field_id_parse(&e.id.unwrap()));
        }

        for en in vec![
            works::C,
            institutions::C,
            sources::C,
            concepts::C,
            topics::C,
            authors::C,
        ] {
            self.ids_from_atts::<SIdObj, _>(en, en, |e| e.get_parsed_id());
        }

        self.builder.write_code("rankless_rs/src/gen_types.rs")
    }

    fn ids_from_atts<T, F>(&mut self, out_name: &str, parent_entity: &str, closure: F)
    where
        T: DeserializeOwned,
        F: Fn(T) -> BigId,
    {
        self.entities_from_iter(
            out_name,
            self.stowage
                .read_csv_objs::<T>(parent_entity, "main")
                .map(closure),
            get_last_filter(&self.stowage, out_name),
        )
    }

    fn entities_from_iter<I>(&mut self, name: &str, iter: I, filter: Option<HashSet<BigId>>)
    where
        I: Iterator<Item = BigId>,
    {
        self.current_name = name.to_string();
        match &filter {
            None => {
                println!("\n{:?} no filter", name);
                self.write(iter);
            }
            Some(fs) => {
                self.write(iter.filter(|e| fs.contains(e)));
                println!("\n{:?} filter of {:?}", name, fs.len());
            }
        };
    }

    fn write<I>(&mut self, iter: I)
    where
        I: Iterator<Item = BigId>,
    {
        IdMappedEntityBuilder::add_iter_owned(&mut self.builder, iter, &self.current_name);
    }
}

pub fn make_ids(stowage: Stowage) -> io::Result<usize> {
    EStow::new(stowage).write_all()
}
