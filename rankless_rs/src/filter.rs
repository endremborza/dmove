use std::io;

use hashbrown::{HashMap, HashSet};
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    common::{oa_id_parse, ParsedId, Stowage},
    csv_writers::{authors, institutions, sources, works},
    oa_structs::{
        post::{Author, Authorship, Location},
        ReferencedWork, Work,
    },
};

use dmove::BigId;

pub const START_YEAR: u16 = 1950;
// pub const START_YEAR: u16 = env!("START_YEAR").parse().unwrap();
pub const FINAL_YEAR: u16 = 2024;

const FIX_AUTHORS: [BigId; 3] = [5064297795, 5005839111, 5078032253];

#[derive(Deserialize)]
struct PersonAuthorship {
    author: String,
    parent_id: String,
}

trait FilterBase {
    fn entity_c() -> &'static str {
        works::C
    }

    fn entity_att() -> &'static str;

    fn get_min() -> usize {
        0
    }
    fn get_max() -> usize {
        usize::max_value()
    }
    fn has_max() -> bool {
        false
    }
    fn filter_targets() -> bool {
        true
    }
    fn iter_edges(&self) -> Vec<[String; 2]>;
}

impl Authorship {
    pub fn iter_insts(&self) -> Vec<String> {
        if let Some(instids) = &self.institutions {
            return instids.split(";").map(|e| e.to_owned()).collect();
        }
        return Vec::new();
    }
}

impl FilterBase for Authorship {
    fn entity_att() -> &'static str {
        works::atts::authorships
    }

    fn get_min() -> usize {
        env!("MIN_PAPERS_FOR_INST").parse().unwrap()
    }

    fn iter_edges(&self) -> Vec<[String; 2]> {
        self.iter_insts()
            .into_iter()
            .map(|e| [e, self.parent_id.clone().unwrap()])
            .collect()
    }
}

impl FilterBase for ReferencedWork {
    fn entity_att() -> &'static str {
        works::atts::referenced_works
    }

    fn get_min() -> usize {
        2
    }

    fn iter_edges(&self) -> Vec<[String; 2]> {
        let pid = self.parent_id.clone().unwrap();
        vec![[self.referenced_work_id.to_string(), pid]]
    }

    fn filter_targets() -> bool {
        false
    }
}

impl FilterBase for Location {
    fn entity_att() -> &'static str {
        works::atts::locations
    }

    fn get_min() -> usize {
        env!("MIN_PAPERS_FOR_SOURCE").parse().unwrap()
    }

    fn iter_edges(&self) -> Vec<[String; 2]> {
        match &self.source_id {
            Some(source_id) => vec![[source_id.to_string(), self.parent_id.clone().unwrap()]],
            None => Vec::new(),
        }
    }
}

impl FilterBase for PersonAuthorship {
    fn entity_att() -> &'static str {
        works::atts::authorships
    }

    fn get_max() -> usize {
        20
    }

    fn has_max() -> bool {
        true
    }

    fn filter_targets() -> bool {
        false
    }

    fn iter_edges(&self) -> Vec<[String; 2]> {
        if self.author.len() > 0 {
            vec![[self.parent_id.to_string(), self.author.to_string()]]
        } else {
            Vec::new()
        }
    }
}

pub fn main(stowage: Stowage) -> io::Result<()> {
    single_filter(&stowage, 10)?;
    filter_step::<ReferencedWork>(&stowage, [works::C, works::C], 11)?;
    filter_step::<Location>(&stowage, [sources::C, works::C], 12)?;
    filter_step::<Authorship>(&stowage, [institutions::C, works::C], 13)?;
    filter_step::<PersonAuthorship>(&stowage, [works::C, authors::C], 14)?;
    author_filter(&stowage, 20)
}

fn single_filter(stowage: &Stowage, step_id: u8) -> io::Result<()> {
    filter_write::<Work, _>(stowage, step_id, works::C, |o| {
        !o.is_retracted.unwrap_or(false)
            & (o.work_type.as_deref().unwrap_or("") == "article")
            & (o.publication_year.unwrap_or(0) > START_YEAR)
            & (o.publication_year.unwrap_or(0) <= FINAL_YEAR)
    })?;
    Ok(())
}

fn author_filter(stowage: &Stowage, step_id: u8) -> io::Result<()> {
    filter_write::<Author, _>(stowage, step_id, authors::C, |o| {
        FIX_AUTHORS.contains(&o.get_parsed_id())
            | ((o.cited_by_count.unwrap_or(0) >= 50000) & (o.works_count.unwrap_or(0) >= 200))
    })?;
    Ok(())
}

fn filter_write<T, F>(
    stowage: &Stowage,
    step_id: u8,
    entity_type: &str,
    closure: F,
) -> io::Result<()>
where
    T: for<'de> Deserialize<'de> + ParsedId,
    F: Fn(&T) -> bool,
{
    stowage.write_filter(
        step_id,
        entity_type,
        stowage
            .read_csv_objs::<T>(entity_type, "main")
            .filter(|o| closure(&o))
            .map(|o| o.get_parsed_id()),
    )
}

fn olen<T>(o: &Option<HashSet<T>>) -> String {
    match o {
        Some(ref l) => l.len().to_string(),
        None => "nothing".to_string(),
    }
}

fn filter_step<T>(stowage: &Stowage, types: [&'static str; 2], step_id: u8) -> io::Result<()>
where
    T: FilterBase + DeserializeOwned,
{
    let [source_type, target_type] = types;
    let [source_set_o, target_set_o] = types.map(|t| stowage.get_last_filter(t));

    println!(
        "filtering {:?} - {:?} --> {:?}. pre-filtered to {} pre-filtered to {}\nMIN: {}",
        step_id,
        source_type,
        target_type,
        olen(&source_set_o),
        olen(&target_set_o),
        T::get_min(),
    );

    let mut source_map: HashMap<u64, HashSet<u64>> = HashMap::new();

    for rec in stowage.read_csv_objs::<T>(T::entity_c(), T::entity_att()) {
        'endloop: for ends in rec.iter_edges().into_iter() {
            let [source_key, target_key] = ends.map(|e| oa_id_parse(&e));
            for (seto, key) in [(&source_set_o, &source_key), (&target_set_o, &target_key)] {
                if let Some(set) = seto {
                    if !set.contains(key) {
                        continue 'endloop;
                    }
                }
            }
            let set_entry = source_map.entry(source_key).or_insert_with(HashSet::new);
            if T::filter_targets() | (set_entry.len() < T::get_min()) | T::has_max() {
                set_entry.insert(target_key);
            }
        }
    }
    let mut taken_sources = Vec::new();
    let mut taken_targets: HashSet<u64> = HashSet::new();
    for (k, v) in source_map.iter() {
        if (v.len() >= T::get_min()) && (v.len() <= T::get_max()) {
            taken_sources.push(*k);
            if T::filter_targets() {
                taken_targets.extend(v);
            }
        }
    }

    if T::filter_targets() {
        stowage.write_filter(step_id, target_type, &mut taken_targets.into_iter())?;
    }
    stowage.write_filter(step_id, source_type, &mut taken_sources.into_iter())?;
    Ok(())
}
