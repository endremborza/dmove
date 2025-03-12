use std::io;

use hashbrown::{HashMap, HashSet};
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    common::{oa_id_parse, ParsedId, Stowage, MAIN_NAME},
    csv_writers::{authors, institutions, sources, works},
    env_consts::{
        FINAL_YEAR, MIN_AUTHOR_CITE_COUNT, MIN_AUTHOR_WORK_COUNT, MIN_PAPERS_FOR_INST,
        MIN_PAPERS_FOR_SOURCE, START_YEAR,
    },
    oa_structs::{
        post::{Author, Authorship, Institution, Location},
        ReferencedWork, Work,
    },
};

use dmove::BigId;

const MAX_AUTHORS: usize = 20;
const MIN_CITATIONS: usize = 1;
const WORK_KINDS: [&str; 3] = ["article", "book", "review"];

const FIX_AUTHORS: [BigId; 6] = [
    5064297795, 5005839111, 5078032253, 5045634725, 5082456380, 5017880363,
];

const FORCE_DROP_INSTS: [BigId; 2] = [4210095297, 4210109586];

#[derive(Deserialize)]
struct PersonAuthorship {
    author: String,
    parent_id: String,
}

trait FilterBase {
    const ENTITY_C: &'static str = works::C;
    const ENTITY_ATT: &'static str;
    const MIN: usize = 0;
    const MAX: usize = usize::MAX;
    const HAS_MAX: bool = false;
    const FILTER_TARGETS: bool = true;
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
    const ENTITY_ATT: &'static str = works::atts::authorships;
    const MIN: usize = MIN_PAPERS_FOR_INST as usize;
    const FILTER_TARGETS: bool = false;

    fn iter_edges(&self) -> Vec<[String; 2]> {
        self.iter_insts()
            .into_iter()
            .map(|e| [e, self.parent_id.clone().unwrap()])
            .collect()
    }
}

impl FilterBase for ReferencedWork {
    const ENTITY_ATT: &'static str = works::atts::referenced_works;
    const MIN: usize = MIN_CITATIONS;
    const FILTER_TARGETS: bool = false;

    fn iter_edges(&self) -> Vec<[String; 2]> {
        let pid = self.parent_id.clone().unwrap();
        vec![[self.referenced_work_id.to_string(), pid]]
    }
}

impl FilterBase for Location {
    const ENTITY_ATT: &'static str = works::atts::locations;
    const MIN: usize = MIN_PAPERS_FOR_SOURCE as usize;

    fn iter_edges(&self) -> Vec<[String; 2]> {
        match &self.source_id {
            Some(source_id) => vec![[source_id.to_string(), self.parent_id.clone().unwrap()]],
            None => Vec::new(),
        }
    }
}

impl FilterBase for PersonAuthorship {
    const ENTITY_ATT: &'static str = works::atts::authorships;
    const MAX: usize = MAX_AUTHORS;
    const HAS_MAX: bool = true;
    const FILTER_TARGETS: bool = true;

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
    author_filter(&stowage, 20)?;
    inst_filter(&stowage, 21)
}

fn single_filter(stowage: &Stowage, step_id: u8) -> io::Result<()> {
    filter_write::<Work, _>(stowage, step_id, works::C, |o| {
        !o.is_retracted.unwrap_or(false)
            & WORK_KINDS.contains(&o.work_type.as_deref().unwrap_or(""))
            & (o.publication_year.unwrap_or(0) > START_YEAR) // > because 0 is "unknown"
            & (o.publication_year.unwrap_or(0) <= FINAL_YEAR)
    })
}

fn author_filter(stowage: &Stowage, step_id: u8) -> io::Result<()> {
    let pre_filter = stowage.get_last_filter(authors::C).unwrap();
    filter_write::<Author, _>(stowage, step_id, authors::C, |o| {
        let aid = o.get_parsed_id();
        FIX_AUTHORS.contains(&aid)
            | (pre_filter.contains(&aid)
                & (o.cited_by_count.unwrap_or(0) >= MIN_AUTHOR_CITE_COUNT.into())
                & (o.works_count.unwrap_or(0) >= MIN_AUTHOR_WORK_COUNT.into()))
    })
}

fn inst_filter(stowage: &Stowage, step_id: u8) -> io::Result<()> {
    let pre_filter = stowage.get_last_filter(institutions::C).unwrap();
    filter_write::<Institution, _>(stowage, step_id, institutions::C, |o| {
        let iid = o.get_parsed_id();
        !FORCE_DROP_INSTS.contains(&iid) && pre_filter.contains(&iid)
    })
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
            .read_csv_objs::<T>(entity_type, MAIN_NAME)
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
        "filtering {:?} - {:?} --> {:?}. pre-filtered to {} pre-filtered to {}",
        step_id,
        source_type,
        target_type,
        olen(&source_set_o),
        olen(&target_set_o),
    );

    let mut source_map: HashMap<u64, HashSet<u64>> = HashMap::new();

    for rec in stowage.read_csv_objs::<T>(T::ENTITY_C, T::ENTITY_ATT) {
        'endloop: for ends in rec.iter_edges().into_iter() {
            let [source_key, target_key] = ends.map(|e| oa_id_parse(&e));
            for (seto, key) in [(&source_set_o, &source_key), (&target_set_o, &target_key)] {
                if let Some(set) = seto {
                    if !set.contains(key) {
                        //prefiltered out;
                        continue 'endloop;
                    }
                }
            }
            let set_entry = source_map.entry(source_key).or_insert_with(HashSet::new);
            if T::FILTER_TARGETS | (set_entry.len() < T::MIN) | T::HAS_MAX {
                set_entry.insert(target_key);
            }
        }
    }
    let mut taken_sources = Vec::new();
    let mut taken_targets: HashSet<u64> = HashSet::new();
    for (k, v) in source_map.iter() {
        if (v.len() >= T::MIN) && (v.len() <= T::MAX) {
            taken_sources.push(*k);
            if T::FILTER_TARGETS {
                taken_targets.extend(v);
            }
        }
    }

    if T::FILTER_TARGETS {
        stowage.write_filter(step_id, target_type, &mut taken_targets.into_iter())?;
    }
    stowage.write_filter(step_id, source_type, &mut taken_sources.into_iter())?;
    Ok(())
}
