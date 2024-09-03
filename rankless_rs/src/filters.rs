use std::{
    fs::{read_dir, File},
    io::{self, Read, Write},
    path::PathBuf,
};

use hashbrown::{HashMap, HashSet};
use serde::{de::DeserializeOwned, Deserialize};

use crate::{
    add_strict_parsed_id_traits,
    common::{oa_id_parse, BigId, ParsedId, Stowage},
    csv_writers::{authors, institutions, sources, works},
    oa_structs::{
        post::{Author, Authorship, Location},
        ReferencedWork,
    },
};

pub const START_YEAR: u16 = 1950;

const FIX_AUTHORS: [BigId; 3] = [5064297795, 5005839111, 5078032253];

#[derive(Deserialize, Debug)]
pub struct PersonAuthorship {
    pub parent_id: String,
    pub author: String,
}

#[derive(Deserialize)]
pub struct SWork {
    pub id: String,
    is_retracted: bool,
    #[serde(rename = "type")]
    work_type: String,
    pub publication_year: Option<u16>,
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

add_strict_parsed_id_traits!(SWork);

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
        700
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
        200
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

pub fn filter_setup(stowage: &Stowage) -> io::Result<()> {
    single_filter(stowage, 10)?;
    filter_step::<ReferencedWork>(stowage, [works::C, works::C], 11)?;
    filter_step::<Location>(stowage, [sources::C, works::C], 12)?;
    filter_step::<Authorship>(stowage, [institutions::C, works::C], 13)?;
    filter_step::<PersonAuthorship>(stowage, [works::C, authors::C], 14)?;
    author_filter(stowage, 20)
}

pub fn get_last_filter(stowage: &Stowage, entity_type: &str) -> Option<HashSet<u64>> {
    let mut out = None;

    if !stowage.entity_csvs.join(entity_type).exists() {
        println!("no such type {}", entity_type);
        return None;
    }
    let dirs = match read_dir(&stowage.filter_steps) {
        Err(_) => vec![],
        Ok(rdir) => {
            let mut v: Vec<PathBuf> = rdir.map(|e| e.unwrap().path()).collect();
            v.sort();
            v
        }
    };
    for edir in dirs {
        let maybe_path = edir.join(entity_type);
        if maybe_path.exists() {
            out = Some(maybe_path);
        }
    }
    match out {
        Some(pb) => Some(read_filter_set(pb)),
        None => None,
    }
}

fn single_filter(stowage: &Stowage, step_id: u8) -> io::Result<()> {
    let out_root = stowage.get_filter_dir(step_id);
    filter_write::<SWork, _>(stowage, works::C, &out_root, |o| {
        !o.is_retracted
            & (o.work_type == "article")
            & (o.publication_year.unwrap_or(0) > START_YEAR)
    })?;
    Ok(())
}

fn author_filter(stowage: &Stowage, step_id: u8) -> io::Result<()> {
    let out_root = stowage.get_filter_dir(step_id);
    filter_write::<Author, _>(stowage, authors::C, &out_root, |o| {
        FIX_AUTHORS.contains(&o.get_parsed_id())
            | ((o.cited_by_count.unwrap_or(0) >= 50000) & (o.works_count.unwrap_or(0) >= 200))
    })?;
    Ok(())
}

fn filter_write<T, F>(
    stowage: &Stowage,
    entity_type: &str,
    out_root: &PathBuf,
    closure: F,
) -> io::Result<()>
where
    T: for<'de> Deserialize<'de> + ParsedId,
    F: Fn(&T) -> bool,
{
    let mut file = File::create(&out_root.join(entity_type))?;

    for o in stowage.read_csv_objs::<T>(entity_type, "main") {
        if closure(&o) {
            file.write_all(&o.get_parsed_id().to_be_bytes())?;
        }
    }
    Ok(())
}

fn olen<T>(o: &Option<HashSet<T>>) -> String {
    match o {
        Some(ref l) => l.len().to_string(),
        None => "nothing".to_string(),
    }
}

fn filter_step<T>(stowage: &Stowage, types: [&'static str; 2], step_id: u8) -> io::Result<()>
where
    T: FilterBase + DeserializeOwned + core::fmt::Debug,
{
    let [source_type, target_type] = types;
    let [source_set_o, target_set_o] = types.map(|t| get_last_filter(stowage, t));

    println!(
        "filtering {:?} - {:?} --> {:?}. pre-filtered to {} pre-filtered to {}",
        step_id,
        source_type,
        target_type,
        olen(&source_set_o),
        olen(&target_set_o),
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

    let out_root = stowage.get_filter_dir(step_id);
    if T::filter_targets() {
        write_ids(&out_root.join(target_type), &mut taken_targets.iter())?;
    }
    write_ids(&out_root.join(source_type), &mut taken_sources.iter())?;

    println!(
        "{}  -  min: {:?}, max: {:?}, {:?}: {:?}, {:?}: {:?}\n\n",
        step_id,
        T::get_min(),
        T::get_max(),
        source_type,
        taken_sources.len(),
        target_type,
        taken_targets.len()
    );
    Ok(())
}

fn write_ids<'a, T>(fname: &PathBuf, id_iter: T) -> io::Result<()>
where
    T: Iterator<Item = &'a u64>,
{
    let mut file = File::create(fname)?;
    for e in id_iter {
        file.write_all(&e.to_be_bytes())?;
    }
    Ok(())
}

fn read_filter_set(pb: PathBuf) -> HashSet<u64> {
    let mut out = HashSet::new();
    let mut br: [u8; 8] = [0; std::mem::size_of::<u64>()];
    let mut file = File::open(pb).unwrap();
    loop {
        if let Err(_e) = file.read_exact(&mut br) {
            break;
        }
        out.insert(u64::from_be_bytes(br));
    }
    out
}
