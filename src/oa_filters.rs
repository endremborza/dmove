use std::{
    collections::{HashMap, HashSet},
    fs::{create_dir_all, read_dir, File},
    io::{self, Read, Write},
    path::PathBuf,
};

use serde::{de::DeserializeOwned, Deserialize};
use tqdm::*;

use crate::{
    add_strict_parsed_id_traits,
    common::{oa_id_parse, BigId, ParsedId, Stowage, AUTHORS, INSTS, SOURCES, WORKS},
};

pub const START_YEAR: u16 = 2004;

trait FilterBase {
    fn csv_path() -> String;
    fn get_min() -> usize {
        0
    }
    fn get_max() -> usize {
        usize::max_value()
    }
    fn has_max() -> bool {
        false
    }
    fn get_edge_end_types() -> [&'static str; 2];
    fn filter_targets() -> bool {
        true
    }
    fn iter_edges(&self) -> Vec<[String; 2]>;
}

#[derive(Deserialize, Debug)]
pub struct InstAuthorship {
    pub parent_id: String,
    pub author: String,
    pub institutions: Option<String>,
}

impl InstAuthorship {
    pub fn iter_insts(&self) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(insts) = &self.institutions {
            for inst_oa_id in insts.split(";") {
                out.push(inst_oa_id.to_string());
            }
        }
        out
    }
}

impl FilterBase for InstAuthorship {
    fn csv_path() -> String {
        format!("{}/authorships", WORKS)
    }

    fn get_min() -> usize {
        1500
    }

    fn get_edge_end_types() -> [&'static str; 2] {
        [INSTS, WORKS]
    }

    fn iter_edges(&self) -> Vec<[String; 2]> {
        let mut out = Vec::new();
        for inst_oa_id in self.iter_insts() {
            out.push([inst_oa_id, self.parent_id.to_string()]);
        }
        out
    }
}

#[derive(Deserialize, Debug)]
pub struct Citation {
    pub parent_id: String,
    pub referenced_work_id: String,
}

impl FilterBase for Citation {
    fn csv_path() -> String {
        format!("{}/referenced_works", WORKS)
    }

    fn get_min() -> usize {
        2
    }

    fn get_edge_end_types() -> [&'static str; 2] {
        [WORKS, WORKS]
    }

    fn iter_edges(&self) -> Vec<[String; 2]> {
        vec![[
            self.referenced_work_id.to_string(),
            self.parent_id.to_string(),
        ]]
    }

    fn filter_targets() -> bool {
        false
    }
}

#[derive(Deserialize, Debug)]
struct Location {
    parent_id: String,
    source: Option<String>,
}

impl FilterBase for Location {
    fn csv_path() -> String {
        format!("{}/locations", WORKS)
    }

    fn get_min() -> usize {
        200
    }

    fn get_edge_end_types() -> [&'static str; 2] {
        [SOURCES, WORKS]
    }

    fn iter_edges(&self) -> Vec<[String; 2]> {
        match &self.source {
            Some(source_id) => vec![[source_id.to_string(), self.parent_id.to_string()]],
            None => Vec::new(),
        }
    }
}

#[derive(Deserialize, Debug)]
pub struct PersonAuthorship {
    pub parent_id: String,
    pub author: String,
}

impl FilterBase for PersonAuthorship {
    fn csv_path() -> String {
        format!("{}/authorships", WORKS)
    }

    fn get_max() -> usize {
        20
    }

    fn has_max() -> bool {
        true
    }

    fn get_edge_end_types() -> [&'static str; 2] {
        [WORKS, AUTHORS]
    }

    fn filter_targets() -> bool {
        false
    }

    fn iter_edges(&self) -> Vec<[String; 2]> {
        let mut out = Vec::new();
        let aid = self.author.to_string();
        if aid.len() > 0 {
            out.push([self.parent_id.to_string(), self.author.to_string()])
        }
        out
    }
}

pub fn filter_setup(stowage: &Stowage) -> io::Result<()> {
    single_filter(stowage)?;
    filter_step::<Citation>(stowage, 11)?;
    filter_step::<Location>(stowage, 12)?;
    filter_step::<InstAuthorship>(stowage, 13)?;
    filter_step::<PersonAuthorship>(stowage, 14)?;
    Ok(())
}

#[derive(Deserialize)]
pub struct SWork {
    pub id: String,
    is_retracted: bool,
    #[serde(rename = "type")]
    work_type: String,
    pub publication_year: Option<u16>,
}

add_strict_parsed_id_traits!(SWork);

fn single_filter(stowage: &Stowage) -> io::Result<()> {
    let step_id = "1";
    let out_root = stowage.filter_steps.join(step_id);
    create_dir_all(&out_root)?;
    filter_write::<SWork, _>(stowage, WORKS, &out_root, |o| {
        !o.is_retracted
            & (o.work_type == "article")
            & (o.publication_year.unwrap_or(0) > START_YEAR)
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

fn filter_step<T>(stowage: &Stowage, step_id: u8) -> io::Result<()>
where
    T: FilterBase + DeserializeOwned + core::fmt::Debug,
{
    let types = T::get_edge_end_types();
    let [source_type, target_type] = types;
    let [source_set_o, target_set_o] = types.map(|t| get_last_filter(stowage, t));

    println!(
        "filtering {:?} - {:?} --> {:?}. pre-filtered to {:?}, pre-filtered to {:?}",
        step_id,
        source_type,
        target_type,
        match source_set_o {
            Some(ref l) => l.len(),
            None => 0,
        },
        match target_set_o {
            Some(ref l) => l.len(),
            None => 0,
        }
    );

    let mut hmap: HashMap<u64, HashSet<u64>> = HashMap::new();
    let mut rdr = stowage.get_reader(T::csv_path());

    for obj in rdr.deserialize::<T>().tqdm() {
        let rec = obj?;
        for ends in rec.iter_edges().iter() {
            let source_key = oa_id_parse(&ends[0]);
            let target_key = oa_id_parse(&ends[1]);
            if let Some(source_set) = &source_set_o {
                if !source_set.contains(&source_key) {
                    continue;
                }
            }
            if let Some(target_set) = &target_set_o {
                if !target_set.contains(&target_key) {
                    continue;
                }
            }
            if !hmap.contains_key(&source_key) {
                let hset = HashSet::new();
                hmap.insert(source_key, hset);
            }
            if let Some(hset) = hmap.get_mut(&source_key) {
                if T::filter_targets() | (hset.len() < T::get_min()) | T::has_max() {
                    hset.insert(target_key);
                }
            }
        }
    }
    let out_root = stowage.filter_steps.join(step_id.to_string());
    create_dir_all(&out_root)?;
    let mut taken_sources = Vec::new();
    let mut taken_targets: HashSet<u64> = HashSet::new();
    for (k, v) in hmap.iter() {
        if (v.len() >= T::get_min()) && (v.len() <= T::get_max()) {
            taken_sources.push(*k);
            if T::filter_targets() {
                taken_targets.extend(v);
            }
        }
    }
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
