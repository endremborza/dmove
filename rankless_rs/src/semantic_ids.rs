use std::collections::BinaryHeap;

use dmove::{
    CompactEntity, Entity, MarkedAttribute, NamespacedEntity, UnsignedNumber, VarAttBuilder, ET,
    MAA,
};
use hashbrown::HashSet;
use muwo_search::StackWordSet;
use serde::de::DeserializeOwned;

use crate::{
    common::{init_empty_slice, MainEntity, ParsedId, MAIN_NAME},
    oa_structs::{
        post::{read_post_str_arr, Institution, Source},
        FieldLike, NamedEntity,
    },
    QuickestNumbered, ReadFixIter, SemanticIdMarker, Stowage, WorkCountMarker,
};

pub trait SemCsvObj {
    type CsvObj: DeserializeOwned + ParsedId + AddSemId;
}

pub trait AddSemId {
    fn get_names(&self) -> Vec<String>;
}

trait DoIfNot<T> {
    fn ifnotin<F>(&mut self, e: T, f: F) -> bool
    where
        F: FnMut(T);
}

impl Stowage {
    pub fn write_semantic_id<E>(&self)
    where
        E: MainEntity + NamespacedEntity + MarkedAttribute<WorkCountMarker> + SemCsvObj,
        MAA<E, WorkCountMarker>: NamespacedEntity + CompactEntity,
        ET<MAA<E, WorkCountMarker>>: UnsignedNumber,
    {
        let mut id_ops = init_empty_slice::<E, Vec<String>>();
        let interface = self.get_entity_interface::<E, QuickestNumbered>();
        for o in self.read_csv_objs::<E::CsvObj>(E::NAME, MAIN_NAME) {
            let oid = o.get_parsed_id();
            if let Some(eid) = interface.0.get(&oid) {
                id_ops[eid.to_usize()] = o.get_names();
            }
        }
        let wcounts = self.get_entity_interface::<MAA<E, WorkCountMarker>, ReadFixIter>();
        let mut miss_heap = BinaryHeap::new();
        let mut sem_set = HashSet::new();
        for (eid, wc) in wcounts.enumerate() {
            miss_heap.push((wc, eid));
        }
        let suffs = get_suffs();
        let mut ids = init_empty_slice::<E, String>();
        while let Some((_wc, eid)) = miss_heap.pop() {
            let id_opts_vec = &id_ops[eid];
            if id_opts_vec.len() == 0 {
                // assert_eq!(_wc.to_usize(), 0, "{}({eid})", E::NAME);
                if _wc.to_usize() > 0 {
                    println!("missing: {}({eid})", E::NAME);
                }
                continue;
            }
            let for_suff = id_opts_vec.iter().last().unwrap();
            for sid in id_ops[eid]
                .clone()
                .into_iter()
                .chain(suffs.iter().map(|e| (for_suff.to_owned() + e)))
            {
                if sem_set.ifnotin(sid, |e| ids[eid] = e) {
                    break;
                }
            }
        }
        self.decsem::<E, _>(ids.iter().map(|e| e.to_owned()))
    }

    pub fn decsem<E, I>(&self, it: I)
    where
        E: Entity,
        I: Iterator<Item = String>,
    {
        self.declare_iter::<VarAttBuilder, _, _, E, SemanticIdMarker>(
            it,
            &format!("{}-semantic-ids", E::NAME),
        );
    }
}

impl AddSemId for NamedEntity {
    fn get_names(&self) -> Vec<String> {
        vec![semantify(&self.display_name)]
    }
}

impl AddSemId for FieldLike {
    fn get_names(&self) -> Vec<String> {
        vec![semantify(&self.display_name)]
    }
}

impl AddSemId for Source {
    fn get_names(&self) -> Vec<String> {
        let mut out = read_post_str_arr(&self.alternate_titles);
        if let Some(abb) = &self.abbreviated_title {
            out.push(abb.clone());
        }
        out.sort_by_key(|e| e.len());
        out.push(self.display_name.clone());
        out.iter().map(semantify).collect()
    }
}

impl AddSemId for Institution {
    fn get_names(&self) -> Vec<String> {
        let mut out = read_post_str_arr(&self.display_name_acronyms);
        out.extend(read_post_str_arr(&self.display_name_alternatives).into_iter());
        let base_name = self.display_name.clone();
        out.push(base_name.clone());
        const PREF: &str = "University of ";
        const SUFF: &str = " University";
        if base_name.starts_with(PREF) {
            out.push(base_name[PREF.len()..].to_string())
        }
        if base_name.ends_with(SUFF) {
            out.push(base_name[base_name.len() - SUFF.len()..].to_string())
        }
        out.push(
            format!(
                "{} {}",
                base_name.trim(),
                self.country_code.as_ref().unwrap_or(&"".to_string())
            )
            .to_string(),
        );
        out.sort_by_key(|e| e.len());
        out.iter().map(semantify).collect()
    }
}

impl DoIfNot<String> for HashSet<String> {
    fn ifnotin<F>(&mut self, e: String, mut f: F) -> bool
    where
        F: FnMut(String),
    {
        if !self.contains(&e) {
            self.insert(e.clone());
            f(e);
            return true;
        }
        false
    }
}

pub fn semantify(s: &String) -> String {
    StackWordSet::new(s).to_words().join("-")
}

fn get_suffs() -> Vec<String> {
    "23456789abcdefghijklmno"
        .chars()
        .map(|e| format!("-{e}").to_string())
        .collect()
}
