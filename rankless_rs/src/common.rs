use std::fs::{DirEntry, ReadDir};
use std::io::prelude::*;
use std::ops::Range;
use std::{
    fs::{create_dir_all, read_dir, File},
    io::{self, BufReader, Write},
    path::{Path, PathBuf},
};

use csv::{DeserializeRecordsIntoIter, Reader, ReaderBuilder};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use hashbrown::{HashMap, HashSet};
use serde::{de::DeserializeOwned, Serialize};
use tqdm::{Iter, Tqdm};

use dmove::{
    BackendLoading, BigId, Entity, MainBuilder, MappableEntity, MetaIntegrator, NamespacedEntity,
    VarAttIterator, VarBox, VarSizedAttributeElement, VariableSizeAttribute,
};

pub type StowReader = Reader<BufReader<GzDecoder<File>>>;

type InIterator<T> = Tqdm<DeserializeRecordsIntoIter<BufReader<flate2::read::GzDecoder<File>>, T>>;

pub const COUNTRIES: &str = "countries";
pub const AREA_FIELDS: &str = "area-fields";

pub const BUILD_LOC: &str = "qc-builds";
// pub const A_STAT_PATH: &str = "attribute-statics";
// pub const QC_CONF: &str = "qc-specs";

pub const ID_PREFIX: &str = "https://openalex.org/";

#[macro_export]
macro_rules! add_parsed_id_traits {
    () => {};
    ($struct:ident $(, $rest:ident)*) => {
        impl ParsedId for $struct {
            fn get_parsed_id(&self) -> BigId {
                oa_id_parse(&self.id.clone().unwrap())
            }
        }
        add_parsed_id_traits!($($rest),*);
    };
}

#[macro_export]
macro_rules! add_strict_parsed_id_traits {
    () => {};
    ($struct:ident $(, $rest:ident)*) => {
        impl ParsedId for $struct {
            fn get_parsed_id(&self) -> BigId {
                oa_id_parse(&self.id.clone())
            }
        }
        add_strict_parsed_id_traits!($($rest),*);
    };
}

#[macro_export]
macro_rules! add_parent_parsed_id_traits {
    ($($struct:ident),*) => {
        $(
        impl ParsedId for $struct {
            fn get_parsed_id(&self) -> BigId {
                oa_id_parse(self.parent_id.as_ref().unwrap())
            }
        }
        )*
    }
}

macro_rules! pathfields_fn {
    ($struct:ident, $($k:ident),*) => {

        struct $struct {
            $($k: PathBuf,)*
        }

        impl $struct {
            pub fn new(root_path: &str) -> Self{
                $(
                    let $k = Path::new(root_path).join(stringify!($k).replace("_","-"));
                    create_dir_all(&$k).unwrap();
                )*

                Self {
                    $(
                        $k,
                    )*
                }
            }
        }
    };
}

macro_rules! nullable_uint {
    ($($t:ty),*) => {
        $(impl Nullable for $t {
            fn null_value() -> Self {0}
        })*
    };
}

pub struct Stowage {
    paths: PathCollection,
    current_name: String,
    current_ns: String,
    pub builder: Option<MainBuilder>,
}

pub struct ObjIter<T>
where
    T: DeserializeOwned,
{
    iterable: InIterator<T>,
}

pub struct QcPathIter {
    builds: ReadDir,
    inner_dir: PathBuf,
    inner_reader: ReadDir,
}

//TODO: this is sort of a mess - can't tell the difference between box and vbox
//varbox seems to be for variable sized entity
pub struct Quickest {}
pub struct QuickestBox {}
pub struct QuickestVBox {}
pub struct ReadIter {}
pub struct IterCompactElement {}

pub trait BackendSelector<E>
where
    E: Entity,
{
    type BE;
}

pathfields_fn!(
    PathCollection,
    entity_csvs,
    filter_steps,
    cache,
    pruned_cache
);

pub trait ParsedId {
    fn get_parsed_id(&self) -> BigId;
}

pub trait Nullable {
    fn null_value() -> Self;
}

nullable_uint!(u8, u16, u32, u64);

impl Stowage {
    pub fn new(root_path: &str) -> Self {
        Self {
            paths: PathCollection::new(root_path),
            current_name: "".to_string(),
            current_ns: "".to_string(),
            builder: None,
        }
    }

    pub fn set_namespace(&mut self, ns: &'static str) {
        self.current_ns = ns.to_string();
        let path = self.path_from_ns(&self.current_ns);
        create_dir_all(&path).unwrap();
        self.builder = Some(MainBuilder::new(&path));
    }

    pub fn write_code(self) -> io::Result<usize> {
        let suffix = self.current_ns.replace("-", "_");
        self.builder.unwrap().write_code(&code_path(&suffix))
    }

    pub fn get_reader<T>(&self, fname: T) -> StowReader
    where
        T: std::convert::AsRef<Path>,
    {
        let reader = get_gz_buf(
            self.paths
                .entity_csvs
                .join(fname)
                .with_extension("csv.gz")
                .to_str()
                .unwrap(),
        );
        ReaderBuilder::new().from_reader(reader)
    }

    pub fn get_out_csv_path(&self) -> &str {
        self.paths.entity_csvs.to_str().unwrap()
    }

    pub fn get_sub_reader<T: std::fmt::Display>(&self, entity: T, sub: T) -> StowReader {
        self.get_reader(format!("{}/{}", entity, sub))
    }

    pub fn get_filter_dir(&self, step_id: u8) -> PathBuf {
        let out_root = self.paths.filter_steps.join(step_id.to_string());
        create_dir_all(&out_root).unwrap();
        out_root
    }

    pub fn get_last_filter(&self, entity_type: &str) -> Option<HashSet<BigId>> {
        let mut out_path = None;

        if !self.paths.entity_csvs.join(entity_type).exists() {
            println!("no such type {}", entity_type);
            return None;
        }
        let dirs = match read_dir(&self.paths.filter_steps) {
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
                out_path = Some(maybe_path);
            }
        }
        match out_path {
            Some(pb) => {
                let mut out = HashSet::new();
                let mut br: [u8; 8] = [0; std::mem::size_of::<BigId>()];
                let mut file = File::open(pb).unwrap();
                while let Ok(_) = file.read_exact(&mut br) {
                    out.insert(BigId::from_be_bytes(br));
                }
                Some(out)
            }
            None => None,
        }
    }

    pub fn write_filter<'a, T>(&self, step_id: u8, entity_type: &str, id_iter: T) -> io::Result<()>
    where
        T: Iterator<Item = BigId>,
    {
        let mut file = File::create(self.get_filter_dir(step_id).join(entity_type))?;
        for e in id_iter {
            file.write_all(&e.to_be_bytes())?;
        }
        Ok(())
    }

    pub fn write_cache<T: Serialize>(&self, obj: &T, path: &str) -> io::Result<()> {
        let out_path = self.paths.cache.join(path);
        create_dir_all(out_path.parent().unwrap())?;
        write_gz(&out_path, obj)
    }

    pub fn write_cache_buf<T: Serialize>(&self, obj: &T, path: &str) -> io::Result<()> {
        let out_path = self.paths.cache.join(path);
        create_dir_all(out_path.parent().unwrap())?;
        write_gz_buf(&out_path, obj)
    }

    pub fn add_iter_owned<B, I, E>(&mut self, iter: I, name_o: Option<&str>)
    where
        B: MetaIntegrator<E>,
        I: Iterator<Item = E>,
    {
        self.set_name(name_o);
        B::add_iter_owned(
            &mut self.builder.as_mut().unwrap(),
            iter,
            &self.current_name,
        );
        self.builder
            .as_mut()
            .unwrap()
            .declare_ns(&self.current_name, &self.current_ns);
    }

    pub fn declare_link<S: Entity, T: Entity>(&mut self, name: &str) {
        self.builder.as_mut().unwrap().declare_link::<S, T>(name);
    }

    pub fn read_csv_objs<T: DeserializeOwned>(
        &self,
        main_path: &str,
        sub_path: &str,
    ) -> ObjIter<T> {
        ObjIter::new(self, main_path, sub_path)
    }

    pub fn iter_cached_qc_locs(&self) -> QcPathIter {
        QcPathIter::new(&self.paths.cache)
    }

    pub fn iter_pruned_qc_locs(&self) -> QcPathIter {
        QcPathIter::new(&self.paths.pruned_cache)
    }

    pub fn get_entity_interface<E, Marker>(&self) -> Marker::BE
    where
        E: Entity + NamespacedEntity,
        Marker: BackendSelector<E>,
        Marker::BE: BackendLoading<E>,
    {
        let path = self.path_from_ns(E::NS);
        Marker::BE::load_backend(&path)
    }

    pub fn set_name(&mut self, name_o: Option<&str>) {
        if let Some(name) = name_o {
            self.current_name = name.to_string();
        }
    }

    fn path_from_ns(&self, ns: &str) -> PathBuf {
        self.paths.entity_csvs.parent().unwrap().join(ns)
    }
}

impl QcPathIter {
    fn new(path: &PathBuf) -> Self {
        let mut builds = read_dir(path.join(BUILD_LOC)).unwrap();
        let inner_dir = builds.next().unwrap().unwrap().path();
        let inner_reader = read_dir(&inner_dir).unwrap();
        Self {
            builds,
            inner_dir,
            inner_reader,
        }
    }
}

impl<E> BackendSelector<E> for Quickest
where
    E: Entity + MappableEntity<BigId>,
{
    //TODO: move back maybe for something like
    //FullyLoadedCacheType type on the trait
    type BE = HashMap<BigId, E::T>;
}

impl<E> BackendSelector<E> for QuickestBox
where
    E: Entity + MappableEntity<E, KeyType = usize>,
{
    type BE = Box<[E::T]>;
}

impl<E> BackendSelector<E> for QuickestVBox
where
    E: Entity + MappableEntity<E, KeyType = usize>,
{
    type BE = VarBox<E::T>;
}

impl<E> BackendSelector<E> for ReadIter
where
    E: Entity + MappableEntity<E, KeyType = usize> + VariableSizeAttribute,
    <E as Entity>::T: VarSizedAttributeElement,
{
    type BE = VarAttIterator<E>;
}

impl<E> BackendSelector<E> for IterCompactElement
where
    E: Entity + MappableEntity<E, KeyType = usize>,
{
    type BE = Range<E::T>;
}

impl Iterator for QcPathIter {
    type Item = (String, DirEntry);

    fn next(&mut self) -> Option<Self::Item> {
        match self.inner_reader.next() {
            Some(inner_file) => {
                return Some((
                    self.inner_dir
                        .file_name()
                        .unwrap()
                        .to_str()
                        .unwrap()
                        .to_owned(),
                    inner_file.unwrap(),
                ))
            }
            None => {
                if let Some(inner_next) = self.builds.next() {
                    self.inner_dir = inner_next.unwrap().path();
                    self.inner_reader = read_dir(&self.inner_dir).unwrap();
                    return self.next();
                }
            }
        }
        None
    }
}

impl<T> Nullable for Vec<T> {
    fn null_value() -> Self {
        Vec::new()
    }
}

impl Nullable for String {
    fn null_value() -> Self {
        "".to_string()
    }
}

impl<T> ObjIter<T>
where
    T: DeserializeOwned,
{
    pub fn new(stowage: &Stowage, main: &str, sub: &str) -> Self {
        let reader = stowage.get_sub_reader(main, sub);
        let iterable = reader
            .into_deserialize::<T>()
            .tqdm()
            .desc(Some(format!("reading {} / {}", main, sub)));
        Self { iterable }
    }
}

impl<T: DeserializeOwned> Iterator for ObjIter<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        if let Some(obj) = self.iterable.next() {
            return Some(obj.unwrap());
        } else {
            return None;
        }
    }
}

pub fn oa_id_parse(id: &str) -> u64 {
    id[(ID_PREFIX.len() + 1)..].parse::<u64>().expect(id)
}

pub fn field_id_parse(id: &str) -> u64 {
    id.split("/").last().unwrap().parse::<u64>().expect(id)
}

pub fn get_gz_buf(file_name: &str) -> BufReader<GzDecoder<File>> {
    let file = File::open(file_name).unwrap();
    let gz_decoder = GzDecoder::new(file);
    BufReader::new(gz_decoder)
}

pub fn write_gz<T>(out_path: &Path, obj: &T) -> io::Result<()>
where
    T: Serialize,
{
    write_gz_meta(
        out_path,
        obj,
        |o| serde_json::to_string(o).unwrap().as_bytes().to_vec(),
        "json",
    )
}

pub fn write_gz_buf<T>(out_path: &Path, obj: &T) -> io::Result<()>
where
    T: Serialize,
{
    write_gz_meta(out_path, obj, |o| bincode::serialize(o).unwrap(), "json")
}

pub fn write_gz_meta<T, F>(out_path: &Path, obj: &T, f: F, suffix: &str) -> io::Result<()>
where
    T: Serialize,
    F: Fn(&T) -> Vec<u8>,
{
    let out_file = File::create(
        out_path
            .with_extension(format!("{}.gz", suffix))
            .to_str()
            .unwrap(),
    )?;
    let encoder = GzEncoder::new(out_file, Compression::default());
    let mut writer = std::io::BufWriter::new(encoder);
    writer.write_all(&f(obj))
}

pub fn _read_js_path<T: DeserializeOwned>(fp: &str) -> Result<T, serde_json::Error> {
    let mut js_str = String::new();
    get_gz_buf(fp).read_to_string(&mut js_str).unwrap();
    serde_json::from_str(&js_str)
}

pub fn _read_buf_path<T: DeserializeOwned>(fp: &str) -> Result<T, bincode::Error> {
    let mut buf: Vec<u8> = Vec::new();
    get_gz_buf(fp).read_to_end(&mut buf)?;
    bincode::deserialize(&buf)
}

pub fn _read_cache<T: DeserializeOwned>(stowage: &Stowage, fname: &str) -> T {
    _read_js_path(
        stowage
            .paths
            .cache
            .join(format!("{}.json.gz", fname))
            .to_str()
            .unwrap(),
    )
    .expect(&format!("tried reading {}", fname))
}

pub fn short_string_to_u64(input: &str) -> BigId {
    let mut padded_input = [0u8; 8];
    let l = input.len().min(8);
    padded_input[..l].copy_from_slice(&input.as_bytes()[..l]);
    BigId::from_le_bytes(padded_input)
}

pub fn init_empty_slice<E: Entity, T: Nullable>() -> Box<[T]> {
    (0..E::N + 1)
        .map(|_| T::null_value())
        .collect::<Vec<T>>()
        .into()
}

pub fn code_path(suffix: &str) -> String {
    format!("rankless_rs/src/gen_{}.rs", suffix)
}
