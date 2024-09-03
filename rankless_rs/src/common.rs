use std::fs::{DirEntry, ReadDir};
use std::io::prelude::*;
use std::{
    fs::{create_dir_all, read_dir, File},
    io::{self, BufReader, Write},
    path::{Path, PathBuf},
};

use csv::{DeserializeRecordsIntoIter, Reader, ReaderBuilder};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tqdm::*;

use dmove::{Entity, FixedAttributeElement, FixedSizeAttribute, IdMap, IdMappedEntity};

pub type BigId = u64;
pub type StowReader = Reader<BufReader<GzDecoder<File>>>;

pub const COUNTRIES: &str = "countries";
pub const QS: &str = "qs";
pub const AREA_FIELDS: &str = "area-fields";

pub const BUILD_LOC: &str = "qc-builds";
pub const A_STAT_PATH: &str = "attribute-statics";
pub const QC_CONF: &str = "qc-specs";

pub const ID_PREFIX: &str = "https://openalex.org/";

macro_rules! pathfields_fn {
    ($($k:ident => $v:literal),*,) => {

        pub fn new(root_path: &str) -> Self{
            $(
                let $k = Path::new(root_path).join($v);
                create_dir_all(&$k).unwrap();
            )*

            Self {
                $(
                    $k,
                )*
            }
        }
    };
}

pub struct Stowage {
    pub entity_csvs: PathBuf,
    pub filter_steps: PathBuf,
    pub key_stores: PathBuf,
    pub fix_atts: PathBuf,
    pub var_atts: PathBuf,
    pub cache: PathBuf,
    pub pruned_cache: PathBuf,
}

#[derive(Deserialize)]
pub struct IdStruct {
    pub id: Option<String>,
}

pub trait ParsedId {
    fn get_parsed_id(&self) -> BigId;
}

pub struct QcPathIter {
    builds: ReadDir,
    inner_dir: PathBuf,
    inner_reader: ReadDir,
}

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

add_parsed_id_traits!(IdStruct);

pub fn oa_id_parse(id: &str) -> u64 {
    id[(ID_PREFIX.len() + 1)..].parse::<u64>().expect(id)
}

pub fn field_id_parse(id: &str) -> u64 {
    id.split("/").last().unwrap().parse::<u64>().expect(id)
}

impl Stowage {
    pathfields_fn!(
        entity_csvs => "entity-csvs",
        filter_steps => "filter-steps",
        key_stores => "key-stores",
        fix_atts => "fix-atts",
        var_atts => "var-atts",
        cache => "cache",
        pruned_cache => "pruned-cache",
    );

    pub fn get_reader<T>(&self, fname: T) -> StowReader
    where
        T: std::convert::AsRef<Path>,
    {
        let reader = get_gz_buf(
            self.entity_csvs
                .join(fname)
                .with_extension("csv.gz")
                .to_str()
                .unwrap(),
        );
        ReaderBuilder::new().from_reader(reader)
    }
    pub fn get_sub_reader<T: std::fmt::Display>(&self, entity: T, sub: T) -> StowReader {
        self.get_reader(format!("{}/{}", entity, sub))
    }

    pub fn get_fix_reader(&self, att_name: &str) -> BufReader<File> {
        BufReader::new(File::open(self.fix_atts.join(att_name)).unwrap())
    }

    pub fn get_filter_dir(&self, step_id: u8) -> PathBuf {
        let out_root = self.filter_steps.join(step_id.to_string());
        create_dir_all(&out_root).unwrap();
        out_root
    }

    pub fn write_cache<T: Serialize>(&self, obj: &T, path: &str) -> io::Result<()> {
        let out_path = self.cache.join(path);
        create_dir_all(out_path.parent().unwrap())?;
        write_gz(&out_path, obj)
    }

    pub fn write_cache_buf<T: Serialize>(&self, obj: &T, path: &str) -> io::Result<()> {
        let out_path = self.cache.join(path);
        create_dir_all(out_path.parent().unwrap())?;
        write_gz_buf(&out_path, obj)
    }

    pub fn read_csv_objs<T: DeserializeOwned>(
        &self,
        main_path: &str,
        sub_path: &str,
    ) -> ObjIter<T> {
        ObjIter::new(self, main_path, sub_path)
    }

    pub fn iter_cached_qc_locs(&self) -> QcPathIter {
        QcPathIter::new(&self.cache)
    }

    pub fn iter_pruned_qc_locs(&self) -> QcPathIter {
        QcPathIter::new(&self.pruned_cache)
    }

    pub fn get_idmap<E: IdMappedEntity>(&self) -> IdMap {
        E::read(&self.key_stores)
    }

    pub fn get_fix_att<E>(&self) -> Box<[E::T]>
    where
        E: FixedSizeAttribute,
        <E as Entity>::T: FixedAttributeElement,
    {
        E::read(&self.fix_atts)
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

type InIterator<T> = Tqdm<DeserializeRecordsIntoIter<BufReader<flate2::read::GzDecoder<File>>, T>>;

pub struct ObjIter<T>
where
    T: DeserializeOwned,
{
    iterable: InIterator<T>,
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

pub fn read_js_path<T: DeserializeOwned>(fp: &str) -> Result<T, serde_json::Error> {
    let mut js_str = String::new();
    get_gz_buf(fp).read_to_string(&mut js_str).unwrap();
    serde_json::from_str(&js_str)
}

pub fn read_buf_path<T: DeserializeOwned>(fp: &str) -> Result<T, bincode::Error> {
    let mut buf: Vec<u8> = Vec::new();
    get_gz_buf(fp).read_to_end(&mut buf)?;
    bincode::deserialize(&buf)
}

pub fn read_cache<T: DeserializeOwned>(stowage: &Stowage, fname: &str) -> T {
    read_js_path(
        stowage
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
