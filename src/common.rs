use std::{
    fs::{create_dir_all, File},
    io::{self, BufReader, Write},
    path::{Path, PathBuf},
};

use csv::{DeserializeRecordsIntoIter, Reader, ReaderBuilder};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use tqdm::*;

pub type BigId = u64;
pub type StowReader = Reader<BufReader<GzDecoder<File>>>;

pub const WORKS: &str = "works";
pub const AUTHORS: &str = "authors";
pub const SOURCES: &str = "sources";
pub const INSTS: &str = "institutions";
pub const COUNTRIES: &str = "countries";
pub const CONCEPTS: &str = "concepts";

pub const MAIN_CONCEPTS: &str = "main-concepts";
pub const SUB_CONCEPTS: &str = "sub-concepts";
pub const QS: &str = "qs";

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
}

#[derive(Deserialize)]
pub struct IdStruct {
    pub id: Option<String>,
}

pub trait ParsedId {
    fn get_parsed_id(&self) -> BigId;
}

pub trait ParentGetter {
    fn parent(&self) -> &str;
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

impl Stowage {
    pathfields_fn!(
        entity_csvs => "entity-csvs",
        filter_steps => "filter-steps",
        key_stores => "key-stores",
        fix_atts => "fix-atts",
        var_atts => "var-atts",
        cache => "cache",
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

    pub fn get_fix_writer(&self, att_name: &str) -> File {
        File::create(self.fix_atts.join(att_name)).unwrap()
    }

    pub fn get_fix_reader(&self, att_name: &str) -> BufReader<File> {
        BufReader::new(File::open(self.fix_atts.join(att_name)).unwrap())
    }

    pub fn write_cache<T: Serialize>(&self, obj: &T, path: &str) -> io::Result<()> {
        let out_path = self.cache.join(path);
        create_dir_all(out_path.parent().unwrap())?;
        write_gz(&out_path, obj)
    }

    pub fn read_csv_objs<T: DeserializeOwned>(
        &self,
        main_path: &str,
        sub_path: &str,
    ) -> ObjIter<T> {
        ObjIter::new(self, main_path, sub_path)
    }
}

type InIterator<T> = Tqdm<
    Result<T, csv::Error>,
    DeserializeRecordsIntoIter<BufReader<flate2::read::GzDecoder<File>>, T>,
>;

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

fn write_gz<T>(out_path: &Path, obj: &T) -> io::Result<()>
where
    T: Serialize,
{
    let out_file = File::create(out_path.with_extension("json.gz").to_str().unwrap())?;
    let encoder = GzEncoder::new(out_file, Compression::default());
    let mut writer = std::io::BufWriter::new(encoder);
    writer
        .write_all(serde_json::to_string(&obj).unwrap().as_bytes())
        .unwrap();
    return Ok(());
}
