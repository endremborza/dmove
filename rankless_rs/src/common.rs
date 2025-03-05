use std::fmt::{Debug, Display};
use std::io::{prelude::*, BufWriter};
use std::ops::Range;
use std::sync::{Arc, Mutex, MutexGuard};
use std::{
    fs::{create_dir_all, read_dir, File},
    io::{self, BufReader, Write},
    path::{Path, PathBuf},
};

use csv::{DeserializeRecordsIntoIter, Reader, ReaderBuilder};
use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use hashbrown::{HashMap, HashSet};
use serde::Deserialize;
use serde::{de::DeserializeOwned, Serialize};
use tqdm::{Iter, Tqdm};

use dmove::{
    BackendLoading, BigId, CompactEntity, Entity, FixAttIterator, FixWriteSizeEntity, InitEmpty,
    LoadedIdMap, MainBuilder, MappableEntity, MarkedAttribute, MetaIntegrator, NamespacedEntity,
    UnsignedNumber, VarAttIterator, VarBox, VarSizedAttributeElement, VariableSizeAttribute,
    VattArrPair, VattReadingMap, ET, MAA,
};

pub type StowReader = Reader<BufReader<GzDecoder<File>>>;
pub type BeS<M, E> = <M as BackendSelector<E>>::BE;
pub type NET<E> = <E as NumberedEntity>::T;

type InIterator<T> = Tqdm<DeserializeRecordsIntoIter<BufReader<flate2::read::GzDecoder<File>>, T>>;

pub const MAIN_NAME: &str = "main";
pub const BUILD_LOC: &str = "qc-builds";
pub const SEM_DIR: &str = "semantic-ids";
// pub const A_STAT_PATH: &str = "attribute-statics";
// pub const QC_CONF: &str = "qc-specs";

pub const ID_PREFIX: &str = "https://openalex.org/";

pub struct NameMarker;
pub struct NameExtensionMarker;
pub struct DoiMarker;
pub struct SemanticIdMarker;
pub struct MainWorkMarker;
pub struct WorkCountMarker;
pub struct CiteCountMarker;
pub struct RefSubfieldsArrayMarker;
pub struct CitSubfieldsArrayMarker;
pub struct YearlyPapersMarker;
pub struct YearlyCitationsMarker;
pub struct InstRelMarker;
pub struct Top3PaperSfMarker;
pub struct Top3CitingSfMarker;
pub struct Top3PaperTopicMarker;
pub struct Top3AuthorMarker;
pub struct Top3CitingTopicMarker;
pub struct Top3JournalMarker;
pub struct Top3AffCountryMarker;

#[macro_export]
macro_rules! add_parsed_id_traits {
    ($($struct:ident),*) => {
        $(impl ParsedId for $struct {
            fn get_parsed_id(&self) -> BigId {
                oa_id_parse(self.id.as_ref().unwrap())
            }
        }
        )*
    };
}

#[macro_export]
macro_rules! add_strict_parsed_id_traits {
    ($($struct:ident),*) => {
        $(impl ParsedId for $struct {
            fn get_parsed_id(&self) -> BigId {
                oa_id_parse(&self.id)
            }
        }
        )*
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

//TODO: wet with interfacing
#[macro_export]
macro_rules! make_interface_struct {
    ($IT:ident, $($e_key:ident > $e_t:ty),*;$($f_key:ident => $f_t:ty),*; $($v_key:ident -> $v_t:ty),*; $($m_key:ident >> $m_t:ty),*) => {
        struct $IT {
            $($e_key: BeS<QuickAttPair, MAA<$e_t, MainWorkMarker>>,)*
            $($f_key: BeS<QuickestBox, $f_t>,)*
            $($v_key: BeS<QuickAttPair, $v_t>,)*
            $($m_key: BeS<QuickMap, $m_t>,)*
        }

        impl $IT {
            fn new(stowage: Arc<Stowage>) -> Self {
                $(
                    let stowage_clone = Arc::clone(&stowage);
                    let $e_key = std::thread::spawn( move || {
                        <$e_t as WorkLoader>::load_work_interface(stowage_clone)
                    });
                )*
                $(
                    let stowage_clone = Arc::clone(&stowage);
                    let $f_key = std::thread::spawn( move || {
                        stowage_clone.get_entity_interface::<$f_t, QuickestBox>()
                    });
                )*
                $(
                    let stowage_clone = Arc::clone(&stowage);
                    let $v_key = std::thread::spawn( move || {
                        stowage_clone.get_entity_interface::<$v_t, QuickAttPair>()
                    });
                )*
                $(
                    let stowage_clone = Arc::clone(&stowage);
                    let $m_key = std::thread::spawn( move || {
                        stowage_clone.get_entity_interface::<$m_t, QuickMap>()
                    });
                )*
                Self {
                    $($e_key: $e_key.join().expect("Thread panicked")),*,
                    $($f_key: $f_key.join().expect("Thread panicked")),*,
                    $($v_key: $v_key.join().expect("Thread panicked")),*,
                    $($m_key: $m_key.join().expect("Thread panicked")),*
                }
            }

            fn _fake() -> Self {
                    Self {
                        $($f_key: Vec::new().into()),*,
                        $($e_key: VattArrPair::empty()),*,
                        $($v_key: VattArrPair::empty()),*,
                        $($m_key: HashMap::new().into()),*
                    }
            }
        }
    };
}

macro_rules! pathfields_fn {
    ($struct:ident, $($k:ident),*) => {

        pub struct $struct {
            $(pub $k: PathBuf,)*
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

pub struct Stowage {
    pub paths: PathCollection,
    current_ns: String,
    builder: Option<Mutex<MainBuilder>>,
}

pub struct ObjIter<T>
where
    T: DeserializeOwned,
{
    iterable: InIterator<T>,
}

//TODO/clarity: this is sort of a mess - could be just generic types
pub struct QuickestNumbered {}
pub struct QuickMap {}
pub struct QuickestBox {}
pub struct QuickAttPair {}
pub struct QuickestVBox {}
pub struct VarFile {}
pub struct ReadIter {}
pub struct ReadFixIter {}
pub struct IterCompactElement {}

#[derive(Deserialize)]
pub struct SemanticElem {
    pub id: BigId,
    pub semantic_id: String,
}

pub trait BackendSelector<E>
where
    E: Entity,
{
    type BE;
}

pub trait MarkedBackendLoader<Mark>: Entity {
    type BE;

    fn load(stowage: &Stowage) -> Self::BE;
}

pub trait WorkLoader: MarkedAttribute<MainWorkMarker>
where
    MAA<Self, MainWorkMarker>: CompactEntity + VariableSizeAttribute + NamespacedEntity,
    ET<MAA<Self, MainWorkMarker>>: VarSizedAttributeElement,
{
    fn load_work_interface(stowage: Arc<Stowage>) -> BeS<QuickAttPair, MAA<Self, MainWorkMarker>> {
        stowage.get_entity_interface::<MAA<Self, MainWorkMarker>, QuickAttPair>()
    }
}

pub trait NumberedEntity: MappableEntity<KeyType = BigId> {
    type T: UnsignedNumber + DeserializeOwned + Serialize + Ord + Copy + Display;
}

pub trait MainEntity: NumberedEntity + Entity<T = NET<Self>> {}

pathfields_fn!(PathCollection, entity_csvs, filter_steps, cache);

pub trait ParsedId {
    fn get_parsed_id(&self) -> BigId;
}

impl Stowage {
    pub fn new(root_path: &str) -> Self {
        Self {
            paths: PathCollection::new(root_path),
            current_ns: "".to_string(),
            builder: None,
        }
    }

    pub fn set_namespace(&mut self, ns: &'static str) {
        self.current_ns = ns.to_string();
        let path = self.path_from_ns(&self.current_ns);
        create_dir_all(&path).unwrap();
        self.builder = Some(Mutex::new(MainBuilder::new(&path)));
    }

    pub fn write_code(&self) -> io::Result<usize> {
        let suffix = self.current_ns.replace("-", "_");
        self.mu_bu().write_code(&code_path(&suffix))
    }

    pub fn get_out_csv_path(&self) -> &str {
        self.paths.entity_csvs.to_str().unwrap()
    }

    pub fn get_filter_dir(&self, step_id: u8) -> PathBuf {
        let out_root = self.paths.filter_steps.join(step_id.to_string());
        create_dir_all(&out_root).unwrap();
        out_root
    }

    pub fn get_last_filter(&self, entity_type: &str) -> Option<HashSet<BigId>> {
        let mut out_path = None;

        if !self.paths.entity_csvs.join(entity_type).exists() {
            println!("no such type {entity_type}");
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

    pub fn add_iter_owned<B, I, T>(&self, iter: I, name_o: Option<&str>)
    where
        B: MetaIntegrator<T>,
        I: Iterator<Item = T>,
    {
        let name = name_o.unwrap(); //TODO - temp
        B::add_iter_owned(&self.builder.as_ref().unwrap(), iter, &name);
        self.mu_bu().declare_ns(&name, &self.current_ns);
    }

    pub fn declare_link<S: Entity, T: Entity>(&self, name: &str) {
        self.mu_bu().declare_link::<S, T>(name);
    }

    pub fn declare<E, Marker>(&self, name: &str) {
        self.mu_bu().declare_marked_attribute::<E, Marker>(&name);
    }

    pub fn declare_iter<B, I, E, S, M>(&self, iter: I, name: &str)
    where
        B: MetaIntegrator<E>,
        I: Iterator<Item = E>,
    {
        self.add_iter_owned::<B, I, E>(iter, Some(name));
        self.declare::<S, M>(name)
    }

    pub fn read_csv_objs<T: DeserializeOwned>(
        &self,
        main_path: &str,
        sub_path: &str,
    ) -> ObjIter<T> {
        read_deser_obj::<T>(&self.paths.entity_csvs, main_path, sub_path)
    }

    pub fn get_entity_interface<E, Marker>(&self) -> Marker::BE
    where
        Marker: BackendSelector<E>,
        E: MarkedBackendLoader<Marker, BE = Marker::BE>,
    {
        E::load(self)
    }

    pub fn get_marked_interface<E, AttMarker, BeMarker>(&self) -> BeMarker::BE
    where
        E: Entity + MarkedAttribute<AttMarker>,
        BeMarker: BackendSelector<E::AttributeEntity>,
        MAA<E, AttMarker>: MarkedBackendLoader<BeMarker, BE = BeMarker::BE>,
    {
        self.get_entity_interface::<MAA<E, AttMarker>, BeMarker>()
    }

    pub fn path_from_ns(&self, ns: &str) -> PathBuf {
        self.paths.entity_csvs.parent().unwrap().join(ns)
    }

    pub fn mu_bu(&self) -> MutexGuard<MainBuilder> {
        self.builder.as_ref().unwrap().lock().unwrap()
    }
}

impl<E> NumberedEntity for E
where
    E: MappableEntity<KeyType = BigId>,
    ET<E>: UnsignedNumber + Serialize + DeserializeOwned,
{
    type T = ET<E>;
}

impl<E> MainEntity for E where E: Entity<T = NET<E>> + NumberedEntity {}

impl<T> ObjIter<T>
where
    T: DeserializeOwned,
{
    pub fn new(reader: StowReader, main: &str, sub: &str) -> Self {
        let iterable = reader
            .into_deserialize::<T>()
            .tqdm()
            .desc(Some(format!("reading {} / {}", main, sub)));
        Self { iterable }
    }
}

impl<E, BeMarker> MarkedBackendLoader<BeMarker> for E
where
    E: NamespacedEntity,
    BeMarker: BackendSelector<E>,
    BeMarker::BE: BackendLoading<E>,
{
    type BE = BeMarker::BE;
    fn load(stowage: &Stowage) -> Self::BE {
        let path = stowage.path_from_ns(E::NS);
        let now = std::time::Instant::now();
        let out = BeMarker::BE::load_backend(&path);
        println!(
            "loaded {} from {path:?} in {}s",
            E::NAME,
            now.elapsed().as_secs()
        );
        out
    }
}

impl<E> BackendSelector<E> for QuickestNumbered
where
    E: MainEntity,
{
    type BE = LoadedIdMap<NET<E>>;
}

impl<E> BackendSelector<E> for QuickMap
where
    E: FixWriteSizeEntity + MappableEntity,
{
    type BE = HashMap<<E as MappableEntity>::KeyType, E::T>;
}

impl<E> BackendSelector<E> for QuickestBox
where
    E: CompactEntity,
{
    type BE = Box<[E::T]>;
}

impl<E> BackendSelector<E> for QuickestVBox
where
    E: CompactEntity,
{
    type BE = VarBox<E::T>;
}

impl<E> BackendSelector<E> for QuickAttPair
where
    E: CompactEntity + VariableSizeAttribute,
    E::T: VarSizedAttributeElement,
{
    type BE = VattArrPair<E, u32>;
}

impl<E> BackendSelector<E> for VarFile
where
    E: CompactEntity + VariableSizeAttribute,
    E::T: VarSizedAttributeElement,
{
    type BE = VattReadingMap<E>;
}

impl<E> BackendSelector<E> for ReadIter
where
    E: Entity + VariableSizeAttribute,
    <E as Entity>::T: VarSizedAttributeElement,
{
    type BE = VarAttIterator<E>;
}

impl<E> BackendSelector<E> for ReadFixIter
where
    E: FixWriteSizeEntity,
{
    type BE = FixAttIterator<E>;
}

impl<E> BackendSelector<E> for IterCompactElement
where
    E: CompactEntity,
{
    type BE = Range<E::T>;
}

impl<E> WorkLoader for E
where
    E: MarkedAttribute<MainWorkMarker>,
    MAA<Self, MainWorkMarker>: CompactEntity + VariableSizeAttribute + NamespacedEntity,
    ET<MAA<Self, MainWorkMarker>>: VarSizedAttributeElement,
{
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

pub fn get_gz_buf<P>(file_name: P) -> BufReader<GzDecoder<File>>
where
    P: AsRef<Path>,
{
    let file = File::open(file_name).unwrap();
    let gz_decoder = GzDecoder::new(file);
    BufReader::new(gz_decoder)
}

pub fn get_gz_bufw<P>(file_name: P) -> BufWriter<GzEncoder<File>>
where
    P: AsRef<Path> + Debug,
{
    let msg = format!("could not create {file_name:?}");
    let file = File::create(file_name).expect(&msg);
    let encoder = GzEncoder::new(file, Compression::default());
    std::io::BufWriter::new(encoder)
}

pub fn read_buf_path<T, P>(fp: P) -> Result<T, bincode::Error>
where
    T: DeserializeOwned,
    P: AsRef<Path>,
{
    bincode::deserialize_from(&mut get_gz_buf(fp))
}

pub fn write_buf_path<T, P>(obj: T, fp: P) -> Result<(), Box<bincode::ErrorKind>>
where
    T: Serialize,
    P: AsRef<Path> + Debug,
{
    bincode::serialize_into(get_gz_bufw(fp), &obj)
}

pub fn read_json_path<T, P>(fp: P) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
    P: AsRef<Path>,
{
    serde_json::from_reader(&mut get_gz_buf(fp))
}

pub fn write_json_path<T, P>(obj: T, fp: P) -> Result<(), serde_json::Error>
where
    T: Serialize,
    P: AsRef<Path> + Debug,
{
    serde_json::to_writer(get_gz_bufw(fp), &obj)
}

pub fn short_string_to_u64(input: &str) -> BigId {
    let mut padded_input = [0u8; 8];
    let l = input.len().min(8);
    padded_input[..l].copy_from_slice(&input.as_bytes()[..l]);
    BigId::from_le_bytes(padded_input)
}

pub fn init_empty_slice<E: Entity, T: InitEmpty>() -> Box<[T]> {
    (0..E::N + 1)
        .map(|_| T::init_empty())
        .collect::<Vec<T>>()
        .into()
}

pub fn code_path(suffix: &str) -> String {
    //TODO: this WET knows gen path :(
    format!("rankless_rs/src/gen/{}.rs", suffix)
}

fn read_deser_obj<T: DeserializeOwned>(root: &Path, main_path: &str, sub_path: &str) -> ObjIter<T> {
    let gz_buf = get_gz_buf(
        root.join(main_path)
            .join(sub_path)
            .with_extension("csv.gz")
            .to_str()
            .unwrap(),
    );
    let reader = ReaderBuilder::new().from_reader(gz_buf);
    ObjIter::new(reader, main_path, sub_path)
}
