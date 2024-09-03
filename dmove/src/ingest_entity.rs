use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

use hashbrown::{HashMap, HashSet};

use crate::common::{
    get_uscale, Entity, MainBuilder, MetaInput, MetaIntegrator, SomeElement, BASIC_TRAIT,
};
pub type BigId = u64;
const ID_TYPE_SIZE: usize = std::mem::size_of::<BigId>();
const ID_RECORD_SIZE: usize = ID_TYPE_SIZE * 2;

pub type LoadedIdMap = HashMap<BigId, BigId>;
type IdRecord = [u8; ID_RECORD_SIZE];

pub struct IdMap {
    map_buffer: PathBuf,
    extensions: Vec<IdRecord>,
    extension_set: HashSet<BigId>,
    pub current_non_null_count: u64,
}

pub struct IdMappedEntityBuilder {
    map: IdMap,
}

pub struct EntityBuilder {
    n: usize,
}

pub trait IdMappedEntity: Entity {
    fn read(parent_dir: &PathBuf) -> IdMap {
        IdMap::new(parent_dir.join(Self::NAME))
    }
}

impl MetaIntegrator<BigId> for IdMappedEntityBuilder {
    fn setup(builder: &MainBuilder, name: &str) -> Self {
        let map = IdMap::new(builder.parent_root.join(name));
        Self { map }
    }

    fn add_elem(&mut self, e: &BigId) {
        self.map.push(*e);
    }

    fn add_elem_owned(&mut self, e: BigId) {
        self.map.push(e);
    }

    fn post(mut self) -> MetaInput<()> {
        self.map.extend();
        let n = self.map.current_non_null_count as usize + 1;
        MetaInput {
            n,
            type_overwrite: Some(get_uscale(n)),
            meta_lines_input: (),
        }
    }
}

impl MetaIntegrator<usize> for EntityBuilder {
    fn setup(_builder: &MainBuilder, _name: &str) -> Self {
        Self { n: 0 }
    }

    fn add_elem(&mut self, _e: &usize) {
        self.n += 1;
    }

    fn post(self) -> MetaInput<()> {
        let n = self.n;
        MetaInput {
            n,
            type_overwrite: Some(get_uscale(n)),
            meta_lines_input: (),
        }
    }
}

impl SomeElement<IdMappedEntityBuilder> for BigId {
    type MetaInputType = ();
    fn main_trait() -> &'static str {
        stringify!(IdMappedEntity)
    }
}

impl SomeElement<EntityBuilder> for usize {
    type MetaInputType = ();
    fn main_trait() -> &'static str {
        BASIC_TRAIT
    }
}

//NOTE: IDS start with one
impl IdMap {
    pub fn new<T>(id_map_path: T) -> Self
    where
        PathBuf: From<T>,
    {
        let map_buffer = PathBuf::from(id_map_path);
        let mut current_non_null_count: u64 = 0;
        if !map_buffer.is_file() {
            File::create(&map_buffer).unwrap();
        } else {
            current_non_null_count = file_record_count(&File::open(&map_buffer).unwrap());
        }
        Self {
            map_buffer,
            extensions: vec![],
            current_non_null_count,
            extension_set: HashSet::new(),
        }
    }

    pub fn extend(&mut self) {
        let mut full_record_vec = Vec::new();
        let mut record_buffer = [0; ID_RECORD_SIZE];
        let mut br = BufReader::new(File::open(&self.map_buffer).unwrap());
        loop {
            if let Ok(_) = br.read_exact(&mut record_buffer) {
                full_record_vec.push(record_buffer.clone());
            } else {
                break;
            }
        }

        full_record_vec.extend(&self.extensions);
        full_record_vec.sort();
        let mut hfile = File::create(&self.map_buffer).unwrap();
        for rec in full_record_vec {
            hfile.write(&rec).unwrap();
        }
    }

    pub fn push(&mut self, id: BigId) {
        if let None = self.get(&id) {
            if !self.extension_set.contains(&id) {
                self.extension_set.insert(id);
                self.current_non_null_count += 1; // determined here that first id is 1 not 0
                let mut rec = [0; ID_RECORD_SIZE];
                rec[0..ID_TYPE_SIZE].copy_from_slice(&id.to_be_bytes());
                rec[ID_TYPE_SIZE..ID_RECORD_SIZE]
                    .copy_from_slice(&self.current_non_null_count.to_be_bytes());
                self.extensions.push(rec);
            }
        }
    }
    pub fn push_many<'a, I>(&mut self, iter: I)
    where
        I: Iterator<Item = &'a BigId>,
    {
        iter.for_each(|id| self.push(*id))
    }

    pub fn get(&mut self, k: &BigId) -> Option<BigId> {
        let hfile = File::open(&self.map_buffer).unwrap();
        let mut br = BufReader::new(&hfile);
        const REC_U64: u64 = ID_RECORD_SIZE as u64;

        let mut key_buffer = [0; ID_TYPE_SIZE];
        let mut value_buffer = [0; ID_TYPE_SIZE];
        let mut seek_blocks_l: u64 = 0;
        let mut seek_blocks_r: u64 = file_record_count(&hfile);
        let mut seek_mid = (seek_blocks_r + seek_blocks_l) / 2;
        loop {
            br.seek(SeekFrom::Start(seek_mid * REC_U64)).unwrap();
            if let Err(_e) = br.read_exact(&mut key_buffer) {
                break;
            }
            let ckey = u64::from_be_bytes(key_buffer);
            if ckey < *k {
                seek_blocks_l = seek_mid + 1;
            } else if ckey > *k {
                if seek_mid == 0 {
                    break;
                }
                seek_blocks_r = seek_mid - 1;
            } else {
                br.read_exact(&mut value_buffer).unwrap();
                return Some(BigId::from_be_bytes(value_buffer));
            }
            if seek_blocks_l > seek_blocks_r {
                break;
            } else {
                seek_mid = (seek_blocks_r + seek_blocks_l) / 2;
            }
        }
        None
    }

    pub fn iter_ids(&self, include_unknown: bool) -> std::ops::Range<BigId> {
        let start = if include_unknown { 0 } else { 1 };
        start..self.current_non_null_count + 1
    }

    pub fn to_map(&self) -> LoadedIdMap {
        let mut record_buffer = [0; ID_RECORD_SIZE];
        let mut br = BufReader::new(File::open(&self.map_buffer).unwrap());
        let mut out = HashMap::new();
        loop {
            if let Ok(_) = br.read_exact(&mut record_buffer) {
                out.insert(
                    BigId::from_be_bytes(record_buffer[..ID_TYPE_SIZE].try_into().unwrap()),
                    BigId::from_be_bytes(record_buffer[ID_TYPE_SIZE..].try_into().unwrap()),
                );
            } else {
                break;
            }
        }
        out
    }
}

fn file_record_count(file: &File) -> u64 {
    file.metadata().unwrap().len() / (ID_RECORD_SIZE as u64)
}
