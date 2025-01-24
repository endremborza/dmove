use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::PathBuf;

use hashbrown::{HashMap, HashSet};

use crate::common::{
    get_type_name, BackendLoading, BigId, Entity, EntityMutableMapperBackend, MainBuilder,
    MappableEntity, MappableEntityTraitMeta, MetaIntegrator, UnsignedNumber,
};
use crate::EntityImmutableMapperBackend;

const ID_TYPE_SIZE: usize = std::mem::size_of::<BigId>();
const ID_RECORD_SIZE: usize = ID_TYPE_SIZE * 2;

type IdRecord = [u8; ID_RECORD_SIZE];

pub struct IdMap {
    map_buffer: PathBuf,
    extensions: Vec<IdRecord>,
    extension_set: HashSet<BigId>,
    pub current_non_null_count: u64,
}
pub struct LoadedIdMap<T>(pub HashMap<BigId, T>);

pub struct Data64MappedEntityBuilder {
    map: IdMap,
    name: String,
}

impl<E> BackendLoading<E> for IdMap
where
    E: Entity,
    <E as Entity>::T: UnsignedNumber,
{
    fn load_backend(path: &PathBuf) -> Self {
        IdMap::new(path.join(E::NAME))
    }
}

impl<E> BackendLoading<E> for LoadedIdMap<E::T>
where
    E: Entity,
    <E as Entity>::T: UnsignedNumber,
{
    fn load_backend(path: &PathBuf) -> Self {
        <IdMap as BackendLoading<E>>::load_backend(path).to_map()
    }
}

impl<E> BackendLoading<E> for Range<E::T>
where
    E: Entity,
    <E as Entity>::T: UnsignedNumber,
{
    fn load_backend(_path: &PathBuf) -> Self {
        let end = <E as Entity>::T::from_usize(<E as Entity>::N);
        let start = <E as Entity>::T::from_usize(0);
        Range { start, end }
    }
}

impl<E> EntityImmutableMapperBackend<E> for LoadedIdMap<E::T>
where
    E: MappableEntity + Entity,
    <E as Entity>::T: UnsignedNumber,
    for<'a> &'a BigId: From<&'a <E as MappableEntity>::KeyType>,
{
    fn get_via_immut(&self, k: &E::KeyType) -> Option<E::T> {
        match self.0.get(k.into()) {
            Some(v) => Some(v.lift()),
            None => None,
        }
    }
}

impl<E> EntityMutableMapperBackend<E> for IdMap
where
    E: MappableEntity + Entity,
    <E as Entity>::T: UnsignedNumber,
    for<'a> &'a BigId: From<&'a <E as MappableEntity>::KeyType>,
{
    fn get_via_mut(&mut self, k: &E::KeyType) -> Option<E::T> {
        match self.get(k.into()) {
            Some(v) => Some(E::T::cast_big_id(v)),
            None => None,
        }
    }
}

impl MetaIntegrator<BigId> for Data64MappedEntityBuilder {
    fn setup(builder: &MainBuilder, name: &str) -> Self {
        let map = IdMap::new(builder.parent_root.join(name));
        Self {
            map,
            name: name.to_string(),
        }
    }

    fn add_elem(&mut self, e: &BigId) {
        self.map.push(*e);
    }

    fn add_elem_owned(&mut self, e: BigId) {
        self.map.push(e);
    }

    fn post(mut self, builder: &mut MainBuilder) {
        self.map.extend();
        let n = self.map.current_non_null_count as usize + 1;
        let camel_name = builder.add_scaled_entity(&self.name, n, false);
        let mappable_type = get_type_name::<BigId>();
        builder
            .meta_elems
            .push(MappableEntityTraitMeta::meta(&camel_name, &mappable_type));
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

    pub fn to_map<T>(&self) -> LoadedIdMap<T>
    where
        T: UnsignedNumber,
    {
        let mut record_buffer = [0; ID_RECORD_SIZE];
        let mut br = BufReader::new(File::open(&self.map_buffer).unwrap());
        let mut out = HashMap::new();
        loop {
            if let Ok(_) = br.read_exact(&mut record_buffer) {
                let val64 = BigId::from_be_bytes(record_buffer[ID_TYPE_SIZE..].try_into().unwrap());
                out.insert(
                    BigId::from_be_bytes(record_buffer[..ID_TYPE_SIZE].try_into().unwrap()),
                    T::cast_big_id(val64),
                );
            } else {
                break;
            }
        }
        LoadedIdMap(out)
    }
}

fn file_record_count(file: &File) -> u64 {
    file.metadata().unwrap().len() / (ID_RECORD_SIZE as u64)
}
