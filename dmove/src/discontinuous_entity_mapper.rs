use std::fs::File;
use std::hash::Hash;
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::marker::PhantomData;
use std::path::PathBuf;

use hashbrown::{HashMap, HashSet};

use crate::common::{
    get_type_name, BackendLoading, Entity, EntityMutableMapperBackend, MainBuilder, MappableEntity,
    MappableEntityTraitMeta, MetaIntegrator, UnsignedNumber,
};
use crate::{EntityImmutableMapperBackend, FixedAttributeElement};

const MAX_MAP_BUF: usize = 0x100;

pub struct UniqueMap<K, V> {
    map_path: PathBuf,
    extensions: Vec<u8>,
    extension_set: HashSet<K>,
    main_buf: [u8; MAX_MAP_BUF],
    key_size: usize,
    full_size: usize,
    p: PhantomData<V>,
}

pub struct DiscoMapEntityBuilder<K, V> {
    map: UniqueMap<K, V>,
    name: String,
}

impl<E, K> BackendLoading<E> for UniqueMap<K, E::T>
where
    E: Entity,
    <E as Entity>::T: FixedAttributeElement,
    K: FixedAttributeElement + Hash + Eq,
{
    fn load_backend(path: &PathBuf) -> Self {
        UniqueMap::new(path.join(E::NAME))
    }
}

// impl<E, K> BackendLoading<E> for HashMap<K, E::T>
// where
//     E: Entity,
//     <E as Entity>::T: FixedAttributeElement,
//     K: FixedAttributeElement + Hash + Eq,
// {
//     default fn load_backend(path: &PathBuf) -> Self {
//         <UniqueMap<K, E::T> as BackendLoading<E>>::load_backend(path).to_map()
//     }
// }

impl<E, MM> EntityMutableMapperBackend<E, MM> for UniqueMap<E::KeyType, E::T>
where
    E: MappableEntity<MM> + Entity,
    <E as Entity>::T: UnsignedNumber,
    <E as MappableEntity<MM>>::KeyType: Hash + Eq,
    E::T: FixedAttributeElement,
    E::KeyType: FixedAttributeElement,
{
    fn get_via_mut(&mut self, k: &E::KeyType) -> Option<E::T> {
        self.get(k)
    }
}

impl<E, MM> EntityImmutableMapperBackend<E, MM> for HashMap<E::KeyType, E::T>
where
    E: MappableEntity<MM> + Entity,
    <E as Entity>::T: UnsignedNumber,
    <E as MappableEntity<MM>>::KeyType: Hash + Eq,
{
    fn get_via_immut(&self, k: &E::KeyType) -> Option<E::T> {
        match self.get(k) {
            Some(v) => Some(v.lift()),
            None => None,
        }
    }
}

impl<K, V> MetaIntegrator<(K, V)> for DiscoMapEntityBuilder<K, V>
where
    K: FixedAttributeElement + Eq + Hash,
    V: FixedAttributeElement,
    (K, V): Copy,
{
    fn setup(builder: &MainBuilder, name: &str) -> Self {
        let map = UniqueMap::<K, V>::new(builder.parent_root.join(name));
        Self {
            map,
            name: name.to_string(),
        }
    }

    fn add_elem(&mut self, e: &(K, V)) {
        self.map.push(*e);
    }

    fn add_elem_owned(&mut self, e: (K, V)) {
        self.map.push(e);
    }

    fn post(mut self, builder: &mut MainBuilder) {
        self.map.extend();
        let n =
            file_record_count(&File::open(self.map.map_path).unwrap(), self.map.full_size) as usize;
        let camel_name = builder.add_scaled_compact_entity(&self.name, n);
        let mappable_type = get_type_name::<K>();
        builder.meta_elems.push(MappableEntityTraitMeta::meta(
            &camel_name,
            &mappable_type,
            &mappable_type,
        ));
    }
}

impl<K, V> UniqueMap<K, V>
where
    K: FixedAttributeElement + Hash + Eq,
    V: FixedAttributeElement,
{
    pub fn new<T>(id_map_path: T) -> Self
    where
        PathBuf: From<T>,
    {
        let map_path = PathBuf::from(id_map_path);
        if !map_path.is_file() {
            File::create(&map_path).unwrap();
        }
        Self {
            map_path,
            extensions: vec![],
            extension_set: HashSet::new(),
            key_size: size_of::<K>(),
            full_size: size_of::<V>() + size_of::<K>(),
            main_buf: [0; MAX_MAP_BUF],
            p: PhantomData,
        }
    }

    pub fn extend(&mut self) {
        let mut full_record_vec: Vec<u8> = Vec::new();
        let mut record_buffer = &mut self.main_buf[..self.full_size];
        let mut br = BufReader::new(File::open(&self.map_path).unwrap());
        loop {
            if let Ok(_) = br.read_exact(&mut record_buffer) {
                full_record_vec.extend(record_buffer.iter());
            } else {
                break;
            }
        }

        full_record_vec.extend(&self.extensions);
        let sorted_record_vec = chunk_sort(full_record_vec, &self.full_size);
        File::create(&self.map_path)
            .unwrap()
            .write(&sorted_record_vec)
            .unwrap();
    }

    pub fn push(&mut self, e: (K, V)) {
        let id = e.0;
        if let None = self.get(&id) {
            if !self.extension_set.contains(&id) {
                self.extensions.extend(id.to_bytes());
                self.extensions.extend(e.1.to_bytes());
                self.extension_set.insert(id);
            }
        }
    }

    pub fn get(&mut self, k: &K) -> Option<V> {
        let hfile = File::open(&self.map_path).unwrap();
        let mut br = BufReader::new(&hfile); //TODO: this might be too slow here
                                             //test it!!
        let k_arr = k.to_bytes();
        let rec_u64: u64 = self.full_size as u64;

        let (key_buffer, value_buffer) =
            self.main_buf[..self.full_size].split_at_mut(self.key_size);
        // let mut key_buffer = &mut self.main_buf[..self.key_size];
        // let mut value_buffer = &mut self.main_buf[self.key_size..self.full_size];
        let mut seek_blocks_l: u64 = 0;
        let mut seek_blocks_r: u64 = file_record_count(&hfile, self.full_size);
        let mut seek_mid = (seek_blocks_r + seek_blocks_l) / 2;
        'outer: loop {
            br.seek(SeekFrom::Start(seek_mid * rec_u64)).unwrap();
            if let Err(_e) = br.read_exact(key_buffer) {
                break;
            }
            for i in 0..self.key_size {
                if key_buffer[i] < k_arr[i] {
                    seek_blocks_l = seek_mid + 1;
                    break;
                } else if key_buffer[i] > k_arr[i] {
                    if seek_mid == 0 {
                        break 'outer;
                    }
                    seek_blocks_r = seek_mid - 1;
                    break;
                } else if i == (self.key_size - 1) {
                    br.read_exact(value_buffer).unwrap();
                    return Some(V::from_bytes(value_buffer));
                }
            }
            if seek_blocks_l > seek_blocks_r {
                break;
            } else {
                seek_mid = (seek_blocks_r + seek_blocks_l) / 2;
            }
        }
        None
    }

    pub fn to_map(&mut self) -> HashMap<K, V> {
        let mut record_buffer = &mut self.main_buf[..self.full_size];
        let mut br = BufReader::new(File::open(&self.map_path).unwrap());
        let mut out = HashMap::new();
        loop {
            if let Ok(_) = br.read_exact(&mut record_buffer) {
                let (karr, varr) = record_buffer.split_at(self.key_size);
                out.insert(K::from_bytes(karr), V::from_bytes(varr));
            } else {
                break;
            }
        }
        out
    }
}

//TODO these _must_ be unit tested
fn chunk_sort(a: Vec<u8>, size: &usize) -> Vec<u8> {
    if a.len() > *size {
        let mid = (a.len() / *size) / 2 * size;
        let (a1, a2) = a.split_at(mid);
        return merge_chunks(
            chunk_sort(a1.to_vec(), size),
            chunk_sort(a2.to_vec(), size),
            size,
        );
    }
    a
}

fn merge_chunks(a1: Vec<u8>, a2: Vec<u8>, size: &usize) -> Vec<u8> {
    let mut out = Vec::new();
    let (mut li, mut ri): (usize, usize) = (0, 0);
    while (li < a1.len()) && (ri < a2.len()) {
        for ii in 0..*size {
            if a1[li + ii] < a2[ri + ii] {
                out.extend(a1[li..(li + *size)].iter());
                li += *size;
                break;
            } else if a1[li + ii] > a2[ri + ii] {
                out.extend(a2[ri..(ri + *size)].iter());
                ri += *size;
                break;
            }
        }
    }
    out
}

fn file_record_count(file: &File, record_size: usize) -> u64 {
    file.metadata().unwrap().len() / (record_size as u64)
}
