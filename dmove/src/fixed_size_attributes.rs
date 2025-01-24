use std::{
    fs::File,
    io::{self, BufReader, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    path::PathBuf,
};

use crate::{
    common::{
        get_type_name, get_uscale, ByteFixArrayInterface, Entity, MainBuilder, MappableEntity,
        MetaIntegrator, MAX_FIXBUF,
    },
    BackendLoading, CompactEntity, EntityImmutableRefMapperBackend, EntityMutableMapperBackend,
    UnsignedNumber,
};

pub struct FixAttBuilder {
    file: File,
    n: usize,
    name: String,
}

pub struct DowncastingBuilder {
    arr: Vec<usize>,
    max: usize,
    name: String,
}

pub struct FixAttIterator<E>
where
    E: FixWriteSizeEntity,
{
    file: File,
    buf: [u8; MAX_FIXBUF],
    p: PhantomData<E>,
}

pub trait FixWriteSizeEntity: Entity {
    const WS: usize;
    type FWT: ByteFixArrayInterface;
}

impl<E> FixWriteSizeEntity for E
where
    E: Entity,
    E::T: ByteFixArrayInterface,
{
    const WS: usize = E::T::S;
    type FWT = E::T;
}

impl<E> EntityMutableMapperBackend<E> for File
where
    E: Entity + MappableEntity<KeyType = usize>,
{
    fn get_via_mut(&mut self, k: &<E as MappableEntity>::KeyType) -> Option<<E as Entity>::T> {
        let pos = SeekFrom::Start(*k as u64);
        self.seek(pos).unwrap();
        todo!()
    }
}

impl<E, V> BackendLoading<E> for Box<[V]>
where
    E: FixWriteSizeEntity<FWT = V> + Entity<T = V>,
    V: ByteFixArrayInterface,
{
    fn load_backend(path: &PathBuf) -> Self {
        let mut out = Vec::new();
        let mut br = BufReader::new(File::open(path.join(E::NAME)).unwrap());
        // let size: usize = std::mem::size_of::<E::T>();
        // const SIZE: usize = std::mem::size_of::<<Self as Entity>::T>();
        let size: usize = E::WS;
        let mut buf: [u8; MAX_FIXBUF] = [0; MAX_FIXBUF];
        while let Ok(_) = br.read_exact(&mut buf[..size]) {
            out.push(E::FWT::from_fbytes(&buf[..size]));
        }
        out.into()
    }
}

impl<E> BackendLoading<E> for FixAttIterator<E>
where
    E: FixWriteSizeEntity,
{
    fn load_backend(path: &PathBuf) -> Self {
        let file = File::open(path.join(E::NAME)).unwrap();
        Self {
            file,
            buf: [0; MAX_FIXBUF],
            p: PhantomData,
        }
    }
}

impl<T> MetaIntegrator<T> for FixAttBuilder
where
    T: ByteFixArrayInterface,
{
    fn setup(builder: &MainBuilder, name: &str) -> Self {
        let file = File::create(builder.parent_root.join(name)).unwrap();
        Self {
            n: 0,
            file,
            name: name.to_string(),
        }
    }

    fn add_elem(&mut self, e: &T) {
        self.file.write(&e.to_fbytes()).unwrap();
        self.n += 1;
    }
    fn post(self, builder: &mut MainBuilder) {
        builder.add_simple_etrait(&self.name, &get_type_name::<T>(), self.n, true);
    }
}

impl MetaIntegrator<usize> for DowncastingBuilder {
    fn setup(_builder: &MainBuilder, name: &str) -> Self {
        Self {
            arr: Vec::new(),
            max: 0,
            name: name.to_string(),
        }
    }

    fn add_elem(&mut self, e: &usize) {
        if *e > self.max {
            self.max = *e;
        }
        self.arr.push(*e)
    }

    fn post(self, builder: &mut MainBuilder) {
        let n = self.max;
        let scale_name = get_uscale(n);
        builder.add_simple_etrait(&self.name, &scale_name, self.arr.len(), true);

        let name = &self.name;
        let arr = self.arr;
        crate::common::downcast_fun!(casted_write, n, name, arr, builder).unwrap();
    }
}

impl<E> EntityImmutableRefMapperBackend<E> for Box<[E::T]>
where
    E: CompactEntity,
{
    fn get_ref_via_immut(&self, k: &usize) -> Option<&E::T> {
        Some(&self[*k])
    }
}

impl<E> Iterator for FixAttIterator<E>
where
    E: FixWriteSizeEntity,
    <E as Entity>::T: ByteFixArrayInterface,
{
    type Item = E::T;
    fn next(&mut self) -> Option<Self::Item> {
        let buf = &mut self.buf[..E::WS];
        if let Ok(_) = self.file.read_exact(buf) {
            return Some(<E as Entity>::T::from_fbytes(buf));
        }
        None
    }
}

fn casted_write<T>(name: &str, arr: Vec<usize>, builder: &MainBuilder) -> io::Result<()>
where
    T: UnsignedNumber + ByteFixArrayInterface,
{
    let mut file = File::create(builder.parent_root.join(name)).unwrap();
    for us in arr.into_iter() {
        let buf = T::from_usize(us).to_fbytes();
        file.write(&buf)?;
    }
    Ok(())
}
