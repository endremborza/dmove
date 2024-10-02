use std::{
    fs::{create_dir_all, File},
    io::{Read, Write},
    marker::PhantomData,
    path::PathBuf,
};

use crate::{
    common::{
        get_type_name, BackendLoading, ByteArrayInterface, Entity, EntityImmutableRefMapperBackend,
        MainBuilder, MetaIntegrator, UnsignedNumber, VariableSizeAttribute,
        VariableSizeAttributeTraitMeta, MAX_BUF, MAX_NUMBUF,
    },
    MappableEntity,
};

pub struct VarBox<T>(Box<[T]>);

pub struct VarAttBuilder {
    files: VattFilePair,
    sizes: Vec<usize>,
    max_size: usize,
    name: String,
}

pub struct VattFilePair {
    counts: File,
    targets: File,
}

pub struct VarAttIterator<T>
where
    T: VariableSizeAttribute + ?Sized,
    <T as Entity>::T: VarSizedAttributeElement,
{
    files: VattFilePair,
    size_size: usize,
    buf: [u8; MAX_BUF],
    size_buf: [u8; MAX_NUMBUF],
    current_size: T::SizeType,
    p: PhantomData<T>,
}

pub struct NumberWriter<I, F>
where
    I: Iterator<Item = usize>,
    F: Write,
{
    file: F,
    numbers: I,
}

pub trait VarSizedAttributeElement: ByteArrayInterface {
    const DIVISOR: usize = 1;
}

impl<T> From<Vec<T>> for VarBox<T> {
    fn from(value: Vec<T>) -> Self {
        Self(value.into())
    }
}

impl VattFilePair {
    fn open(parent_dir: &PathBuf) -> Self {
        let counts = File::open(&parent_dir.join("sizes")).unwrap();
        let targets = File::open(&parent_dir.join("targets")).unwrap();
        Self { counts, targets }
    }

    fn create(parent_dir: &PathBuf) -> Self {
        let counts = File::create(&parent_dir.join("sizes")).unwrap();
        let targets = File::create(&parent_dir.join("targets")).unwrap();
        Self { counts, targets }
    }
}

impl<E> VarAttIterator<E>
where
    E: Entity + VariableSizeAttribute + ?Sized,
    <E as Entity>::T: VarSizedAttributeElement,
{
    fn new(parent_dir: &PathBuf) -> Self {
        let size_size = std::mem::size_of::<E::SizeType>();
        let att_dir = parent_dir.join(E::NAME);
        Self {
            files: VattFilePair::open(&att_dir),
            size_size,
            buf: [0; MAX_BUF],
            size_buf: [0; MAX_NUMBUF],
            current_size: E::SizeType::from_usize(0),
            p: PhantomData,
        }
    }
}

impl<I, F> NumberWriter<I, F>
where
    I: Iterator<Item = usize>,
    F: Write,
{
    fn write<N>(mut self) -> String
    where
        N: ByteArrayInterface + UnsignedNumber,
    {
        for n in self.numbers {
            let buf = N::from_usize(n).to_bytes();
            self.file.write(&buf).expect("writing number");
        }
        std::any::type_name::<N>().to_string()
    }

    pub fn write_minimal(self, max_size: usize) -> String {
        if max_size < 2_usize.pow(8) {
            self.write::<u8>()
        } else if max_size < 2_usize.pow(16) {
            self.write::<u16>()
        } else if max_size < 2_usize.pow(32) {
            self.write::<u32>()
        } else if max_size < 2_usize.pow(64) {
            self.write::<u64>()
        } else {
            self.write::<u128>()
        }
    }
}

impl VarSizedAttributeElement for String {}

impl<T> VarSizedAttributeElement for Box<[T]>
where
    T: Sized + ByteArrayInterface,
{
    const DIVISOR: usize = size_of::<T>();
}

impl<T> MetaIntegrator<T> for VarAttBuilder
where
    T: VarSizedAttributeElement,
{
    fn setup(builder: &MainBuilder, name: &str) -> Self {
        let att_dir = builder.parent_root.join(name);
        create_dir_all(&att_dir).unwrap();
        let files = VattFilePair::create(&att_dir);
        Self {
            files,
            sizes: Vec::new(),
            max_size: 0,
            name: name.to_string(),
        }
    }

    fn add_elem(&mut self, e: &T) {
        let barr = e.to_bytes();
        self.files.targets.write(&barr).expect("target writing");
        let current_size = barr.len() / T::DIVISOR;
        if current_size > self.max_size {
            self.max_size = current_size
        }
        self.sizes.push(current_size);
    }
    fn post(self, builder: &mut MainBuilder) {
        //NOTE: all sizes need to fit into memory
        //if that's infeasable, sizes usize
        // let n = S::N;
        // assert_eq!(sizes.len(), n);
        let n = self.sizes.len();

        let number_writer = NumberWriter {
            file: self.files.counts,
            numbers: self.sizes.into_iter(),
        };
        let size_scale = number_writer.write_minimal(self.max_size);
        let camel_name = builder.add_simple_etrait(&self.name, &get_type_name::<T>(), n);
        builder
            .meta_elems
            .push(VariableSizeAttributeTraitMeta::meta(
                &camel_name,
                &size_scale,
            ));
    }
}

impl<E> BackendLoading<E> for VarAttIterator<E>
where
    E: Entity + VariableSizeAttribute,
    <E as Entity>::T: VarSizedAttributeElement,
{
    fn load_backend(path: &PathBuf) -> Self {
        VarAttIterator::<E>::new(path)
    }
}

impl<E> BackendLoading<E> for VarBox<E::T>
where
    E: Entity + VariableSizeAttribute,
    <E as Entity>::T: VarSizedAttributeElement,
{
    fn load_backend(path: &PathBuf) -> Self {
        VarAttIterator::<E>::new(path).collect::<Vec<E::T>>().into()
    }
}

impl<E> EntityImmutableRefMapperBackend<E, E> for VarBox<E::T>
where
    E: Entity + MappableEntity<E, KeyType = usize>,
{
    fn get_ref_via_immut(
        &self,
        k: &<E as MappableEntity<E>>::KeyType,
    ) -> Option<&<E as Entity>::T> {
        Some(&self.0[*k])
    }
}

impl<E> Iterator for VarAttIterator<E>
where
    E: VariableSizeAttribute + Entity,
    <E as Entity>::T: VarSizedAttributeElement,
{
    type Item = E::T;
    fn next(&mut self) -> Option<Self::Item> {
        let size_slice = &mut self.size_buf[..self.size_size];

        if let Ok(_) = self.files.counts.read_exact(size_slice) {
            self.current_size = E::SizeType::from_bytes(size_slice);
            let mut remaining_count = self.current_size.to_usize() * E::T::DIVISOR;
            let mut bvec: Vec<u8> = Vec::new();
            while remaining_count > 0 {
                let endidx = if remaining_count > MAX_BUF {
                    MAX_BUF
                } else {
                    remaining_count
                };
                let content_slice = &mut self.buf[..endidx];
                self.files
                    .targets
                    .read_exact(content_slice)
                    .expect(&format!(
                        "filling failed: end: {}, csize: {}, divisor: {}, remains {}, entity: {}",
                        endidx,
                        self.current_size.to_usize(),
                        E::T::DIVISOR,
                        remaining_count,
                        E::NAME
                    ));
                bvec.extend(content_slice.iter());
                remaining_count -= endidx;
            }
            return Some(<E as Entity>::T::from_bytes(&bvec));
        }
        None
    }
}
