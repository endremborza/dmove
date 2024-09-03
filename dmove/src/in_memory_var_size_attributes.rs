use std::{
    fs::{create_dir_all, File},
    io::{Read, Write},
    path::PathBuf,
};

use crate::{
    common::{
        ByteArrayInterface, Entity, MainBuilder, MetaInput, MetaIntegrator, SomeElement,
        UnsignedNumber, MAX_BUF, MAX_NUMBUF,
    },
    FixedAttributeElement,
};

pub struct InMemoryVarAttBuilder {
    counts_file: File,
    targets_file: File,
    sizes: Vec<usize>,
    max_size: usize,
}

pub trait InMemoryVarAttributeElement: ByteArrayInterface {
    const DIVISOR: usize = 1;
}

pub trait VariableSizeInMemoryAttribute: Entity
where
    <Self as Entity>::T: InMemoryVarAttributeElement,
{
    type SizeType: ByteArrayInterface + UnsignedNumber;
    // type LargestBuffer;
    // const LARGEST: usize;
    // on non inmemory thing: have type for offset (with divisor divided)
    // store largest size, so that it can be read without vec

    fn read(parent_dir: &PathBuf) -> Box<[Self::T]> {
        let size_size = std::mem::size_of::<Self::SizeType>();
        let att_dir = parent_dir.join(Self::NAME);
        let mut counts_file = File::open(&att_dir.join("sizes")).unwrap();
        let mut targets_file = File::open(&att_dir.join("targets")).unwrap();
        let mut buf: [u8; MAX_BUF] = [0; MAX_BUF];
        let mut size_buf: [u8; MAX_NUMBUF] = [0; MAX_NUMBUF];
        let mut in_memory_atts = Vec::new();
        let mut current_size: Self::SizeType;
        while let Ok(_) = counts_file.read_exact(&mut size_buf[..size_size]) {
            current_size = Self::SizeType::from_bytes(&buf[..size_size]);
            let mut remaining_count = current_size.to_usize() * Self::T::DIVISOR;
            let mut bvec: Vec<u8> = Vec::new();
            while remaining_count > 0 {
                let endidx = if remaining_count > MAX_BUF {
                    MAX_BUF
                } else {
                    remaining_count
                };
                targets_file.read_exact(&mut buf[..endidx]).unwrap();
                bvec.extend(buf[..endidx].iter());
                remaining_count -= endidx;
            }
            in_memory_atts.push(Self::T::from_bytes(&bvec));
        }
        in_memory_atts.into()
    }
}

impl InMemoryVarAttributeElement for String {}

impl<T> InMemoryVarAttributeElement for T
where
    T: FixedAttributeElement,
{
    const DIVISOR: usize = std::mem::size_of::<T>();
}

impl<T> InMemoryVarAttributeElement for Box<[T]>
where
    T: InMemoryVarAttributeElement,
{
    const DIVISOR: usize = T::DIVISOR;
}

impl<T> InMemoryVarAttributeElement for Vec<T>
where
    T: InMemoryVarAttributeElement,
{
    const DIVISOR: usize = T::DIVISOR;
}

impl<T> SomeElement<InMemoryVarAttBuilder> for T
where
    T: InMemoryVarAttributeElement,
{
    type MetaInputType = String;
    fn main_trait() -> &'static str {
        "VariableSizeInMemoryAttribute"
    }
    fn trait_impl_innards(i: Self::MetaInputType) -> Vec<[String; 2]> {
        vec![["type SizeType".to_string(), i]]
    }
}

impl<T> MetaIntegrator<T> for InMemoryVarAttBuilder
where
    T: InMemoryVarAttributeElement,
{
    fn setup(builder: &MainBuilder, name: &str) -> Self {
        let att_dir = builder.parent_root.join(name);
        create_dir_all(&att_dir).unwrap();
        let targets_file = File::create(&att_dir.join("targets")).unwrap();
        let counts_file = File::create(&att_dir.join("sizes")).unwrap();

        Self {
            counts_file,
            targets_file,
            sizes: Vec::new(),
            max_size: 0,
        }
    }

    fn add_elem(&mut self, e: &T) {
        let barr = e.to_bytes();
        self.targets_file.write(&barr).expect("target writing");
        let current_size = barr.len();
        if current_size > self.max_size {
            self.max_size = current_size
        }
        self.sizes.push(current_size);
    }
    fn post(self) -> MetaInput<<T as SomeElement<Self>>::MetaInputType> {
        //NOTE: all sizes need to fit into memory
        //if that's infeasable, sizes usize
        // let n = S::N;
        // assert_eq!(sizes.len(), n);
        let n = self.sizes.len();

        let number_writer = NumberWriter {
            file: self.counts_file,
            numbers: self.sizes.into_iter(),
        };

        let size_scale = number_writer.write_minimal(self.max_size);
        MetaInput {
            n,
            type_overwrite: None,
            meta_lines_input: size_scale.to_string(),
        }
    }
}

pub struct NumberWriter<I, F>
where
    I: Iterator<Item = usize>,
    F: Write,
{
    file: F,
    numbers: I,
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
