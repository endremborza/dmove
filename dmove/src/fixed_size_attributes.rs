use std::{
    fs::File,
    io::{BufReader, Read, Write},
    path::PathBuf,
};

use crate::{
    common::{
        ByteArrayInterface, Entity, MainBuilder, MetaInput, MetaIntegrator, SomeElement, MAX_NUMBUF,
    },
    some_elem_impl,
};

pub trait FixedAttributeElement: ByteArrayInterface + Sized {}

pub trait FixedSizeAttribute: Entity
where
    <Self as Entity>::T: FixedAttributeElement,
{
    fn read(parent_dir: &PathBuf) -> Box<[Self::T]> {
        let mut out = Vec::new();
        let mut br = BufReader::new(File::open(parent_dir.join(Self::NAME)).unwrap());
        let size: usize = std::mem::size_of::<Self::T>();
        let mut buf: [u8; MAX_NUMBUF] = [0; MAX_NUMBUF];
        while let Ok(_) = br.read_exact(&mut buf[..size]) {
            out.push(Self::T::from_bytes(&buf[..size]));
        }
        out.into()
    }
}

pub struct FixAttBuilder {
    file: File,
    n: usize,
}
some_elem_impl!(FixAttBuilder, FixedAttributeElement, FixedSizeAttribute);

impl<T> MetaIntegrator<T> for FixAttBuilder
where
    T: FixedAttributeElement + SomeElement<FixAttBuilder, MetaInputType = ()>,
{
    fn setup(builder: &MainBuilder, name: &str) -> Self {
        let file = File::create(builder.parent_root.join(name)).unwrap();
        let n = 0;
        Self { n, file }
    }

    fn add_elem(&mut self, e: &T) {
        self.file.write(&e.to_bytes()).unwrap();
        self.n += 1;
    }
    fn post(self) -> MetaInput<<T as SomeElement<Self>>::MetaInputType> {
        MetaInput {
            n: self.n,
            type_overwrite: None,
            meta_lines_input: (),
        }
    }
}

macro_rules! empty_impl {
     ($($t:ty),*) => {
        $(impl FixedAttributeElement for $t {})*

    };
}

empty_impl!(u8, u16, u32, u64, u128);
