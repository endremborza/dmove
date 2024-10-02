use std::{
    fs::File,
    io::{BufReader, Read, Seek, SeekFrom, Write},
    path::PathBuf,
};

use crate::{
    common::{
        get_type_name, ByteArrayInterface, Entity, FixedSizeAttributeTraitMeta, MainBuilder,
        MappableEntity, MetaIntegrator, MAX_NUMBUF,
    },
    BackendLoading, EntityImmutableRefMapperBackend, EntityMutableMapperBackend,
    FixedSizeAttribute,
};

pub struct FixAttBuilder {
    file: File,
    n: usize,
    name: String,
}

pub trait FixedAttributeElement: ByteArrayInterface + Sized {}

impl<E, MM> EntityMutableMapperBackend<E, MM> for File
where
    E: Entity + MappableEntity<MM, KeyType = usize>,
{
    fn get_via_mut(&mut self, k: &<E as MappableEntity<MM>>::KeyType) -> Option<<E as Entity>::T> {
        let pos = SeekFrom::Start(*k as u64);
        self.seek(pos).unwrap();
        todo!()
    }
}

impl<E> BackendLoading<E> for Box<[E::T]>
where
    E: FixedSizeAttribute,
    <E as Entity>::T: FixedAttributeElement,
{
    fn load_backend(path: &PathBuf) -> Self {
        let mut out = Vec::new();
        let mut br = BufReader::new(File::open(path.join(E::NAME)).unwrap());
        let size: usize = std::mem::size_of::<E::T>();
        // const SIZE: usize = std::mem::size_of::<<Self as Entity>::T>();
        let mut buf: [u8; MAX_NUMBUF] = [0; MAX_NUMBUF];
        while let Ok(_) = br.read_exact(&mut buf[..size]) {
            out.push(E::T::from_bytes(&buf[..size]));
        }
        out.into()
    }
}

impl<T> MetaIntegrator<T> for FixAttBuilder
where
    T: FixedAttributeElement,
{
    fn setup(builder: &MainBuilder, name: &str) -> Self {
        let file = File::create(builder.parent_root.join(name)).unwrap();
        let n = 0;
        Self {
            n,
            file,
            name: name.to_string(),
        }
    }

    fn add_elem(&mut self, e: &T) {
        self.file.write(&e.to_bytes()).unwrap();
        self.n += 1;
    }
    fn post(self, builder: &mut MainBuilder) {
        let camel_name = builder.add_simple_etrait(&self.name, &get_type_name::<T>(), self.n);
        builder
            .meta_elems
            .push(FixedSizeAttributeTraitMeta::meta(&camel_name));
    }
}

impl<T> FixedAttributeElement for T where T: ByteArrayInterface + Sized {}

impl<E> EntityImmutableRefMapperBackend<E, E> for Box<[E::T]>
where
    E: Entity + MappableEntity<E, KeyType = usize>,
{
    fn get_ref_via_immut(
        &self,
        k: &<E as MappableEntity<E>>::KeyType,
    ) -> Option<&<E as Entity>::T> {
        Some(&self[*k])
    }
}

// macro_rules! empty_impl {
//      ($($t:ty),*) => {
//         $(impl FixedAttributeElement for $t {})*
//     };
// }

// macro_rules! arr_impl {
//      ($($n:literal),*) => {
//         $(impl<T> FixedAttributeElement for [T; $n] where T: Sized {})*
//     };
// }

// empty_impl!(u8, u16, u32, u64, u128);

// arr_impl!(2);
