use std::{
    fs::File,
    io::{self, Write},
    path::PathBuf,
    rc::Rc,
    sync::Arc,
};

use dmove_macro::{def_me_struct, derive_meta_trait};
use hashbrown::HashSet;

pub const MAX_BUF: usize = 0x1000;
pub const MAX_NUMBUF: usize = 0x10;
pub const MAX_FIXBUF: usize = 0x100;

const PACK_NAME: &'static str = "dmove";

pub type BigId = u64;
pub type ET<E> = <E as Entity>::T;
pub type MAA<T, M> = <T as MarkedAttribute<M>>::AttributeEntity;

pub struct MainBuilder {
    pub meta_elems: Vec<MetaElem>,
    pub definables: HashSet<String>,
    pub parent_root: PathBuf,
}

pub trait BackendLoading<E>
where
    E: Entity,
{
    fn load_backend(path: &PathBuf) -> Self;
}

pub trait EntityMutableMapperBackend<E>
where
    E: Entity + MappableEntity,
{
    fn get_via_mut(&mut self, k: &E::KeyType) -> Option<E::T>;
}

pub trait EntityImmutableMapperBackend<E>
where
    E: Entity + MappableEntity,
{
    fn get_via_immut(&self, k: &E::KeyType) -> Option<E::T>;
    //TODO: offer unsafe/unchecked, faster options
}

pub trait EntityImmutableRefMapperBackend<E>
where
    E: Entity + MappableEntity,
{
    fn get_ref_via_immut(&self, k: &E::KeyType) -> Option<&E::T>;
}

def_me_struct!();

#[derive_meta_trait]
pub trait Entity {
    type T;
    const N: usize;
    const NAME: &str;
}

#[derive_meta_trait]
pub trait NamespacedEntity: Entity {
    const NS: &str;
}

#[derive_meta_trait]
pub trait MappableEntity: Entity {
    type KeyType;
}

#[derive_meta_trait]
pub trait VariableSizeAttribute: Entity
where
    Self::T: VarSizedAttributeElement,
{
    type SizeType: UnsignedNumber;
    // type LargestBuffer;
    // const LARGEST: usize;
    // on non inmemory thing: have type for offset (with divisor divided)
    // store largest size, so that it can be read without vec

    fn full_size_from_buf(size_slice: &[u8]) -> usize {
        Self::full_size_from_st(Self::SizeType::from_fbytes(size_slice))
    }

    fn full_size_from_st(size: Self::SizeType) -> usize {
        size.to_usize() * Self::T::DIVISOR
    }
}

#[derive_meta_trait]
pub trait Link: Entity {
    type Source: Entity;
    type Target: Entity;
}

#[derive_meta_trait]
pub trait MarkedAttribute<Marker>: Entity {
    type AttributeEntity: Entity;
}

#[derive_meta_trait]
pub trait CompactEntity: MappableEntity<KeyType = usize> {}

impl<T> CompactEntity for T where T: MappableEntity<KeyType = usize> {}

//TODO
// #[derive_meta_trait]
// pub trait UnitLinkAttribute: Entity + Link {}
//
// #[derive_meta_trait]
// pub trait PluraLinkAttribute: Entity + Link {}

// pub trait HierarchalEntityElement: Iterator<Item = Self::ChildType> {
//     //sometimes reference
//     //iteration might be enogh :/
//     type ChildType;
// }

//LinkCompactEntityToData
//LinkDataToCompactEntity
//LinkDataToCompactEntities
//LinkCompactEntitiesToData
//LinkCompactEntit(ies)ToData(andCompactEntity(es))
//

// pub trait ByteFixArrayInterface<const S: usize> {
//     const S: usize = S;
//     fn to_fbytes(&self) -> [u8; S];
//     fn from_fbytes(buf: &[u8; S]) -> Self;
// }

pub trait ByteFixArrayInterface {
    //serialized, sized
    //can be different from in-memory size due to padding
    const S: usize;

    fn to_fbytes(&self) -> Box<[u8]>;
    fn from_fbytes(buf: &[u8]) -> Self;
}

pub trait ByteArrayInterface {
    fn to_bytes(&self) -> Box<[u8]>;
    fn from_bytes(buf: &[u8]) -> Self;
}

pub trait UnsignedNumber: Ord + Clone + Copy + Sized + ByteFixArrayInterface {
    fn to_usize(&self) -> usize;
    fn from_usize(n: usize) -> Self;
    fn cast_big_id(n: BigId) -> Self;
    fn lift(&self) -> Self;
}

pub trait MetaIntegrator<T>: Sized {
    fn setup(builder: &MainBuilder, name: &str) -> Self;

    fn add_elem(&mut self, e: &T);

    fn post(self, _builder: &mut MainBuilder) {}

    fn add_elem_owned(&mut self, e: T) {
        self.add_elem(&e)
    }

    fn add_iter<'a, I>(builder: &mut MainBuilder, elems: I, name: &str)
    where
        I: Iterator<Item = &'a T>,
        T: 'a,
    {
        Self::add_iter_wrap(builder, elems, name, Self::add_elem);
    }

    fn add_iter_owned<I>(builder: &mut MainBuilder, elems: I, name: &str)
    where
        I: Iterator<Item = T>,
    {
        Self::add_iter_wrap(builder, elems, name, Self::add_elem_owned)
    }

    fn add_iter_wrap<I, F, MT>(builder: &mut MainBuilder, elems: I, name: &str, c: F)
    where
        I: Iterator<Item = MT>,
        F: Fn(&mut Self, MT) -> (),
    {
        let mut s = Self::setup(builder, name);
        for e in elems {
            c(&mut s, e);
        }
        s.post(builder);
    }
}

impl MainBuilder {
    pub fn new(parent_root: &PathBuf) -> Self {
        Self {
            parent_root: parent_root.to_path_buf(),
            meta_elems: Vec::new(),
            definables: HashSet::new(),
        }
    }

    pub fn add_simple_etrait(
        &mut self,
        name: &str,
        type_name: &str,
        n: usize,
        compact: bool,
    ) -> String {
        let camel_name = camel_case(&name);
        self.meta_elems
            .push(EntityTraitMeta::meta(&camel_name, type_name, n, name));
        if compact {
            self.meta_elems
                .push(MappableEntityTraitMeta::meta(&camel_name, "usize"));
        }
        self.definables.insert(camel_name.clone());
        camel_name
    }

    pub fn add_scaled_entity(&mut self, name: &str, n: usize, compact: bool) -> String {
        self.add_simple_etrait(name, &get_uscale(n), n, compact)
    }

    pub fn declare_ns(&mut self, name: &str, ns: &str) {
        self.meta_elems
            .push(NamespacedEntityTraitMeta::meta(&camel_case(name), ns))
    }

    pub fn declare_marked_attribute<Main, Marker>(&mut self, name: &str) {
        self.meta_elems.push(MarkedAttributeTraitMeta::meta(
            &get_type_name::<Main>(),
            &get_type_name::<Marker>(),
            &camel_case(name),
        ))
    }

    pub fn declare_link<S, T>(&mut self, name: &str) {
        self.meta_elems.push(LinkTraitMeta::meta(
            &camel_case(name),
            &get_type_name::<S>(),
            &get_type_name::<T>(),
        ))
    }

    pub fn write_code(&self, path: &str) -> io::Result<usize> {
        let mut imports: HashSet<String> = HashSet::new();
        self.meta_elems.iter().for_each(|me| {
            me.importables.iter().for_each(|i| {
                imports.insert(i.clone());
            })
        });
        let mut all_defs = vec![format!(
            "use {PACK_NAME}::{{{}}};",
            imports.into_iter().collect::<Vec<String>>().join(", ")
        )];
        all_defs.extend(
            self.definables
                .iter()
                .map(|e| format!("pub struct {} {{ }}", e)),
        );
        all_defs.extend(self.meta_elems.iter().map(|e| e.impl_str.clone()));
        File::create(path)?.write(&all_defs.join("\n\n").into_bytes())
    }
}

impl ByteArrayInterface for String {
    fn to_bytes(&self) -> Box<[u8]> {
        self.to_owned().into_bytes().into()
    }

    fn from_bytes(buf: &[u8]) -> Self {
        std::str::from_utf8(buf).unwrap().to_string()
    }
}

impl<F, L> ByteFixArrayInterface for (F, L)
where
    F: ByteFixArrayInterface,
    L: ByteFixArrayInterface,
{
    const S: usize = F::S + L::S;
    fn to_fbytes(&self) -> Box<[u8]> {
        let mut o = self.0.to_fbytes().to_vec();
        o.extend(self.1.to_fbytes());
        o.into()
    }
    fn from_fbytes(buf: &[u8]) -> Self {
        let fsize = size_of::<F>();
        (F::from_fbytes(&buf[..fsize]), L::from_fbytes(&buf[fsize..]))
    }
}

macro_rules! iter_ba_impl {
    ($($iter_type:ty),*) => {
        $(impl<T> ByteArrayInterface for $iter_type
        where
            T: ByteFixArrayInterface,
        {
            fn to_bytes(&self) -> Box<[u8]> {
                let mut out = Vec::new();
                for e in self.iter() {
                    out.extend(e.to_fbytes().iter())
                }
                out.into()
            }

            fn from_bytes(buf: &[u8]) -> Self {
                let size = T::S;
                let mut out = Vec::new();
                let (mut s, mut e) = (0, size);
                while e <= buf.len() {
                    out.push(T::from_fbytes(&buf[s..e]));
                    (s, e) = (e, e + size);
                }
                out.into()
            }
        })*
    };
}

macro_rules! uint_impl {
     ($($t:ty),*) => {
        $(impl UnsignedNumber for $t {
            fn from_usize(n: usize) -> Self {
                n as Self
            }

            fn to_usize(&self) -> usize {
                *self as usize
            }

            fn cast_big_id(n: BigId) -> Self {
                n as Self
            }

            fn lift(&self) -> Self {
                *self
            }
        })*
    };
}

macro_rules! num_impl {
     ($($t:ty),*) => {
        $(impl ByteFixArrayInterface for $t {

            const S: usize = size_of::<$t>();

            fn from_fbytes(barr: &[u8]) -> Self {
                Self::from_be_bytes(barr.try_into().unwrap())
            }
            fn to_fbytes(&self) -> Box<[u8]> {
                self.to_be_bytes().into()
            }
        })*
    };
}

macro_rules! downcast_fun {
    ($fun: ident, $n: ident, $($arg: ident),*) => {
        if ($n >> 8) == 0 {
            $fun::<u8>($($arg),*)
        } else if ($n >> 16) == 0 {
            $fun::<u16>($($arg),*)
        } else if ($n >> 32) == 0 {
            $fun::<u32>($($arg),*)
        } else if ($n >= 2_usize.pow(64)) {
            $fun::<u64>($($arg),*)
        } else {
            $fun::<u128>($($arg),*)

        }
    };
}
pub(crate) use downcast_fun;

use crate::VarSizedAttributeElement;

uint_impl!(u8, u16, u32, u64, u128);
num_impl!(u8, u16, u32, u64, u128, f32, f64);
iter_ba_impl!(Box<[T]>, Vec<T>, Rc<[T]>, Arc<[T]>);

pub fn camel_case(s: &str) -> String {
    let mut out = "".to_string();
    let mut next_big = true;
    for mut c in s.chars() {
        if next_big {
            c = c.to_uppercase().next().unwrap();
            next_big = false;
        }
        if "-_0123456789".chars().any(|e| e == c) {
            next_big = true;
        } else {
            out.push(c);
        }
    }
    out
}

pub fn get_uscale(n: usize) -> String {
    for poss_scale in [8, 16, 32, 64] {
        if (n >> poss_scale) == 0 {
            return format!("u{}", poss_scale);
        }
    }
    "u128".to_string()
}

pub fn get_type_name<T>() -> String {
    //needs to import it if it is from this lib
    clean_name(std::any::type_name::<T>().to_string())
}

fn clean_name(base_name: String) -> String {
    let mut base_iter = base_name.split("::");

    let mut clean_blocks = Vec::new(); // x::y::v<a::b::c> -> x, y, v<cleaned>
    while let Some(mut elem) = base_iter.next() {
        let (mut lc, mut rc) = (0, 0);
        let mut presub = "".to_string();
        let mut sub = "".to_string();
        loop {
            for chr in elem.chars() {
                if chr == '<' {
                    lc += 1;
                    if lc == 1 {
                        continue;
                    }
                } else if chr == '>' {
                    rc += 1;
                    if lc == rc {
                        //assert that this is the last character
                        continue;
                    }
                }
                if lc == 0 {
                    presub.push(chr);
                } else {
                    sub.push(chr);
                }
            }
            if lc == rc {
                break;
            }
            sub.extend("::".chars());
            elem = base_iter.next().expect("inner iter");
        }
        let clean_elem = if sub.len() > 0 {
            format!("{}<{}>", presub, clean_name(sub))
        } else {
            presub
        };

        clean_blocks.push(clean_elem);
    }

    let root = &clean_blocks[0];
    if root == "alloc" || root == PACK_NAME {
        return clean_blocks.last().unwrap().to_string();
    }
    if clean_blocks.len() > 1 && root != "std" {
        clean_blocks[0] = "crate".to_string();
    }
    clean_blocks.join("::").to_string()
}
