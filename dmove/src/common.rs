use std::{
    fs::File,
    io::{self, Write},
    path::PathBuf,
    rc::Rc,
    sync::Arc,
};

use dmove_macro::{def_me_struct, derive_meta_trait};
use hashbrown::HashSet;

use crate::FixedAttributeElement;

pub const MAX_BUF: usize = 0x1000;
pub const MAX_NUMBUF: usize = 0x10;

const PACK_NAME: &'static str = "dmove";

pub type BigId = u64;

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

pub trait EntityMutableMapperBackend<E, MM>
where
    E: Entity + MappableEntity<MM>,
{
    fn get_via_mut(&mut self, k: &E::KeyType) -> Option<E::T>;
}

pub trait EntityImmutableMapperBackend<E, MM>
where
    E: Entity + MappableEntity<MM>,
{
    fn get_via_immut(&self, k: &E::KeyType) -> Option<E::T>;
    //TODO: offer unsafe/unchecked, faster options
}

pub trait EntityImmutableRefMapperBackend<E, MM>
where
    E: Entity + MappableEntity<MM>,
{
    fn get_ref_via_immut(&self, k: &E::KeyType) -> Option<&E::T>;
    //TODO: offer unsafe/unchecked, faster options
}

pub trait EntityMutablIterateLinksBackend<E, I>
where
    E: Entity,
{
    fn links(&mut self) -> Option<E::T>;
}

def_me_struct!();

#[derive_meta_trait]
pub trait Entity {
    type T;
    const N: usize;
    const NAME: &'static str;
}

#[derive_meta_trait]
pub trait NamespacedEntity: Entity {
    const NS: &str;
}

#[derive_meta_trait]
pub trait MappableEntity<Marker>: Entity {
    //mapped usize with Self marker means compact entity, currently
    type KeyType;
}

#[derive_meta_trait]
pub trait VariableSizeAttribute: Entity {
    type SizeType: ByteArrayInterface + UnsignedNumber;
    // type LargestBuffer;
    // const LARGEST: usize;
    // on non inmemory thing: have type for offset (with divisor divided)
    // store largest size, so that it can be read without vec
}

#[derive_meta_trait]
pub trait FixedSizeAttribute: Entity
where
    <Self as Entity>::T: FixedAttributeElement,
{
}

#[derive_meta_trait]
pub trait Link: Entity {
    type Source: Entity;
    type Target: Entity;
}

#[derive_meta_trait]
pub trait UnitLinkAttribute: Entity + Link {}

#[derive_meta_trait]
pub trait PluraLinkAttribute: Entity + Link {}

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

pub trait ByteArrayInterface {
    fn to_bytes(&self) -> Box<[u8]>;
    fn from_bytes(buf: &[u8]) -> Self;
}

pub trait UnsignedNumber {
    fn to_usize(&self) -> usize;
    fn from_usize(n: usize) -> Self;
    fn cast_big_id(n: BigId) -> Self;
    fn lift(&self) -> Self;
}

pub trait MetaIntegrator<T>
where
    Self: Sized,
{
    fn setup(builder: &MainBuilder, name: &str) -> Self;

    fn add_elem(&mut self, e: &T);

    fn post(self, _builder: &mut MainBuilder) {}

    fn add_elem_owned(&mut self, e: T) {
        self.add_elem(&e)
    }

    //adding just one in the body of an iteration is a limit
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

    pub fn add_simple_etrait(&mut self, name: &str, type_name: &str, n: usize) -> String {
        let camel_name = camel_case(&name);
        self.meta_elems
            .push(EntityTraitMeta::meta(&camel_name, type_name, n, name));
        self.meta_elems
            .push(MappableEntityTraitMeta::meta(&camel_name, "Self", "usize"));
        self.definables.insert(camel_name.clone());
        camel_name
    }

    pub fn add_scaled_compact_entity(&mut self, name: &str, n: usize) -> String {
        self.add_simple_etrait(name, &get_uscale(n), n)
    }

    pub fn declare_ns(&mut self, name: &str, ns: &str) {
        self.meta_elems
            .push(NamespacedEntityTraitMeta::meta(&camel_case(name), ns))
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
            "use {}::{{{}}};",
            PACK_NAME,
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

impl<F, L> ByteArrayInterface for (F, L)
where
    F: Sized + ByteArrayInterface,
    L: ByteArrayInterface,
{
    fn to_bytes(&self) -> Box<[u8]> {
        let mut o = self.0.to_bytes().to_vec();
        o.extend(self.1.to_bytes());
        o.into()
    }
    fn from_bytes(buf: &[u8]) -> Self {
        let fsize = size_of::<F>();
        (F::from_bytes(&buf[..fsize]), L::from_bytes(&buf[fsize..]))
    }
}

macro_rules! iter_ba_impl {
    ($($iter_type:ty),*) => {
        $(impl<T> ByteArrayInterface for $iter_type
        where
            T: ByteArrayInterface + Sized,
        {
            fn to_bytes(&self) -> Box<[u8]> {
                let mut out = Vec::new();
                for e in self.iter() {
                    out.extend(e.to_bytes().iter())
                }
                out.into()
            }

            fn from_bytes(buf: &[u8]) -> Self {
                let size = std::mem::size_of::<T>();
                let mut out = Vec::new();
                let (mut s, mut e) = (0, size);
                while e < buf.len() {
                    out.push(T::from_bytes(&buf[s..e]));
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
        $(impl ByteArrayInterface for $t {
            fn from_bytes(barr: &[u8]) -> Self {
                Self::from_be_bytes(barr.try_into().unwrap())
            }
            fn to_bytes(&self) -> Box<[u8]> {
                self.to_be_bytes().into()
            }
        })*
    };
}

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
