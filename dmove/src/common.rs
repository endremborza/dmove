use std::{
    collections::HashSet,
    fs::File,
    io::{self, Write},
    path::PathBuf,
    rc::Rc,
    sync::Arc,
    u8,
};

pub const MAX_BUF: usize = 0x1000;
pub const MAX_NUMBUF: usize = 0x10;

const PACK_NAME: &'static str = "dmove";
pub const BASIC_TRAIT: &'static str = "Entity";
pub const LINK_TRAIT: &'static str = "Link";

pub struct MainBuilder {
    pub struct_defs: Vec<String>,
    pub prefix_imports: HashSet<String>,
    pub parent_root: PathBuf,
}

pub struct MetaInput<T> {
    pub n: usize,
    pub type_overwrite: Option<String>,
    //TODO: make a kind that overwrites possibly string to cat, based on nunique
    pub meta_lines_input: T,
}

pub trait Entity {
    type T;
    type FullT;
    const N: usize;
    const NAME: &'static str;
}

pub trait Link<S: Entity, T: Entity>: Entity {}

pub trait TargetGetter<S: Entity, T: Entity>: Entity {
    fn get(i: <Self as Entity>::T) -> <T as Entity>::T;
}

pub trait ByteArrayInterface {
    fn to_bytes(&self) -> Box<[u8]>;
    fn from_bytes(buf: &[u8]) -> Self;
}

pub trait UnsignedNumber {
    fn to_usize(&self) -> usize;
    fn from_usize(n: usize) -> Self;
}

pub trait SomeElement<Marker> {
    type MetaInputType;
    fn main_trait() -> &'static str;
    fn trait_impl_innards(_i: Self::MetaInputType) -> Vec<[String; 2]> {
        Vec::new()
    }
}

pub trait MetaIntegrator<T: SomeElement<Self>>
where
    Self: Sized,
{
    fn setup(builder: &MainBuilder, name: &str) -> Self;

    fn add_elem(&mut self, e: &T);

    fn post(self) -> MetaInput<T::MetaInputType>;

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
            // s.add_elem(e);
        }
        let meta_input = s.post();

        let type_name = meta_input.type_overwrite.unwrap_or(get_type_name::<T>());

        builder.struct_defs.push(Self::get_meta_lines(
            name,
            &type_name,
            meta_input.n,
            meta_input.meta_lines_input,
        ));
        builder.prefix_imports.insert(T::main_trait().to_string());
    }

    fn get_meta_lines(
        entity_name: &str,
        type_name: &str,
        n: usize,
        lines_input: T::MetaInputType,
    ) -> String {
        let camel_entity = camel_case(entity_name);

        let mut blocks = vec![
            format!("pub struct {};", camel_entity),
            format!(
                "impl {} for {} {{
    type T = {};
    const N: usize = {};
    type FullT = [{}; {}];
    const NAME: &'static str = \"{}\";
}}",
                BASIC_TRAIT, camel_entity, type_name, n, type_name, n, entity_name
            ),
        ];
        let trait_name = T::main_trait();
        if trait_name != BASIC_TRAIT {
            let spec_innards = T::trait_impl_innards(lines_input)
                .iter()
                .map(|e| format!("    {} = {};", e[0], e[1]))
                .collect::<Vec<String>>()
                .join("\n");

            blocks.push(format!(
                "impl {} for {} {{{}}}",
                trait_name, camel_entity, spec_innards
            ));
        }
        blocks.join("\n\n")
    }
}

impl MainBuilder {
    pub fn new(parent_root: &PathBuf) -> Self {
        let mut prefix_imports = HashSet::new();
        prefix_imports.insert(BASIC_TRAIT.to_string());
        Self {
            parent_root: parent_root.to_path_buf(),
            struct_defs: Vec::new(),
            prefix_imports,
        }
    }

    pub fn write_code(&self, path: &str) -> io::Result<usize> {
        let mut all_defs = vec![format!(
            "use {}::{{{}}};",
            PACK_NAME,
            self.prefix_imports
                .iter()
                .map(|e| e.to_owned())
                .collect::<Vec<String>>()
                .join(", ")
        )];
        all_defs.extend(self.struct_defs.clone());
        File::create(path)?.write(&all_defs.join("\n\n").into_bytes())
    }

    pub fn declare_link<S: Entity, T: Entity>(&mut self, name: &str) {
        self.prefix_imports.insert(LINK_TRAIT.to_string());
        self.struct_defs.push(format!(
            "impl {}<{},{}> for {} {{}}",
            LINK_TRAIT,
            get_type_name::<S>(),
            get_type_name::<T>(),
            camel_case(name)
        ))
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

impl<S, T, A> TargetGetter<S, T> for A
where
    A: Link<S, T> + Entity<T = <T as Entity>::T>,
    T: Entity,
    S: Entity,
{
    fn get(i: <Self as Entity>::T) -> <T as Entity>::T {
        i
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

#[macro_export]
macro_rules! some_elem_impl {
    ($builder:ident, $elem_trait:ident, $trait:ident) => {
        impl<T> SomeElement<$builder> for T
        where
            T: $elem_trait,
        {
            type MetaInputType = ();
            fn main_trait() -> &'static str {
                stringify!($trait)
            }
        }
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

fn get_type_name<T>() -> String {
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

    if clean_blocks[0] == "alloc" {
        return clean_blocks.last().unwrap().to_string();
    }
    if clean_blocks.len() > 1 {
        clean_blocks[0] = "crate".to_string();
    }
    clean_blocks.join("::").to_string()
}
