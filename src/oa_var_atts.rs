use std::{
    fs::{create_dir_all, File},
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use hashbrown::HashMap;
use serde::{de::DeserializeOwned, Deserialize};
use tqdm::Iter;

use crate::{
    add_strict_parsed_id_traits,
    common::{field_id_parse, oa_id_parse, BigId, ParsedId, Stowage, COUNTRIES, QS},
    ingest_entity::{get_idmap, LoadedIdMap},
    oa_csv_writers::{authors, fields, institutions, sources, subfields, topics, works},
    oa_entity_mapping::short_string_to_u64,
    oa_fix_atts::{names, read_fix_att},
    oa_structs::{
        post::{Authorship, Location},
        ReferencedWork, WorkTopic,
    },
    para::Worker,
};

pub mod vnames {
    pub const I2W: &str = "i2w";
    pub const A2W: &str = "a2w";
    pub const CONCEPT_H: &str = "concept-hierarchy";
    pub const COUNTRY_H: &str = "country-hierarchy";
    pub const W2S: &str = "w2s";
    pub const W2QS: &str = "w2qs";
    pub const TO_CITING: &str = "to-citing";
}

pub type AttributeResolverMap = HashMap<String, MappContainer>;
pub type MidId = u32;
pub type SmolId = u16;
// TODO figure this shit out to be dynamic properly
pub type CountryId = u8;
type QId = u8;
type FieldId = u8;
type SubFieldId = u8;
pub type InstId = u16;
type SourceId = u16;
pub type WorkId = u32;

pub enum MappedAttributes {
    List(Box<[SmolId]>),
    Map(Box<[(SmolId, MappedAttributes)]>),
}

pub struct MappContainer {
    mapps: Box<[MappedAttributes]>,
}

pub struct WeightedEdge<T> {
    pub id: T,
    rate: f32,
}

#[derive(Debug)]
struct HierEdge<T1, T2> {
    pub id: T1,
    pub subs: Vec<T2>,
}

struct InstToWorkPrep {
    total_authors: u16,
    inst_specs: Vec<InstPrepInner>,
}

struct InstPrepInner {
    inst_id: InstId,
    authors: u16,
}

#[derive(Deserialize)]
struct NamedEntity {
    id: String,
    display_name: String,
}

#[derive(Deserialize)]
struct NamedFieldEntity {
    id: String,
    display_name: String,
}

#[derive(Deserialize)]
struct Geo {
    country: String,
    country_code: String,
}

#[derive(Deserialize)]
struct WorkQ {
    id: BigId,
    source: BigId,
    best_q: QId,
}

#[derive(Debug)]
struct HEdgeSet<T1, T2>(Vec<HierEdge<T1, T2>>);

struct StrFiller<T, F>
where
    F: Fn(T) -> String,
{
    id_map: LoadedIdMap,
    names: Mutex<Box<[String]>>,
    name_getter: F,
    phantom: PhantomData<T>,
}

pub struct FilePointer {
    offset: u64,
    pub count: u32, // these might also be optimized to be smaller
}

impl<T, F> StrFiller<T, F>
where
    F: Fn(T) -> String,
{
    fn new(stowage: &Stowage, map_base: &str, name_getter: F) -> Self {
        let id_map = get_idmap(stowage, map_base);
        let mut names = Vec::new();
        for _ in id_map.iter_ids(true) {
            names.push("".to_string());
        }
        Self {
            id_map: id_map.to_map(),
            names: Mutex::new(names.into_boxed_slice()),
            name_getter,
            phantom: PhantomData,
        }
    }
}

impl ParsedId for Geo {
    fn get_parsed_id(&self) -> BigId {
        short_string_to_u64(&self.country_code)
    }
}

pub trait ByteConvert
where
    Self: Sized,
{
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(buf: &[u8]) -> (Self, usize);
}

const MAX_BUF: usize = 0x5FFFF; // how to minimize this?
struct VarReader<T> {
    counts_file: File,
    targets_file: File,
    buf: [u8; MAX_BUF],
    phantom: PhantomData<T>,
}

impl<T> VarReader<T> {
    fn new(stowage: &Stowage, att_name: &str) -> Self {
        let att_dir = stowage.var_atts.join(att_name);
        let counts_file = File::open(&att_dir.join("sizes")).unwrap();
        let targets_file = File::open(&att_dir.join("targets")).unwrap();
        let buf: [u8; MAX_BUF] = [0; MAX_BUF];
        Self {
            counts_file,
            targets_file,
            buf,
            phantom: PhantomData::<T>,
        }
    }
}

impl<T: ByteConvert> Iterator for VarReader<T> {
    type Item = T;
    fn next(&mut self) -> Option<Self::Item> {
        match FilePointer::read_next(&mut self.counts_file) {
            Some(fp) => {
                self.targets_file.seek(SeekFrom::Start(fp.offset)).unwrap();
                let mut remaining_count = fp.count as usize;
                let mut bvec: Vec<u8> = Vec::new();
                while remaining_count > 0 {
                    let endidx = if (remaining_count as usize) > MAX_BUF {
                        MAX_BUF
                    } else {
                        remaining_count
                    };
                    self.targets_file
                        .read_exact(&mut self.buf[..endidx])
                        .unwrap();
                    bvec.extend(self.buf[..endidx].iter());
                    remaining_count -= endidx;
                }
                let (v, _) = T::from_bytes(&bvec);
                Some(v)
            }
            None => None,
        }
    }
}

impl<T, F> Worker<T> for StrFiller<T, F>
where
    Arc<Self>: Send,
    T: ParsedId + Send + Sync,
    F: Fn(T) -> String + Send,
{
    fn proc(&self, input: T) {
        if let Some(id) = self.id_map.get(&input.get_parsed_id()) {
            self.names.lock().unwrap()[*id as usize] = (self.name_getter)(input);
        }
    }
}

add_strict_parsed_id_traits!(NamedEntity);

impl ParsedId for NamedFieldEntity {
    fn get_parsed_id(&self) -> BigId {
        field_id_parse(&self.id)
    }
}

impl<T1: Eq, T2: Eq> HEdgeSet<T1, T2> {
    fn new() -> Self {
        Self(vec![])
    }

    fn add(&mut self, main_id: T1, sub_id: Option<T2>) {
        for ch in &mut self.0 {
            if ch.id == main_id {
                if let Some(sid) = sub_id {
                    for presid in &ch.subs {
                        if sid == *presid {
                            return;
                        }
                    }
                    ch.subs.push(sid);
                }
                return;
            }
        }
        self.0.push(HierEdge::new(main_id, sub_id))
    }
}

impl<T1: ByteConvert, T2: ByteConvert> ByteConvert for HEdgeSet<T1, T2> {
    fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    fn from_bytes(buf: &[u8]) -> (Self, usize) {
        let (v, vs) = Vec::<HierEdge<T1, T2>>::from_bytes(buf);
        (Self(v), vs)
    }
}

impl<T1, T2> HierEdge<T1, T2> {
    fn new(id: T1, sub_id: Option<T2>) -> Self {
        let subs = match sub_id {
            Some(sid) => vec![sid],
            None => vec![],
        };
        Self { id, subs }
    }
}

impl ByteConvert for String {
    fn to_bytes(&self) -> Vec<u8> {
        self.to_owned().into_bytes()
    }

    fn from_bytes(buf: &[u8]) -> (Self, usize) {
        (std::str::from_utf8(buf).unwrap().to_string(), buf.len())
    }
}

impl<T: ByteConvert> ByteConvert for Vec<T> {
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for e in self {
            out.extend(e.to_bytes());
        }
        let mut prefix = (out.len() as u32).to_be_bytes();
        prefix.reverse();

        for b in prefix {
            out.insert(0, b)
        }
        out
    }

    fn from_bytes(buf: &[u8]) -> (Self, usize) {
        let mut out = Vec::new();
        let mut i = std::mem::size_of::<u32>();
        let l = u32::from_be_bytes(buf[..i].try_into().unwrap()) as usize + i;
        while i < l {
            let (e, size) = T::from_bytes(&buf[i..]);
            out.push(e);
            i = i + size;
        }
        (out, i)
    }
}

impl<T: ByteConvert + Clone> ByteConvert for Box<[T]> {
    fn to_bytes(&self) -> Vec<u8> {
        let v: Vec<T> = self.to_vec();
        v.to_bytes()
    }

    fn from_bytes(buf: &[u8]) -> (Self, usize) {
        let (v, l) = Vec::<T>::from_bytes(buf);
        (v.into_boxed_slice(), l)
    }
}

//TODO look into how to do this properly
impl<T: ByteConvert> ByteConvert for WeightedEdge<T> {
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(self.id.to_bytes());
        out.extend(self.rate.to_be_bytes());
        out
    }

    fn from_bytes(buf: &[u8]) -> (Self, usize) {
        let i1 = std::mem::size_of::<T>();
        let i2 = std::mem::size_of::<f32>() + i1;
        let (id, _) = T::from_bytes(&buf[..i1]);
        let rate = f32::from_be_bytes(buf[i1..i2].try_into().unwrap());
        (Self { id, rate }, i2)
    }
}

impl<T1: ByteConvert, T2: ByteConvert> ByteConvert for HierEdge<T1, T2> {
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(self.id.to_bytes());
        out.extend(Vec::<T2>::to_bytes(&self.subs));
        out
    }

    fn from_bytes(buf: &[u8]) -> (Self, usize) {
        let i2 = std::mem::size_of::<T1>();
        let (id, _) = T1::from_bytes(&buf[..i2]);
        let (subs, vsize) = Vec::<T2>::from_bytes(buf[i2..].try_into().unwrap());
        (Self { id, subs }, vsize + i2)
    }
}

impl InstToWorkPrep {
    fn new() -> Self {
        Self {
            total_authors: 0,
            inst_specs: Vec::new(),
        }
    }
}

impl InstPrepInner {
    fn new(inst_id: InstId) -> Self {
        Self {
            inst_id,
            authors: 0,
        }
    }
}

impl FilePointer {
    fn read_next<T: Read>(reader: &mut T) -> Option<Self> {
        const S1: usize = std::mem::size_of::<u64>();
        const S2: usize = std::mem::size_of::<u32>();
        const TOTAL_SIZE: usize = S1 + S2;
        let mut buf: [u8; TOTAL_SIZE] = [0; TOTAL_SIZE];
        match reader.read_exact(&mut buf) {
            Ok(_) => Some(Self {
                offset: u64::from_be_bytes(buf[0..S1].try_into().unwrap()),
                count: u32::from_be_bytes(buf[S1..].try_into().unwrap()),
            }),
            Err(_) => return None,
        }
    }
}

impl MappContainer {
    pub fn get(&self, id: &MidId) -> Option<&MappedAttributes> {
        Some(&self.mapps[*id as usize])
    }

    pub fn from_name<T1, T2>(stowage: &Stowage, var_att_name: &str) -> Self
    where
        SmolId: From<T1> + From<T2>,
        T1: Copy + ByteConvert,
        T2: Copy + ByteConvert,
    {
        let base = VarReader::<Vec<HierEdge<T1, T2>>>::new(stowage, var_att_name);
        let mut mapp = Vec::new();
        for hedges in base
            .tqdm()
            .desc(Some(format!("ares from {}", var_att_name)))
        {
            mapp.push(MappedAttributes::from_hedges(hedges));
        }
        Self {
            mapps: mapp.into_boxed_slice(),
        }
    }
}

impl MappedAttributes {
    fn from_hedges<T1, T2>(hedges: Vec<HierEdge<T1, T2>>) -> Self
    where
        SmolId: From<T1> + From<T2>,
        T1: Copy,
        T2: Copy,
    {
        type InnerType = SmolId;
        //TODO: spare some space with repetitions
        let mut outer = Vec::new();
        for hedge in hedges {
            let hedge_main = InnerType::try_from(hedge.id).unwrap();
            let mut inner = Vec::new();
            for subsubid in &hedge.subs {
                inner.push(InnerType::try_from(*subsubid).unwrap());
            }
            outer.push((hedge_main, MappedAttributes::List(inner.into_boxed_slice())))
        }
        Self::Map(outer.into_boxed_slice())
    }

    pub fn iter_inner(&self) -> std::slice::Iter<'_, (SmolId, MappedAttributes)> {
        match self {
            Self::List(_) => panic!("no more levels"),
            Self::Map(vhs) => vhs.iter(),
        }
    }
}

macro_rules! fillem {
    ($id_map:ident, $($vname:ident => $cls:ident,)*) => {
        $(let mut $vname = Vec::new();)*

        for _ in (0..$id_map.len() + 1) {
            $($vname.push($cls::new());)*
        }

        $(let mut $vname = $vname.into_boxed_slice();)*
    };
}

macro_rules! id_impl {
    ($($idt:ident,)*) => {
        $(impl ByteConvert for $idt {
            fn to_bytes(&self) -> Vec<u8> {
                let mut out: Vec<u8> = vec![];
                out.extend(self.to_be_bytes());
                out
            }
            fn from_bytes(buf: &[u8]) -> (Self, usize) {
                let cs = std::mem::size_of::<$idt>();
                (Self::from_be_bytes(buf[..cs].try_into().unwrap()), cs)
            }
        })*
    };
}

id_impl!(u8, u16, u32, u64,);

fn var_write_meta<R, F, S>(
    stowage: &Stowage,
    mut out: Box<[S]>,
    att: &'static str,
    clojure: F,
    out_name: &'static str,
) -> io::Result<()>
where
    R: DeserializeOwned,
    F: Fn(R, &mut Box<[S]>),
    S: ByteConvert,
{
    for obj in stowage.read_csv_objs::<R>(works::C, att) {
        clojure(obj, &mut out);
    }
    write_var_att(stowage, out_name, out.iter())
}

#[allow(unused_mut)]
pub fn write_var_atts(stowage: &Stowage) -> io::Result<()> {
    write_names(stowage)?;

    let inst_countries = read_fix_att(stowage, names::I2C);

    let topic_id_map = get_idmap(stowage, topics::C).to_map();
    let inst_id_map = get_idmap(stowage, institutions::C).to_map();
    let work_id_map = get_idmap(stowage, works::C).to_map();
    let source_id_map = get_idmap(stowage, sources::C).to_map();
    let author_id_map = get_idmap(stowage, authors::C).to_map();

    let subfield_ancestors: Vec<FieldId> = read_fix_att(stowage, names::ANCESTOR);
    let topic_subfields: Vec<FieldId> = read_fix_att(stowage, names::TOPIC_SUBFIELDS);

    fillem!(
        work_id_map,
        rel_preps => InstToWorkPrep,
        country_hiers => HEdgeSet,
        concept_hiers => HEdgeSet,
        q_hiers => HEdgeSet,
        // to_cited => Vec,
        to_citing => Vec,
        to_source => Vec,
    );
    fillem!(inst_id_map, i2w => Vec,);
    fillem!(author_id_map, a2w => Vec,);

    var_write_meta::<WorkTopic, _, _>(
        stowage,
        concept_hiers,
        works::atts::topics,
        |w_conc, c_hiers| {
            if w_conc.score.unwrap() < 0.6 {
                return;
            };
            if let (Some(work_id), Some(topic_id)) = (
                work_id_map.get(&oa_id_parse(&w_conc.parent_id.unwrap())),
                topic_id_map.get(&oa_id_parse(&w_conc.topic_id.unwrap())),
            ) {
                let ch_set: &mut HEdgeSet<FieldId, SubFieldId> = &mut c_hiers[*work_id as usize];
                let subfield_id = topic_subfields[*topic_id as usize];
                let field_id = subfield_ancestors[subfield_id as usize];
                ch_set.add(field_id, Some(subfield_id));
            }
        },
        vnames::CONCEPT_H,
    )?;

    let wq_cloj = |wq: WorkQ, qhs: &mut Box<[HEdgeSet<QId, SourceId>]>| {
        if let (Some(work_id), Some(source_id)) =
            (work_id_map.get(&wq.id), source_id_map.get(&wq.source))
        {
            let h_set: &mut HEdgeSet<QId, SourceId> = &mut qhs[*work_id as usize];
            h_set.add(wq.best_q, Some(*source_id as SourceId));
        };
    };

    var_write_meta(stowage, q_hiers, QS, wq_cloj, vnames::W2QS)?;

    var_write_meta::<Location, _, _>(
        stowage,
        to_source,
        works::atts::locations,
        |sobj, tos| {
            if let Some(source_id_str) = sobj.source_id {
                if let (Some(pid), Some(source_id)) = (
                    work_id_map.get(&oa_id_parse(&sobj.parent_id.unwrap())),
                    source_id_map.get(&oa_id_parse(&source_id_str)),
                ) {
                    tos[*pid as usize].push(*source_id as SourceId);
                }
            }
        },
        vnames::W2S,
    )?;

    var_write_meta::<ReferencedWork, _, _>(
        stowage,
        to_citing,
        works::atts::referenced_works,
        |ref_obj, to_citing_box| {
            if let (Some(pid), Some(refid)) = (
                work_id_map.get(&oa_id_parse(&ref_obj.parent_id.unwrap())),
                work_id_map.get(&oa_id_parse(&ref_obj.referenced_work_id)),
            ) {
                // to_cited[*pid as usize].push(*refid as WorkId);
                to_citing_box[*refid as usize].push(*pid as WorkId);
            }
        },
        vnames::TO_CITING,
    )?;

    for a_ship in stowage.read_csv_objs::<Authorship>(works::C, works::atts::authorships) {
        if let Some(work_id) = work_id_map.get(&oa_id_parse(&a_ship.parent_id.clone().unwrap())) {
            let rel_prep = &mut rel_preps[*work_id as usize];
            rel_prep.total_authors += 1;
            add_to_prep(&a_ship.iter_insts(), &inst_id_map, rel_prep);
            let ch_set: &mut HEdgeSet<CountryId, InstId> = &mut country_hiers[*work_id as usize];
            for iid_str in &a_ship.iter_insts() {
                if let Some(iid) = inst_id_map.get(&oa_id_parse(&iid_str)) {
                    let country_id = inst_countries[*iid as usize];
                    ch_set.add(country_id, Some(*iid as InstId));
                }
            }

            if let Some(aid) = author_id_map.get(&oa_id_parse(&a_ship.author_id.clone().unwrap())) {
                a2w[*aid as usize].push(*work_id as WorkId);
            }
        };
    }
    write_var_att(stowage, vnames::COUNTRY_H, country_hiers.iter())?;
    write_var_att(stowage, vnames::A2W, a2w.iter())?;

    // let mut w2i = Vec::new();
    for (wi, ship_prep) in rel_preps.iter().enumerate() {
        // w2i.push(WeightedEdge::<InstId>::new_vec(&ship_prep));
        for ispec in &ship_prep.inst_specs {
            i2w[ispec.inst_id as usize].push(WeightedEdge {
                id: WorkId::try_from(wi).unwrap(),
                rate: int_div(ispec.authors, ship_prep.total_authors),
            })
        }
    }

    // write_var_att(stowage, vnames::W2I, w2i.iter())?;
    write_var_att(stowage, vnames::I2W, i2w.iter())?;
    Ok(())
}

pub fn get_name_name(entity_name: &str) -> String {
    format!("{}-names", entity_name)
}

pub fn get_attribute_resolver_map(stowage: &Stowage) -> AttributeResolverMap {
    let mut ares_map = HashMap::new();
    build_ar_map::<CountryId, InstId>(stowage, vnames::COUNTRY_H, &mut ares_map);
    build_ar_map::<FieldId, SubFieldId>(stowage, vnames::CONCEPT_H, &mut ares_map);
    build_ar_map::<QId, SourceId>(stowage, vnames::W2QS, &mut ares_map);
    ares_map
}

pub fn read_var_att<T: ByteConvert>(stowage: &Stowage, att_name: &str) -> Vec<T> {
    println!("reading var length attributes: {}", att_name);
    let mut out = Vec::new();
    for v in VarReader::new(stowage, att_name) {
        out.push(v)
    }
    out
}

pub fn get_mapped_atts(resolver_id: &str) -> Vec<String> {
    let mut hm = HashMap::new();
    hm.insert(
        vnames::COUNTRY_H,
        vec![COUNTRIES.to_string(), institutions::C.to_string()],
    );
    hm.insert(
        vnames::CONCEPT_H,
        vec![fields::C.to_string(), subfields::C.to_string()],
    );
    // hm.insert(vnames::W2S, vec![sources::C.to_string()]);
    hm.insert(vnames::W2QS, vec![QS.to_string(), sources::C.to_string()]);
    hm.get(resolver_id).unwrap().to_vec()
}

fn write_names(stowage: &Stowage) -> io::Result<()> {
    let nclosure = |o: NamedFieldEntity| o.display_name;
    for fid in vec![fields::C, subfields::C] {
        inner_str_write::<NamedFieldEntity, _>(stowage, fid, fid, fid, "main", nclosure)?;
    }
    inner_str_write::<Geo, _>(
        stowage,
        COUNTRIES,
        COUNTRIES,
        institutions::C,
        institutions::atts::geo,
        |o| o.country,
    )?;

    let mut q_names: Vec<String> = (0..5).map(|i| format!("Q{}", i)).collect();
    q_names.push("Uncategorized".to_owned());
    write_var_att(stowage, &get_name_name(QS), q_names.iter())?;

    for ename in vec![institutions::C, sources::C, authors::C] {
        inner_str_write::<NamedEntity, _>(stowage, ename, ename, ename, "main", |o| o.display_name)?
    }
    Ok(())
}

fn inner_str_write<T, F>(
    stowage: &Stowage,
    map_base: &str,
    entity_name: &str,
    main: &str,
    sub: &str,
    name_getter: F,
) -> io::Result<()>
where
    T: DeserializeOwned + ParsedId + Send + Sync,
    F: Fn(T) -> String + Send,
    Arc<StrFiller<T, F>>: Send,
{
    let filler = StrFiller::<T, F>::new(stowage, map_base, name_getter)
        .para(stowage.read_csv_objs::<T>(main, sub));
    write_var_att(
        stowage,
        &get_name_name(entity_name),
        filler.names.lock().unwrap().iter(),
    )?;
    Ok(())
}

fn add_to_prep(
    str_ids: &Vec<String>,
    inst_id_map: &HashMap<BigId, BigId>,
    rel_prep: &mut InstToWorkPrep,
) {
    'outer: for inst_id_str in str_ids {
        if let Some(iid_raw) = inst_id_map.get(&oa_id_parse(&inst_id_str)) {
            let iid = *iid_raw as InstId;
            for iprep in &mut rel_prep.inst_specs {
                if iprep.inst_id == iid {
                    iprep.authors += 1;
                    continue 'outer;
                }
            }
            rel_prep.inst_specs.push(InstPrepInner::new(iid))
        }
    }
}

fn write_var_att<'a, T, E>(stowage: &Stowage, att_name: &str, targets: T) -> io::Result<()>
where
    T: Iterator<Item = &'a E>,
    E: ByteConvert + 'a,
{
    let mut ptr = FilePointer {
        offset: 0,
        count: 0,
    };
    let att_dir = stowage.var_atts.join(att_name);
    create_dir_all(&att_dir).unwrap();
    let mut counts_file = File::create(&att_dir.join("sizes")).unwrap();
    let mut targets_file = File::create(&att_dir.join("targets")).unwrap();
    for ts in targets
        .tqdm()
        .desc(Some(format!("writing var-atts {}", att_name)))
    {
        let barr = ts.to_bytes();
        targets_file.write(&barr)?;
        ptr.count = barr.len() as u32;
        write_to_sizes(&mut counts_file, &ptr)?;
        ptr.offset += ptr.count as u64;
    }
    Ok(())
}

fn build_ar_map<T1, T2>(stowage: &Stowage, var_att_name: &str, ares_map: &mut AttributeResolverMap)
where
    SmolId: From<T1> + From<T2>,
    T1: Copy + ByteConvert,
    T2: Copy + ByteConvert,
{
    ares_map.insert(
        var_att_name.to_string(),
        MappContainer::from_name::<T1, T2>(stowage, var_att_name),
    );
    println!("built, inserted");
}

fn write_to_sizes<T: Write>(writer: &mut T, ptr: &FilePointer) -> io::Result<()> {
    writer.write(&ptr.offset.to_be_bytes())?;
    writer.write(&ptr.count.to_be_bytes())?;
    Ok(())
}

fn int_div<T>(dividend: T, divisor: T) -> f32
where
    f32: From<T>,
{
    f32::from(dividend) / f32::from(divisor)
}
