use std::{
    fs::{create_dir_all, File},
    io::{self, Read, Seek, SeekFrom, Write},
    marker::PhantomData,
    usize,
};

use hashbrown::HashMap;
use serde::{de::DeserializeOwned, Deserialize};
use tqdm::Iter;

use crate::{
    add_strict_parsed_id_traits,
    common::{
        oa_id_parse, BigId, ParsedId, Stowage, CONCEPTS, COUNTRIES, INSTS, MAIN_CONCEPTS, QS,
        SOURCES, SUB_CONCEPTS, WORKS,
    },
    ingest_entity::get_idmap,
    oa_entity_mapping::short_string_to_u64,
    oa_filters::InstAuthorship,
    oa_fix_atts::{names, read_fix_att},
    oa_structs::{Ancestor, Location, ReferencedWork, WorkConcept},
};

pub mod vnames {
    pub const I2W: &str = "i2w";
    pub const W2I: &str = "w2i";
    pub const CONCEPT_H: &str = "concept-hierarchy";
    pub const COUNTRY_H: &str = "country-hierarchy";
    pub const CONCEPT_ANC: &str = "concept-ancestors";
    pub const W2S: &str = "w2s";
    pub const W2QS: &str = "w2qs";
    pub const TO_CITED: &str = "to-cited";
    pub const TO_CITING: &str = "to-citing";
}

pub type MidId = u32;
pub type SmolId = u16;
type CountryId = u8;
type QId = u8;
type InstId = u16; // TODO figure this shit out to be dynamic properly
type ConceptId = u16;
type SourceId = u16;
pub type WorkId = u32;
pub type AttributeResolverMap = HashMap<String, HashMap<MidId, MappedAttributes>>;

pub enum MappedAttributes {
    List(Vec<SmolId>),
    Map(Vec<(SmolId, MappedAttributes)>),
}

pub struct WeightedEdge<T> {
    pub id: T,
    rate: f32,
}

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

struct HEdgeSet<T1, T2>(Vec<HierEdge<T1, T2>>);

pub struct FilePointer {
    offset: u64,
    pub count: u32, // these might also be optimized to be smaller
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
                if (fp.count as usize) > MAX_BUF {
                    panic!("too large block {:?}", fp.count);
                }
                self.targets_file
                    .read_exact(&mut self.buf[..fp.count as usize])
                    .unwrap();
                let (v, _) = T::from_bytes(&self.buf[..fp.count as usize]);
                Some(v)
            }
            None => None,
        }
    }
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
add_strict_parsed_id_traits!(NamedEntity);

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

impl WeightedEdge<InstId> {
    fn new_vec(prep: &InstToWorkPrep) -> Vec<Self> {
        let mut out = Vec::new();
        for ip in &prep.inst_specs {
            out.push(Self {
                id: ip.inst_id,
                rate: int_div(ip.authors, prep.total_authors),
            })
        }
        out
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

impl MappedAttributes {
    fn from_hedges<T1, T2>(hedges: Vec<HierEdge<T1, T2>>) -> Self
    where
        SmolId: From<T1> + From<T2>,
        T1: Copy,
        T2: Copy,
    {
        type InnerType = SmolId;
        let mut sub_matts = Vec::new();
        for hedge in hedges {
            let hedge_main = InnerType::try_from(hedge.id).unwrap();
            let mut sub_sub = Vec::new();
            for subsubid in &hedge.subs {
                sub_sub.push(InnerType::try_from(*subsubid).unwrap());
            }
            sub_matts.push((hedge_main, Self::List(sub_sub)));
        }
        Self::Map(sub_matts)
    }

    pub fn iter_inner(&self) -> std::slice::Iter<'_, (SmolId, MappedAttributes)> {
        match self {
            Self::List(_) => panic!("no more levels"),
            Self::Map(vhs) => vhs.iter(),
        }
    }
}

pub fn write_var_atts(stowage: &Stowage) -> io::Result<()> {
    let inst_countries = read_fix_att(stowage, names::I2C);
    let concept_levels = read_fix_att(stowage, names::CLEVEL);

    //NAMES
    for ename in vec![INSTS, SOURCES, CONCEPTS] {
        write_names(stowage, ename)?
    }
    let mut q_names: Vec<String> = (0..5).map(|i| format!("Q{}", i)).collect();
    q_names.push("Uncategorized".to_owned());
    write_var_att(stowage, &get_name_name(QS), q_names.iter())?;

    // concept parents
    //
    println!("getting maps");

    let conc_id_map = get_idmap(stowage, CONCEPTS).to_map();
    let inst_id_map = get_idmap(stowage, INSTS).to_map();
    let work_id_map = get_idmap(stowage, WORKS).to_map();
    let source_id_map = get_idmap(stowage, SOURCES).to_map();
    println!("got maps");

    inner_str_write::<Geo, _>(stowage, COUNTRIES, INSTS, "geo", |o| o.country)?;

    let mut ancestors: Vec<Vec<ConceptId>> = Vec::new();
    for _ in 0..(conc_id_map.len() + 1) {
        //TODO this pattern repeats a _lot_
        ancestors.push(Vec::new());
    }

    let mut rel_preps: Vec<InstToWorkPrep> = Vec::new();
    let mut country_hiers: Vec<HEdgeSet<CountryId, InstId>> = Vec::new();
    let mut concept_hiers: Vec<HEdgeSet<ConceptId, ConceptId>> = Vec::new();
    let mut q_hiers: Vec<HEdgeSet<QId, SourceId>> = Vec::new();
    let mut to_source: Vec<Vec<SourceId>> = Vec::new();
    let mut to_cited: Vec<Vec<WorkId>> = Vec::new();
    let mut to_citing: Vec<Vec<WorkId>> = Vec::new();

    for _ in 0..(work_id_map.len() + 1) {
        rel_preps.push(InstToWorkPrep::new());
        to_cited.push(Vec::new());
        to_citing.push(Vec::new());
        to_source.push(Vec::new());
        country_hiers.push(HEdgeSet::new());
        concept_hiers.push(HEdgeSet::new());
        q_hiers.push(HEdgeSet::new());
    }

    let mut i2w: Vec<Vec<WeightedEdge<WorkId>>> = Vec::new();
    for _ in 0..(inst_id_map.len() + 1) {
        i2w.push(Vec::new());
    }

    for wq in stowage.read_csv_objs::<WorkQ>(WORKS, "qs") {
        if let (Some(work_id), Some(source_id)) =
            (work_id_map.get(&wq.id), source_id_map.get(&wq.source))
        {
            let h_set = &mut q_hiers[*work_id as usize];
            h_set.add(wq.best_q, Some(*source_id as SourceId));
        };
    }
    write_var_att(stowage, vnames::W2QS, q_hiers.iter())?;

    for anc in stowage.read_csv_objs::<Ancestor>(CONCEPTS, "ancestors") {
        if let (Some(pid), Some(anc_id)) = (
            conc_id_map.get(&oa_id_parse(&anc.parent_id.unwrap())),
            conc_id_map.get(&oa_id_parse(&anc.ancestor_id)),
        ) {
            ancestors[*pid as usize].push(*anc_id as ConceptId);
        }
    }

    write_var_att(stowage, vnames::CONCEPT_ANC, ancestors.iter())?;

    for w_conc in stowage.read_csv_objs::<WorkConcept>(WORKS, "concepts") {
        if w_conc.score.unwrap() < 0.6 {
            continue;
        };
        if let (Some(work_id), Some(cid)) = (
            work_id_map.get(&oa_id_parse(&w_conc.parent_id.unwrap())),
            conc_id_map.get(&oa_id_parse(&w_conc.concept_id.unwrap())),
        ) {
            let ch_set = &mut concept_hiers[*work_id as usize];
            if concept_levels[*cid as usize] == 0 {
                ch_set.add(*cid as ConceptId, None);
            } else {
                for anc in &ancestors[*cid as usize] {
                    ch_set.add(*anc, Some(*cid as ConceptId))
                }
            }
        }
    }

    write_var_att(stowage, vnames::CONCEPT_H, concept_hiers.iter())?;

    for a_ship in stowage.read_csv_objs::<InstAuthorship>(WORKS, "authorships") {
        if let Some(work_id) = work_id_map.get(&oa_id_parse(&a_ship.parent_id)) {
            let rel_prep = &mut rel_preps[*work_id as usize];
            rel_prep.total_authors += 1;
            add_to_prep(&a_ship.iter_insts(), &inst_id_map, rel_prep);

            let ch_set = &mut country_hiers[*work_id as usize];
            for iid_str in &a_ship.iter_insts() {
                if let Some(iid) = inst_id_map.get(&oa_id_parse(&iid_str)) {
                    let country_id = inst_countries[*iid as usize];
                    ch_set.add(country_id, Some(*iid as InstId));
                }
            }
        };
    }

    write_var_att(stowage, vnames::COUNTRY_H, country_hiers.iter())?;

    for sobj in stowage.read_csv_objs::<Location>(WORKS, "locations") {
        if let Some(source_id_str) = sobj.source_id {
            if let (Some(pid), Some(source_id)) = (
                work_id_map.get(&oa_id_parse(&sobj.parent_id.unwrap())),
                source_id_map.get(&oa_id_parse(&source_id_str)),
            ) {
                to_source[*pid as usize].push(*source_id as SourceId);
            }
        }
    }
    write_var_att(stowage, vnames::W2S, to_source.iter())?;

    for ref_obj in stowage.read_csv_objs::<ReferencedWork>(WORKS, "referenced_works") {
        if let (Some(pid), Some(refid)) = (
            work_id_map.get(&oa_id_parse(&ref_obj.parent_id.unwrap())),
            work_id_map.get(&oa_id_parse(&ref_obj.referenced_work_id)),
        ) {
            // to_cited[*pid as usize].push(*refid as WorkId);
            to_citing[*refid as usize].push(*pid as WorkId);
        }
    }
    write_var_att(stowage, vnames::TO_CITING, to_citing.iter())?;
    // write_var_att(stowage, vnames::TO_CITED, to_cited.iter())?;

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
    build_ar_map::<ConceptId, ConceptId>(stowage, vnames::CONCEPT_H, &mut ares_map);
    build_ar_map::<QId, SourceId>(stowage, vnames::W2QS, &mut ares_map);
    ares_map
}

fn write_names(stowage: &Stowage, entity_name: &str) -> io::Result<()> {
    inner_str_write::<NamedEntity, _>(stowage, entity_name, entity_name, "main", |o| {
        o.display_name
    })
}

fn inner_str_write<T, F>(
    stowage: &Stowage,
    entity_name: &str,
    main: &str,
    sub: &str,
    name_getter: F,
) -> io::Result<()>
where
    T: DeserializeOwned + ParsedId,
    F: Fn(T) -> String,
{
    let mut id_map = get_idmap(stowage, entity_name);
    let mut names = Vec::new();
    for _ in 0..(id_map.current_total + 1) {
        names.push("".to_string());
    }
    for obj in stowage.read_csv_objs::<T>(main, sub) {
        if let Some(id) = id_map.get(&obj.get_parsed_id()) {
            names[id as usize] = name_getter(obj);
        }
    }
    write_var_att(stowage, &get_name_name(entity_name), names.iter())?;
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
    for ts in targets.tqdm() {
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
    type InnerType = SmolId;
    // let base: Vec<Vec<HierEdge<T1, T2>>> = read_var_att(stowage, var_att_name);
    let base = VarReader::<Vec<HierEdge<T1, T2>>>::new(stowage, var_att_name);
    // let mut ci = Vec::new();
    let mut ci = HashMap::new();
    println!("building from hedges {}", var_att_name);
    for (mid, hedges) in base.enumerate().tqdm() {
        // ci.push(MappedAttributes::Map(sub_matts));
        // if sub_matts.len() > 0 {
        //     ci.insert(mid as MidId, MappedAttributes::Map(sub_matts));
        // }
        ci.insert(mid as MidId, MappedAttributes::from_hedges(hedges));
    }
    ares_map.insert(var_att_name.to_string(), ci);
    println!("built, inserted");
}

pub fn read_var_att<T: ByteConvert>(stowage: &Stowage, att_name: &str) -> Vec<T> {
    println!("reading var length attributes: {}", att_name);
    let mut out = Vec::new();
    for v in VarReader::new(stowage, att_name) {
        out.push(v)
    }
    out
}

fn write_to_sizes<T: Write>(writer: &mut T, ptr: &FilePointer) -> io::Result<()> {
    writer.write(&ptr.offset.to_be_bytes())?;
    writer.write(&ptr.count.to_be_bytes())?;
    Ok(())
}

pub fn get_mapped_atts(resolver_id: &str) -> Vec<String> {
    let mut hm = HashMap::new();
    hm.insert(
        vnames::COUNTRY_H,
        vec![COUNTRIES.to_string(), INSTS.to_string()],
    );
    hm.insert(
        vnames::CONCEPT_H,
        vec![MAIN_CONCEPTS.to_string(), SUB_CONCEPTS.to_string()],
    );
    // hm.insert(vnames::W2S, vec![SOURCES.to_string()]);
    hm.insert(vnames::W2QS, vec![QS.to_string(), SOURCES.to_string()]);
    hm.get(resolver_id).unwrap().to_vec()
}

fn int_div<T>(dividend: T, divisor: T) -> f32
where
    f32: From<T>,
{
    f32::from(dividend) / f32::from(divisor)
}
