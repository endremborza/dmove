use std::{
    fs::{create_dir_all, File},
    io::{self, Read, Seek, SeekFrom, Write},
    usize,
};

use hashbrown::HashMap;
use serde::Deserialize;
use tqdm::Iter;

use crate::{
    add_strict_parsed_id_traits,
    common::{
        oa_id_parse, BigId, ParsedId, Stowage, CONCEPTS, COUNTRIES, INSTS, MAIN_CONCEPTS, QS,
        SOURCES, SUB_CONCEPTS, WORKS,
    },
    ingest_entity::get_idmap,
    oa_filters::InstAuthorship,
    oa_fix_atts::read_fix_att,
    oa_structs::{Ancestor, Location, ReferencedWork, WorkConcept},
};

pub type MidId = u32;
type CountryId = u8;
type InstId = u16; // TODO figure this shit out to be dynamic properly
type ConceptId = u16;
type SourceId = u16;
type WorkId = u32;
pub type AttributeResolverMap = HashMap<String, HashMap<MidId, MappedAttributes>>;

pub enum MappedAttributes {
    List(Vec<MidId>),
    Map(HashMap<MidId, MappedAttributes>),
}

struct WeightedEdge<T> {
    id: T,
    rate: f32,
}

struct HierEdge<T1, T2> {
    id: T1,
    subs: Vec<T2>,
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

trait ByteConvert {
    fn to_bytes(&self) -> Vec<u8>;
    fn from_bytes(buf: &[u8]) -> Self;
}

macro_rules! id_impl {
    ($($idt:ident,)*) => {
        $(impl ByteConvert for $idt {
            fn to_bytes(&self) -> Vec<u8> {
                let mut out: Vec<u8> = vec![];
                out.extend(self.to_be_bytes());
                out
            }
            fn from_bytes(buf: &[u8]) -> Self {
                Self::from_be_bytes(buf.try_into().unwrap())
            }
        })*
    };
}

id_impl!(u8, u16, u32, u64,);
add_strict_parsed_id_traits!(NamedEntity);

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

    fn from_bytes(buf: &[u8]) -> Self {
        std::str::from_utf8(buf).unwrap().to_string()
    }
}

impl<T: ByteConvert> ByteConvert for Vec<T> {
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for e in self {
            out.extend(e.to_bytes());
        }
        out
    }

    fn from_bytes(buf: &[u8]) -> Self {
        let mut out = Vec::new();
        let mut i = 0;
        // const S: usize = std::mem::size_of::<T>();
        let size: usize = std::mem::size_of::<T>();
        loop {
            out.push(T::from_bytes(&buf[i..(i + size)]));
            i = i + size;
            if i == buf.len() {
                break;
            };
        }
        out
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

    fn from_bytes(buf: &[u8]) -> Self {
        let i2 = std::mem::size_of::<T>();
        let id = T::from_bytes(&buf[..i2]);
        let rate = f32::from_be_bytes(buf[i2..].try_into().unwrap());
        Self { id, rate }
    }
}

impl<T1: ByteConvert, T2: ByteConvert> ByteConvert for HierEdge<T1, T2> {
    fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::new();
        out.extend(self.id.to_bytes());
        out.extend(Vec::<T2>::to_bytes(&self.subs));
        out
    }

    fn from_bytes(buf: &[u8]) -> Self {
        let i2 = std::mem::size_of::<T1>();
        let id = T1::from_bytes(&buf[..i2]);
        let subs = Vec::<T2>::from_bytes(buf[i2..].try_into().unwrap());
        Self { id, subs }
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

pub fn write_var_atts(stowage: &Stowage) -> io::Result<()> {
    //NAMES
    for ename in vec![INSTS, CONCEPTS, SOURCES] {
        write_names(stowage, ename)?
    }

    // concept parents
    //
    println!("getting maps");
    let conc_id_map = get_idmap(stowage, CONCEPTS).to_map();
    let inst_id_map = get_idmap(stowage, INSTS).to_map();
    let work_id_map = get_idmap(stowage, WORKS).to_map();
    let source_id_map = get_idmap(stowage, SOURCES).to_map();
    println!("got maps");

    let mut concept_rdr = stowage.get_sub_reader(CONCEPTS, "ancestors");

    let mut ancestors: Vec<Vec<ConceptId>> = Vec::new();
    for _ in 0..(conc_id_map.len() + 1) {
        //TODO this repeats a _lot_
        ancestors.push(Vec::new());
    }

    for anc_r in concept_rdr
        .deserialize::<Ancestor>()
        .tqdm()
        .desc(Some("ancestors"))
    {
        let anc = anc_r?;
        if let (Some(pid), Some(anc_id)) = (
            conc_id_map.get(&oa_id_parse(&anc.parent_id.unwrap())),
            conc_id_map.get(&oa_id_parse(&anc.ancestor_id)),
        ) {
            ancestors[*pid as usize].push(*anc_id as ConceptId);
        }
    }

    write_var_att(stowage, "concept-ancestors", ancestors.iter())?;

    //1. inst-> papers of inst (with some weighting)
    let mut rdr = stowage.get_sub_reader(WORKS, "authorships");
    let mut ref_rdr = stowage.get_sub_reader(WORKS, "referenced_works");
    let mut conc_rdr = stowage.get_sub_reader(WORKS, "concepts");
    let mut source_rdr = stowage.get_sub_reader(WORKS, "locations");

    let mut rel_preps: Vec<InstToWorkPrep> = Vec::new();
    let mut country_hiers: Vec<HEdgeSet<CountryId, InstId>> = Vec::new();
    let mut concept_hiers: Vec<HEdgeSet<ConceptId, ConceptId>> = Vec::new();
    let mut to_source: Vec<Vec<SourceId>> = Vec::new();
    let mut to_cited: Vec<Vec<WorkId>> = Vec::new();
    let mut to_citing: Vec<Vec<WorkId>> = Vec::new();

    let inst_countries = read_fix_att(stowage, "inst-country");
    let concept_levels = read_fix_att(stowage, "concept-levels");

    for _ in 0..(work_id_map.len() + 1) {
        rel_preps.push(InstToWorkPrep::new());
        to_cited.push(Vec::new());
        to_citing.push(Vec::new());
        to_source.push(Vec::new());
        country_hiers.push(HEdgeSet::new());
        concept_hiers.push(HEdgeSet::new());
    }

    for w_conc_r in conc_rdr
        .deserialize::<WorkConcept>()
        .tqdm()
        .desc(Some("work-concepts"))
    {
        let w_conc = w_conc_r?;
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

    //inst - work relationships
    for a_ship_r in rdr
        .deserialize::<InstAuthorship>()
        .tqdm()
        .desc(Some("authorships"))
    {
        let a_ship = a_ship_r?;
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

    write_var_att(stowage, "work-country-inst", country_hiers.iter())?;
    write_var_att(stowage, "work-concept-hier", concept_hiers.iter())?;

    for source_r in source_rdr
        .deserialize::<Location>()
        .tqdm()
        .desc(Some("sources"))
    {
        let sobj = source_r?;
        if let Some(source_id_str) = sobj.source_id {
            if let (Some(pid), Some(source_id)) = (
                work_id_map.get(&oa_id_parse(&sobj.parent_id.unwrap())),
                source_id_map.get(&oa_id_parse(&source_id_str)),
            ) {
                to_source[*pid as usize].push(*source_id as SourceId);
            }
        }
    }
    write_var_att(stowage, "work-sources", to_source.iter())?;

    for ref_obj_r in ref_rdr
        .deserialize::<ReferencedWork>()
        .tqdm()
        .desc(Some("references"))
    {
        let ref_obj = ref_obj_r?;
        if let (Some(pid), Some(refid)) = (
            work_id_map.get(&oa_id_parse(&ref_obj.parent_id.unwrap())),
            work_id_map.get(&oa_id_parse(&ref_obj.referenced_work_id)),
        ) {
            to_citing[*pid as usize].push(*refid as WorkId);
            to_citing[*refid as usize].push(*pid as WorkId);
        }
    }

    let mut i2w: Vec<Vec<WeightedEdge<WorkId>>> = Vec::new();
    for _ in 0..(inst_id_map.len() + 1) {
        i2w.push(Vec::new());
    }

    let mut w2i = Vec::new();
    for (wi, ship_prep) in rel_preps.iter().enumerate() {
        w2i.push(WeightedEdge::<InstId>::new_vec(&ship_prep));
        for ispec in &ship_prep.inst_specs {
            i2w[ispec.inst_id as usize].push(WeightedEdge {
                id: WorkId::try_from(wi).unwrap(),
                rate: int_div(ispec.authors, ship_prep.total_authors),
            })
        }
    }

    write_var_att(stowage, "w2i", w2i.iter())?;
    write_var_att(stowage, "i2w", i2w.iter())?;
    write_var_att(stowage, "to-cited", to_cited.iter())?;
    write_var_att(stowage, "to-citing", to_citing.iter())?;
    Ok(())
}

fn write_names(stowage: &Stowage, entity_name: &str) -> io::Result<()> {
    let mut id_map = get_idmap(stowage, entity_name);
    let mut names = Vec::new();
    let mut rdr = stowage.get_sub_reader(entity_name, "main");
    for _ in 0..(id_map.current_total + 1) {
        names.push("".to_string());
    }
    for obj_r in rdr.deserialize::<NamedEntity>().tqdm() {
        let obj = obj_r?;
        if let Some(id) = id_map.get(&obj.get_parsed_id()) {
            names[id as usize] = obj.display_name;
        }
    }
    write_var_att(stowage, &format!("{}-names", entity_name), names.iter())?;
    Ok(())
}

pub fn read_some(stowage: &Stowage) -> io::Result<()> {
    println!("NAMES: {:?}", read_var_att::<String>(stowage, "inames"));
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

pub fn get_attribute_resolver_map(stowage: &Stowage) -> AttributeResolverMap {
    HashMap::new()
}

pub fn iter_ref_paths(stowage: &Stowage, iid: InstId) -> Vec<Vec<MidId>> {
    Vec::new()
}

fn read_var_att<T: ByteConvert>(stowage: &Stowage, att_name: &str) -> Vec<T> {
    let mut out = Vec::new();
    let att_dir = stowage.var_atts.join(att_name);
    create_dir_all(&att_dir).unwrap();
    let mut counts_file = File::open(&att_dir.join("sizes")).unwrap();
    let mut targets_file = File::open(&att_dir.join("targets")).unwrap();

    const max_buf: usize = 0xFFFF;
    let mut buf: [u8; max_buf] = [0; max_buf];

    loop {
        match FilePointer::read_next(&mut counts_file) {
            Some(fp) => {
                targets_file.seek(SeekFrom::Start(fp.offset)).unwrap();
                targets_file
                    .read_exact(&mut buf[..fp.count as usize])
                    .unwrap();
                out.push(T::from_bytes(&buf[..fp.count as usize]));
            }
            None => break,
        }
    }

    out
}

pub struct FilePointer {
    offset: u64,
    pub count: u32, // this might also be optimized to be smaller
}

impl FilePointer {
    fn read_next<T: Read>(reader: &mut T) -> Option<Self> {
        const s1: usize = std::mem::size_of::<u64>();
        const s2: usize = std::mem::size_of::<u32>();
        const total_size: usize = s1 + s2;
        let mut buf: [u8; total_size] = [0; s1 + s2];
        match reader.read_exact(&mut buf) {
            Ok(_) => Some(Self {
                offset: u64::from_be_bytes(buf[0..s1].try_into().unwrap()),
                count: u32::from_be_bytes(buf[s1..].try_into().unwrap()),
            }),
            Err(_) => return None,
        }
    }
}

fn write_to_sizes<T: Write>(writer: &mut T, ptr: &FilePointer) -> io::Result<()> {
    writer.write(&ptr.offset.to_be_bytes())?;
    writer.write(&ptr.count.to_be_bytes())?;
    Ok(())
}

pub fn read_from_sizes<T: Read + Seek>(reader: &mut T, idx: u32) -> io::Result<FilePointer> {
    let mut obuf: [u8; std::mem::size_of::<u64>()] = [0; std::mem::size_of::<u64>()];
    let mut cbuf: [u8; std::mem::size_of::<u32>()] = [0; std::mem::size_of::<u32>()];
    reader.seek(SeekFrom::Start(idx as u64 * 12))?;
    reader.read_exact(&mut obuf)?;
    reader.read_exact(&mut cbuf)?;
    Ok(FilePointer {
        offset: u64::from_be_bytes(obuf),
        count: u32::from_be_bytes(cbuf),
    })
}

fn get_mapped_atts() {
    let v = vec![
        (
            "country-inst",
            vec![COUNTRIES.to_string(), INSTS.to_string()],
        ),
        (
            "concept-hier",
            vec![MAIN_CONCEPTS.to_string(), SUB_CONCEPTS.to_string()],
        ),
        ("paper-source", vec![SOURCES.to_string()]),
        ("qed-source", vec![QS.to_string(), SOURCES.to_string()]),
    ];
}

struct HEdgeSet<T1, T2>(Vec<HierEdge<T1, T2>>);

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

    fn from_bytes(buf: &[u8]) -> Self {
        Self(Vec::<HierEdge<T1, T2>>::from_bytes(buf))
    }
}

fn int_div<T>(dividend: T, divisor: T) -> f32
where
    f32: From<T>,
{
    f32::from(dividend) / f32::from(divisor)
}
