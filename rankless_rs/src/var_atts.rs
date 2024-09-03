use std::{
    io,
    marker::PhantomData,
    sync::{Arc, Mutex},
};

use hashbrown::HashMap;
use serde::{de::DeserializeOwned, Deserialize};
use tqdm::Iter;

use crate::{
    add_strict_parsed_id_traits,
    common::{oa_id_parse, short_string_to_u64, BigId, ParsedId, Stowage, AREA_FIELDS, QS},
    csv_writers::{institutions, sources, works},
    entity_mapping::SourceArea,
    gen_fix_att_structs::{InstCountries, SubfieldAncestors, TopicSubfields},
    gen_types::{
        AreaFields, Authors, Countries, Fields, Institutions, Qs, Sources, Subfields, Topics, Works,
    },
    oa_structs::{
        post::{Authorship, Location},
        FieldLike, Geo, ReferencedWork, WorkTopic,
    },
};

use dmove::{
    para::Worker, ByteArrayInterface, Entity, IdMappedEntity, InMemoryVarAttBuilder,
    InMemoryVarAttributeElement, LoadedIdMap, MainBuilder, MetaIntegrator,
};

type InstId = <Institutions as Entity>::T;
type QId = <Qs as Entity>::T;
type SourceId = <Sources as Entity>::T;
type WorkId = <Works as Entity>::T;
type CountryId = <Countries as Entity>::T;
type FieldId = <Fields as Entity>::T;
type SubFieldId = <Subfields as Entity>::T;
type AreaFieldId = <AreaFields as Entity>::T;

macro_rules! fillem {
    ($id_map:ident, $($vname:ident => $cls:ident,)*) => {
        $(let mut $vname = Vec::new();)*

        for _ in (0..$id_map.len() + 1) {
            $($vname.push($cls::new());)*
        }

        $(let mut $vname = $vname.into_boxed_slice();)*
    };
}

add_strict_parsed_id_traits!(NamedEntity);

pub struct WeightedEdge<T> {
    pub id: T,
    rate: f32,
}

#[derive(Debug)]
pub struct HierEdge<T1, T2> {
    pub id: T1,
    pub subs: Vec<T2>,
}

#[derive(Debug)]
pub struct HEdgeSet<T1, T2>(Vec<HierEdge<T1, T2>>);

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
struct WorkQ {
    id: BigId,
    source: BigId,
    best_q: QId,
}

struct StrFiller<T, E>
where
    E: Entity,
{
    id_map: LoadedIdMap,
    names: Mutex<Box<[String]>>,
    phantom: PhantomData<T>,
    phantom_e: PhantomData<E>,
}

struct VarStow {
    stowage: Stowage,
    builder: MainBuilder,
}

trait Named {
    fn get_name(&self) -> String;
}

impl VarStow {
    fn new(stowage: Stowage) -> Self {
        let builder = MainBuilder::new(&stowage.var_atts);
        Self { stowage, builder }
    }

    #[allow(unused_mut)]
    fn write_all(mut self) -> io::Result<usize> {
        self.write_names()?;

        let topic_id_map = self.stowage.get_idmap::<Topics>().to_map();
        let inst_id_map = self.stowage.get_idmap::<Institutions>().to_map();
        let work_id_map = self.stowage.get_idmap::<Works>().to_map();
        let source_id_map = self.stowage.get_idmap::<Sources>().to_map();
        let author_id_map = self.stowage.get_idmap::<Authors>().to_map();
        let af_id_map = self.stowage.get_idmap::<AreaFields>().to_map();

        let inst_countries = self.stowage.get_fix_att::<InstCountries>();
        let subfield_ancestors = self.stowage.get_fix_att::<SubfieldAncestors>();
        let topic_subfields = self.stowage.get_fix_att::<TopicSubfields>();

        //TODO: cast them to Box<[T]> for writing out
        fillem!(
            work_id_map,
            rel_preps => InstToWorkPrep,
            country_hiers => HEdgeSet,
            topic_hiers => HEdgeSet,
            q_hiers => HEdgeSet,
            // to_cited => Vec,
            to_citing => Vec,
            to_source => Vec,
        );
        fillem!(inst_id_map, i2w => Vec,);
        fillem!(author_id_map, a2w => Vec,);
        fillem!(source_id_map, s2af => Vec,);

        let w_t_cj = |w_topic: WorkTopic,
                      topic_hiers: &mut Box<[HEdgeSet<FieldId, SubFieldId>]>| {
            if w_topic.score.unwrap() < 0.6 {
                return;
            };
            if let (Some(work_id), Some(topic_id)) = (
                work_id_map.get(&oa_id_parse(&w_topic.parent_id.unwrap())),
                topic_id_map.get(&oa_id_parse(&w_topic.topic_id.unwrap())),
            ) {
                let ch_set = &mut topic_hiers[*work_id as usize];
                let subfield_id = topic_subfields[*topic_id as usize];
                let field_id = subfield_ancestors[subfield_id as usize];
                ch_set.add(field_id, Some(subfield_id));
            }
        };

        let w_q_cj = |wq: WorkQ, qhs: &mut Box<[HEdgeSet<QId, SourceId>]>| {
            if let (Some(work_id), Some(source_id)) =
                (work_id_map.get(&wq.id), source_id_map.get(&wq.source))
            {
                let h_set: &mut HEdgeSet<QId, SourceId> = &mut qhs[*work_id as usize];
                h_set.add(wq.best_q, Some(*source_id as SourceId));
            };
        };

        let w_l_cj = |sobj: Location, tos: &mut Box<[Vec<SourceId>]>| {
            if let Some(source_id_str) = sobj.source_id {
                if let (Some(pid), Some(source_id)) = (
                    work_id_map.get(&oa_id_parse(&sobj.parent_id.unwrap())),
                    source_id_map.get(&oa_id_parse(&source_id_str)),
                ) {
                    tos[*pid as usize].push(*source_id as SourceId);
                }
            }
        };

        let w_cite_cj = |ref_obj: ReferencedWork, to_citing_box: &mut Box<[Vec<WorkId>]>| {
            if let (Some(pid), Some(refid)) = (
                work_id_map.get(&oa_id_parse(&ref_obj.parent_id.unwrap())),
                work_id_map.get(&oa_id_parse(&ref_obj.referenced_work_id)),
            ) {
                // to_cited[*pid as usize].push(*refid as WorkId);
                to_citing_box[*refid as usize].push(*pid as WorkId);
            }
        };

        {
            use works::atts::{locations, referenced_works, topics};
            use works::C;
            self.write_meta(topic_hiers, C, topics, w_t_cj, "w2topic-hier")?;
            self.write_meta(q_hiers, C, QS, w_q_cj, "w2qs")?;
            self.write_meta(to_source, C, locations, w_l_cj, "w2loc")?;
            self.write_meta(to_citing, C, referenced_works, w_cite_cj, "w2citing")?;
        }
        self.write_meta(
            s2af,
            sources::C,
            AREA_FIELDS,
            |ssource: SourceArea, cbox: &mut Box<[Vec<AreaFieldId>]>| {
                if let (Some(af_id), Some(source_id)) = (
                    af_id_map.get(&ssource.raw_area_id()),
                    source_id_map.get(&ssource.get_parsed_id()),
                ) {
                    cbox[*source_id as usize].push(*af_id as AreaFieldId);
                }
            },
            "s2af",
        )?;

        for a_ship in self
            .stowage
            .read_csv_objs::<Authorship>(works::C, works::atts::authorships)
        {
            if let Some(work_id) = work_id_map.get(&oa_id_parse(&a_ship.parent_id.clone().unwrap()))
            {
                let rel_prep = &mut rel_preps[*work_id as usize];
                rel_prep.total_authors += 1;
                add_to_prep(&a_ship.iter_insts(), &inst_id_map, rel_prep);
                let ch_set: &mut HEdgeSet<CountryId, InstId> =
                    &mut country_hiers[*work_id as usize];
                for iid_str in &a_ship.iter_insts() {
                    if let Some(iid) = inst_id_map.get(&oa_id_parse(&iid_str)) {
                        let country_id = inst_countries[*iid as usize];
                        ch_set.add(country_id, Some(*iid as InstId));
                    }
                }

                if let Some(aid) =
                    author_id_map.get(&oa_id_parse(&a_ship.author_id.clone().unwrap()))
                {
                    a2w[*aid as usize].push(*work_id as WorkId);
                }
            };
        }
        self.write_one(country_hiers.iter(), "w2country-hier")?;
        self.write_one(a2w.iter(), "a2w")?;

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
        self.write_one(i2w.iter(), "i2w")?;
        self.builder
            .write_code("rankless_rs/src/gen_var_att_structs.rs")
    }

    fn write_names(&mut self) -> io::Result<usize> {
        self.main_str_write::<FieldLike, Fields>()?;
        self.main_str_write::<FieldLike, Subfields>()?;
        self.main_str_write::<NamedEntity, Institutions>()?;
        self.main_str_write::<NamedEntity, Sources>()?;
        self.main_str_write::<NamedEntity, Authors>()?;

        self.inner_str_write::<Geo, Countries>(Institutions::NAME, institutions::atts::geo)?;

        let mut q_names: Vec<String> = vec!["Uncategorized".to_owned()];
        (1..5).for_each(|i| q_names.push(format!("Q{}", i)));
        InMemoryVarAttBuilder::add_iter(&mut self.builder, q_names.iter(), &get_name_name(QS));
        Ok(0)
    }

    fn main_str_write<T, E>(&mut self) -> io::Result<usize>
    where
        T: DeserializeOwned + ParsedId + Named + Send + Sync,
        Arc<StrFiller<T, E>>: Send,
        E: Entity + IdMappedEntity,
    {
        self.inner_str_write::<T, E>(E::NAME, "main")
    }

    fn inner_str_write<T, E>(&mut self, main: &str, sub: &str) -> io::Result<usize>
    where
        T: DeserializeOwned + ParsedId + Named + Send + Sync,
        Arc<StrFiller<T, E>>: Send,
        E: Entity + IdMappedEntity,
    {
        let filler =
            StrFiller::<T, E>::new(&self.stowage).para(self.stowage.read_csv_objs::<T>(main, sub));
        let names = filler.names.lock().unwrap();
        self.write_one(names.iter(), &get_name_name(E::NAME))
    }

    fn write_meta<R, F, S>(
        &mut self,
        mut out: Box<[S]>,
        parent: &'static str,
        att: &'static str,
        clojure: F,
        out_name: &'static str,
    ) -> io::Result<usize>
    where
        R: DeserializeOwned,
        F: Fn(R, &mut Box<[S]>),
        S: InMemoryVarAttributeElement,
    {
        for obj in self.stowage.read_csv_objs::<R>(parent, att) {
            clojure(obj, &mut out);
        }
        self.write_one(
            out.iter()
                .tqdm()
                .desc(Some(format!("writing var att {}", out_name))),
            out_name,
        )
    }

    fn write_one<'a, I, T>(&mut self, iter: I, name: &str) -> io::Result<usize>
    where
        I: Iterator<Item = &'a T>,
        T: InMemoryVarAttributeElement,
        T: 'a,
    {
        InMemoryVarAttBuilder::add_iter(&mut self.builder, iter, name);
        Ok(0)
    }
}

impl<T, E> StrFiller<T, E>
where
    E: Entity + IdMappedEntity,
{
    fn new(stowage: &Stowage) -> Self {
        //TODO: some of these have already been read
        let id_map = stowage.get_idmap::<E>();
        let mut names = Vec::new();
        for _ in id_map.iter_ids(true) {
            names.push("".to_string());
        }
        Self {
            id_map: id_map.to_map(),
            names: Mutex::new(names.into_boxed_slice()),
            phantom: PhantomData,
            phantom_e: PhantomData,
        }
    }
}

impl ParsedId for Geo {
    fn get_parsed_id(&self) -> BigId {
        short_string_to_u64(&self.country_code.clone().unwrap_or("".to_string()))
    }
}

impl<T, E> Worker<T> for StrFiller<T, E>
where
    Arc<Self>: Send,
    T: ParsedId + Named + Send + Sync,
    E: Entity,
{
    fn proc(&self, input: T) {
        if let Some(id) = self.id_map.get(&input.get_parsed_id()) {
            self.names.lock().unwrap()[*id as usize] = input.get_name();
        }
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

impl<T1, T2> HierEdge<T1, T2> {
    fn new(id: T1, sub_id: Option<T2>) -> Self {
        let subs = match sub_id {
            Some(sid) => vec![sid],
            None => vec![],
        };
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

impl Named for NamedEntity {
    fn get_name(&self) -> String {
        self.display_name.clone()
    }
}

impl Named for FieldLike {
    fn get_name(&self) -> String {
        self.display_name.clone()
    }
}

impl Named for Geo {
    fn get_name(&self) -> String {
        self.country.clone().unwrap_or("".to_string())
    }
}

impl<T1, T2> ByteArrayInterface for HEdgeSet<T1, T2>
where
    T1: ByteArrayInterface,
    T2: ByteArrayInterface,
{
    fn to_bytes(&self) -> Box<[u8]> {
        self.0.to_bytes()
    }

    fn from_bytes(buf: &[u8]) -> Self {
        Self(Vec::<HierEdge<T1, T2>>::from_bytes(buf))
    }
}

impl<T1, T2> InMemoryVarAttributeElement for HEdgeSet<T1, T2>
where
    T1: ByteArrayInterface,
    T2: ByteArrayInterface,
{
}

impl<T1, T2> ByteArrayInterface for HierEdge<T1, T2>
where
    T1: ByteArrayInterface,
    T2: ByteArrayInterface,
{
    fn to_bytes(&self) -> Box<[u8]> {
        let mut out = self.id.to_bytes().to_vec();
        out.extend(self.subs.to_bytes());
        out.into()
    }

    fn from_bytes(buf: &[u8]) -> Self {
        let cutoff = std::mem::size_of::<T1>();
        Self {
            id: T1::from_bytes(&buf[..cutoff]),
            subs: Vec::<T2>::from_bytes(&buf[cutoff..]),
        }
    }
}

impl<T1, T2> InMemoryVarAttributeElement for HierEdge<T1, T2>
where
    T1: ByteArrayInterface,
    T2: ByteArrayInterface,
{
}

impl<T> ByteArrayInterface for WeightedEdge<T>
where
    T: ByteArrayInterface,
{
    fn to_bytes(&self) -> Box<[u8]> {
        let mut out = self.id.to_bytes().to_vec();
        out.extend(self.rate.to_bytes());
        out.into()
    }

    fn from_bytes(buf: &[u8]) -> Self {
        let cutoff = std::mem::size_of::<T>();
        Self {
            id: T::from_bytes(&buf[..cutoff]),
            rate: f32::from_bytes(&buf[cutoff..]),
        }
    }
}

// impl InMemoryVarAttributeElement for WeightedEdge<u32> {
//     const DIVISOR: usize = std::mem::size_of::<u32>();
// }

impl<T> InMemoryVarAttributeElement for WeightedEdge<T>
where
    T: ByteArrayInterface,
{
    // default const DIVISOR: usize = 1;
}

pub fn write_var_atts(stowage: Stowage) -> io::Result<usize> {
    return VarStow::new(stowage).write_all();
}

pub fn get_name_name(entity_name: &str) -> String {
    format!("{}-names", entity_name)
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

fn int_div<T>(dividend: T, divisor: T) -> f32
where
    f32: From<T>,
{
    f32::from(dividend) / f32::from(divisor)
}
