use std::{io, mem::replace, sync::Arc};

use dmove::{
    ByteFixArrayInterface, DowncastingBuilder, Entity, FixAttBuilder, InitEmpty, MarkedAttribute,
    MetaIntegrator, NamespacedEntity, UnsignedNumber, VariableSizeAttribute, VattArrPair, ET, MAA,
};
use dmove_macro::ByteFixArrayInterface;
use hashbrown::HashMap;
use tqdm::Iter;

use crate::{
    common::{
        init_empty_slice, BeS, CitSubfieldsArrayMarker, InstRelMarker, MainWorkMarker,
        QuickAttPair, QuickMap, RefSubfieldsArrayMarker, WorkLoader, YearlyCitationsMarker,
        YearlyPapersMarker,
    },
    env_consts::{FINAL_YEAR, START_YEAR},
    gen::{
        a1_entity_mapping::{
            Authors, Countries, Institutions, Qs, Sources, Subfields, Topics, Works,
        },
        a2_init_atts::{
            AuthorshipAuthor, AuthorshipInstitutions, InstCountries, SourceYearQs, WorkAuthorships,
            WorkYears,
        },
        derive_links1::{WorkInstitutions, WorkSubfields},
    },
    make_interface_struct, CiteCountMarker, QuickestBox, ReadIter, Stowage,
};

use super::a1_entity_mapping::Years;

pub const N_RELS: usize = 8;
pub const ERA_SIZE: usize = 11;
pub const MAX_YEAR: usize = (FINAL_YEAR - START_YEAR) as usize;
pub const MIN_YEAR: usize = MAX_YEAR - ERA_SIZE + 1;

type YT = ET<Years>;
type IT = ET<Institutions>;
type SfDistRec = [u32; Subfields::N];
pub type EraRec = [u32; ERA_SIZE];

#[derive(Debug, ByteFixArrayInterface)]
pub struct InstRelation {
    pub start: YT,
    pub end: YT,
    pub inst: IT,
    pub papers: u16,
    pub citations: u32,
}

struct ExtensionContainer {
    refed: Vec<SfDistRec>,
    citing: Vec<SfDistRec>,
    papers_by_years: Vec<EraRec>,
    citing_by_years: Vec<EraRec>,
    rels: Vec<[InstRelation; N_RELS]>,
    ref_rec: SfDistRec,
    cit_rec: SfDistRec,
    year_paper_rec: EraRec,
    year_cit_rec: EraRec,
    rel_map_rec: HashMap<ET<Institutions>, InstRelation>,
}

struct CiteDeriver {
    pub stowage: Stowage,
    backends: CDBackends,
}

make_interface_struct!(CDBackends,
    wciting > Works;
    year => WorkYears,
    icountry => InstCountries,
    shipa => AuthorshipAuthor;
    wsubfields -> WorkSubfields,
    winsts -> WorkInstitutions,
    wships -> WorkAuthorships,
    ship_instss -> AuthorshipInstitutions;
);

trait IRelAdder: Entity {
    fn get_by_work(wu: usize, bends: &CDBackends, _id: ET<Self>) -> Vec<IT> {
        let mut o = Vec::new();
        for iid in bends.winsts.get(&wu).unwrap() {
            o.push(*iid);
        }
        o
    }
}

impl InstRelation {
    fn new(inst: ET<Institutions>) -> Self {
        Self {
            start: ET::<Years>::MAX,
            end: ET::<Years>::MIN,
            inst,
            papers: 0,
            citations: 0,
        }
    }

    fn update(&mut self, ccount: u32, year: ET<Years>) {
        self.papers += 1;
        self.citations += ccount;
        if year > self.end {
            self.end = year.clone()
        }
        if year < self.start {
            self.start = year
        }
    }
}

impl ExtensionContainer {
    fn new() -> Self {
        Self {
            refed: Vec::new(),
            citing: Vec::new(),
            papers_by_years: Vec::new(),
            citing_by_years: Vec::new(),
            rels: Vec::new(),
            ref_rec: SfDistRec::init_empty(),
            cit_rec: SfDistRec::init_empty(),
            year_paper_rec: EraRec::init_empty(),
            year_cit_rec: EraRec::init_empty(),
            rel_map_rec: HashMap::new(),
        }
    }

    fn extend_get_ccount<E>(
        &mut self,
        id: ET<E>,
        wid: &ET<Works>,
        bends: &CDBackends,
        year: YT,
    ) -> usize
    where
        E: IRelAdder,
    {
        inc_year(&mut self.year_paper_rec, year);
        let wu = wid.to_usize();
        for sf_id in bends.wsubfields.get(&wu).unwrap() {
            self.ref_rec[*sf_id as usize] += 1;
        }
        let wcs = bends.wciting.get(&wu).unwrap();
        for c_wid in wcs {
            for sf_id in bends.wsubfields.get(&c_wid.to_usize()).unwrap() {
                self.cit_rec[*sf_id as usize] += 1;
            }
            inc_year(&mut self.year_cit_rec, bends.year[c_wid.to_usize()]);
        }
        let o = E::get_by_work(wu, &bends, id);
        for iid in o.into_iter() {
            self.rel_map_rec
                .entry(iid)
                .or_insert_with(|| InstRelation::new(iid))
                .update(wcs.len() as u32, year);
        }
        wcs.len()
    }

    fn push(&mut self) {
        let map_base = replace(&mut self.rel_map_rec, HashMap::new());
        let mut rel_vec: Vec<InstRelation> = map_base.into_values().collect();
        rel_vec.sort_by(|l, r| (r.papers, (r.end - r.start)).cmp(&(l.papers, l.end - l.start)));
        for _ in rel_vec.len()..(N_RELS + 1) {
            rel_vec.push(InstRelation::new(0));
        }
        rel_vec.truncate(N_RELS);
        self.rels.push(rel_vec.try_into().unwrap());
        self.refed
            .push(replace(&mut self.ref_rec, SfDistRec::init_empty()));
        self.citing
            .push(replace(&mut self.cit_rec, SfDistRec::init_empty()));
        self.papers_by_years
            .push(replace(&mut self.year_paper_rec, EraRec::init_empty()));
        self.citing_by_years
            .push(replace(&mut self.year_cit_rec, EraRec::init_empty()));
    }

    fn add_iters<E>(self, stowage: &mut Stowage)
    where
        E: Entity,
    {
        add_marked::<E, FixAttBuilder, CitSubfieldsArrayMarker, _>(
            stowage,
            self.citing,
            format!("{}-cit-subfields", E::NAME),
        );

        add_marked::<E, FixAttBuilder, RefSubfieldsArrayMarker, _>(
            stowage,
            self.refed,
            format!("{}-ref-subfields", E::NAME),
        );

        add_marked::<E, FixAttBuilder, YearlyPapersMarker, _>(
            stowage,
            self.papers_by_years,
            format!("{}-papers-yearly", E::NAME),
        );

        add_marked::<E, FixAttBuilder, YearlyCitationsMarker, _>(
            stowage,
            self.citing_by_years,
            format!("{}-citations-yearly", E::NAME),
        );

        add_marked::<E, FixAttBuilder, InstRelMarker, _>(
            stowage,
            self.rels,
            format!("{}-rel-insts", E::NAME),
        );
    }
}

impl CiteDeriver {
    pub fn new(stowage: Stowage) -> Self {
        let astow = Arc::new(stowage);
        let backends = CDBackends::new(astow.clone());
        Self {
            backends,
            stowage: Arc::into_inner(astow).unwrap(),
        }
    }

    pub fn cite_count<E>(&mut self)
    where
        E: MarkedAttribute<MainWorkMarker> + IRelAdder,
        E::T: UnsignedNumber,
        MAA<E, MainWorkMarker>:
            Entity<T = Box<[ET<Works>]>> + NamespacedEntity + VariableSizeAttribute,
    {
        let wc_interface = self
            .stowage
            .get_entity_interface::<MAA<E, MainWorkMarker>, ReadIter>();

        let mut sf_rec = ExtensionContainer::new();

        let iter = wc_interface
            .enumerate()
            .tqdm()
            .desc(Some(E::NAME))
            .map(|(i, ws)| {
                let eid = ET::<E>::from_usize(i);
                let sum = ws
                    .iter()
                    .map(|wid| {
                        let year = self.backends.year[wid.to_usize()];
                        sf_rec.extend_get_ccount::<E>(eid, wid, &self.backends, year)
                    })
                    .sum();
                sf_rec.push();
                sum
            });

        add_iter::<E, _>(&mut self.stowage, iter);
        sf_rec.add_iters::<E>(&mut self.stowage);
    }

    fn q_ccs(&mut self) {
        let mut q_maps = init_empty_slice::<Qs, HashMap<ET<Works>, usize>>();
        let wc_interface = self
            .stowage
            .get_entity_interface::<MAA<Sources, MainWorkMarker>, ReadIter>();
        let qy_map = self
            .stowage
            .get_entity_interface::<SourceYearQs, QuickMap>();
        let qc_name = format!("qs-cite-count");

        let mut sf_rec = ExtensionContainer::new();

        let iter = wc_interface.enumerate().map(|(i, ws)| {
            let sid = ET::<Sources>::from_usize(i);
            let sum = ws
                .iter()
                .map(|e| {
                    let wind = *e as usize;
                    let year = self.backends.year[wind];
                    let q = qy_map.get(&(sid, year)).unwrap_or(&0);
                    let wcount = sf_rec.extend_get_ccount::<Sources>(sid, e, &self.backends, year);
                    q_maps[*q as usize].insert(*e, wcount);
                    wcount
                })
                .sum();
            sf_rec.push();
            sum
        });

        add_iter::<Sources, _>(&mut self.stowage, iter);
        sf_rec.add_iters::<Sources>(&mut self.stowage);

        self.stowage.add_iter_owned::<DowncastingBuilder, _, _>(
            q_maps.iter().map(|e| e.values().sum()),
            Some(&qc_name),
        );
        self.stowage.declare::<Qs, CiteCountMarker>(&qc_name);
    }
}

impl IRelAdder for Sources {}
impl IRelAdder for Subfields {}
impl IRelAdder for Topics {}
impl IRelAdder for Institutions {}
impl IRelAdder for Countries {
    fn get_by_work(wu: usize, bends: &CDBackends, id: ET<Self>) -> Vec<IT> {
        let mut o = Vec::new();
        for iid in bends.winsts.get(&wu).unwrap() {
            if *bends.icountry.get(iid.to_usize()).unwrap() == id {
                o.push(*iid);
            }
        }
        o
    }
}
impl IRelAdder for Authors {
    fn get_by_work(wu: usize, bends: &CDBackends, aid: ET<Self>) -> Vec<IT> {
        let mut o = Vec::new();
        for ship in bends.wships.get(&wu).unwrap() {
            let shu = ship.to_usize();
            if bends.shipa[shu] == aid {
                for iid in bends.ship_instss.get(&shu).unwrap() {
                    o.push(*iid)
                }
            }
        }
        o
    }
}

pub fn main(stowage: Stowage) -> io::Result<()> {
    let mut deriver = CiteDeriver::new(stowage);
    deriver.cite_count::<Institutions>();
    deriver.cite_count::<Authors>();
    deriver.cite_count::<Countries>();
    deriver.cite_count::<Subfields>();
    deriver.cite_count::<Topics>();
    deriver.q_ccs();
    deriver.stowage.write_code()?;
    Ok(())
}

fn add_marked<E, B, M, T>(stowage: &mut Stowage, vec: Vec<T>, name: String)
where
    B: MetaIntegrator<T>,
{
    stowage.add_iter_owned::<B, _, T>(vec.into_iter(), Some(&name));
    stowage.declare::<E, M>(&name);
}

fn add_iter<E, I>(stowage: &mut Stowage, iter: I)
where
    E: Entity,
    I: Iterator<Item = usize>,
{
    let cc_name = format!("{}-cite-count", E::NAME);
    stowage.add_iter_owned::<DowncastingBuilder, _, _>(iter, Some(&cc_name));
    stowage.declare::<E, CiteCountMarker>(&cc_name);
}

fn inc_year(era_rec: &mut EraRec, year: YT) {
    let yi = year.to_usize();
    if (yi >= MIN_YEAR) & (yi <= MAX_YEAR) {
        era_rec[yi - MIN_YEAR] += 1
    }
}
