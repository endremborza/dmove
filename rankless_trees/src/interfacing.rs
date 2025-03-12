use std::{f64, sync::Arc};

use crate::{
    ids::AttributeLabelUnion,
    io::{AttributeLabel, WT},
};
use rankless_rs::{
    common::{
        init_empty_slice, BeS, MainEntity, MainWorkMarker, MarkedBackendLoader, NumberedEntity,
        QuickAttPair, QuickMap, QuickestBox, QuickestVBox, Stowage, Top3AffCountryMarker,
        Top3AuthorMarker, Top3CitingSfMarker, Top3JournalMarker, Top3PaperSfMarker,
        Top3PaperTopicMarker, WorkLoader, YearlyCitationsMarker, YearlyPapersMarker, NET,
    },
    gen::{
        a1_entity_mapping::{Authors, Countries, Institutions, Sources, Subfields, Topics, Works},
        a2_init_atts::{
            AuthorshipAuthor, AuthorshipInstitutions, InstCountries, SourceYearQs, TopicSubfields,
            WorkAuthorships, WorkSources, WorkTopics, WorkYears, WorksNames,
        },
        derive_links1::{WorkInstitutions, WorkSubfields},
        derive_links2::{WorkCitingCounts, WorkCountries, WorkTopSource},
    },
    steps::{
        derive_links1::{CountryInsts, WorkPeriods},
        derive_links5::EraRec,
    },
    CiteCountMarker, NameExtensionMarker, NameMarker, QuickestNumbered, SemanticIdMarker,
    WorkCountMarker,
};

use dmove::{
    BackendLoading, BigId, ByteFixArrayInterface, CompactEntity, Entity,
    EntityImmutableRefMapperBackend, Locators, MappableEntity, MarkedAttribute, NamespacedEntity,
    UnsignedNumber, VaST, VarAttBuilder, VarBox, VattArrPair, ET, MAA,
};
use hashbrown::HashMap;
use rand::Rng;

const SPEC_CORR_RATE: f64 = 0.45;

type VB<E> = BeS<QuickAttPair, E>;
type FB<E> = BeS<QuickestBox, E>;
type MB<E> = BeS<QuickMap, E>;
type TopRec<E> = [(u32, ET<E>); 3];
type TopRec5<E> = [(u32, ET<E>); 5];

pub struct Getters {
    ifs: Interfaces,
    pub stowage: Arc<Stowage>,
    pub wn_locators: Locators<WorksNames, u64>,
    pub inst_oa: Box<[BigId]>,
    pub work_oa: Box<[BigId]>,
}

macro_rules! make_interfaces {
    ($($e_key:ident > $e_t:ty),*;$($f_key:ident => $f_t:ty),*; $($v_key:ident -> $v_t:ty),*; $($m_key:ident >> $m_t:ty),*) => {
        struct Interfaces {
            $($e_key: VB<MAA<$e_t, MainWorkMarker>>,)*
            $($f_key: FB<$f_t>,)*
            $($v_key: VB<$v_t>,)*
            $($m_key: MB<$m_t>,)*
        }

        impl Interfaces {
            fn new(stowage: Arc<Stowage>) -> Self {

                $(
                    let stowage_clone = Arc::clone(&stowage);
                    let $e_key = std::thread::spawn( move || {
                        <$e_t as WorkLoader>::load_work_interface(stowage_clone)
                    });
                )*
                $(
                    let stowage_clone = Arc::clone(&stowage);
                    let $f_key = std::thread::spawn( move || {
                        stowage_clone.get_entity_interface::<$f_t, QuickestBox>()
                    });
                )*
                $(
                    let stowage_clone = Arc::clone(&stowage);
                    let $v_key = std::thread::spawn( move || {
                        stowage_clone.get_entity_interface::<$v_t, QuickAttPair>()
                    });
                )*
                $(
                    let stowage_clone = Arc::clone(&stowage);
                    let $m_key = std::thread::spawn( move || {
                        stowage_clone.get_entity_interface::<$m_t, QuickMap>()
                    });
                )*
                Self {
                    $($e_key: $e_key.join().expect("Thread panicked")),*,
                    $($f_key: $f_key.join().expect("Thread panicked")),*,
                    $($v_key: $v_key.join().expect("Thread panicked")),*,
                    $($m_key: $m_key.join().expect("Thread panicked")),*,
                }
            }

            fn fake() -> Self {
                    Self {
                        $($f_key: Vec::new().into()),*,
                        $($e_key: VattArrPair::empty()),*,
                        $($v_key: VattArrPair::empty()),*,
                        $($m_key: HashMap::new().into()),*
                    }
            }
        }

        impl Getters {

            $(
                pub fn $f_key<'a, K: UnsignedNumber>(&'a self, key: &'a K) -> &'a ET<$f_t> {
                    type BE = FB<$f_t>;
                    let uk = key.to_usize();
                    <BE as EntityImmutableRefMapperBackend<$f_t>>::get_ref_via_immut(&self.ifs.$f_key, &uk).expect(&format!("e: {}, k: {}", <$f_t as Entity>::NAME, uk))

                }
            )*

            $(
                pub fn $v_key<'a, K: UnsignedNumber>(&'a self, key: K) -> &'a [VaST<$v_t>] {
                    let uk = key.to_usize();
                    self.ifs.$v_key.get(&uk).expect(&format!("e: {}, k: {}", <$v_t as Entity>::NAME, uk))

                }
            )*

            $(
                pub fn $e_key<'a>(&'a self, key: ET<$e_t>) -> &'a [VaST<MAA<$e_t, MainWorkMarker>>] {
                    let uk = key.to_usize();
                    self.ifs.$e_key.get(&uk).expect(&format!("e: {} works, k: {}", <$e_t as Entity>::NAME, uk))

                }
            )*

            $(
                pub fn $m_key<'a, >(&'a self, key: &'a <$m_t as MappableEntity>::KeyType) -> &'a ET<$m_t> {
                    type BE = MB<$m_t>;
                    <BE as EntityImmutableRefMapperBackend<$m_t>>::get_ref_via_immut(&self.ifs.$m_key, &key)
                    .unwrap_or_else( ||
                        {
                            // println!("not found in map e: {}, k: {:?}", <$m_t as Entity>::NAME, key);
                            &0
                        }
                    )

                }
            )*

        }
        $(
        impl WorksFromMemory for $e_t {
            fn works_from_ram(gets: &Getters, id: NET<Self>) -> &[WT] {
                gets.$e_key(id)
            }
        }
        )*

    };
}

//TODO/clarity wet pattern
macro_rules! make_ent_interfaces {
    ($S:ident, $T:ident, $($f_key:ident => $f_mark:ty),*; $($r_key:ident -> $r_mark:ty),*; $($fix_key:ident - $fix_mark:ty | $fix_t:ty),*; $($float_key:ident : $float_mark:ty),*) => {
        pub struct $S<T> where T: $T
        {
            $(pub $f_key: VarBox<String>),*,
            $(pub $r_key: Box<[<T as NumAtt<$r_mark>>::Num]>),*,
            $(pub $fix_key: Box<[<T as FixAtt<$fix_mark>>::FT]>),*
            $(, pub $float_key: Box<[f64]>)*
        }

        impl<E> $S<E> where E: $T
        {
            pub fn new(stowage: &Stowage) -> Self {
                Self {
                    $($f_key: <E as StringAtt<$f_mark>>::load(stowage)),*,
                    $($r_key: <E as NumAtt<$r_mark>>::load(stowage)),*,
                    $($fix_key:  <E as FixAtt<$fix_mark>>::load(stowage)),*
                    $(, $float_key:  <E as FloatAtt<$float_mark>>::load(stowage))*
                }
            }
        }

        pub trait $T: Entity
            $( + StringAtt<$f_mark>)*
            $( + NumAtt<$r_mark>)*
            $( + FixAtt<$fix_mark, FT=$fix_t>)*
            $( + FloatAtt<$float_mark>)* {}

        impl <T> $T for T where T: Entity
            $( + StringAtt<$f_mark>)*
            $( + NumAtt<$r_mark>)*
            $( + FixAtt<$fix_mark, FT=$fix_t>)*
            $( + FloatAtt<$float_mark>)* {}

    };
}

make_interfaces!(
    citing > Works,
    cworks > Countries,
    iworks > Institutions,
    aworks > Authors,
    soworks > Sources,
    sfworks > Subfields;
    year => WorkYears,
    top_source => WorkTopSource,
    wperiod => WorkPeriods,
    tsuf => TopicSubfields,
    icountry => InstCountries,
    shipa => AuthorshipAuthor,
    wccount => WorkCitingCounts;
    wtopics -> WorkTopics,
    wsubfields -> WorkSubfields,
    winsts -> WorkInstitutions,
    wships -> WorkAuthorships,
    wsources -> WorkSources,
    wcountries -> WorkCountries,
    shipis -> AuthorshipInstitutions,
    country_insts -> CountryInsts;
    sqy >> SourceYearQs
);

make_ent_interfaces!(
    RootInterfaces,
    RootInterfaceable,
    names => NameMarker, name_exts => NameExtensionMarker, sem_ids => SemanticIdMarker;
    wcounts -> WorkCountMarker, ccounts -> CiteCountMarker;
    yearly_papers - YearlyPapersMarker | EraRec,
    yearly_cites - YearlyCitationsMarker | EraRec,
    top_journals - Top3JournalMarker | TopRec<Sources>,
    top_authors - Top3AuthorMarker | TopRec5<Authors>,
    top_aff_countries - Top3AffCountryMarker | TopRec<Countries>,
    top_paper_topic - Top3PaperTopicMarker | TopRec<Topics>,
    top_citing_sfc - Top3CitingSfMarker | TopRec<Subfields>,
    top_paper_sfc - Top3PaperSfMarker | TopRec<Subfields>;
    // inst_rels - InstRelMarker | [InstRelation; N_RELS];
    // ref_sfc : RefSubfieldsConcentrationMarker,
    // cit_sfc : CitSubfieldsConcentrationMarker

);

make_ent_interfaces!(
    NodeInterfaces,
    NodeInterfaceable,
    names => NameMarker;
    ccounts -> CiteCountMarker;;
);

pub trait StringAtt<Mark>: MarkedAttribute<Mark> {
    fn load(stowage: &Stowage) -> VarBox<String>;
}

pub trait NumAtt<Mark>: MarkedAttribute<Mark> {
    type Num: UnsignedNumber;
    fn load(stowage: &Stowage) -> Box<[Self::Num]>;
}

pub trait FloatAtt<Mark>: MarkedAttribute<Mark> {
    fn load(stowage: &Stowage) -> Box<[f64]>;
}

pub trait FixAtt<Mark>: MarkedAttribute<Mark> {
    type FT: ByteFixArrayInterface;
    fn load(stowage: &Stowage) -> Box<[Self::FT]>;
}

pub trait WorksFromMemory: MarkedAttribute<MainWorkMarker> + NumberedEntity {
    fn works_from_ram(gets: &Getters, id: NET<Self>) -> &[WT];
}

impl<E> NodeInterfaces<E>
where
    E: NodeInterfaceable,
{
    pub fn update_stats(self, stats: &mut AttributeLabelUnion, full_cc: f64) {
        update_stats::<E>(self.names, self.ccounts, Vec::new(), stats, full_cc)
    }
}

impl<E> RootInterfaces<E>
where
    E: NodeInterfaceable + RootInterfaceable,
{
    pub fn update_stats(self, stats: &mut AttributeLabelUnion, full_cc: f64) {
        update_stats::<E>(
            self.names,
            self.ccounts,
            self.sem_ids.0.to_vec(),
            stats,
            full_cc,
        )
    }
}

impl Getters {
    pub fn total_cite_count(&self) -> f64 {
        let o: u32 = self.ifs.wccount.iter().map(|e| *e as u32).sum();
        f64::from(o)
    }

    pub fn new(stowage: Arc<Stowage>) -> Self {
        let inst_oa = reverse_id::<Institutions>(&stowage);
        let work_oa = reverse_id::<Works>(&stowage);
        let path = stowage.path_from_ns(WorksNames::NS);
        let wn_locators =
            <Locators<WorksNames, _> as BackendLoading<WorksNames>>::load_backend(&path);
        let ifs = Interfaces::new(stowage.clone());
        println!("loaded all ifs");
        Self {
            ifs,
            wn_locators,
            stowage,
            inst_oa,
            work_oa,
        }
    }

    pub fn fake() -> Self {
        let id: u64 = rand::thread_rng().gen();
        let mut stowage = Stowage::new(&format!("/tmp/tmp-stow/{id}"));

        stowage.set_namespace("a2_init_atts");
        let last = (1..200)
            .map(|e| e.to_string())
            .collect::<Vec<String>>()
            .join(".");
        stowage.add_iter_owned::<VarAttBuilder, _, _>(
            ["W0", "W1", "W2", "W3", &last]
                .iter()
                .map(|e| e.to_string()),
            Some(WorksNames::NAME),
        );

        let path = stowage.path_from_ns(WorksNames::NS);
        let wn_locators =
            <Locators<WorksNames, _> as BackendLoading<WorksNames>>::load_backend(&path);

        Self {
            stowage: Arc::new(stowage),
            wn_locators,
            ifs: Interfaces::fake(),
            inst_oa: Vec::new().into(),
            work_oa: (0..20000000).collect::<Vec<BigId>>().into(),
        }
    }
}

impl<T, Mark> StringAtt<Mark> for T
where
    T: MarkedAttribute<Mark>,
    MAA<T, Mark>:
        CompactEntity + Entity<T = String> + MarkedBackendLoader<QuickestVBox, BE = VarBox<String>>,
{
    fn load(stowage: &Stowage) -> VarBox<String> {
        stowage.get_marked_interface::<Self, Mark, QuickestVBox>()
    }
}

impl<T, Mark> NumAtt<Mark> for T
where
    T: MarkedAttribute<Mark>,
    MAA<T, Mark>: NamespacedEntity + CompactEntity,
    ET<MAA<T, Mark>>: UnsignedNumber,
{
    type Num = ET<MAA<Self, Mark>>;
    fn load(stowage: &Stowage) -> Box<[Self::Num]> {
        stowage.get_marked_interface::<Self, Mark, QuickestBox>()
    }
}

impl<T, Mark> FloatAtt<Mark> for T
where
    T: MarkedAttribute<Mark>,
    MAA<T, Mark>: NamespacedEntity + CompactEntity + Entity<T = f64>,
{
    fn load(stowage: &Stowage) -> Box<[f64]> {
        stowage.get_marked_interface::<Self, Mark, QuickestBox>()
    }
}

impl<T, Mark> FixAtt<Mark> for T
where
    T: MarkedAttribute<Mark>,
    MAA<T, Mark>: NamespacedEntity + CompactEntity,
    ET<MAA<T, Mark>>: ByteFixArrayInterface,
{
    type FT = ET<MAA<Self, Mark>>;
    fn load(stowage: &Stowage) -> Box<[Self::FT]> {
        stowage.get_marked_interface::<Self, Mark, QuickestBox>()
    }
}

fn reverse_id<E>(stowage: &Stowage) -> Box<[BigId]>
where
    E: MainEntity + NamespacedEntity,
{
    let interface = stowage.get_entity_interface::<E, QuickestNumbered>();
    let mut out = init_empty_slice::<E, BigId>();
    for (k, v) in interface.0 {
        out[v.to_usize()] = k;
    }
    out
}

fn update_stats<E>(
    names: VarBox<String>,
    ccounts: Box<[<E as NumAtt<CiteCountMarker>>::Num]>,
    semantic_ids: Vec<String>,
    stats: &mut AttributeLabelUnion,
    full_cc: f64,
) where
    E: NodeInterfaceable,
{
    const SPEC_RATE: f64 = 1.0 - SPEC_CORR_RATE;
    let numer_add = (full_cc / f64::from(E::N as u32)) * SPEC_CORR_RATE;
    let elevel = names
        .0
        .to_vec()
        .into_iter()
        .enumerate()
        .map(|(i, name)| {
            //TODO: u32 counts (max 4B) need to be ensured
            let numer = f64::from(ccounts[i].to_usize() as u32) * SPEC_RATE + numer_add;
            let spec_baseline = numer / full_cc;
            AttributeLabel {
                name,
                semantic_id: semantic_ids.get(i).unwrap_or(&"".to_string()).to_string(),
                spec_baseline,
            }
        })
        .collect();
    stats.insert(E::NAME.to_string(), elevel);
}
