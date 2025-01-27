use std::{fmt::Display, sync::Arc};

use crate::{
    ids::AttributeLabelUnion,
    io::{AttributeLabel, WT},
};
use rankless_rs::{
    common::{
        BackendSelector, MainWorkMarker, QuickAttPair, QuickMap, QuickestBox, QuickestVBox, Stowage,
    },
    gen::{
        a1_entity_mapping::{Authors, Countries, Institutions, Sources, Subfields, Works},
        a2_init_atts::{
            AuthorshipAuthor, AuthorshipInstitutions, InstCountries, SourceYearQs, TopicSubfields,
            WorkAuthorships, WorkSources, WorkTopics, WorkYears, WorksNames,
        },
        derive_links1::{WorkInstitutions, WorkSubfields},
        derive_links2::WorkCitingCounts,
    },
    steps::derive_links1::{CountryInsts, WorkPeriods},
    CiteCountMarker, NameExtensionMarker, NameMarker, SemanticIdMarker, WorkCountMarker,
};

use dmove::{
    BackendLoading, CompactEntity, Entity, EntityImmutableRefMapperBackend, Locators,
    MappableEntity, MarkedAttribute, NamespacedEntity, UnsignedNumber, VaST, VarAttBuilder, VarBox,
    VarSizedAttributeElement, VariableSizeAttribute, VattArrPair, ET, MAA,
};
use hashbrown::HashMap;
use rand::Rng;
use serde::{de::DeserializeOwned, Serialize};

const SPEC_CORR_RATE: f64 = 0.35;

pub type NET<E> = <E as NumberedEntity>::T;
type VB<E> = <QuickAttPair as BackendSelector<E>>::BE;
type FB<T> = <QuickestBox as BackendSelector<T>>::BE;
type MB<T> = <QuickMap as BackendSelector<T>>::BE;

pub struct Getters {
    ifs: Interfaces,
    pub stowage: Arc<Stowage>,
    pub wn_locators: Locators<WorksNames, u64>,
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

macro_rules! make_ent_interfaces {
    ($S:ident, $T:ident, $($f_key:ident => $f_mark:ty),*; $($r_key:ident -> $r_mark:ty),*) => {
        pub struct $S<T> where T: $T
        {
            $(pub $f_key: VarBox<String>),*,
            $(pub $r_key: Box<[<T as NumAtt<$r_mark>>::Num]>),*
        }

        impl<E> $S<E> where E: $T
        {
            pub fn new(stowage: &Stowage) -> Self {
                Self {
                    $($f_key: <E as StringAtt<$f_mark>>::load(stowage)),*,
                    $($r_key: <E as NumAtt<$r_mark>>::load(stowage)),*,
                }
            }
        }

        pub trait $T: Entity
            $( + StringAtt<$f_mark>)*
            $( + NumAtt<$r_mark>)* {}

        impl <T> $T for T where T: Entity
            $( + StringAtt<$f_mark>)*
            $( + NumAtt<$r_mark>)* {}

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
    shipis -> AuthorshipInstitutions,
    country_insts -> CountryInsts;
    sqy >> SourceYearQs
);

make_ent_interfaces!(
    RootInterfaces,
    RootInterfaceable,
    names => NameMarker, name_exts => NameExtensionMarker, sem_ids => SemanticIdMarker;
    wcounts -> WorkCountMarker, ccounts -> CiteCountMarker
);

make_ent_interfaces!(
    NodeInterfaces,
    NodeInterfaceable,
    names => NameMarker;
    ccounts -> CiteCountMarker
);

pub trait StringAtt<Mark>: MarkedAttribute<Mark> {
    fn load(stowage: &Stowage) -> VarBox<String>;
}

pub trait NumAtt<Mark>: MarkedAttribute<Mark> {
    type Num: UnsignedNumber;
    fn load(stowage: &Stowage) -> Box<[Self::Num]>;
}

pub trait NumberedEntity: Entity {
    type T: UnsignedNumber + DeserializeOwned + Serialize + Ord + Copy + Display;
}

pub trait WorkLoader: MarkedAttribute<MainWorkMarker>
where
    MAA<Self, MainWorkMarker>: CompactEntity + VariableSizeAttribute + NamespacedEntity,
    ET<MAA<Self, MainWorkMarker>>: VarSizedAttributeElement,
{
    fn load_work_interface(stowage: Arc<Stowage>) -> VB<MAA<Self, MainWorkMarker>> {
        stowage.get_entity_interface::<MAA<Self, MainWorkMarker>, QuickAttPair>()
    }
}

pub trait WorksFromMemory: MarkedAttribute<MainWorkMarker> + NumberedEntity {
    fn works_from_ram(gets: &Getters, id: NET<Self>) -> &[WT];
}

impl<E> WorkLoader for E
where
    E: MarkedAttribute<MainWorkMarker>,

    MAA<Self, MainWorkMarker>: CompactEntity + VariableSizeAttribute + NamespacedEntity,
    ET<MAA<Self, MainWorkMarker>>: VarSizedAttributeElement,
{
}

impl<E> NumberedEntity for E
where
    E: Entity,
    ET<E>: UnsignedNumber + Serialize + DeserializeOwned + Ord + Copy + Display,
{
    type T = ET<E>;
}

impl<E> NodeInterfaces<E>
where
    E: NodeInterfaceable,
{
    pub fn update_stats(self, stats: &mut AttributeLabelUnion, full_cc: f64) {
        update_stats::<E>(self.names, self.ccounts, stats, full_cc)
    }
}

impl<E> RootInterfaces<E>
where
    E: NodeInterfaceable + RootInterfaceable,
{
    pub fn update_stats(self, stats: &mut AttributeLabelUnion, full_cc: f64) {
        update_stats::<E>(self.names, self.ccounts, stats, full_cc)
    }
}

impl Getters {
    pub fn total_cite_count(&self) -> f64 {
        let o: u32 = self.ifs.wccount.iter().map(|e| *e as u32).sum();
        f64::from(o)
    }

    pub fn new(stowage: Arc<Stowage>) -> Self {
        let path = stowage.path_from_ns(WorksNames::NS);
        let wn_locators =
            <Locators<WorksNames, _> as BackendLoading<WorksNames>>::load_backend(&path);
        let ifs = Interfaces::new(stowage.clone());
        println!("loaded all ifs");
        Self {
            ifs,
            wn_locators,
            stowage,
        }
    }

    pub fn fake() -> Self {
        let id: u64 = rand::thread_rng().gen();
        let mut stowage = Stowage::new(&format!("/tmp/tmp-stow-{id}"));

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
        }
    }
}

impl<T, Mark> StringAtt<Mark> for T
where
    T: MarkedAttribute<Mark>,
    MAA<T, Mark>: NamespacedEntity + CompactEntity + VariableSizeAttribute + Entity<T = String>,
{
    fn load(stowage: &Stowage) -> VarBox<ET<MAA<Self, Mark>>> {
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
        let nums = stowage.get_marked_interface::<Self, Mark, QuickestBox>();
        nums
    }
}

fn update_stats<E>(
    names: VarBox<String>,
    ccounts: Box<[<E as NumAtt<CiteCountMarker>>::Num]>,
    stats: &mut AttributeLabelUnion,
    full_cc: f64,
) where
    E: NodeInterfaceable,
{
    let mut elevel = Vec::new();
    let numer_add = (full_cc / f64::from(E::N as u32)) * SPEC_CORR_RATE;
    let full_denom = full_cc;
    for (i, name) in names.0.to_vec().into_iter().enumerate() {
        //TODO: u32 counts (max 4B) need to be ensured
        let spec_baseline = (f64::from(ccounts[i].to_usize() as u32) * (1.0 - SPEC_CORR_RATE)
            + numer_add)
            / full_denom;
        elevel.push(AttributeLabel {
            name,
            spec_baseline,
        });
    }
    stats.insert(E::NAME.to_string(), elevel.into());
}
