use std::{iter::Peekable, marker::PhantomData, slice::Iter};

use dmove::{Entity, UnsignedNumber, ET};
use dmove_macro::impl_stack_basees;
use rankless_rs::{
    agg_tree::{FoldingStackConsumer, ReinstateFrom, SortedRecord, Updater},
    common::{NumberedEntity, NET},
    gen::a1_entity_mapping::{
        Authors, Authorships, Countries, Institutions, Sources, Subfields, Topics, Works,
    },
    steps::a1_entity_mapping::{Qs, N_PERS},
};

use crate::{
    instances::{Collapsing, DisJTree, FoldStackBase, IntXTree, WorkTree},
    interfacing::{Getters, WorksFromMemory},
    io::{BreakdownSpec, WT},
    part_iterator::PartitioningIterator,
};

const UNKNOWN_ID: usize = 0;

pub type StackFr<S> = <<S as StackBasis>::SortedRec as SortedRecord>::FlatRecord;
pub type PartitionId = u8;

type ExtendedFr<'a, I> = (PartitionId, StackFr<<I as RefWorkBasedIter<'a>>::SB>);
type ExtItem<'a, I> = <ExtendedFr<'a, I> as ExtendWithInst>::To;
type RwbiItem<'a, I> = <StackFr<<I as RefWorkBasedIter<'a>>::SB> as ExtendedWithRefWid>::From;
type FoldingStackLeaf = WorkTree;
// type FoldingStackLeaf = ();
pub struct DisJ<E: Entity, const N: usize, const S: bool>(E::T);
pub struct IntX<E: Entity, const N: usize, const S: bool>(E::T);

pub struct PostRefIterWrap<'a, E, I>
where
    E: NumberedEntity,
{
    it: Option<I>,
    gets: &'a Getters,
    refs_it: Peekable<Iter<'a, WT>>,
    p: PhantomData<E>,
}

//specific roots

pub struct CountryInstsPost<'a, I, SB> {
    pr_it: Option<PostRefIterWrap<'a, Institutions, I>>,
    gets: &'a Getters,
    insts: Peekable<Iter<'a, ET<Institutions>>>,
    p: PhantomData<SB>,
}

pub struct CountryBesties<'a> {
    gets: &'a Getters,
    id: ET<Countries>,
    ref_wids: Peekable<Iter<'a, WT>>,
    ref_insts: Option<Peekable<Iter<'a, ET<Institutions>>>>,
    ref_sfs: Option<Peekable<Iter<'a, ET<Subfields>>>>,
    cit_wids: Option<Iter<'a, ET<Works>>>,
}

pub struct AuthorBesties<'a> {
    gets: &'a Getters,
    id: ET<Authors>,
    ref_wids: Peekable<Iter<'a, WT>>,
    ref_ships: Option<Peekable<Iter<'a, ET<Authorships>>>>,
    cit_insts: Option<Iter<'a, ET<Institutions>>>,
    cit_wids: Option<Peekable<Iter<'a, ET<Works>>>>,
}

pub struct AuthorBestiePapers<'a> {
    gets: &'a Getters,
    id: ET<Authors>,
    ref_wids: Peekable<Iter<'a, WT>>,
    ref_ships: Option<Peekable<Iter<'a, ET<Authorships>>>>,
    cit_sfs: Option<Iter<'a, ET<Subfields>>>,
    cit_wids: Option<Peekable<Iter<'a, ET<Works>>>>,
}

pub struct InstBesties<'a> {
    gets: &'a Getters,
    id: ET<Institutions>,
    ref_wids: Peekable<Iter<'a, WT>>,
    ref_insts: Option<Peekable<Iter<'a, ET<Institutions>>>>,
    ref_sfs: Option<Peekable<Iter<'a, ET<Subfields>>>>,
    cit_wids: Option<Iter<'a, ET<Works>>>,
}

pub struct WorkingAuthors<'a> {
    gets: &'a Getters,
    id: ET<Institutions>,
    ref_wids: Peekable<Iter<'a, WT>>,
    ref_ships: Option<Peekable<Iter<'a, ET<Authorships>>>>,
    cit_insts: Option<Iter<'a, ET<Institutions>>>,
    cit_wids: Option<Peekable<Iter<'a, ET<Works>>>>,
}

pub struct SubfieldRefTopicCountryInst<'a> {
    id: ET<Subfields>,
    ref_wids: Peekable<Iter<'a, WT>>,
    ref_topics: Option<Peekable<Iter<'a, ET<Topics>>>>,
    ref_insts: Option<Peekable<Iter<'a, ET<Institutions>>>>,
    cit_wids: Option<Iter<'a, ET<Works>>>,
    gets: &'a Getters,
}

// generics

pub struct CitingCoInstSuToByRef<'a> {
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_tops: Option<Peekable<Iter<'a, ET<Topics>>>>,
    cit_insts: Option<Iter<'a, ET<Institutions>>>,
    gets: &'a Getters,
}

pub struct CitingCoSuToByRef<'a> {
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_tops: Option<Peekable<Iter<'a, ET<Topics>>>>,
    cit_countries: Option<Iter<'a, ET<Countries>>>,
    gets: &'a Getters,
}

pub struct CitingSourceCoSuByRef<'a> {
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_sfs: Option<Peekable<Iter<'a, ET<Subfields>>>>,
    cit_insts: Option<Iter<'a, ET<Institutions>>>,
    gets: &'a Getters,
}

pub struct WCoIByRef<'a> {
    ref_wid: &'a WT,
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_insts: Option<Iter<'a, ET<Institutions>>>,
    gets: &'a Getters,
}

pub struct SubfieldCountryInstByRef<'a> {
    ref_wid: &'a WT,
    ref_sfs: Peekable<Iter<'a, ET<Subfields>>>,
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_insts: Option<Iter<'a, ET<Institutions>>>,
    gets: &'a Getters,
}

pub struct SourceSubfieldCiCoByRef<'a> {
    ref_wid: &'a WT,
    ref_sfs: Peekable<Iter<'a, ET<Subfields>>>,
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_cous: Option<Iter<'a, ET<Countries>>>,
    gets: &'a Getters,
}

pub struct FullRefSourceCountryInstByRef<'a> {
    ref_wid: &'a WT,
    ref_insts: Peekable<Iter<'a, ET<Institutions>>>,
    cit_wids: Iter<'a, ET<Works>>,
    gets: &'a Getters,
}

pub struct FullRefCountryInstSubfieldByRef<'a> {
    ref_wid: &'a WT,
    ref_sfs: Peekable<Iter<'a, ET<Subfields>>>,
    ref_insts: Peekable<Iter<'a, ET<Institutions>>>,
    cit_wids: Iter<'a, ET<Works>>,
    gets: &'a Getters,
}

pub struct SourceWCoiByRef<'a> {
    ref_wid: &'a WT,
    wcoi: Peekable<WCoIByRef<'a>>,
    gets: &'a Getters,
}

pub struct SubfieldWCoiByRef<'a> {
    ref_wid: &'a WT,
    wcoi: Peekable<WCoIByRef<'a>>,
    ref_sfs: Iter<'a, ET<Subfields>>,
    gets: &'a Getters,
}

pub struct SubfieldCountryInstSourceByRef<'a> {
    sci_top: Peekable<SubfieldCountryInstByRef<'a>>,
    gets: &'a Getters,
}

pub struct SubfieldCountryInstSubfieldByRef<'a> {
    sci_top: Peekable<SubfieldCountryInstByRef<'a>>,
    cit_sfs: Option<Iter<'a, ET<Subfields>>>,
    gets: &'a Getters,
}

pub struct InstSubfieldCountryInstByRef<'a> {
    ref_wid: &'a WT,
    sci_top: Peekable<SubfieldCountryInstByRef<'a>>,
    ref_insts: Iter<'a, ET<Institutions>>,
    gets: &'a Getters,
}

pub struct RefSubCiSubTByRef<'a> {
    ref_wid: &'a WT,
    ref_sfs: Peekable<Iter<'a, ET<Subfields>>>,
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_topics: Option<Iter<'a, ET<Topics>>>,
    gets: &'a Getters,
}

pub struct RefSubSourceTop<'a> {
    ref_wid: &'a WT,
    ref_sfs: Peekable<Iter<'a, ET<Subfields>>>,
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_topics: Option<Iter<'a, ET<Topics>>>,
    gets: &'a Getters,
}

pub struct CiteSubSourceTop<'a> {
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_sfs: Option<Peekable<Iter<'a, ET<Subfields>>>>,
    cit_topics: Option<Iter<'a, ET<Topics>>>,
    gets: &'a Getters,
}

pub struct QedInf<'a> {
    ref_wid: &'a WT,
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cite_sfs: Option<Peekable<Iter<'a, ET<Subfields>>>>,
    cite_countries: Option<Iter<'a, ET<Countries>>>,

    gets: &'a Getters,
}

macro_rules! wrap_or_next_o {
    ($child_i: expr, $parent_i: expr, $f: ident, $rein: expr) => {
        match $f(&mut $child_i, &mut $parent_i, || $rein.iter()) {
            Some(e) => e,
            None => continue,
        }
    }; //.as_mut().unwrap() common pattern
}

macro_rules! opt_peek {
    ($child_i: expr, $parent_i: expr, $rein: expr) => {
        wrap_or_next_o!($child_i, $parent_i, peek_and_roll_o, $rein)
    };
}

macro_rules! opt_next {
    ($child_i: expr, $parent_i: expr, $rein: expr) => {
        wrap_or_next_o!($child_i, $parent_i, next_and_roll_o, $rein)
    };
}

macro_rules! reg_peek {
    ($child_it: expr, $parent_it: expr, $child_recalc: expr) => {
        match $child_it.peek() {
            Some(v) => *v,
            None => {
                $child_it = $child_recalc.iter().peekable();
                $parent_it.next();
                continue;
            }
        }
    };

    ($child_it: expr) => {
        match $child_it.peek() {
            Some(v) => *v,
            None => {
                return None;
            }
        }
    };
}

macro_rules! reg_next {
    ($child_it: expr, $parent_it: expr, $child_recalc: expr) => {
        match $child_it.next() {
            Some(v) => *v,
            None => {
                $child_it = $child_recalc.iter();
                $parent_it.next();
                continue;
            }
        }
    };

    ($child_it: expr) => {
        match $child_it.next() {
            Some(v) => v,
            None => {
                return None;
            }
        }
    };
}

pub trait StackBasis {
    type Stack;
    type SortedRec: SortedRecord;
    type TopTree;

    fn get_bds() -> Vec<BreakdownSpec>;

    fn fold_into<R, I>(root: &mut R, iter: I)
    where
        I: Iterator<Item = Self::SortedRec>,
        R: Updater<Self::TopTree>;
}

pub trait RefWorkBasedIter<'a>:
    Iterator<Item = <StackFr<Self::SB> as ExtendedWithRefWid>::From>
where
    StackFr<Self::SB>: ExtendedWithRefWid,
{
    type SB: StackBasis;
    const RWB_IS_SPEC: bool = true;
    fn new(ref_wid: &'a WT, gets: &'a Getters) -> Self;
}

pub trait ExtendedWithRefWid {
    type From;
    fn extend(src: Self::From, value: WT) -> Self;
}

pub trait ExtendWithInst {
    type To;
    fn extend(self, value: ET<Institutions>) -> (PartitionId, Self::To);
}

impl<T> StackBasis for T
where
    T: FoldStackBase<FoldingStackLeaf>,
    T::StackElement: Collapsing
        + From<NET<T::LevelEntity>>
        + ReinstateFrom<NET<T::LevelEntity>>
        + Updater<FoldingStackLeaf>,
    T::LevelEntity: NumberedEntity,
{
    type Stack = T::StackElement;
    type TopTree = Self::Stack;
    type SortedRec = rankless_rs::agg_tree::SRecord3<NET<T::LevelEntity>, WT, WT>;
    fn get_bds() -> Vec<BreakdownSpec> {
        vec![to_bds::<Self, _>()]
    }
    fn fold_into<R, I>(root: &mut R, iter: I)
    where
        I: Iterator<Item = Self::SortedRec>,
        R: Updater<Self::TopTree>,
    {
        Self::SortedRec::fold(iter, root);
    }
}

impl_stack_basees!(5);

impl<E, C, const N: usize, const S: bool> FoldStackBase<C> for IntX<E, N, S>
where
    E: NumberedEntity,
    C: Collapsing,
{
    type StackElement = IntXTree<E, C>;
    type LevelEntity = E;
    const SPEC_DENOM_IND: usize = N;
    const SOURCE_SIDE: bool = S;
}

impl<E, C, const N: usize, const S: bool> FoldStackBase<C> for DisJ<E, N, S>
where
    E: NumberedEntity,
    C: Collapsing,
{
    type StackElement = DisJTree<E, C>;
    type LevelEntity = E;
    const SPEC_DENOM_IND: usize = N;
    const SOURCE_SIDE: bool = S;
}

impl FoldingStackConsumer for WorkTree {
    type Consumable = WT;
    fn consume(&mut self, child: Self::Consumable) {
        self.0.children.push(child);
    }
}

impl<T1, T2, T3> ExtendedWithRefWid for (T1, T2, T3, WT, WT) {
    type From = (T1, T2, T3, WT);
    fn extend(src: Self::From, value: WT) -> Self {
        (src.0, src.1, src.2, value, src.3)
    }
}

impl<T1, T2, T3, T4> ExtendedWithRefWid for (T1, T2, T3, T4, WT, WT) {
    type From = (T1, T2, T3, T4, WT);
    fn extend(src: Self::From, value: WT) -> Self {
        (src.0, src.1, src.2, src.3, value, src.4)
    }
}

impl<T1, T2, T3> ExtendWithInst for (PartitionId, (T1, T2, T3, WT, WT)) {
    type To = (ET<Institutions>, T1, T2, T3, WT, WT);
    fn extend(self, value: ET<Institutions>) -> (PartitionId, Self::To) {
        (
            self.0,
            (value, self.1 .0, self.1 .1, self.1 .2, self.1 .3, self.1 .4),
        )
    }
}

impl<'a> RefWorkBasedIter<'a> for SubfieldCountryInstByRef<'a> {
    type SB = (
        IntX<Subfields, 0, true>,
        IntX<Countries, 1, false>,
        IntX<Institutions, 1, false>,
    );
    fn new(ref_wid: &'a WT, gets: &'a Getters) -> Self {
        let ref_sfs = gets.wsubfields(*ref_wid).iter().peekable();
        let cit_wids = gets.citing(*ref_wid).iter().peekable();
        Self {
            ref_wid,
            ref_sfs,
            cit_wids,
            cit_insts: None,
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for SourceSubfieldCiCoByRef<'a> {
    type SB = (
        IntX<Sources, 0, true>,
        IntX<Subfields, 1, true>,
        IntX<Countries, 2, false>,
    );
    const RWB_IS_SPEC: bool = false;
    fn new(ref_wid: &'a WT, gets: &'a Getters) -> Self {
        let ref_sfs = gets.wsubfields(*ref_wid).iter().peekable();
        let cit_wids = gets.citing(*ref_wid).iter().peekable();
        Self {
            ref_wid,
            ref_sfs,
            cit_wids,
            cit_cous: None,
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for FullRefSourceCountryInstByRef<'a> {
    type SB = (
        IntX<Sources, 0, true>,
        IntX<Countries, 1, true>,
        IntX<Institutions, 1, true>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        let cit_wids = gets.citing(*ref_wid).iter();
        let ref_insts = gets.winsts(*ref_wid).iter().peekable();
        Self {
            ref_wid,
            cit_wids,
            gets,
            ref_insts,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for FullRefCountryInstSubfieldByRef<'a> {
    type SB = (
        IntX<Countries, 0, true>,
        IntX<Institutions, 0, true>,
        IntX<Subfields, 2, true>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        let cit_wids = gets.citing(*ref_wid).iter();
        let ref_insts = gets.winsts(*ref_wid).iter().peekable();
        let ref_sfs = gets.wsubfields(*ref_wid).iter().peekable();
        Self {
            ref_wid,
            cit_wids,
            gets,
            ref_sfs,
            ref_insts,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for CitingCoSuToByRef<'a> {
    type SB = (
        IntX<Countries, 0, false>,
        IntX<Subfields, 1, false>,
        IntX<Topics, 1, false>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        let cit_wids = gets.citing(*ref_wid).iter().peekable();
        Self {
            cit_wids,
            cit_tops: None,
            cit_countries: None,
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for CitingCoInstSuToByRef<'a> {
    type SB = (
        IntX<Countries, 0, false>,
        IntX<Institutions, 0, false>,
        IntX<Subfields, 2, false>,
        IntX<Topics, 2, false>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        let cit_wids = gets.citing(*ref_wid).iter().peekable();
        Self {
            cit_wids,
            cit_tops: None,
            cit_insts: None,
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for CitingSourceCoSuByRef<'a> {
    type SB = (
        IntX<Sources, 0, false>,
        IntX<Countries, 1, false>,
        IntX<Subfields, 2, false>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        let cit_wids = gets.citing(*ref_wid).iter().peekable();
        Self {
            cit_wids,
            cit_sfs: None,
            cit_insts: None,
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for WCoIByRef<'a> {
    type SB = (
        IntX<Works, 0, true>,
        IntX<Countries, 1, false>,
        IntX<Institutions, 1, false>,
    );

    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        let cit_wids = gets.citing(*ref_wid).iter().peekable();
        Self {
            ref_wid,
            cit_wids,
            cit_insts: None,
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for SourceWCoiByRef<'a> {
    type SB = (
        IntX<Sources, 0, true>,
        IntX<Works, 0, true>,
        IntX<Countries, 2, false>,
        IntX<Institutions, 2, false>,
    );
    const RWB_IS_SPEC: bool = false;
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        Self {
            ref_wid,
            wcoi: WCoIByRef::new(ref_wid, gets).peekable(),
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for SubfieldWCoiByRef<'a> {
    type SB = (
        IntX<Subfields, 0, true>,
        IntX<Works, 0, true>,
        IntX<Countries, 2, false>,
        IntX<Institutions, 2, false>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        Self {
            ref_wid,
            ref_sfs: gets.wsubfields(*ref_wid).iter(),
            wcoi: WCoIByRef::new(ref_wid, gets).peekable(),
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for SubfieldCountryInstSourceByRef<'a> {
    type SB = (
        IntX<Subfields, 0, true>,
        IntX<Countries, 1, false>,
        IntX<Institutions, 1, false>,
        IntX<Sources, 3, false>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        Self {
            sci_top: SubfieldCountryInstByRef::new(ref_wid, gets).peekable(),
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for SubfieldCountryInstSubfieldByRef<'a> {
    type SB = (
        IntX<Subfields, 0, true>,
        IntX<Countries, 1, false>,
        IntX<Institutions, 1, false>,
        IntX<Subfields, 3, false>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        Self {
            cit_sfs: None,
            sci_top: SubfieldCountryInstByRef::new(ref_wid, gets).peekable(),
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for InstSubfieldCountryInstByRef<'a> {
    type SB = (
        IntX<Institutions, 0, true>,
        IntX<Subfields, 1, true>,
        IntX<Countries, 2, false>,
        IntX<Institutions, 2, false>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        Self {
            ref_wid,
            ref_insts: gets.winsts(*ref_wid).iter(),
            sci_top: SubfieldCountryInstByRef::new(ref_wid, gets).peekable(),
            gets,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for RefSubCiSubTByRef<'a> {
    type SB = (
        IntX<Subfields, 0, true>,
        IntX<Subfields, 1, false>,
        IntX<Topics, 1, false>,
    );
    fn new(ref_wid: &'a WT, gets: &'a Getters) -> Self {
        Self {
            gets,
            ref_wid,
            cit_wids: gets.citing(*ref_wid).iter().peekable(),
            ref_sfs: gets.wsubfields(*ref_wid).iter().peekable(),
            cit_topics: None,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for CiteSubSourceTop<'a> {
    type SB = (
        IntX<Subfields, 0, false>,
        IntX<Sources, 1, false>,
        IntX<Topics, 1, false>,
    );
    fn new(ref_wid: &'a WT, gets: &'a Getters) -> Self {
        Self {
            gets,
            cit_wids: gets.citing(*ref_wid).iter().peekable(),
            cit_topics: None,
            cit_sfs: None,
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for RefSubSourceTop<'a> {
    type SB = (
        IntX<Subfields, 0, true>,
        IntX<Sources, 1, false>,
        IntX<Topics, 1, false>,
    );
    fn new(ref_wid: &'a WT, gets: &'a Getters) -> Self {
        Self {
            ref_wid,
            gets,
            cit_wids: gets.citing(*ref_wid).iter().peekable(),
            cit_topics: None,
            ref_sfs: gets.wsubfields(*ref_wid).iter().peekable(),
        }
    }
}

impl<'a> RefWorkBasedIter<'a> for QedInf<'a> {
    type SB = (
        IntX<Qs, 0, true>,
        IntX<Sources, 0, true>,
        IntX<Subfields, 2, false>,
        IntX<Countries, 3, false>,
    );
    fn new(ref_wid: &'a WT, gets: &'a Getters) -> Self {
        Self {
            ref_wid,
            gets,
            cit_wids: gets.citing(*ref_wid).iter().peekable(),
            cite_sfs: None,
            cite_countries: None,
        }
    }
}

impl<'a> Iterator for SubfieldCountryInstByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_sf = reg_peek!(self.ref_sfs);
            let cit_wid = reg_peek!(self.cit_wids, self.ref_sfs, self.gets.citing(*self.ref_wid));
            let cit_inst = opt_next!(self.cit_insts, self.cit_wids, self.gets.winsts(*cit_wid));
            return Some((*ref_sf, *self.gets.icountry(cit_inst), *cit_inst, *cit_wid));
        }
    }
}

impl<'a> Iterator for SourceSubfieldCiCoByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_sf = reg_peek!(self.ref_sfs);
            let ref_source = self.gets.top_source(self.ref_wid);
            let cit_wid = reg_peek!(self.cit_wids, self.ref_sfs, self.gets.citing(*self.ref_wid));
            let cit_country =
                opt_next!(self.cit_cous, self.cit_wids, self.gets.wcountries(*cit_wid));
            return Some((*ref_source, *ref_sf, *cit_country, *cit_wid));
        }
    }
}

impl<'a> Iterator for FullRefSourceCountryInstByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_inst = reg_peek!(self.ref_insts);
            let ref_source = self.gets.top_source(self.ref_wid);
            let cit_wid = reg_next!(
                self.cit_wids,
                self.ref_insts,
                self.gets.citing(*self.ref_wid)
            );
            return Some((
                *ref_source,
                *self.gets.icountry(ref_inst),
                *ref_inst,
                cit_wid,
            ));
        }
    }
}

impl<'a> Iterator for FullRefCountryInstSubfieldByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        //TODO full-ref WET
        loop {
            let ref_sf = reg_peek!(self.ref_sfs);
            let ref_inst = reg_peek!(
                self.ref_insts,
                self.ref_sfs,
                self.gets.winsts(*self.ref_wid)
            );
            let cit_wid = reg_next!(
                self.cit_wids,
                self.ref_insts,
                self.gets.citing(*self.ref_wid)
            );
            return Some((*self.gets.icountry(ref_inst), *ref_inst, *ref_sf, cit_wid));
        }
    }
}

impl<'a> Iterator for CitingCoSuToByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cit_wid = reg_peek!(self.cit_wids);
            let cit_topic = opt_peek!(self.cit_tops, self.cit_wids, self.gets.wtopics(*cit_wid));
            let cit_country = opt_next!(
                self.cit_countries,
                self.cit_tops.as_mut().unwrap(),
                self.gets.wcountries(*cit_wid)
            );
            return Some((
                *cit_country,
                *self.gets.tsuf(cit_topic),
                *cit_topic,
                *cit_wid,
            ));
        }
    }
}

impl<'a> Iterator for CitingCoInstSuToByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cit_wid = reg_peek!(self.cit_wids);
            let cit_topic = opt_peek!(self.cit_tops, self.cit_wids, self.gets.wtopics(*cit_wid));
            let cit_inst = opt_next!(
                self.cit_insts,
                self.cit_tops.as_mut().unwrap(),
                self.gets.winsts(*cit_wid)
            );
            return Some((
                *self.gets.icountry(cit_inst),
                *cit_inst,
                *self.gets.tsuf(cit_topic),
                *cit_topic,
                *cit_wid,
            ));
        }
    }
}

impl<'a> Iterator for CitingSourceCoSuByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cit_wid = reg_peek!(self.cit_wids);
            let citing_sf = opt_peek!(self.cit_sfs, self.cit_wids, self.gets.wsubfields(*cit_wid));
            let citing_inst = opt_next!(
                self.cit_insts,
                self.cit_sfs.as_mut().unwrap(),
                self.gets.winsts(*cit_wid)
            );
            let citing_source = self.gets.top_source(cit_wid);
            return Some((
                *citing_source,
                *self.gets.icountry(citing_inst),
                *citing_sf,
                *cit_wid,
            ));
        }
    }
}

impl<'a> Iterator for WCoIByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cit_wid = reg_peek!(self.cit_wids);
            let citing_inst = opt_next!(self.cit_insts, self.cit_wids, self.gets.winsts(*cit_wid));
            return Some((
                *self.ref_wid,
                *self.gets.icountry(citing_inst),
                *citing_inst,
                *cit_wid,
            ));
        }
    }
}

impl<'a> Iterator for SubfieldCountryInstSourceByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let top_tup = reg_next!(self.sci_top);
            let cit_source = self.gets.top_source(&top_tup.3);
            return Some((top_tup.0, top_tup.1, top_tup.2, *cit_source, top_tup.3));
        }
    }
}

impl<'a> Iterator for SourceWCoiByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let wcoi_tup = reg_next!(self.wcoi);
            let ref_source = self.gets.top_source(self.ref_wid);
            return Some((*ref_source, wcoi_tup.0, wcoi_tup.1, wcoi_tup.2, wcoi_tup.3));
        }
    }
}

impl<'a> Iterator for SubfieldWCoiByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let wcoi_tup = reg_peek!(self.wcoi);
            let ref_sf = reg_next!(self.ref_sfs, self.wcoi, self.gets.wsubfields(*self.ref_wid));
            return Some((ref_sf, wcoi_tup.0, wcoi_tup.1, wcoi_tup.2, wcoi_tup.3));
        }
    }
}

impl<'a> Iterator for SubfieldCountryInstSubfieldByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let top_tup = reg_peek!(self.sci_top);
            let cit_sf = opt_next!(self.cit_sfs, self.sci_top, self.gets.wsubfields(top_tup.3));
            return Some((top_tup.0, top_tup.1, top_tup.2, *cit_sf, top_tup.3));
        }
    }
}

impl<'a> Iterator for InstSubfieldCountryInstByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let top_tup = reg_peek!(self.sci_top);
            let ref_inst = reg_next!(
                self.ref_insts,
                self.sci_top,
                self.gets.winsts(*self.ref_wid)
            );
            return Some((ref_inst, top_tup.0, top_tup.1, top_tup.2, top_tup.3));
        }
    }
}

impl<'a> Iterator for RefSubCiSubTByRef<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_sf = reg_peek!(self.ref_sfs);
            let cit_wid = reg_peek!(self.cit_wids, self.ref_sfs, self.gets.citing(*self.ref_wid));
            let cit_topic = opt_next!(self.cit_topics, self.cit_wids, self.gets.wtopics(*cit_wid));
            return Some((*ref_sf, *self.gets.tsuf(cit_topic), *cit_topic, *cit_wid));
        }
    }
}

impl<'a> Iterator for RefSubSourceTop<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_sf = reg_peek!(self.ref_sfs);
            let cit_wid = reg_peek!(self.cit_wids, self.ref_sfs, self.gets.citing(*self.ref_wid));
            let cit_source = self.gets.top_source(self.ref_wid);
            let cit_topic = opt_next!(self.cit_topics, self.cit_wids, self.gets.wtopics(*cit_wid));
            return Some((*ref_sf, *cit_source, *cit_topic, *cit_wid));
        }
    }
}

impl<'a> Iterator for CiteSubSourceTop<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cit_wid = reg_peek!(self.cit_wids);
            let cit_sf = opt_peek!(self.cit_sfs, self.cit_wids, self.gets.wsubfields(*cit_wid));
            let cit_source = self.gets.top_source(cit_wid);
            let cit_topic = opt_next!(
                self.cit_topics,
                self.cit_sfs.as_mut().unwrap(),
                self.gets.wtopics(*cit_wid)
            );
            return Some((*cit_sf, *cit_source, *cit_topic, *cit_wid));
        }
    }
}

impl<'a> Iterator for QedInf<'a> {
    type Item = RwbiItem<'a, Self>;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let cit_wid = reg_peek!(self.cit_wids);
            let ref_source = self.gets.top_source(self.ref_wid);
            let ref_year = self.gets.year(self.ref_wid);
            let ref_q = *self.gets.sqy(&(*ref_source, *ref_year));
            let cit_sf = opt_peek!(self.cite_sfs, self.cit_wids, self.gets.wsubfields(*cit_wid));
            let cit_country = opt_next!(
                self.cite_countries,
                self.cite_sfs.as_mut().unwrap(),
                self.gets.wcountries(*cit_wid)
            );
            return Some((ref_q, *ref_source, *cit_sf, *cit_country, *cit_wid));
        }
    }
}

impl<'a, E, I> Iterator for PostRefIterWrap<'a, E, I>
where
    E: NumberedEntity + WorksFromMemory,
    I: RefWorkBasedIter<'a>,
    StackFr<I::SB>: ExtendedWithRefWid,
{
    type Item = (PartitionId, StackFr<I::SB>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_wid = reg_peek!(self.refs_it);
            let ref_per = self.gets.wperiod(ref_wid);
            match &mut self.it {
                Some(it) => match it.next() {
                    Some(cts) => {
                        let ext_fr = <StackFr<I::SB> as ExtendedWithRefWid>::extend(cts, *ref_wid);
                        return Some((*ref_per, ext_fr));
                    }
                    None => {
                        self.it = None;
                        self.refs_it.next();
                        continue;
                    }
                },
                None => {
                    self.it = Some(I::new(&ref_wid, &self.gets));
                }
            }
        }
    }
}

impl<'a, I, SB> Iterator for CountryInstsPost<'a, I, SB>
where
    I: RefWorkBasedIter<'a>,
    ExtendedFr<'a, I>: ExtendWithInst,
    StackFr<I::SB>: ExtendedWithRefWid,
{
    type Item = (PartitionId, ExtItem<'a, I>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_inst = reg_peek!(self.insts);
            match &mut self.pr_it {
                Some(it) => match it.next() {
                    Some(sub_e) => {
                        return Some(sub_e.extend(*ref_inst));
                    }
                    None => {
                        self.pr_it = None;
                        self.insts.next();
                        continue;
                    }
                },
                None => {
                    self.pr_it = Some(PostRefIterWrap::new(*ref_inst, self.gets));
                }
            }
        }
    }
}

impl<'a> Iterator for CountryBesties<'a> {
    type Item = (
        PartitionId,
        StackFr<<Self as PartitioningIterator<'a>>::StackBasis>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_wid = reg_peek!(self.ref_wids);
            let ref_per = self.gets.wperiod(ref_wid);
            let ref_sf = opt_peek!(self.ref_sfs, self.ref_wids, self.gets.wsubfields(*ref_wid));
            let (ref_country, ref_inst) = match &mut self.ref_insts {
                Some(it) => match it.peek() {
                    Some(iid) => {
                        let rc = self.gets.icountry(*iid);
                        if *rc == self.id {
                            it.next();
                            continue;
                        }
                        (*rc, *iid)
                    }
                    None => {
                        self.ref_insts = None;
                        self.ref_sfs.as_mut().unwrap().next();
                        continue;
                    }
                },
                None => {
                    self.ref_insts = Some(self.gets.winsts(*ref_wid).iter().peekable());
                    continue;
                }
            };
            let cit_wid = opt_next!(
                self.cit_wids,
                self.ref_insts.as_mut().unwrap(),
                self.gets.citing(*ref_wid)
            );
            return Some((
                *ref_per,
                (ref_country, *ref_inst, *ref_sf, *ref_wid, *cit_wid),
            ));
        }
    }
}

impl<'a> Iterator for AuthorBestiePapers<'a> {
    type Item = (
        PartitionId,
        StackFr<<Self as PartitioningIterator<'a>>::StackBasis>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_wid = reg_peek!(self.ref_wids);
            let ref_per = self.gets.wperiod(ref_wid);

            let ref_ship = opt_peek!(self.ref_ships, self.ref_wids, self.gets.wships(*ref_wid));
            let ref_author = self.gets.shipa(ref_ship);
            if *ref_author == self.id {
                self.ref_ships.as_mut().unwrap().next();
                continue;
            }

            let cit_wid = opt_peek!(
                self.cit_wids,
                self.ref_ships.as_mut().unwrap(),
                self.gets.citing(*ref_wid)
            );

            let cit_sf = opt_next!(
                self.cit_sfs,
                self.cit_wids.as_mut().unwrap(),
                self.gets.wsubfields(*cit_wid)
            );

            return Some((
                *ref_per,
                (*ref_author, *ref_wid, *cit_sf, *ref_wid, *cit_wid),
            ));
        }
    }
}

impl<'a> Iterator for AuthorBesties<'a> {
    type Item = (
        PartitionId,
        StackFr<<Self as PartitioningIterator<'a>>::StackBasis>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_wid = reg_peek!(self.ref_wids);
            let ref_per = self.gets.wperiod(ref_wid);

            let ref_ship = opt_peek!(self.ref_ships, self.ref_wids, self.gets.wships(*ref_wid));
            let ref_author = self.gets.shipa(ref_ship);
            if *ref_author == self.id {
                self.ref_ships.as_mut().unwrap().next();
                continue;
            }

            let cit_wid = opt_peek!(
                self.cit_wids,
                self.ref_ships.as_mut().unwrap(),
                self.gets.citing(*ref_wid)
            );

            let cit_inst = opt_next!(
                self.cit_insts,
                self.cit_wids.as_mut().unwrap(),
                self.gets.winsts(*cit_wid)
            );

            return Some((
                *ref_per,
                (
                    *ref_author,
                    *self.gets.icountry(cit_inst),
                    *cit_inst,
                    *ref_wid,
                    *cit_wid,
                ),
            ));
        }
    }
}

impl<'a> Iterator for InstBesties<'a> {
    type Item = (
        PartitionId,
        StackFr<<Self as PartitioningIterator<'a>>::StackBasis>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_wid = reg_peek!(self.ref_wids);
            let ref_per = self.gets.wperiod(ref_wid);

            let ref_inst = opt_peek!(self.ref_insts, self.ref_wids, self.gets.winsts(*ref_wid));
            if *ref_inst == self.id {
                self.ref_insts.as_mut().unwrap().next();
                continue;
            }

            let ref_sf = opt_peek!(
                self.ref_sfs,
                self.ref_insts.as_mut().unwrap(),
                self.gets.wsubfields(*ref_wid)
            );

            let cit_wid = opt_next!(
                self.cit_wids,
                self.ref_sfs.as_mut().unwrap(),
                self.gets.citing(*ref_wid)
            );

            return Some((
                *ref_per,
                (
                    *self.gets.icountry(ref_inst),
                    *ref_sf,
                    *ref_inst,
                    *ref_wid,
                    *cit_wid,
                ),
            ));
        }
    }
}

impl<'a> Iterator for WorkingAuthors<'a> {
    type Item = (
        PartitionId,
        StackFr<<Self as PartitioningIterator<'a>>::StackBasis>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_wid = reg_peek!(self.ref_wids);
            let ref_per = self.gets.wperiod(ref_wid);
            let ref_ship = opt_peek!(self.ref_ships, self.ref_wids, self.gets.wships(*ref_wid));
            let au_id = self.gets.shipa(ref_ship);
            if (au_id.to_usize() == UNKNOWN_ID)
                || self
                    .gets
                    .shipis(*ref_ship)
                    .into_iter()
                    .find(|e| **e == self.id)
                    .is_none()
            {
                self.ref_ships.as_mut().unwrap().next();
                continue;
            }
            let cit_wid = opt_peek!(
                self.cit_wids,
                self.ref_ships.as_mut().unwrap(),
                self.gets.citing(*ref_wid)
            );
            let cit_inst = opt_next!(
                self.cit_insts,
                self.cit_wids.as_mut().unwrap(),
                self.gets.winsts(*ref_wid)
            );
            return Some((
                *ref_per,
                (
                    *au_id,
                    *self.gets.icountry(cit_inst),
                    *cit_inst,
                    *ref_wid,
                    *cit_wid,
                ),
            ));
        }
    }
}

impl<'a> Iterator for SubfieldRefTopicCountryInst<'a> {
    type Item = (
        PartitionId,
        StackFr<<Self as PartitioningIterator<'a>>::StackBasis>,
    );
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_wid = reg_peek!(self.ref_wids);
            let ref_per = self.gets.wperiod(ref_wid);
            let ref_topic = opt_peek!(self.ref_topics, self.ref_wids, self.gets.wtopics(*ref_wid));
            let sf_id = self.gets.tsuf(ref_topic);
            if *sf_id != self.id {
                self.ref_topics.as_mut().unwrap().next();
                continue;
            }
            let ref_inst = opt_peek!(
                self.ref_insts,
                self.ref_topics.as_mut().unwrap(),
                self.gets.winsts(*ref_wid)
            );
            let cit_wid = opt_next!(
                self.cit_wids,
                self.ref_insts.as_mut().unwrap(),
                self.gets.citing(*ref_wid)
            );
            return Some((
                *ref_per,
                (
                    *ref_topic,
                    *self.gets.icountry(ref_inst),
                    *ref_inst,
                    *ref_wid,
                    *cit_wid,
                ),
            ));
        }
    }
}

impl<'a, E, I> PartitioningIterator<'a> for PostRefIterWrap<'a, E, I>
where
    E: NumberedEntity + WorksFromMemory,
    I: RefWorkBasedIter<'a>,
    StackFr<I::SB>: ExtendedWithRefWid,
{
    type Root = E;
    type StackBasis = I::SB;
    const PARTITIONS: usize = N_PERS;
    const IS_SPEC: bool = I::RWB_IS_SPEC;
    const DEFAULT_PARTITION: u8 = 3; //2020
    fn new(id: NET<E>, gets: &'a Getters) -> Self {
        let refs_it = E::works_from_ram(&gets, id).iter().peekable();
        Self {
            gets,
            refs_it,
            it: None,
            p: PhantomData,
        }
    }
}

impl<'a, I, SB> PartitioningIterator<'a> for CountryInstsPost<'a, I, SB>
where
    I: RefWorkBasedIter<'a>,
    StackFr<I::SB>: ExtendedWithRefWid,
    SB: StackBasis,
    SB::SortedRec: SortedRecord<FlatRecord = ExtItem<'a, I>>,
    (PartitionId, StackFr<I::SB>): ExtendWithInst,
    ExtendedFr<'a, I>: ExtendWithInst,
{
    type Root = Countries;
    type StackBasis = SB;
    const PARTITIONS: usize = N_PERS;
    const IS_SPEC: bool = false;
    fn new(id: NET<Countries>, gets: &'a Getters) -> Self {
        let insts = gets.country_insts(id).iter().peekable();
        Self {
            gets,
            insts,
            pr_it: None,
            p: PhantomData,
        }
    }
}

impl<'a> PartitioningIterator<'a> for CountryBesties<'a> {
    type StackBasis = (
        IntX<Countries, 0, true>,
        IntX<Institutions, 0, true>,
        IntX<Subfields, 1, true>,
    );
    type Root = Countries;
    const PARTITIONS: usize = N_PERS;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self {
        Self {
            gets,
            id,
            ref_wids: gets.cworks(id).iter().peekable(),
            ref_insts: None,
            ref_sfs: None,
            cit_wids: None,
        }
    }
}

impl<'a> PartitioningIterator<'a> for AuthorBestiePapers<'a> {
    type StackBasis = (
        IntX<Authors, 0, true>,
        IntX<Works, 0, true>,
        IntX<Subfields, 2, false>,
    );
    type Root = Authors;
    const PARTITIONS: usize = N_PERS;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self {
        Self {
            gets,
            id,
            ref_wids: gets.aworks(id).iter().peekable(),
            ref_ships: None,
            cit_sfs: None,
            cit_wids: None,
        }
    }
}

impl<'a> PartitioningIterator<'a> for AuthorBesties<'a> {
    type StackBasis = (
        IntX<Authors, 0, true>,
        IntX<Countries, 1, false>,
        IntX<Institutions, 1, false>,
    );
    type Root = Authors;
    const PARTITIONS: usize = N_PERS;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self {
        Self {
            gets,
            id,
            ref_wids: gets.aworks(id).iter().peekable(),
            ref_ships: None,
            cit_insts: None,
            cit_wids: None,
        }
    }
}

impl<'a> PartitioningIterator<'a> for InstBesties<'a> {
    type StackBasis = (
        IntX<Countries, 0, true>,
        IntX<Subfields, 1, true>,
        IntX<Institutions, 0, true>,
    );
    type Root = Institutions;
    const PARTITIONS: usize = N_PERS;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self {
        Self {
            gets,
            id,
            ref_wids: gets.iworks(id).iter().peekable(),
            ref_insts: None,
            ref_sfs: None,
            cit_wids: None,
        }
    }
}

impl<'a> PartitioningIterator<'a> for WorkingAuthors<'a> {
    type StackBasis = (
        IntX<Authors, 0, true>,
        IntX<Countries, 1, false>,
        IntX<Institutions, 1, false>,
    );
    type Root = Institutions;
    const PARTITIONS: usize = N_PERS;
    const IS_SPEC: bool = false;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self {
        Self {
            gets,
            id,
            ref_wids: gets.iworks(id).iter().peekable(),
            cit_insts: None,
            ref_ships: None,
            cit_wids: None,
        }
    }
}

impl<'a> PartitioningIterator<'a> for SubfieldRefTopicCountryInst<'a> {
    type StackBasis = (
        IntX<Topics, 0, true>,
        IntX<Countries, 1, true>,
        IntX<Institutions, 1, true>,
    );
    type Root = Subfields;
    const PARTITIONS: usize = N_PERS;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self {
        Self {
            gets,
            id,
            ref_wids: gets.sfworks(id).iter().peekable(),
            ref_insts: None,
            ref_topics: None,
            cit_wids: None,
        }
    }
}

pub fn to_bds<T, C>() -> BreakdownSpec
where
    T: FoldStackBase<C>,
{
    BreakdownSpec {
        attribute_type: T::LevelEntity::NAME.to_string(),
        spec_denom_ind: T::SPEC_DENOM_IND as u8,
        source_side: T::SOURCE_SIDE,
    }
}

fn peek_and_roll_o<IC, IP, TC, TP, F>(
    i_child: &mut Option<Peekable<IC>>,
    i_parent: &mut IP,
    getter: F,
) -> Option<TC>
where
    IC: Iterator<Item = TC>,
    IP: Iterator<Item = TP>,
    F: Fn() -> IC,
    TC: Copy,
{
    match i_child {
        Some(it) => match it.peek() {
            Some(eid) => return Some(*eid),
            None => {
                i_parent.next();
                *i_child = None;
            }
        },
        None => {
            *i_child = Some(getter().peekable());
        }
    }
    None
}

fn next_and_roll_o<IC, IP, TC, TP, F>(
    i_child: &mut Option<IC>,
    i_parent: &mut IP,
    getter: F,
) -> Option<TC>
where
    IC: Iterator<Item = TC>,
    IP: Iterator<Item = TP>,
    F: Fn() -> IC,
    TC: Copy,
{
    match i_child {
        Some(it) => match it.next() {
            Some(eid) => return Some(eid),
            None => {
                i_parent.next();
                *i_child = None;
            }
        },
        None => {
            *i_child = Some(getter());
        }
    }
    None
}
