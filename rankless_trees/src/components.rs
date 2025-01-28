use std::{fs::create_dir_all, iter::Peekable, marker::PhantomData, slice::Iter};

use dmove::{Entity, NamespacedEntity, UnsignedNumber, VattReadingRefMap, ET};
use dmove_macro::impl_stack_basees;
use rankless_rs::{
    agg_tree::{FoldingStackConsumer, HeapIterator, MinHeap, ReinstateFrom, SortedRecord, Updater},
    common::{read_buf_path, write_buf_path},
    env_consts::START_YEAR,
    gen::{
        a1_entity_mapping::{Authors, Countries, Institutions, Sources, Subfields, Topics, Works},
        a2_init_atts::WorksNames,
    },
    steps::{a1_entity_mapping::N_PERS, derive_links1::WorkPeriods},
};

use crate::{
    ids::get_atts,
    instances::{Collapsing, DisJTree, FoldStackBase, IntXTree, TopTree, WorkTree},
    interfacing::{Getters, NumberedEntity, WorksFromMemory, NET},
    io::{BreakdownSpec, BufSerTree, TreeQ, TreeResponse, TreeSpec, WT},
    prune::prune,
    AttributeLabelUnion,
};

const MAX_PARTITIONS: usize = 16;

pub type StackFr<S> = <<S as StackBasis>::SortedRec as SortedRecord>::FlatRecord;
pub type PartitionId = u8;

type ExtendedFr<'a, I> = (
    PartitionId,
    <<I as RefWorkBasedIter<'a>>::IT as ExtendWithRefWid>::To,
);

type ExtItem<'a, I> = <ExtendedFr<'a, I> as ExtendWithInst>::To;

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

pub struct CitingCoSuToByRef<'a> {
    cit_wids: Peekable<Iter<'a, ET<Works>>>,
    cit_tops: Option<Peekable<Iter<'a, ET<Topics>>>>,
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

pub struct FullRefSourceCountryInstByRef<'a> {
    ref_wid: &'a WT,
    ref_sources: Peekable<Iter<'a, ET<Sources>>>,
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

pub struct SubfieldCountryInstSourceByRef<'a> {
    sci_top: Peekable<SubfieldCountryInstByRef<'a>>,
    cit_sources: Option<Iter<'a, ET<Sources>>>,
    gets: &'a Getters,
}

pub struct InstSubfieldCountryInstByRef<'a> {
    ref_wid: &'a WT,
    sci_top: Peekable<SubfieldCountryInstByRef<'a>>,
    ref_insts: Iter<'a, ET<Institutions>>,
    gets: &'a Getters,
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

pub trait PartitioningIterator<'a>:
    Iterator<Item = (PartitionId, StackFr<Self::StackBasis>)> + Sized
{
    type Root: NumberedEntity;
    type StackBasis: StackBasis;
    const PARTITIONS: usize;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self;

    fn get_spec() -> TreeSpec {
        let breakdowns = Self::StackBasis::get_bds();
        let root_type = Self::Root::NAME.to_string();
        TreeSpec {
            root_type,
            breakdowns,
        }
    }

    fn tree_resp<CT, SR, FR>(
        q: TreeQ,
        gets: &'a Getters,
        stats: &AttributeLabelUnion,
    ) -> TreeResponse
    where
        Self::StackBasis: StackBasis<TopTree = CT, SortedRec = SR>,
        SR: SortedRecord<FlatRecord = FR>,
        FR: Ord + Clone,
        CT: Collapsing + TopTree,
        DisJTree<Self::Root, CT>: Into<BufSerTree>,
        IntXTree<Self::Root, CT>: Updater<CT>,
    {
        let tid = q.tid.unwrap_or(0);
        let eid = q.eid.to_usize();
        let req_id = format!("{}({}:{})", Self::Root::NAME, eid, tid);
        println!("requested entity: {req_id}");
        let period = WorkPeriods::from_year(q.year.unwrap_or(START_YEAR));
        let tree_cache_dir = gets
            .stowage
            .paths
            .cache
            .join(Self::Root::NAME)
            .join(q.eid.to_string())
            .join(tid.to_string());
        let get_path = |pid: usize| tree_cache_dir.join(format!("{pid}.gz"));
        let mut full_tree: BufSerTree = if !tree_cache_dir.exists() {
            create_dir_all(tree_cache_dir.clone()).unwrap();
            let mut heaps = [(); MAX_PARTITIONS].map(|_| MinHeap::<FR>::new());
            let et_id = NET::<Self::Root>::from_usize(eid);
            let now = std::time::Instant::now();
            let maker = Self::new(et_id, &gets);
            for (pid, rec) in maker {
                heaps[pid as usize].push(rec)
            }
            println!("{req_id}: got heaps in {}", now.elapsed().as_millis());
            let now = std::time::Instant::now();
            let mut roots = Vec::new();
            heaps.into_iter().take(Self::PARTITIONS).for_each(|heap| {
                let hither_o: Option<HeapIterator<<Self::StackBasis as StackBasis>::SortedRec>> =
                    heap.into();
                let mut part_root: IntXTree<Self::Root, CT> = et_id.into();
                if let Some(hither) = hither_o {
                    Self::StackBasis::fold_into(&mut part_root, hither);
                } else {
                    println!("nothing in a partition")
                }
                roots.push(part_root.collapse());
            });
            println!("{req_id}: got roots in {}", now.elapsed().as_millis());

            let now = std::time::Instant::now();
            let mut root_it = roots.into_iter().enumerate().rev();
            let (pid, root_n) = root_it.next().unwrap();
            let mut ser_tree: BufSerTree = root_n.into();
            write_buf_path(&ser_tree, get_path(pid)).unwrap();
            for (pid, part_root) in root_it {
                let part_ser: BufSerTree = part_root.into();
                ser_tree.ingest_disjunct(part_ser);
                write_buf_path(&ser_tree, get_path(pid)).unwrap();
            }
            println!(
                "{req_id}: converted and wrote trees in {}",
                now.elapsed().as_millis()
            );
            ser_tree
        } else {
            //TODO WARN possible race condition! if multithreaded thing, one starts writing,
            //created the directory, but did not write all the files yet, this can start reading
            //shit
            //need ot fix it with some lock store like thing
            read_buf_path(get_path(period as usize)).unwrap()
        };

        let now = std::time::Instant::now();
        let bds = Self::get_spec().breakdowns;
        prune(&mut full_tree, stats, &bds);
        println!("{req_id}: pruned in {}", now.elapsed().as_millis());
        //cache pruned response, use it if no connections are requested

        let parent = gets
            .stowage
            .path_from_ns(<WorksNames as NamespacedEntity>::NS);
        let mut work_name_basis =
            VattReadingRefMap::<WorksNames>::from_locator(&gets.wn_locators, &parent);

        let now = std::time::Instant::now();
        let atts = get_atts(&full_tree, &bds, stats, &mut work_name_basis);
        println!("{req_id}: got atts in {}", now.elapsed().as_millis());

        let now = std::time::Instant::now();
        let tree = full_tree.into();
        println!("{req_id}: converted in {}", now.elapsed().as_millis());
        TreeResponse { tree, atts }
    }
}

pub trait RefWorkBasedIter<'a>: Iterator<Item = Self::IT> {
    type SB: StackBasis;
    type IT: ExtendWithRefWid<To = StackFr<Self::SB>>;
    fn new(ref_wid: &'a WT, gets: &'a Getters) -> Self;
}

pub trait ExtendWithRefWid {
    type To;
    fn extend(self, value: WT) -> Self::To;
}

pub trait ExtendWithInst {
    type To;
    fn extend(self, value: ET<Institutions>) -> (PartitionId, Self::To);
}

type FoldingStackLeaf = WorkTree;
// type FoldingStackLeaf = ();

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

impl<T1, T2, T3> ExtendWithRefWid for (T1, T2, T3, WT) {
    type To = (T1, T2, T3, WT, WT);
    fn extend(self, value: WT) -> Self::To {
        (self.0, self.1, self.2, value, self.3)
    }
}

impl<T1, T2, T3, T4> ExtendWithRefWid for (T1, T2, T3, T4, WT) {
    type To = (T1, T2, T3, T4, WT, WT);
    fn extend(self, value: WT) -> Self::To {
        (self.0, self.1, self.2, self.3, value, self.4)
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
    type IT = (ET<Subfields>, ET<Countries>, ET<Institutions>, ET<Works>);
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

impl<'a> RefWorkBasedIter<'a> for FullRefSourceCountryInstByRef<'a> {
    type SB = (
        IntX<Sources, 0, true>,
        IntX<Countries, 1, true>,
        IntX<Institutions, 1, true>,
    );
    type IT = (ET<Sources>, ET<Countries>, ET<Institutions>, ET<Works>);
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        let cit_wids = gets.citing(*ref_wid).iter();
        let ref_insts = gets.winsts(*ref_wid).iter().peekable();
        let ref_sources = gets.wsources(*ref_wid).iter().peekable();
        Self {
            ref_wid,
            cit_wids,
            gets,
            ref_sources,
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
    type IT = (ET<Countries>, ET<Institutions>, ET<Subfields>, ET<Works>);
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
    type IT = (ET<Countries>, ET<Subfields>, ET<Topics>, ET<Works>);
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

impl<'a> RefWorkBasedIter<'a> for WCoIByRef<'a> {
    type SB = (
        IntX<Works, 0, true>,
        IntX<Countries, 1, false>,
        IntX<Institutions, 1, false>,
    );
    type IT = (ET<Works>, ET<Countries>, ET<Institutions>, ET<Works>);

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

impl<'a> RefWorkBasedIter<'a> for SubfieldCountryInstSourceByRef<'a> {
    type SB = (
        IntX<Subfields, 0, true>,
        IntX<Countries, 1, false>,
        IntX<Institutions, 1, false>,
        IntX<Sources, 3, false>,
    );
    type IT = (
        ET<Subfields>,
        ET<Countries>,
        ET<Institutions>,
        ET<Sources>,
        ET<Works>,
    );
    fn new(ref_wid: &'a ET<Works>, gets: &'a Getters) -> Self {
        Self {
            cit_sources: None,
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
    type IT = (
        ET<Institutions>,
        ET<Subfields>,
        ET<Countries>,
        ET<Institutions>,
        ET<Works>,
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

impl<'a> Iterator for SubfieldCountryInstByRef<'a> {
    type Item = <Self as RefWorkBasedIter<'a>>::IT;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_sf = match self.ref_sfs.peek() {
                Some(v) => *v,
                None => return None,
            };
            let cit_wid = match self.cit_wids.peek() {
                Some(v) => *v,
                None => {
                    self.cit_wids = self.gets.citing(*self.ref_wid).iter().peekable();
                    self.ref_sfs.next();
                    continue;
                }
            };
            let cit_inst = match &mut self.cit_insts {
                Some(cit_insts) => match cit_insts.next() {
                    Some(iid) => iid,
                    None => {
                        self.cit_wids.next();
                        self.cit_insts = None;
                        continue;
                    }
                },
                None => {
                    self.cit_insts = Some(self.gets.winsts(*cit_wid).iter());
                    continue;
                }
            };
            return Some((
                ref_sf.lift(),
                self.gets.icountry(cit_inst).lift(),
                cit_inst.lift(),
                cit_wid.lift(),
            ));
        }
    }
}

impl<'a> Iterator for FullRefSourceCountryInstByRef<'a> {
    type Item = <Self as RefWorkBasedIter<'a>>::IT;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let refed_source = match self.ref_sources.peek() {
                Some(v) => *v,
                None => return None,
            };
            let refed_inst = match self.ref_insts.peek() {
                Some(iid) => *iid,
                None => {
                    self.ref_sources.next();
                    self.ref_insts = self.gets.winsts(*self.ref_wid).iter().peekable();
                    continue;
                }
            };
            let citing_wid = match self.cit_wids.next() {
                Some(wid) => wid,
                None => {
                    self.ref_insts.next();
                    self.cit_wids = self.gets.citing(*self.ref_wid).iter();
                    continue;
                }
            };
            return Some((
                refed_source.lift(),
                self.gets.icountry(refed_inst).lift(),
                refed_inst.lift(),
                citing_wid.lift(),
            ));
        }
    }
}

impl<'a> Iterator for FullRefCountryInstSubfieldByRef<'a> {
    type Item = <Self as RefWorkBasedIter<'a>>::IT;

    fn next(&mut self) -> Option<Self::Item> {
        //TODO full-ref WET
        loop {
            let refed_sf = match self.ref_sfs.peek() {
                Some(v) => *v,
                None => return None,
            };
            let refed_inst = match self.ref_insts.peek() {
                Some(iid) => *iid,
                None => {
                    self.ref_sfs.next();
                    self.ref_insts = self.gets.winsts(*self.ref_wid).iter().peekable();
                    continue;
                }
            };
            let citing_wid = match self.cit_wids.next() {
                Some(wid) => wid,
                None => {
                    self.ref_insts.next();
                    self.cit_wids = self.gets.citing(*self.ref_wid).iter();
                    continue;
                }
            };
            return Some((
                self.gets.icountry(refed_inst).lift(),
                refed_inst.lift(),
                refed_sf.lift(),
                citing_wid.lift(),
            ));
        }
    }
}

impl<'a> Iterator for CitingCoSuToByRef<'a> {
    type Item = <Self as RefWorkBasedIter<'a>>::IT;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let citing_wid = match self.cit_wids.peek() {
                Some(v) => *v,
                None => return None,
            };
            let citing_topic = match &mut self.cit_tops {
                Some(citing_topics) => match citing_topics.peek() {
                    Some(tid) => *tid,
                    None => {
                        self.cit_wids.next();
                        self.cit_tops = None;
                        continue;
                    }
                },
                None => {
                    self.cit_tops = Some(self.gets.wtopics(*citing_wid).iter().peekable());
                    continue;
                }
            };
            let citing_inst = match &mut self.cit_insts {
                Some(citing_insts) => match citing_insts.next() {
                    Some(iid) => iid,
                    None => {
                        self.cit_tops.as_mut().unwrap().next();
                        self.cit_insts = None;
                        continue;
                    }
                },
                None => {
                    self.cit_insts = Some(self.gets.winsts(*citing_wid).iter());
                    continue;
                }
            };
            return Some((
                self.gets.icountry(citing_inst).lift(),
                self.gets.tsuf(citing_topic).lift(),
                citing_topic.lift(),
                citing_wid.lift(),
            ));
        }
    }
}

impl<'a> Iterator for WCoIByRef<'a> {
    type Item = <Self as RefWorkBasedIter<'a>>::IT;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let citing_wid = match self.cit_wids.peek() {
                Some(v) => *v,
                None => return None,
            };
            let citing_inst = match &mut self.cit_insts {
                Some(citing_insts) => match citing_insts.next() {
                    Some(iid) => iid,
                    None => {
                        self.cit_insts = None;
                        self.cit_wids.next();
                        continue;
                    }
                },
                None => {
                    self.cit_insts = Some(self.gets.winsts(*citing_wid).iter());
                    continue;
                }
            };
            return Some((
                self.ref_wid.lift(),
                self.gets.icountry(citing_inst).lift(),
                citing_inst.lift(),
                citing_wid.lift(),
            ));
        }
    }
}

impl<'a> Iterator for SubfieldCountryInstSourceByRef<'a> {
    type Item = <Self as RefWorkBasedIter<'a>>::IT;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let top_tup = match self.sci_top.peek() {
                Some(v) => *v,
                None => return None,
            };
            let cit_source = match &mut self.cit_sources {
                Some(cit_sources) => match cit_sources.next() {
                    Some(sid) => sid,
                    None => {
                        self.sci_top.next();
                        self.cit_sources = None;
                        continue;
                    }
                },
                None => {
                    self.cit_sources = Some(self.gets.wsources(top_tup.3).iter());
                    continue;
                }
            };
            return Some((
                top_tup.0.lift(),
                top_tup.1.lift(),
                top_tup.2.lift(),
                cit_source.lift(),
                top_tup.3.lift(),
            ));
        }
    }
}

impl<'a> Iterator for InstSubfieldCountryInstByRef<'a> {
    type Item = <Self as RefWorkBasedIter<'a>>::IT;

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let top_tup = match self.sci_top.peek() {
                Some(v) => *v,
                None => return None,
            };
            let ref_inst = match self.ref_insts.next() {
                Some(sid) => sid,
                None => {
                    self.sci_top.next();
                    self.ref_insts = self.gets.winsts(*self.ref_wid).iter();
                    continue;
                }
            };
            return Some((
                ref_inst.lift(),
                top_tup.0.lift(),
                top_tup.1.lift(),
                top_tup.2.lift(),
                top_tup.3.lift(),
            ));
        }
    }
}

impl<'a, E, I> Iterator for PostRefIterWrap<'a, E, I>
where
    E: NumberedEntity + WorksFromMemory,
    I: RefWorkBasedIter<'a>,
{
    type Item = (PartitionId, StackFr<I::SB>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_wid = match self.refs_it.peek() {
                Some(v) => *v,
                None => return None,
            };
            let ref_per = self.gets.wperiod(ref_wid);
            match &mut self.it {
                Some(it) => match it.next() {
                    Some(cts) => {
                        return Some((*ref_per, cts.extend(*ref_wid)));
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
{
    type Item = (PartitionId, ExtItem<'a, I>);
    fn next(&mut self) -> Option<Self::Item> {
        loop {
            let ref_inst = match self.insts.peek() {
                Some(v) => *v,
                None => return None,
            };
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
            let ref_wid = match self.ref_wids.peek() {
                Some(v) => *v,
                None => return None,
            };
            let ref_per = self.gets.wperiod(ref_wid);

            let ref_sf = match &mut self.ref_sfs {
                Some(it) => match it.peek() {
                    Some(sid) => *sid,
                    None => {
                        self.ref_wids.next();
                        self.ref_sfs = None;
                        continue;
                    }
                },
                None => {
                    self.ref_sfs = Some(self.gets.wsubfields(*ref_wid).iter().peekable());
                    continue;
                }
            };

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

            let cit_wid = match &mut self.cit_wids {
                Some(it) => match it.next() {
                    Some(wid) => *wid,
                    None => {
                        self.ref_insts.as_mut().unwrap().next();
                        self.cit_wids = None;
                        continue;
                    }
                },
                None => {
                    self.cit_wids = Some(self.gets.citing(*ref_wid).iter());
                    continue;
                }
            };
            return Some((
                *ref_per,
                (ref_country, *ref_inst, *ref_sf, *ref_wid, cit_wid),
            ));
        }
    }
}

impl<'a, E, I> PartitioningIterator<'a> for PostRefIterWrap<'a, E, I>
where
    E: NumberedEntity + WorksFromMemory,
    I: RefWorkBasedIter<'a>,
{
    type Root = E;
    type StackBasis = I::SB;
    const PARTITIONS: usize = N_PERS;
    fn new(id: NET<E>, gets: &'a Getters) -> Self {
        let refs_it = E::works_from_ram(&gets, id.lift()).iter().peekable();
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
    SB: StackBasis,
    SB::SortedRec: SortedRecord<FlatRecord = ExtItem<'a, I>>,
    (PartitionId, StackFr<I::SB>): ExtendWithInst,
    ExtendedFr<'a, I>: ExtendWithInst,
{
    type Root = Countries;
    type StackBasis = SB;
    const PARTITIONS: usize = N_PERS;
    fn new(id: NET<Countries>, gets: &'a Getters) -> Self {
        let insts = gets.country_insts(id.lift()).iter().peekable();
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
