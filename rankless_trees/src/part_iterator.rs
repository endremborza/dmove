use std::{
    fs::{create_dir_all, remove_dir_all, File},
    io::{BufReader, BufWriter, Read, Write},
    path::PathBuf,
    str::FromStr,
    sync::Mutex,
};

use dmove::{ByteFixArrayInterface, Entity, InitEmpty, UnsignedNumber};
use hashbrown::hash_map::Entry;
use rand::Rng;
use rankless_rs::{
    agg_tree::{HeapIterator, MinHeap, SortedRecord, Updater},
    common::{read_buf_path, write_buf_path},
    steps::{
        a1_entity_mapping::{YearInterface, POSSIBLE_YEAR_FILTERS},
        derive_links1::WorkPeriods,
    },
};
use tqdm::Iter;

use crate::{
    components::{PartitionId, StackBasis, StackFr},
    instances::{Collapsing, DisJTree, IntXTree, TopTree},
    interfacing::{Getters, NumberedEntity, NET},
    io::{
        BoolCvp, BufSerTree, CacheMap, CacheValue, FullTreeQuery, ResCvp, TreeBasisState,
        TreeResponse, TreeSpec, WT,
    },
    prune::prune,
};

const MAX_PARTITIONS: usize = 16;
const MAX_BUFSIZE: usize = 512;

type SrHeap<'a, S> = MinHeap<StackFr<<S as PartitioningIterator<'a>>::StackBasis>>;

enum Progress {
    Wait(BoolCvp),
    Calculate,
    // Prune,
    Load,
}

pub trait PartitioningIterator<'a>:
    Iterator<Item = (PartitionId, StackFr<Self::StackBasis>)> + Sized
{
    type Root: NumberedEntity;
    type StackBasis: StackBasis;
    const PARTITIONS: usize;
    const IS_SPEC: bool = true;
    const DEFAULT_PARTITION: u8 = 0;
    fn new(id: NET<Self::Root>, gets: &'a Getters) -> Self;

    fn get_spec() -> TreeSpec {
        let breakdowns = Self::StackBasis::get_bds();
        let root_type = Self::Root::NAME.to_string();
        TreeSpec {
            root_type,
            breakdowns,
            is_spec: Self::IS_SPEC,
            allow_spec: Self::IS_SPEC,
            default_partition: POSSIBLE_YEAR_FILTERS[Self::DEFAULT_PARTITION as usize],
        }
    }

    fn fill_res_cvp<CT, SR>(fq: FullTreeQuery, state: &'a TreeBasisState, res_cvp: ResCvp)
    where
        Self::StackBasis: StackBasis<TopTree = CT, SortedRec = SR>,
        StackFr<Self::StackBasis>: Ord + Clone + ByteFixArrayInterface + GetRefWork,
        SR: SortedRecord,
        CT: Collapsing + TopTree,
        DisJTree<Self::Root, CT>: Into<BufSerTree>,
        IntXTree<Self::Root, CT>: Updater<CT>,
    {
        println!("requested entity: {fq}");
        let pruned_path = state.pruned_cache_file(&fq);
        let prog = Progress::from_e(&state.im_cache, &fq);
        //getting one of e(tid) might trigger all others
        match prog {
            Progress::Calculate => {
                return Self::fill_calculate(fq, state, res_cvp);
            }
            Progress::Wait(cvp) => {
                let (lock, cvar) = &*cvp;
                let mut done = lock.lock().unwrap();
                while !*done {
                    done = cvar.wait(done).unwrap();
                }
            }
            Progress::Load => {}
        }

        let now = std::time::Instant::now();
        let pruned_tree: BufSerTree =
            read_buf_path(&pruned_path).expect(&format!("failed reading {pruned_path:?}"));
        let bds = Self::get_spec().breakdowns;
        let resp = TreeResponse::from_pruned(pruned_tree, &fq, &bds, state);
        {
            let (lock, cvar) = &*res_cvp;
            let mut data = lock.lock().unwrap();
            *data = Some(resp);
            cvar.notify_all();
        }
        println!(
            "{fq}: loaded and sent cache in {}",
            now.elapsed().as_millis()
        );
    }

    fn fill_calculate<CT, SR>(fq: FullTreeQuery, state: &'a TreeBasisState, res_cvp: ResCvp)
    where
        Self::StackBasis: StackBasis<TopTree = CT, SortedRec = SR>,
        SR: SortedRecord,
        StackFr<Self::StackBasis>: Ord + Clone + GetRefWork + ByteFixArrayInterface,
        CT: Collapsing + TopTree,
        DisJTree<Self::Root, CT>: Into<BufSerTree>,
        IntXTree<Self::Root, CT>: Updater<CT>,
    {
        let mut pids = Vec::new();
        let et_id = NET::<Self::Root>::from_usize(fq.ck.eid as usize);

        let get_path = |pid: u8| state.full_cache_file_period(&fq, pid);
        let mut check_w = |pid: usize, tree: &BufSerTree| {
            let pid8 = pid as u8;
            Self::write_resp(&tree, &fq, state, res_cvp.clone(), pid8);
            pids.push(pid8);
            pid8
        };
        create_dir_all(get_path(0).parent().unwrap()).unwrap();

        if fq.q.big.unwrap_or(false) {
            let piter = Self::new(et_id, &state.gets);
            fill_big_calculate::<Self, CT, SR, _, _>(piter, &fq, state, check_w, get_path);
        } else {
            let heaps = Self::fill_heaps(&fq, &et_id, state);

            let now = std::time::Instant::now();
            let mut roots = Vec::new();
            heaps.into_iter().take(Self::PARTITIONS).for_each(|heap| {
                let hither_o: Option<HeapIterator<SR>> = heap.into();
                let mut part_root: IntXTree<Self::Root, CT> = et_id.into();
                match hither_o {
                    Some(hither) => Self::StackBasis::fold_into(&mut part_root, hither),
                    None => println!("nothing in a partition"),
                }
                roots.push(part_root.collapse());
            });
            println!("{fq}: got roots in {}", now.elapsed().as_millis());

            let now = std::time::Instant::now();
            let mut ser_tree_o = None;
            for (pid, part_root) in roots.into_iter().enumerate().rev() {
                Self::fold_tree(&mut ser_tree_o, part_root);
                let stref = &ser_tree_o.as_ref().unwrap();
                let pid8 = check_w(pid, &stref);
                write_buf_path(&stref, get_path(pid8)).unwrap();
            }
            println!(
                "{fq}: converted, ingested and wrote trees in {}",
                now.elapsed().as_millis()
            );
        }
        let mut cache_map = state.im_cache.lock().unwrap();
        let cv = CacheValue::Done(pids);
        let bcvp = match cache_map.insert(fq.ck, cv).unwrap() {
            CacheValue::InProgress(cvp) => cvp,
            _ => {
                println!("WARNING: non inprogress cache");
                return;
            }
        };
        let (lock, cvar) = &*bcvp;
        let mut data = lock.lock().unwrap();
        *data = true;
        cvar.notify_all();
    }

    fn fold_tree<R>(ser_tree_o: &mut Option<BufSerTree>, part_root: R)
    where
        R: Into<BufSerTree>,
    {
        let part_ser: BufSerTree = part_root.into();
        match ser_tree_o {
            Some(ser_tree) => ser_tree.ingest_disjunct(part_ser),
            None => *ser_tree_o = Some(part_ser),
        };
    }

    fn fill_heaps(
        fq: &FullTreeQuery,
        et_id: &NET<Self::Root>,
        state: &'a TreeBasisState,
    ) -> [SrHeap<'a, Self>; MAX_PARTITIONS]
    where
        StackFr<Self::StackBasis>: Ord,
    {
        let mut heaps = [(); MAX_PARTITIONS].map(|_| SrHeap::<'a, Self>::new());
        let now = std::time::Instant::now();
        let maker = Self::new(*et_id, &state.gets);
        for (pid, rec) in maker {
            heaps[pid as usize].push(rec);
        }
        println!("{fq}: got heaps in {}", now.elapsed().as_millis());
        heaps
    }

    fn write_resp(
        full_tree: &BufSerTree,
        fq: &FullTreeQuery,
        state: &'a TreeBasisState,
        res_cvp: ResCvp,
        pid: u8,
    ) {
        let now = std::time::Instant::now();
        let bds = Self::get_spec().breakdowns;
        let pruned_tree = prune(full_tree, &state.att_union, &bds);
        println!("{fq}: pruned in {}", now.elapsed().as_millis());
        //cache pruned response, use it if no connections are requested
        let full_resp = TreeResponse::from_pruned(pruned_tree.clone(), fq, &bds, state);
        if pid == fq.period {
            let (lock, cvar) = &*res_cvp;
            let mut data = lock.lock().unwrap();
            *data = Some(full_resp);
            cvar.notify_all();
        }
        let resp_path = state.pruned_cache_file_period(fq, pid);
        write_buf_path(pruned_tree, &resp_path).unwrap();
        println!("{fq}: wrote to {:?}", resp_path);
    }
}

pub trait GetRefWork {
    fn rwid(&self) -> WT;
}

impl Progress {
    fn from_e(value: &Mutex<CacheMap>, fq: &FullTreeQuery) -> Self {
        //if any of the periods in progress, somehow queue this period too?
        //in full progress, vs in pruning progress
        match value.lock().unwrap().entry(fq.ck.clone()) {
            Entry::Vacant(e) => {
                e.insert(CacheValue::InProgress(BoolCvp::init_empty()));
                Progress::Calculate
            }
            Entry::Occupied(cv) => match cv.get() {
                CacheValue::InProgress(cvp) => Progress::Wait(cvp.clone()),
                CacheValue::Done(done_periods) => {
                    if done_periods.contains(&fq.period) {
                        Progress::Load
                    } else {
                        println!("not implemented partial waiting");
                        Progress::Calculate
                    }
                }
            },
        }
    }
}

impl<T1> GetRefWork for (T1, WT, WT) {
    fn rwid(&self) -> WT {
        self.1.lift()
    }
}

impl<T1, T2> GetRefWork for (T1, T2, WT, WT) {
    fn rwid(&self) -> WT {
        self.2.lift()
    }
}

impl<T1, T2, T3> GetRefWork for (T1, T2, T3, WT, WT) {
    fn rwid(&self) -> WT {
        self.3.lift()
    }
}

impl<T1, T2, T3, T4> GetRefWork for (T1, T2, T3, T4, WT, WT) {
    fn rwid(&self) -> WT {
        self.4.lift()
    }
}

fn fill_big_calculate<'a, PI, CT, SR, F1, F2>(
    piter: PI,
    fq: &FullTreeQuery,
    state: &'a TreeBasisState,
    mut check_w: F1,
    get_path: F2,
) where
    PI: PartitioningIterator<'a>,
    PI::StackBasis: StackBasis<TopTree = CT, SortedRec = SR>,
    SR: SortedRecord,
    StackFr<PI::StackBasis>: Ord + Clone + ByteFixArrayInterface + GetRefWork,
    CT: Collapsing + TopTree,
    DisJTree<PI::Root, CT>: Into<BufSerTree>,
    IntXTree<PI::Root, CT>: Updater<CT>,
    F1: FnMut(usize, &BufSerTree) -> u8,
    F2: Fn(u8) -> PathBuf,
{
    let et_id = NET::<PI::Root>::from_usize(fq.ck.eid as usize);
    let id: u64 = rand::thread_rng().gen();
    let cache_root = PathBuf::from_str(&format!("/tmp/dmove-parts/{id}")).expect("making tmp path");
    create_dir_all(&cache_root).expect("making tmp dir");
    let mut writers = Vec::new();
    for yp in YearInterface::iter() {
        writers.push(BufWriter::new(
            File::create(cache_root.join(yp.to_string())).expect("create year cache file"),
        ));
    }
    for e in piter.tqdm().desc(Some(format!("part-iter {fq}"))) {
        let frec = e.1;
        let rwid = frec.rwid();
        let y = state.gets.year(&rwid);
        writers[*y as usize]
            .write(&frec.to_fbytes())
            .expect("writing to cache");
    }

    let mut buf: [u8; MAX_BUFSIZE] = [0; MAX_BUFSIZE];
    let mut ser_tree_o = None;
    let mut year_bp_iter = POSSIBLE_YEAR_FILTERS.iter().rev();
    let mut next_bp_o = year_bp_iter.next();
    let bufr = &mut buf[..StackFr::<PI::StackBasis>::S];
    for y in YearInterface::iter()
        .rev()
        .tqdm()
        .desc(Some("reading years"))
    {
        let mut reader =
            BufReader::new(File::open(cache_root.join(y.to_string())).expect("reading year cache"));
        let mut year_heap = MinHeap::new();
        while let Ok(_) = reader.read_exact(bufr) {
            let fr = StackFr::<PI::StackBasis>::from_fbytes(bufr);
            year_heap.push(fr);
        }
        let hither_o: Option<HeapIterator<SR>> = year_heap.into();
        let mut part_root: IntXTree<PI::Root, CT> = et_id.into();
        match hither_o {
            Some(hither) => PI::StackBasis::fold_into(&mut part_root, hither),
            None => println!("nothing in a partition"),
        }
        PI::fold_tree(&mut ser_tree_o, part_root.collapse());
        let stref = &ser_tree_o.as_ref().unwrap();
        let y16 = YearInterface::reverse(y);
        if let Some(next_bp) = next_bp_o {
            if y16 == *next_bp {
                let pid = WorkPeriods::from_year(y16);
                let pid8 = check_w(pid.to_usize(), &stref);
                write_buf_path(&stref, get_path(pid8)).unwrap();
                next_bp_o = year_bp_iter.next();
            }
        } else {
            println!("finished with bps");
        }
    }
    remove_dir_all(cache_root).expect("removing cache");
}
