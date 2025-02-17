use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header::CACHE_CONTROL, HeaderMap, HeaderValue, Method},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use dmove::{Entity, UnsignedNumber, ET};
use hashbrown::HashMap;
use kd_tree::{KdPoint, KdTree};
use rand::{seq::SliceRandom, Rng};
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    net::SocketAddr,
    sync::{Arc, Condvar, Mutex},
    thread::JoinHandle,
};

use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    CompressionLevel,
};

use muwo_search::SearchEngine;
use rankless_rs::{
    gen::a1_entity_mapping::{Authors, Countries, Institutions, Qs, Sources, Subfields, Topics},
    steps::{
        a1_entity_mapping::{RawYear, YearInterface, Years},
        derive_links5::{EraRec, InstRelation},
    },
    Stowage,
};
use rankless_trees::{
    interfacing::{Getters, NodeInterfaces, RootInterfaceable, RootInterfaces, NET},
    io::{TreeQ, TreeResponse, TreeRunManager},
    AttributeLabelUnion,
};

const N_THREADS: usize = 16;
// const UPPER_LIMIT: u32 = 1_000_000;
const UPPER_LIMIT: u32 = 100_000;

type InstTrm = TreeRunManager<(Institutions, Authors, Subfields, Countries, Sources)>;

#[derive(Deserialize)]
struct BasicQ {
    q: Option<String>,
}

#[derive(Serialize)]
struct ViewResult {
    #[serde(flatten)]
    sr: SearchResult,
    #[serde(flatten)]
    ext: ResultExtension,
    similars: Vec<SearchResult>,
    comparisons: Vec<SearchResult>,
}

#[derive(Serialize)]
struct TopResult {
    name: String,
    entities: Vec<SearchResult>,
}

#[derive(Serialize)]
struct EntityDescription {
    name: String,
    count: usize,
}

struct NameState {
    engine: SearchEngine,
    responses: Box<[SearchResult]>,
    exts: Box<[ResultExtension]>,
    sf_touchstones: Box<[SearchResult]>,
    means: Box<[f64; 2]>,
    vars: Box<[f64; 2]>,
    pub semantic_id_map: HashMap<String, SemVal>,
    query_tree: KdTree<KDItem>,
}

#[derive(Clone)]
struct SemVal {
    result_id: usize,
    dm_id: usize,
}
#[derive(Serialize, Clone)]
struct InstRelOut {
    start: u16,
    end: u16,
    #[serde(rename = "semId")]
    inst_sem_id: String,
    #[serde(rename = "name")]
    inst_name: String,
    papers: u16,
    citations: u32,
}

#[derive(Serialize, Clone)]
struct SearchResult {
    name: String,
    #[serde(rename = "semanticId")]
    semantic_id: String,
    #[serde(skip_serializing)]
    full_name: String,
    #[serde(rename = "dmId")]
    dm_id: usize,
    papers: u32,
    citations: u32,
}

#[derive(Serialize, Clone)]
struct ResultExtension {
    #[serde(rename = "instRels")]
    inst_rels: Box<[InstRelOut]>,
    #[serde(rename = "sfCoords")]
    sf_coords: (f64, f64),
    #[serde(rename = "startYear")]
    start_year: RawYear,
    #[serde(rename = "yearlyPapers")]
    yearly_papers: EraRec,
    #[serde(rename = "yearlyCites")]
    yearly_cites: EraRec,
}

struct KDItem {
    point: [f64; 2],
    id: usize,
}

trait PrepFilter {
    fn filter_sr(sr: &SearchResult, _gets: &Getters) -> bool {
        (sr.full_name.trim().len() > 0)
            & (sr.semantic_id.trim().len() > 0)
            & (sr.papers > 1)
            & (sr.citations > 2)
            & (sr.citations <= UPPER_LIMIT)
    }
}

macro_rules! i_fil {
    ($($t:ty),*) => {
        $(impl PrepFilter for $t {})*
    };
}

i_fil!(Countries, Subfields, Institutions);

impl PrepFilter for Authors {
    fn filter_sr(sr: &SearchResult, _gets: &Getters) -> bool {
        use serde_json::to_string_pretty;
        let out = (sr.full_name.trim().len() > 0)
            & (sr.semantic_id.trim().len() > 0)
            & (sr.papers > 1)
            & (sr.citations > 2)
            & (sr.papers < 1000);
        if [2261109, 2066590, 3253229].contains(&sr.dm_id) {
            println!(
                "\n\nFOUND ONE {out}\n{}\n\n\n",
                to_string_pretty(sr).unwrap()
            );
        }
        out
    }
}

impl PrepFilter for Sources {
    fn filter_sr(sr: &SearchResult, gets: &Getters) -> bool {
        let id = NET::<Sources>::from_usize(sr.dm_id);
        let mut best_q = 5;
        for ty8 in YearInterface::iter() {
            let q = gets.sqy(&(id, ty8)).lift();
            if q != 0 {
                best_q = min(best_q, q);
            }
        }
        (sr.full_name.trim().len() > 0)
            & (sr.semantic_id.trim().len() > 0)
            & (sr.papers > 10)
            & (sr.citations > 20)
            & (sr.citations <= UPPER_LIMIT)
            & (best_q <= 2)
    }
}

impl KdPoint for KDItem {
    type Scalar = f64;
    type Dim = typenum::U2;
    fn at(&self, k: usize) -> f64 {
        self.point[k]
    }
}

impl SearchResult {
    fn new<E>(
        i: usize,
        name: String,
        ext: String,
        semantic_id: String,
        entif: &RootInterfaces<E>,
    ) -> Self
    where
        E: RootInterfaceable,
    {
        Self {
            full_name: format!("{name} {ext}").trim().to_string(),
            name,
            semantic_id,
            papers: entif.wcounts[i].to_usize() as u32,
            citations: entif.ccounts[i].to_usize() as u32,
            dm_id: i,
        }
    }
}

impl ResultExtension {
    fn from_resps<E>(
        responses: &Box<[SearchResult]>,
        entif: &RootInterfaces<E>,
        gets: &Getters,
    ) -> Box<[Self]>
    where
        E: RootInterfaceable,
    {
        let iif = RootInterfaces::<Institutions>::new(&gets.stowage);
        let mut out = Vec::new();
        for res in responses.iter() {
            let i = res.dm_id;
            let inst_rels = entif.inst_rels[i]
                .iter()
                .take_while(|e| e.papers > 0)
                .map(|e| InstRelOut::from(e, &iif, gets))
                .collect();

            let mut sy_ind = 0;
            for (yi, ycount) in entif.yearly_papers[i].iter().enumerate() {
                if (sy_ind == 0) & (*ycount > 0) {
                    sy_ind = yi;
                    break;
                }
            }
            // let get_rem = |arr: &Box<[EraRec]>| arr[i].iter().skip(sy_ind).map(|e| *e).collect();
            // let yearly_cites = get_rem(&entif.yearly_cites);
            // let yearly_papers = get_rem(&entif.yearly_papers);

            out.push(Self {
                inst_rels,
                sf_coords: (entif.ref_sfc[i], entif.cit_sfc[i]),
                start_year: YearInterface::reverse(sy_ind as ET<Years>),
                yearly_cites: entif.yearly_cites[i].clone(),
                yearly_papers: entif.yearly_papers[i].clone(),
            })
        }

        out.into()
    }
}

impl NameState {
    fn new<E>(entif: &RootInterfaces<E>, gets: &Getters) -> Self
    where
        E: RootInterfaceable + PrepFilter,
    {
        let responses = Self::get_resps(entif, gets);
        let exts = ResultExtension::from_resps(&responses, entif, gets);
        let engine = SearchEngine::new(responses.iter().map(|e| e.full_name.clone()));
        let mut sem_map = HashMap::new();
        let mut kdt_base = Vec::new();
        let mut sf_kdt_base = Vec::new();
        let (mut means, mut vars) = ([0.0, 0.0], [0.0, 0.0]);
        let float_n = f64::from(responses.len() as u32);
        let n_max_comps: usize = min(max(20, responses.len() / 20), 1000);
        for (i, res) in responses.iter().enumerate() {
            let kd_rec = get_arr_base(res);
            for j in 0..kd_rec.len() {
                means[j] += kd_rec[j] / float_n;
            }
            kdt_base.push(kd_rec);
            if i < n_max_comps {
                sf_kdt_base.push([exts[i].sf_coords.0, exts[i].sf_coords.1]);
            }
            sem_map.insert(
                res.semantic_id.clone(),
                SemVal {
                    result_id: i,
                    dm_id: responses[i].dm_id,
                },
            );
        }

        for rec in kdt_base.iter_mut() {
            for i in 0..rec.len() {
                rec[i] -= means[i];
                vars[i] += rec[i].powi(2) / float_n;
            }
        }

        for rec in kdt_base.iter_mut() {
            for i in 0..rec.len() {
                rec[i] /= vars[i].sqrt();
            }
        }

        let query_tree = tree_from_iter(kdt_base);
        let sf_tree = tree_from_iter(sf_kdt_base);

        let mut sf_touchstones = Vec::new();
        const ENDS: [f64; 2] = [-100.0, 100.0];
        for x in ENDS {
            for y in ENDS {
                let query = [x, y];
                let mut poss_tss: Vec<usize> = sf_tree
                    .nearests(&query, 2)
                    .iter()
                    .map(|e| e.item.id.clone())
                    .collect();
                if coord_dist(&exts[poss_tss[0]], &exts[poss_tss[1]]) < 0.2 {
                    poss_tss.pop();
                }
                sf_touchstones.extend(poss_tss.iter().map(|e| responses[*e].clone()));
            }
        }

        Self {
            engine: engine.into(),
            responses,
            exts,
            semantic_id_map: sem_map.into(),
            query_tree,
            means: means.into(),
            vars: vars.into(),
            sf_touchstones: sf_touchstones.into(),
        }
    }

    fn get_resps<E>(entif: &RootInterfaces<E>, gets: &Getters) -> Box<[SearchResult]>
    where
        E: RootInterfaceable + PrepFilter,
    {
        let mut responses: Vec<SearchResult> = entif
            .names
            .0
            .iter()
            .zip(entif.name_exts.0.iter())
            .zip(entif.sem_ids.0.iter())
            .enumerate()
            .map(|(i, ((name, ext), semantic_id))| {
                SearchResult::new(
                    i,
                    name.to_string(),
                    ext.to_string(),
                    semantic_id.to_string(),
                    entif,
                )
            })
            .filter(|sr| E::filter_sr(sr, gets))
            .collect();
        responses.sort_by_key(|e| u32::MAX - e.citations);
        responses.into()
    }
}

fn coord_dist(l: &ResultExtension, r: &ResultExtension) -> f64 {
    (l.sf_coords.0 - r.sf_coords.0).powf(2.0) + (l.sf_coords.1 - r.sf_coords.1).powf(2.0)
}

impl EntityDescription {
    fn new<E: Entity>(count: usize) -> Self {
        Self {
            name: <E as Entity>::NAME.to_string(),
            count,
        }
    }
}

impl InstRelOut {
    fn from(v: &InstRelation, iif: &RootInterfaces<Institutions>, gets: &Getters) -> Self {
        let iid = v.inst.to_usize();
        let inst_name = iif.names.0.get(iid).unwrap().clone();
        let mut inst_sem_id = iif.sem_ids.0.get(iid).unwrap().clone();

        let i_sr = SearchResult::new(
            iid,
            inst_name.to_string(),
            "".to_string(),
            inst_sem_id.to_string(),
            iif,
        );
        if !Institutions::filter_sr(&i_sr, gets) {
            inst_sem_id = "".to_string();
        }

        Self {
            start: YearInterface::reverse(v.start),
            end: YearInterface::reverse(v.end),
            inst_name,
            inst_sem_id,
            citations: v.citations,
            papers: v.papers,
        }
    }
}

macro_rules! multi_route {
    ($s: ident, $($T: ty),*) => {
        {
            let gets = Arc::new(Getters::new(Arc::new($s)));
            let static_att_union: Arc<Mutex<AttributeLabelUnion>> = Arc::new(Mutex::new(HashMap::new()));
            let mut ei_ns_map = HashMap::new();
            let cv_pair = Arc::new((Mutex::new(None), Condvar::new()));
            $(
                add_thread::<$T>(&gets, &static_att_union, &cv_pair, &mut ei_ns_map);
            )*

            let ccount = gets.total_cite_count();
            {
                let (lock, cvar) = &*cv_pair;
                let mut data = lock.lock().unwrap();
                *data = Some(ccount);
                cvar.notify_all();
            }
            NodeInterfaces::<Topics>::new(&gets.stowage).update_stats(&mut static_att_union.lock().unwrap(), ccount);
            NodeInterfaces::<Qs>::new(&gets.stowage).update_stats(&mut static_att_union.lock().unwrap(), ccount);

            let mut tops = Vec::new();
            let mut v = Vec::new();
            let mut sem_maps = HashMap::new();
            let name_state_route = Router::new()
            $(.route(&format!("/names/{}", <$T as Entity>::NAME), get(name_get))
                .route(&format!("/slice/{}/:from/:to", <$T as Entity>::NAME), get(slice_get))
                .route(&format!("/views/{}/:semantic_id", <$T as Entity>::NAME), get(view_get))
                .with_state({
                    let nstate = ei_ns_map.remove(<$T>::NAME).unwrap().join().expect("NState thread panicked");
                    let entities = top_slice(&nstate.responses);
                    let name = <$T as Entity>::NAME.to_string();
                    let dmid_map = HashMap::from_iter(nstate.semantic_id_map.clone().into_iter().map(|(k, v)| (k, v.dm_id)));
                    sem_maps.insert(name.clone(), dmid_map);
                    tops.push(TopResult {name,  entities });
                    v.push(EntityDescription::new::<$T>(nstate.responses.len()));
                    nstate.into()
                })
            )*;
            for t in ei_ns_map.into_values() {
                t.join().unwrap();
            }


            let tm: Arc<InstTrm> = TreeRunManager::new(gets, static_att_union, sem_maps, N_THREADS);
            (name_state_route, tm, v, tops)
        }
    };
}

fn add_thread<E>(
    gets: &Arc<Getters>,
    atts: &Arc<Mutex<AttributeLabelUnion>>,
    cv_pair: &Arc<(Mutex<Option<f64>>, Condvar)>,
    ei_ns_map: &mut HashMap<&'static str, JoinHandle<NameState>>,
) where
    E: RootInterfaceable + PrepFilter,
{
    let gets_clone = Arc::clone(&gets);
    let au_clone = Arc::clone(&atts);
    let shared_cvp = Arc::clone(&cv_pair);
    let thread = std::thread::spawn(move || {
        let ent_intf = RootInterfaces::<E>::new(&gets_clone.stowage);
        let nstate = NameState::new::<E>(&ent_intf, &gets_clone);
        let (lock, cvar) = &*shared_cvp;
        let mut data = lock.lock().unwrap();
        while data.is_none() {
            data = cvar.wait(data).unwrap();
        }
        let ccount = *data.as_ref().unwrap();
        ent_intf.update_stats(&mut au_clone.lock().unwrap(), ccount);
        nstate
    });
    ei_ns_map.insert(<E>::NAME, thread);
}

#[tokio::main(worker_threads = 16)]
async fn main() {
    let path: String = std::env::args().last().unwrap();
    let now = std::time::Instant::now();
    println!("reading from path: {}", path);
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any);

    let compression = CompressionLayer::new()
        .gzip(true)
        .quality(CompressionLevel::Fastest);

    let stowage = Stowage::new(&path);
    let (response_api, tree_manager, entity_descriptions, tops) = multi_route!(
        stowage,
        Authors,
        Institutions,
        Sources,
        Subfields,
        Countries
    );

    let count_api = static_router(&entity_descriptions);
    let specs_api = static_router(&tree_manager.specs);

    let tops_api = Router::new()
        .route("/", get(tops_get))
        .with_state(Arc::new(tops));

    let tree_api = Router::new()
        .route("/:root_type/:semantic_id", get(tree_get))
        .with_state(tree_manager.clone());

    let api = Router::new()
        .nest("/", response_api)
        .nest("/trees", tree_api)
        .nest("/counts", count_api)
        .nest("/specs", specs_api)
        .nest("/tops", tops_api)
        .layer(ServiceBuilder::new().layer(cors).layer(compression));

    let app = Router::new().nest("/v1", api);

    let loc_addr = SocketAddr::from(([127, 0, 0, 1], 3038));
    println!("loaded and set-up in {}", now.elapsed().as_secs());
    println!("listening on local: {}", loc_addr);
    axum_server::bind(loc_addr)
        .serve(app.clone().into_make_service())
        .await
        .unwrap()
}

async fn slice_get(
    Path(ends): Path<(usize, usize)>,
    state: State<Arc<NameState>>,
) -> Response<Body> {
    const MAX_SLICE: usize = 1000;
    let start = min(ends.0, state.responses.len() - 1);
    let end = min(
        max(start + 1, min(start + MAX_SLICE, ends.1)),
        state.responses.len(),
    );
    Json(&state.responses[start..end]).into_response()
}

async fn state_get(str_state: State<Arc<str>>) -> (HeaderMap, Response<Body>) {
    (cache_header(60), str_state.to_string().into_response())
}

async fn tree_get(
    Path((root_type, semantic_id)): Path<(String, String)>,
    tree_q: Query<TreeQ>,
    state: State<Arc<InstTrm>>,
) -> (HeaderMap, Json<Option<TreeResponse>>) {
    let resp = Json(state.get_resp(tree_q.0, &root_type, &semantic_id));
    (cache_header(60), resp)
}

async fn tops_get(tops_state: State<Arc<Vec<TopResult>>>) -> Json<Vec<TopResult>> {
    let mut rng = rand::thread_rng();
    const TOP_N: usize = 5;
    let out = tops_state
        .iter()
        .map(|e| {
            let mut entities = Vec::new();
            for _ in 0..TOP_N {
                entities.push(e.entities[rng.gen_range(0..e.entities.len())].clone())
            }
            TopResult {
                name: e.name.clone(),
                entities,
            }
        })
        .collect();
    Json(out)
}

async fn view_get(
    Path(semantic_id): Path<String>,
    state: State<Arc<NameState>>,
) -> Json<Option<ViewResult>> {
    let iopt = state.semantic_id_map.get(&semantic_id);
    let out = match iopt {
        None => None,
        Some(sem_val) => {
            let i = sem_val.result_id;
            let srs = &state.responses[i];
            let ext = &state.exts[i];
            let query = get_query_arr(&srs, &state);
            let n_close = min(state.responses.len() / 20, 500);
            let mut closes = state.query_tree.nearests(&query, n_close);
            closes.shuffle(&mut rand::thread_rng());
            let similars = closes
                .iter()
                .take(8)
                .filter(|e| e.item.id != i)
                .map(|e| state.responses[e.item.id].clone())
                .collect();

            let comparisons = state.sf_touchstones.clone().to_vec();
            let vr = ViewResult {
                similars,
                comparisons,
                ext: ext.clone(),
                sr: srs.clone(),
            };
            Some(vr)
        }
    };
    Json(out)
}

async fn name_get(
    q: Query<BasicQ>,
    state: State<Arc<NameState>>,
) -> (HeaderMap, Json<Vec<SearchResult>>) {
    let q_string = q.q.clone().unwrap();
    let top_n_inds = state.engine.query(&q_string);
    let resp = Json(
        top_n_inds
            .into_iter()
            .filter(|e| (*e as usize) < state.responses.len())
            .map(|e| state.responses[e as usize].clone())
            .collect(),
    );
    (cache_header(60), resp)
}

fn cache_header(mins: usize) -> HeaderMap {
    let mut headers = HeaderMap::new();
    headers.insert(
        CACHE_CONTROL,
        HeaderValue::from_str(&format!("public, max-age={}", mins * 60)).unwrap(),
    );
    headers
}

fn get_arr_base(res: &SearchResult) -> [f64; 2] {
    [
        f64::from(max(res.citations, 1)).ln(),
        f64::from(res.citations) / f64::from(max(res.papers, 3)),
    ]
}

fn get_query_arr(res: &SearchResult, state: &State<Arc<NameState>>) -> [f64; 2] {
    let mut rec = get_arr_base(res);
    for i in 0..rec.len() {
        rec[i] -= state.means[i];
        rec[i] /= state.vars[i].sqrt();
    }
    rec
}

fn top_slice<T: Clone>(v: &Box<[T]>) -> Vec<T> {
    let ve = max(max(200, v.len() / 10), 10000);
    let end = min(ve, v.len());
    return v[..end].to_vec();
}

fn static_router<O: Serialize>(o: &O) -> Router {
    let arc: Arc<str> = Arc::from(serde_json::to_string(o).unwrap().as_str());
    Router::new().route("/", get(state_get)).with_state(arc)
}

fn tree_from_iter(v: Vec<[f64; 2]>) -> KdTree<KDItem> {
    KdTree::build_by_ordered_float(
        v.into_iter()
            .enumerate()
            .map(|(id, point)| KDItem { id, point })
            .collect(),
    )
}
