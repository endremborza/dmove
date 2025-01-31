use axum::{
    body::Body,
    extract::{Path, Query, State},
    http::{header::CACHE_CONTROL, HeaderMap, HeaderValue, Method},
    response::{IntoResponse, Response},
    routing::get,
    Json, Router,
};
use dmove::{Entity, UnsignedNumber};
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
    Stowage,
};
use rankless_trees::{
    interfacing::{Getters, NodeInterfaces, RootInterfaceable, RootInterfaces},
    io::{TreeQ, TreeResponse, TreeRunManager},
    AttributeLabelUnion,
};

const N_THREADS: usize = 16;

type SemanticIdMap = HashMap<String, usize>;
type InstTrm = TreeRunManager<(Institutions, Authors, Subfields, Countries, Sources)>;

#[derive(Deserialize)]
struct BasicQ {
    q: Option<String>,
}

#[derive(Serialize)]
struct ViewResult {
    name: String,
    citations: usize,
    #[serde(rename = "dmId")]
    dm_id: usize,
    papers: usize,
    similars: Vec<SearchResult>,
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
    means: Box<[f64; 2]>,
    vars: Box<[f64; 2]>,
    semantic_id_map: SemanticIdMap,
    query_tree: KdTree<KDItem>,
}

#[derive(Serialize, Clone)]
struct SearchResult {
    name: String,
    #[serde(rename = "semanticId")]
    semantic_id: String,
    #[serde(skip_serializing)]
    full_name: String,
    #[serde(skip_serializing)]
    dm_id: usize,
    papers: u32,
    citations: u32,
}

struct KDItem {
    point: [f64; 2],
    id: usize,
}

trait PrepFilter {
    fn filter_sr(sr: &SearchResult) -> bool {
        (sr.full_name.trim().len() > 0) & (sr.papers > 1) & (sr.citations > 2)
    }
}

macro_rules! i_fil {
    ($($t:ty),*) => {
        $(impl PrepFilter for $t {})*
    };
}

i_fil!(Countries, Subfields, Institutions, Sources);

impl PrepFilter for Authors {
    fn filter_sr(sr: &SearchResult) -> bool {
        (sr.full_name.len() > 0) & (sr.papers > 1) & (sr.citations > 2) & (sr.papers < 1000)
    }
}

impl KdPoint for KDItem {
    type Scalar = f64;
    type Dim = typenum::U2;
    fn at(&self, k: usize) -> f64 {
        self.point[k]
    }
}

impl NameState {
    fn new<E>(entif: &RootInterfaces<E>) -> Self
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
            .map(|(i, ((name, ext), semantic_id))| SearchResult {
                full_name: format!("{} {}", name, ext).trim().to_string(),
                name: name.to_string(),
                semantic_id: semantic_id.to_string(),
                papers: entif.wcounts[i].to_usize() as u32,
                citations: entif.ccounts[i].to_usize() as u32,
                dm_id: i,
            })
            .filter(|sr| E::filter_sr(sr))
            .collect();
        responses.sort_by_key(|e| u32::MAX - e.citations);
        let engine = SearchEngine::new(responses.iter().map(|e| e.full_name.clone()));
        let mut sem_map = HashMap::new();
        let mut kdt_base = Vec::new();
        let mut means = [0.0, 0.0];
        let mut vars = [0.0, 0.0];
        let float_n = f64::from(responses.len() as u32);
        for (i, res) in responses.iter().enumerate() {
            let kd_rec = get_arr_base(res);
            for i in 0..kd_rec.len() {
                means[i] += kd_rec[i] / float_n;
            }
            kdt_base.push(kd_rec);
            sem_map.insert(res.semantic_id.clone(), i);
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

        let query_tree = KdTree::build_by_ordered_float(
            kdt_base
                .into_iter()
                .enumerate()
                .map(|(id, point)| KDItem { id, point })
                .collect(),
        );

        Self {
            engine: engine.into(),
            responses: responses.into(),
            semantic_id_map: sem_map.into(),
            query_tree: query_tree.into(),
            means: means.into(),
            vars: vars.into(),
        }
    }
}

impl EntityDescription {
    fn new<E: Entity>() -> Self {
        Self {
            name: <E as Entity>::NAME.to_string(),
            count: <E as Entity>::N,
        }
    }
}

macro_rules! multi_route {
    ($s: ident, $($T: ty),*) => {
        {
            let static_att_union: Arc<Mutex<AttributeLabelUnion>> = Arc::new(Mutex::new(HashMap::new()));
            let mut ei_ns_map = HashMap::new();
            let cv_pair = Arc::new((Mutex::new(None), Condvar::new()));
            $(
                add_thread::<$T>(&$s, &static_att_union, &cv_pair, &mut ei_ns_map);
            )*

            let gets = Getters::new($s.clone());
            let ccount = gets.total_cite_count();
            {
                let (lock, cvar) = &*cv_pair;
                let mut data = lock.lock().unwrap();
                *data = Some(ccount);
                cvar.notify_all();
            }
            NodeInterfaces::<Topics>::new(&$s).update_stats(&mut static_att_union.lock().unwrap(), ccount);
            NodeInterfaces::<Qs>::new(&$s).update_stats(&mut static_att_union.lock().unwrap(), ccount);

            let mut tops = Vec::new();
            let name_state_route = Router::new()
            $(.route(&format!("/names/{}", <$T as Entity>::NAME), get(name_get))
                .route(&format!("/slice/{}/:from/:to", <$T as Entity>::NAME), get(slice_get))
                .route(&format!("/views/{}/:semantic_id", <$T as Entity>::NAME), get(view_get))
                .with_state({
                    let nstate = ei_ns_map.remove(<$T>::NAME).unwrap().join().expect("NState thread panicked");
                    let entities = top_slice(&nstate.responses);
                    tops.push(
                        TopResult {name: <$T as Entity>::NAME.to_string(), entities }
                    );
                    nstate.into()
                })
            )*;

            let tm: Arc<InstTrm> = TreeRunManager::new(gets, static_att_union, N_THREADS);
            let mut v = Vec::new();
            $(v.push(EntityDescription::new::<$T>());)*

            (name_state_route, tm, v, tops)
        }
    };
}

fn add_thread<E>(
    stowgae: &Arc<Stowage>,
    atts: &Arc<Mutex<AttributeLabelUnion>>,
    cv_pair: &Arc<(Mutex<Option<f64>>, Condvar)>,
    ei_ns_map: &mut HashMap<&'static str, JoinHandle<NameState>>,
) where
    E: RootInterfaceable + PrepFilter,
{
    let stowage_clone = Arc::clone(&stowgae);
    let au_clone = Arc::clone(&atts);
    let shared_cvp = Arc::clone(&cv_pair);
    let thread = std::thread::spawn(move || {
        let ent_intf = RootInterfaces::<E>::new(&stowage_clone);
        let nstate = NameState::new::<E>(&ent_intf);
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

    let astow = Arc::new(Stowage::new(&path));
    let (response_api, tree_manager, entity_descriptions, tops) =
        multi_route!(astow, Authors, Institutions, Sources, Subfields, Countries);

    let count_api = static_router(&entity_descriptions);
    let specs_api = static_router(&tree_manager.specs);

    let tops_api = Router::new()
        .route("/", get(tops_get))
        .with_state(Arc::new(tops));

    let tree_api = Router::new()
        .route("/:root_type", get(tree_get))
        .with_state(tree_manager);

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
    Path(root_type): Path<String>,
    tree_q: Query<TreeQ>,
    state: State<Arc<InstTrm>>,
) -> (HeaderMap, Json<Option<TreeResponse>>) {
    let resp = Json(state.get_resp(tree_q.0, &root_type));
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
) -> Json<ViewResult> {
    let default = ViewResult {
        name: "-".to_string(),
        citations: 0,
        papers: 0,
        dm_id: 0,
        similars: Vec::new(),
    };
    let iopt = state.semantic_id_map.get(&semantic_id);
    let out = match iopt {
        None => default,
        Some(i) => {
            let srs = &state.responses[*i];
            let query = get_query_arr(&srs, &state);
            let n_close = min(state.responses.len() / 20, 500);
            let mut closes = state.query_tree.nearests(&query, n_close);
            closes.shuffle(&mut rand::thread_rng());
            let similars = closes
                .iter()
                .take(8)
                .filter(|e| e.item.id != *i)
                .map(|e| state.responses[e.item.id].clone())
                .collect();
            ViewResult {
                name: srs.name.clone(),
                dm_id: srs.dm_id.clone(),
                citations: srs.citations as usize,
                papers: srs.papers as usize,
                similars,
            }
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
