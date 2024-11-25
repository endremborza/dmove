use axum::{
    extract::{Path, Query, State},
    http::Method,
    routing::get,
    Json, Router,
};
use axum_server::tls_rustls::RustlsConfig;
use core::f64;
use dmove::{
    ByteArrayInterface, CompactEntity, Entity, MarkedAttribute, NamespacedEntity, UnsignedNumber,
    VariableSizeAttribute,
};
use hashbrown::HashMap;
use kd_tree::{KdPoint, KdTree};
use rand::{seq::SliceRandom, Rng};
use serde::{Deserialize, Serialize};
use std::{
    cmp::{max, min},
    net::SocketAddr,
    sync::Arc,
};
use tower::ServiceBuilder;
use tower_http::{
    compression::CompressionLayer,
    cors::{Any, CorsLayer},
    CompressionLevel,
};

use muwo_search::SearchEngine;
use rankless_rs::{
    gen_entity_mapping::{Authors, Countries, Institutions, Sources, Subfields},
    CiteCountMarker, NameExtensionMarker, NameMarker, QuickestBox, ReadIter, SemanticIdMarker,
    Stowage, WorkCountMarker,
};

#[derive(Deserialize)]
struct BasicQ {
    q: Option<String>,
}

#[derive(Serialize, Clone)]
struct ViewResult {
    name: String,
    citations: usize,
    papers: usize,
    similars: Vec<SearchResult>,
}

#[derive(Serialize, Clone)]
struct TopResult {
    name: String,
    entities: Vec<SearchResult>,
}

#[derive(Serialize, Clone)]
struct EntityDescription {
    name: String,
    count: usize,
}

#[derive(Clone)]
struct NameState {
    engine: Arc<SearchEngine>,
    responses: Arc<[SearchResult]>,
    means: Arc<[f64; 2]>,
    vars: Arc<[f64; 2]>,
    semantic_id_map: Arc<HashMap<String, usize>>,
    query_tree: Arc<KdTree<KDItem>>,
}

#[derive(Serialize, Clone)]
struct SearchResult {
    name: String,
    #[serde(rename = "semanticId")]
    semantic_id: String,
    #[serde(skip_serializing)]
    full_name: String,
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
    fn new<E>(stowage: &Stowage) -> Self
    where
        E: PrepFilter
            + MarkedAttribute<NameMarker>
            + MarkedAttribute<WorkCountMarker>
            + MarkedAttribute<CiteCountMarker>
            + MarkedAttribute<NameExtensionMarker>
            + MarkedAttribute<SemanticIdMarker>,
        <E as MarkedAttribute<NameMarker>>::AttributeEntity:
            NamespacedEntity + Entity<T = String> + VariableSizeAttribute + CompactEntity,

        <E as MarkedAttribute<SemanticIdMarker>>::AttributeEntity:
            NamespacedEntity + Entity<T = String> + VariableSizeAttribute + CompactEntity,

        <E as MarkedAttribute<NameExtensionMarker>>::AttributeEntity:
            NamespacedEntity + Entity<T = String> + VariableSizeAttribute + CompactEntity,

        <E as MarkedAttribute<WorkCountMarker>>::AttributeEntity: NamespacedEntity + CompactEntity,
        <<E as MarkedAttribute<WorkCountMarker>>::AttributeEntity as Entity>::T:
            ByteArrayInterface + UnsignedNumber,
        <E as MarkedAttribute<CiteCountMarker>>::AttributeEntity: NamespacedEntity + CompactEntity,
        <<E as MarkedAttribute<CiteCountMarker>>::AttributeEntity as Entity>::T:
            ByteArrayInterface + UnsignedNumber,
    {
        let anames = stowage.get_marked_interface::<E, NameMarker, ReadIter>();
        let name_exts = stowage.get_marked_interface::<E, NameExtensionMarker, ReadIter>();
        let sem_ids = stowage.get_marked_interface::<E, SemanticIdMarker, ReadIter>();
        let wcounts = stowage.get_marked_interface::<E, WorkCountMarker, QuickestBox>();
        let ccounts = stowage.get_marked_interface::<E, CiteCountMarker, QuickestBox>();

        let mut responses: Vec<SearchResult> = anames
            .zip(name_exts)
            .zip(sem_ids)
            .enumerate()
            .map(|(i, ((name, ext), semantic_id))| SearchResult {
                full_name: format!("{} {}", name, ext).trim().to_string(),
                name,
                semantic_id,
                papers: wcounts[i].to_usize() as u32,
                citations: ccounts[i].to_usize() as u32,
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

macro_rules! multi_route {
    ($s: ident, $($T: ty),*) => {
        {
            let mut v = Vec::new();
            let mut tops = Vec::new();
            $(v.push(
                EntityDescription {
                    name: <$T as Entity>::NAME.to_string(),
                    count:<$T as Entity>::N}
                );
            )*
            (Router::new()
            $(.route(&format!("/names/{}", <$T as Entity>::NAME), get(name_get))
                .route(&format!("/slice/{}/:from/:to", <$T as Entity>::NAME), get(slice_get))
                .route(&format!("/views/{}/:semantic_id", <$T as Entity>::NAME), get(view_get))
                .with_state({
                    let nstate = NameState::new::<$T>(&$s);
                    let entities = top_slice(&nstate.responses);
                    tops.push(
                        TopResult {name: <$T as Entity>::NAME.to_string(), entities }
                    );
                    nstate
                })
            )*,
            v,
            tops
            )
        }
    };
}

#[tokio::main]
async fn main() {
    let path: String = std::env::args().last().unwrap();
    println!("read from path: {}", path);
    let stowage = Stowage::new(&path);
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::POST])
        .allow_origin(Any);

    let compression = CompressionLayer::new()
        .gzip(true)
        .quality(CompressionLevel::Fastest);

    let (response_api, entity_descriptions, tops) = multi_route!(
        stowage,
        Authors,
        Institutions,
        Sources,
        Subfields,
        Countries
    );

    let count_api = Router::new()
        .route("/", get(count_get))
        .with_state(entity_descriptions);

    let tops_api = Router::new().route("/", get(tops_get)).with_state(tops);

    let api = Router::new()
        .nest("/", response_api)
        .nest("/counts", count_api)
        .nest("/tops", tops_api)
        .layer(ServiceBuilder::new().layer(cors).layer(compression));

    let app = Router::new().nest("/v1", api);

    // let listener = tokio::net::TcpListener::bind("0.0.0.0:3039").await.unwrap();
    // axum::serve(listener, app).await.unwrap();

    let config = RustlsConfig::from_pem_file(
        "ssl/alpha.rankless.org/fullchain1.pem",
        "ssl/alpha.rankless.org/privkey1.pem",
    )
    .await
    .unwrap();

    let loc_addr = SocketAddr::from(([127, 0, 0, 1], 3038));
    let rem_addr = SocketAddr::from(([0, 0, 0, 0], 3039));
    println!("listening on local: {}; remote: {}", loc_addr, rem_addr);
    axum_server::bind(loc_addr)
        .serve(app.clone().into_make_service())
        .await
        .unwrap();
    axum_server::bind_rustls(rem_addr, config)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn slice_get(
    Path(ends): Path<(usize, usize)>,
    state: State<NameState>,
) -> Json<Vec<SearchResult>> {
    const MAX_SLICE: usize = 1000;
    let start = min(ends.0, state.responses.len() - 1);
    let end = min(
        max(start + 1, min(start + MAX_SLICE, ends.1)),
        state.responses.len(),
    );
    let out = state.responses[start..end].to_vec();
    Json(out)
}

async fn count_get(count_state: State<Vec<EntityDescription>>) -> Json<Vec<EntityDescription>> {
    Json(count_state.to_vec())
}

async fn tops_get(tops_state: State<Vec<TopResult>>) -> Json<Vec<TopResult>> {
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

async fn view_get(Path(semantic_id): Path<String>, state: State<NameState>) -> Json<ViewResult> {
    let default = ViewResult {
        name: "-".to_string(),
        citations: 0,
        papers: 0,
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
                citations: srs.citations as usize,
                papers: srs.papers as usize,
                similars,
            }
        }
    };
    Json(out)
}

async fn name_get(q: Query<BasicQ>, state: State<NameState>) -> Json<Vec<SearchResult>> {
    let q_string = q.q.clone().unwrap();
    let top_n_inds = state.engine.query(&q_string);
    Json(
        top_n_inds
            .into_iter()
            .filter(|e| (*e as usize) < state.responses.len())
            .map(|e| state.responses[e as usize].clone())
            .collect(),
    )
}

fn get_arr_base(res: &SearchResult) -> [f64; 2] {
    [
        f64::from(max(res.citations, 1)).ln(),
        f64::from(res.citations) / f64::from(max(res.papers, 3)),
    ]
}

fn get_query_arr(res: &SearchResult, state: &State<NameState>) -> [f64; 2] {
    let mut rec = get_arr_base(res);
    for i in 0..rec.len() {
        rec[i] -= state.means[i];
        rec[i] /= state.vars[i].sqrt();
    }
    rec
}

fn top_slice<T: Clone>(v: &Arc<[T]>) -> Vec<T> {
    let ve = max(max(200, v.len() / 10), 10000);
    let end = min(ve, v.len());
    return v[..end].to_vec();
}
