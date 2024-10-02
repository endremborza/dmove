use dmove::{Entity, UnsignedNumber};
use rankless_rs::gen_entity_mapping::Authors;
// use futures_util::stream::iter;
use deunicode::deunicode;
use serde::Deserialize;
use std::sync::Arc;
use warp::Filter;

use rankless_rs::gen_init_links::AuthorsNames;
use rankless_rs::{QuickestBox, QuickestVBox, ReadIter, Stowage};

use simsearch::{SearchOptions, SimSearch};

#[derive(Deserialize)]
struct BasicQ {
    q: Option<String>,
}

#[derive(Deserialize)]
struct SearchResult {
    name: String,
    id: String,
    #[serde(rename = "semanticId")]
    semantic_id: String,
    #[serde(rename = "rootType")]
    root_type: String,
    papers: u32,
    citations: u32,
}

#[tokio::main]
async fn main() {
    let path: String = std::env::args().last().unwrap();

    // let main_ctx = StowServer::new(&main_root_str);
    let stowage_arc = Arc::new(Stowage::new(&path));

    type SimEngine = SimSearch<<Authors as Entity>::T>;
    let mut engine: SimEngine = SimSearch::new_with(
        SearchOptions::new()
            .threshold(0.5)
            .case_sensitive(false)
            .levenshtein(true),
    );
    let anames = stowage_arc.get_entity_interface::<AuthorsNames, ReadIter>();
    let mut aname_vec = Vec::new();
    for (i, aname) in anames.enumerate() {
        let parsed_str = deunicode(&aname.to_lowercase());
        if aname.len() > 0 {
            engine.insert(i.try_into().unwrap(), &parsed_str);
        }
        aname_vec.push(aname);
    }
    let aname_arc: Arc<[String]> = aname_vec.into();
    let engine_arc = Arc::new(engine);

    let context_filter = warp::any().map(move || stowage_arc.clone());
    let engine_filter = warp::any().map(move || engine_arc.clone());
    let name_slice_filter = warp::any().map(move || aname_arc.clone());

    // let quercus_route = warp::path!("v1" / "quercus" / String / u32)
    //     .and(context_filter.clone())
    //     .map(|spec_id: String, pc_id, ctx: Arc<FullContext>| {
    //         warp::sse::reply(
    //             warp::sse::keep_alive()
    //                 .stream(iter(QuercusResponseIterator::new(ctx, pc_id, &spec_id))),
    //         )
    //     })
    //     .with(warp::cors().allow_any_origin())
    //     .with(warp::filters::compression::gzip());
    //
    // let filter_route = warp::path!("v1" / "filtered" / String / u32 / String)
    //     .and(context_filter.clone())
    //     .map(
    //         |spec_id: String, pc_id, branch_string: String, ctx: Arc<FullContext>| {
    //             let filter_base = branch_string
    //                 .split('-')
    //                 .filter_map(|s| s.parse().ok())
    //                 .collect();
    //             ctx.serve_filtered_paths(&spec_id, pc_id, filter_base)
    //         },
    //     )
    //     .with(warp::cors().allow_any_origin())
    //     .with(warp::filters::compression::gzip());
    //
    // let specs_route = warp::path!("v1" / "specs")
    //     .and(context_filter.clone())
    //     .map(|ctx: Arc<FullContext>| ctx.serve_specs())
    //     .with(warp::cors().allow_any_origin());
    //
    // let root_selection_route = warp::path!("v1" / "root" / String)
    //     .and(context_filter.clone())
    //     .map(|s, ctx: Arc<FullContext>| ctx.serve_root_selection(&s))
    //     .with(warp::cors().allow_any_origin())
    //     .with(warp::filters::compression::gzip());
    //
    // let attribute_statics_route = warp::path!("v1" / "attribute" / String)
    //     .and(context_filter.clone())
    //     .map(|s, ctx: Arc<FullContext>| ctx.serve_attribute_statics(&s))
    //     .with(warp::cors().allow_any_origin())
    //     .with(warp::filters::compression::gzip());

    let names_route = warp::path!("v1" / "name" / String)
        .and(warp::query::<BasicQ>())
        // .and(context_filter.clone())
        .and(engine_filter.clone())
        .and(name_slice_filter.clone())
        .map(
            |_s, q: BasicQ, engine: Arc<SimEngine>, names: Arc<[String]>| {
                // let names = stowage.get_entity_interface::<AuthorsNames, QuickestVBox>();
                let mut out = Vec::new();
                if let Some(qstr) = q.q {
                    let parsed_qstr = deunicode(&qstr.to_lowercase());
                    let results = engine.search(&parsed_qstr);
                    for res in results.into_iter() {
                        out.push(&names[res.to_usize()]);
                    }
                }
                warp::reply::json(&out)
            },
        )
        .with(warp::filters::compression::gzip())
        .with(warp::cors().allow_any_origin());

    let init_route = warp::path!("v1" / "init")
        .map(|| "init-resp")
        .with(warp::cors().allow_any_origin());

    let test_route = warp::path!("v1" / "test")
        .map(|| "test-resp")
        .with(warp::filters::compression::gzip())
        .with(warp::cors().allow_any_origin());

    let routes = init_route
        //.or(quercus_route)
        // .or(filter_route)
        // .or(specs_route)
        // .or(root_selection_route)
        // .or(attribute_statics_route)
        .or(names_route)
        .or(test_route);

    warp::serve(routes)
        // .tls()
        // .cert_path("../cert.pem")
        // .key_path("../key.pem")
        // .run(([0, 0, 0, 0], 5557))
        .run(([0, 0, 0, 0], 3030))
        .await;
}
