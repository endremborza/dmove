use core::panic;
use std::{fmt::Display, str::FromStr, usize};

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse_macro_input, parse_str};

const TYPE_ARG_NAME: &str = "entity_name";
const IMPL_FIELD: &str = "impl_str";
const IMPORT_FIELD: &str = "importables";
const ME_STRUCT: &str = "MetaElem";
const SRECORD_PREFIX: &str = "rankless_rs::agg_tree";
const SRECORD_ENUM: &str = "SRecord";
const BASIS_TRAIT: &str = "FoldStackBase";
const FC_TRAIT: &str = "FoldingStackConsumer";

const MAX_DEPTH: usize = 7;

#[proc_macro]
pub fn def_me_struct(_: TokenStream) -> TokenStream {
    let struct_def = format!(
        "pub struct {ME_STRUCT} {{
    pub {IMPL_FIELD}: String,
    pub {IMPORT_FIELD}: Vec<String>,
}}"
    );
    TokenStream::from_str(&struct_def).unwrap()
}

#[proc_macro_attribute]
pub fn derive_meta_trait(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut out = item.clone();
    let input = parse_macro_input!(item as syn::Item);
    let mut arg_names = vec![TYPE_ARG_NAME.to_string()];
    let mut signature_elems = vec![sarg(TYPE_ARG_NAME)];
    let mut apush = |a: &str, t: &str, aa: bool| {
        let al = a.to_lowercase();
        signature_elems.push(format!("{al}: {t}"));
        if aa {
            arg_names.push(al)
        };
    };
    let mut trait_name_ext = "".to_string();
    let mut trait_name = "".to_string();
    let mut meta_fmt_ext = Vec::new();
    if let syn::Item::Trait(trait_def) = input {
        if let syn::Visibility::Public(_) = trait_def.vis {
            trait_name = trait_def.ident.to_string();
            let gn = trait_def.generics.params.len();
            if gn > 0 {
                trait_name_ext = format!(
                    "<{}>",
                    cjoin(trait_def.generics.params.iter().map(|param| {
                        let plow = param.to_token_stream().to_string().to_lowercase();
                        apush(&plow, "&str", false);
                        format!("{{{plow}}}")
                    }))
                );
            };
            for item in trait_def.items.iter() {
                match item {
                    syn::TraitItem::Const(c) => {
                        let var_name = c.ident.to_string();
                        let ty = c.ty.to_token_stream().to_string();
                        apush(&var_name, &ty, true);
                        meta_fmt_ext.push(format!("const {var_name}: {ty} = "));
                        let mext = if ty == "& str" { "\\\"{}\\\"" } else { "{}" };
                        meta_fmt_ext.push(format!("{mext};"));
                    }
                    syn::TraitItem::Type(t) => {
                        let type_name = t.ident.to_string();
                        meta_fmt_ext.push(format!("type {type_name} = {{}};"));
                        apush(&type_name, "&str", true);
                    }
                    syn::TraitItem::Fn(_) => {
                        //yay some defined function
                        ()
                    }
                    _ => panic!("wrong elem"),
                }
            }
        }
    }

    let mut meta_function_fmtstr = vec![
        "impl".to_string(),
        format!("{trait_name}{trait_name_ext}"),
        "for {}".to_string(),
        "{{".to_string(),
    ];

    meta_function_fmtstr.extend(meta_fmt_ext.into_iter());

    meta_function_fmtstr.push("}}".to_string());
    let meta_fun_signature = signature_elems.join(", ");
    let meta_fun_format_stmt = format!(
        "format!(\"{}\", {})",
        meta_function_fmtstr.join(" "),
        arg_names.join(",")
    );

    let format_line = format!("let {IMPL_FIELD} = {meta_fun_format_stmt}");
    let imps_line = format!("let {IMPORT_FIELD} = vec![\"{trait_name}\".to_string()]",);
    let return_line = format!("{ME_STRUCT} {{ {IMPL_FIELD}, {IMPORT_FIELD} }}");

    let meta_fun_body = vec![format_line, imps_line, return_line].join(";\n");

    let struct_name = format!("{trait_name}TraitMeta");
    let struct_def = format!("pub struct {struct_name} {{}}");
    let struct_impls = format!(
        "impl {} {{ pub fn meta({}) -> {} {{ {} }} }}",
        struct_name, meta_fun_signature, ME_STRUCT, meta_fun_body
    );

    let lines = vec![struct_def, struct_impls];
    let post = TokenStream::from_str(&lines.join("\n\n"));
    out.extend(post);
    out
}

#[proc_macro]
pub fn def_srecs(_: TokenStream) -> TokenStream {
    let mut out = Vec::new();
    for i in 2..(MAX_DEPTH + 1) {
        let its = prefed(1..i + 1, "T");
        let s_gens = prefed(1..i, "S");
        let it_tup: syn::Expr = syn::parse_str(&format!("({its})")).expect("Ts");
        let tgen: syn::AngleBracketedGenericArguments =
            syn::parse_str(&format!("<{its}>")).expect("T-gens");
        let enum_name = format!("{SRECORD_ENUM}{i}<{its}>");
        let enum_ident: syn::Type = syn::parse_str(&enum_name).expect(&enum_name);

        let rec_prefix = format!("Self::Rec");
        //note this is rev-ed so Rec3(T6, T5, T4) is possible
        let get_prec =
            |s: usize| format!("{rec_prefix}{}(({}))", i - s, prefed((s..i).rev(), "rec."));
        let frec_str = get_prec(0);
        let full_srec_from_rec: syn::Expr = syn::parse_str(&frec_str).expect("full record");

        let else_if_innards = join(
            (1..i).map(|e| {
                format!(
                    " }} else if rec.{e} != last_rec.{e} {{ Some({}) ",
                    get_prec(e)
                )
            }),
            "",
        );
        let empties = cjoin((1..i + 1).rev().map(|i| format!("T{i}::init_empty()")));
        let empty_srec: syn::Expr =
            syn::parse_str(&format!("{rec_prefix}{i}(({empties}))")).unwrap();

        let t_init_empties = cjoin((1..i + 1).map(|e| format!("T{e}: InitEmpty")));
        let t_comps = cjoin((1..i + 1).map(|e| format!("T{e}: PartialEq")));
        let t_to_ss = cjoin((1..i).map(|e| format!("T{e}: Into<S{e}>")));
        let reinstate_wheres = cjoin((1..i).map(|e| format!("S{e}: ReinstateFrom<T{e}>")));
        let intos_txt = join((2..i).rev().map(|e| format!("rec.{e}.into(),")), "");
        let match_internals = cjoin(
            (1..i + 1)
                .map(|e| format!("Self::Rec{e}(rec) => {{\n{}\n    }}", spec_match(e, i - 2))),
        );
        let consume_where = format!("S{}: {FC_TRAIT}<Consumable=T{i}>", i - 1);
        let update_wheres = cjoin((0..i - 1).map(|e| format!("S{e}: Updater<S{}>", e + 1)));

        let t_cmp_where: syn::WhereClause = syn::parse_str(&format!("where {t_comps}")).unwrap();
        let t_ie_where: syn::WhereClause =
            syn::parse_str(&format!("where {t_init_empties}")).unwrap();

        let fold_fn_txt = format!(
            "pub fn fold<{s_gens}, S0, I>(mut it: I, root: &mut S0) where 
                I: Iterator<Item=Self>, 
                {t_to_ss}, 
                {t_init_empties}, 
                {reinstate_wheres}, 
                {consume_where},
                {update_wheres} 
                {{
                    let mut stack = it.next().unwrap().to_stack();
                    let flusher = vec![Self::init_empty()].into_iter();
                    for rec in it.chain(flusher) {{
                        rec.update_stack(&mut stack, root);
                    }}
                }}"
        );

        let to_stack_fn_txt = format!(
            "pub fn to_stack<{s_gens}>(self) -> ({s_gens}) where 
                {t_to_ss},
                {consume_where}
            {{
                if let Self::Rec{i}(rec) = self {{
                    let mut leaf = rec.1.into();
                    leaf.consume(rec.0);
                    ({intos_txt} leaf)
                }} else {{
                    panic!(\"wrong starter\")
                }}
            }}"
        );

        let update_stack_fn_txt = format!(
            "pub fn update_stack<{s_gens}, S0>(self, stack: &mut ({s_gens}), root: &mut S0) where 
                {reinstate_wheres}, 
                {consume_where}, 
                {update_wheres} 
            {{
                match self {{
                    {match_internals}
                }}
            }}"
        );

        let fold_fn: syn::Stmt = syn::parse_str(&fold_fn_txt).expect(&fold_fn_txt);
        let to_stack_fn: syn::Stmt = syn::parse_str(&to_stack_fn_txt).expect(&to_stack_fn_txt);
        let update_stack_fn: syn::Stmt =
            syn::parse_str(&update_stack_fn_txt).expect(&update_stack_fn_txt);

        let shift_innards: syn::Expr = syn::parse_str(&format!(
            "
    if rec.0 != last_rec.0 {{
        Some({frec_str})
    {else_if_innards}
    }} else {{
        None
    }}",
        ))
        .expect("shift innards");

        let enum_impls = quote! {

            impl #tgen SortedRecord for #enum_ident #t_cmp_where {
                type FlatRecord = #it_tup;
                fn from_cmp(last_rec: &Self::FlatRecord, rec: Self::FlatRecord) -> Option<Self> {
                    #shift_innards
                }
            }

            impl #tgen From<#it_tup> for #enum_ident {
                fn from(rec: #it_tup) -> Self {
                    #full_srec_from_rec
                }
            }


            impl #tgen InitEmpty for #enum_ident #t_ie_where  {
                fn init_empty() -> Self {
                    #empty_srec
                }
            }

            impl #tgen #enum_ident {
                #fold_fn

                #to_stack_fn

                #update_stack_fn

            }
        };

        let enum_def = format!(
            "#[derive(Debug)]
            pub enum {enum_name} {{
                Rec1(T{i}),
                {}
            }}",
            join(
                (2..i + 1)
                    .map(|e| format!("Rec{e}(({})),", prefed(((i - e + 1)..i + 1).rev(), "T"))),
                "\n    "
            )
        );

        out.push(enum_def);
        out.push(enum_impls.to_token_stream().to_string());
    }
    // println!("{}", out.join("\n\n"));
    out.join("\n\n").parse().unwrap()
}

#[proc_macro_attribute]
pub fn derive_tree_getter(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as syn::Item);
    let mut tree_types: Vec<String> = Vec::new();
    // let derive_w_args: syn::Meta = parse_str(&format!("{MAKER}({attr})")).unwrap();
    let mod_name = match input {
        syn::Item::Mod(ref mut imp_syn) => {
            for item in imp_syn.content.as_mut().unwrap().1.iter_mut() {
                match item {
                    syn::Item::Type(item_type) => {
                        tree_types.push(item_type.ident.clone().into_token_stream().to_string());
                    }
                    _ => (),
                }
            }
            // imp_syn.attrs;
            imp_syn.ident.clone()
        }
        _ => panic!("not an impl block"),
    };

    let spec_pushes = join(
        tree_types
            .iter()
            .map(|e| format!("specs.push({mod_name}::{e}::get_spec());")),
        "\n",
    );

    let if_inners = join(
        tree_types.iter().enumerate().map(|(i, tn)| {
            format!(
                "if tid == {i} {{
            return Some({mod_name}::{tn}::tree_resp(q, gets, stats));
        }}"
            )
        }),
        "\n",
    );

    let added_impl = format!(
        "impl TreeGetter for {attr} {{
    fn get_tree(
        gets: &Getters,
        stats: &AttributeLabelUnion,
        q: TreeQ,
    ) -> Option<TreeResponse> {{
        let tid = q.tid.unwrap_or(0);
        {if_inners}
        None
    }}

    fn get_specs() -> Vec<TreeSpec> {{
        let mut specs = Vec::new();
        {spec_pushes}
        specs
    }}

}}"
    );
    let added_syn: syn::Item = parse_str(&added_impl).unwrap();
    let out = quote! {
        #input
        #added_syn
    };
    // println!("\n\n\nout: \"{out}\"");
    out.into()
}

#[proc_macro]
pub fn impl_stack_basees(ts: TokenStream) -> TokenStream {
    let n: usize = ts.to_string().parse().unwrap();
    let mut imps = Vec::new();
    for i in 2..(n + 1) {
        imps.push(derive_stack_basis(i).to_string());
    }
    let out = imps.join("\n\n\n");
    // println!("{}", out);
    TokenStream::from_str(&out).unwrap()
}

fn derive_stack_basis(n: usize) -> TokenStream {
    let mut in_types = (1..(n + 1)).map(get_gen_basis).collect::<Vec<String>>();
    let all_gens = format!("<{}>", cjoin((1..(n + 1)).map(get_gen_set)));
    let all_gens_syn: syn::Generics = parse_str(&all_gens).unwrap();

    let bd_specs = cjoin(
        in_types
            .iter()
            .map(|e| format!("to_bds::<{e}, FoldingStackLeaf>()",)),
    );

    in_types.push("FoldingStackLeaf".to_string());
    let rev_stack_types = get_stack_type_elems(&in_types);

    let net_wheres = cjoin((1..(n + 1)).map(|e| format!("E{e}: NumberedEntity")));
    let mut fsb_wheres = Vec::new();
    let mut fold_wheres = Vec::new();
    for (i, st) in rev_stack_types.iter().enumerate().take(n) {
        fsb_wheres.push(format!("{}: {BASIS_TRAIT}<{st}>", in_types[n - (i + 1)]));
    }
    for (i, st) in rev_stack_types.iter().enumerate().skip(1) {
        let ei = n - i + 1;
        let child = &rev_stack_types[i - 1];
        fold_wheres.push(format!(
            "{st}: From<NET<E{ei}>> + ReinstateFrom<NET<E{ei}>> + Updater<{child}>"
        ))
    }

    let all_wheres: syn::WhereClause = parse_str(&format!(
        "where {net_wheres}, {}, {}",
        cjoin(fsb_wheres.into_iter()),
        cjoin(fold_wheres.into_iter())
    ))
    .unwrap();

    let bds_fn_txt = format!(
        "fn get_bds() -> Vec<BreakdownSpec> {{
            vec![{bd_specs}]
        }}"
    );

    let bds_fn: syn::ImplItem = parse_str(&bds_fn_txt).expect("spec fn");
    let entity_types = cjoin(
        (1..(n + 1))
            .map(|e| format!("NET<E{}>", e))
            .chain(vec!["WT".to_string(), "WT".to_string()].into_iter()),
    );
    let srn = n + 2;
    let sr_type: syn::ImplItem = parse_str(&format!(
        "type SortedRec = {SRECORD_PREFIX}::{SRECORD_ENUM}{srn}<{entity_types}>;"
    ))
    .unwrap();

    let top_tree: syn::ImplItem = parse_str(&format!(
        "type TopTree = {};",
        rev_stack_types.last().unwrap()
    ))
    .unwrap();

    let fold_into = quote! {
        fn fold_into<R, I>(root: &mut R, iter: I)
        where
            I: Iterator<Item = Self::SortedRec>,
            R: Updater<Self::TopTree>
        {
            Self::SortedRec::fold(iter, root);
        }
    };

    let stack_type: syn::ImplItem = parse_str(&format!(
        "type Stack = ({});",
        cjoin(rev_stack_types.into_iter().rev())
    ))
    .unwrap();

    let ty_name: syn::Type =
        parse_str(&format!("({})", cjoin(in_types.into_iter().take(n)))).unwrap();
    quote! {
        impl #all_gens_syn StackBasis for #ty_name #all_wheres
        {

            #sr_type
            #stack_type
            #bds_fn
            #top_tree
            #fold_into
        }
    }
    .into()
}

fn get_gen_set(i: usize) -> String {
    format!("E{i}, const N{i}: usize, const S{i}: bool")
}

fn get_gen_basis(i: usize) -> String {
    format!("IntX<E{i}, N{i}, S{i}>")
}

fn get_stack_type_elems(in_types: &Vec<String>) -> Vec<String> {
    let mut type_iter = in_types.iter().rev();
    let mut child = type_iter.next().expect("at least one type needed").clone();
    let mut stack_type_elems: Vec<String> = vec![child.clone()];
    for t in type_iter {
        let as_trait = format!("<{t} as {BASIS_TRAIT}<{child}>>");
        child = format!("{as_trait}::StackElement");
        stack_type_elems.push(child.clone());
    }
    stack_type_elems
}

fn spec_match(rec_size: usize, last_si: usize) -> String {
    //last_si is the leaf
    if last_si == 0 {
        if rec_size == 1 {
            return "stack.consume(rec)".to_string();
        }
        return "root.update(stack, rec.1);stack.consume(rec.0);".to_string();
    }
    if rec_size == 1 {
        return format!("stack.{last_si}.consume(rec)");
    }
    let mut lines = Vec::new();
    for j in 1..rec_size {
        let line = if j > last_si {
            format!("root.update(&mut stack.0, rec.{j})")
        } else {
            format!(
                "stack.{}.update(&mut stack.{}, rec.{j})",
                last_si - j,
                last_si - j + 1,
            )
        };
        lines.push(line);
    }
    lines.push(format!("stack.{last_si}.consume(rec.0)"));
    join(lines.iter().map(|e| format!("            {e}")), ";\n")
}

fn sarg(arg: &str) -> String {
    format!("{}: &str", arg)
}

fn prefed<I: Iterator<Item = E>, E: Display>(it: I, prefix: &str) -> String {
    cjoin(it.map(|e| format!("{}{}", prefix, e)))
}

fn join<I: Iterator<Item = String>>(it: I, jn: &str) -> String {
    it.collect::<Vec<String>>().join(jn)
}

fn cjoin<I: Iterator<Item = String>>(it: I) -> String {
    join(it, ", ")
}
