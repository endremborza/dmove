use core::panic;
use std::{fmt::Display, str::FromStr};

use proc_macro::TokenStream;
use quote::{quote, ToTokens};
use syn::{parse::Parser, parse_macro_input, parse_str, ImplItem};

const TYPE_ARG_NAME: &str = "entity_name";
const IMPL_FIELD: &str = "impl_str";
const IMPORT_FIELD: &str = "importables";
const ME_STRUCT: &str = "MetaElem";
const SRECORD_PREFIX: &str = "crate::agg_tree";
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

        let s_gens = prefed(0..i + 1, "S");
        let t_init_empties = cjoin((1..i + 1).map(|e| format!("T{e}: InitEmpty")));
        let t_comps = cjoin((1..i + 1).map(|e| format!("T{e}: PartialEq")));
        let t_to_ss = cjoin((1..i + 1).map(|e| format!("T{e}: Into<S{e}>")));
        let t0_to_s0 = "T0: Into<S0>";
        let intos_txt = cjoin((0..i).rev().map(|e| format!("rec.{e}.into()")));
        let match_internals = cjoin(
            (1..i + 1).map(|e| format!("Self::Rec{e}(rec) => {{\n{}\n    }}", spec_match(e, i))),
        );
        let consume_wheres =
            cjoin((0..i).map(|e| format!("S{e}: {FC_TRAIT}<Consumable=S{}>", e + 1)));

        let t_cmp_where: syn::WhereClause = syn::parse_str(&format!("where {t_comps}")).unwrap();
        let t_ie_where: syn::WhereClause =
            syn::parse_str(&format!("where {t_init_empties}")).unwrap();

        let fold_fn_txt = format!(
            "pub fn fold<{s_gens}, T0, I>(mut it: I, root: T0) -> ({s_gens}) where I: Iterator<Item=Self>, {t0_to_s0}, {t_to_ss}, {t_init_empties}, {consume_wheres} {{
                    let first = it.next().unwrap();
                    let mut stack = first.to_stack(root);
                    let flusher = vec![Self::init_empty()].into_iter();
                    for rec in it.chain(flusher) {{
                        rec.update_stack(&mut stack);
                    }}
                    stack
                }}"
        );

        let to_stack_fn_txt = format!(
            "pub fn to_stack<{s_gens}, T0>(self, root: T0) -> ({s_gens}) where {t0_to_s0}, {t_to_ss} {{
                if let Self::Rec{i}(rec) = self {{
                    (root.into(), {intos_txt})
                }} else {{
                    panic!(\"wrong starter\")
                }}
            }}"
        );

        let update_stack_fn_txt = format!(
            "pub fn update_stack<{s_gens}>(self, stack: &mut ({s_gens})) where {t_to_ss}, {consume_wheres} {{
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
pub fn derive_tree_maker(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as syn::Item);
    match input {
        syn::Item::Impl(ref mut imp_syn) => {
            let add_items = tree_maker_impl_extensions(imp_syn, attr.to_string());
            imp_syn.items.extend(add_items.into_iter());
        }
        _ => panic!("not an impl block"),
    };

    let out = quote! {
        #input
    };
    // println!("\n\n\nout: \"{out}\"");
    out.into()
}

#[proc_macro_attribute]
pub fn derive_tree_getter(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut input = parse_macro_input!(item as syn::Item);
    const MAKER: &str = "derive_tree_maker";
    let mut tree_defs: Vec<String> = Vec::new();
    // let derive_w_args: syn::Meta = parse_str(&format!("{MAKER}({attr})")).unwrap();
    let derive_attr: Vec<syn::Attribute> = syn::Attribute::parse_outer
        .parse_str(&format!("#[{MAKER}({attr})]"))
        .unwrap();
    let mod_name = match input {
        syn::Item::Mod(ref mut imp_syn) => {
            for item in imp_syn.content.as_mut().unwrap().1.iter_mut() {
                match item {
                    syn::Item::Struct(struct_def) => {
                        if let syn::Visibility::Public(_) = struct_def.vis {
                            tree_defs.push(struct_def.ident.to_token_stream().to_string());
                        }
                    }
                    syn::Item::Impl(item_impl) => {
                        item_impl.attrs = derive_attr.clone();
                        // for mderive in item_impl.attrs.iter_mut() {
                        //     if let syn::Meta::Path(mval) = &mderive.meta {
                        //         if mval.segments.last().unwrap().ident.to_string() == MAKER {
                        //             mderive.meta = derive_w_args.clone();
                        //             break;
                        //         };
                        //     }
                        // }
                    }
                    _ => (),
                }
            }
            imp_syn.ident.clone()
        }
        _ => panic!("not an impl block"),
    };

    let spec_pushes = join(
        tree_defs
            .iter()
            .map(|e| format!("specs.push({mod_name}::{e}::get_spec());")),
        "\n",
    );

    let if_inners = join(
        tree_defs.iter().enumerate().map(|(i, tn)| {
            format!(
                "if tid == {i} {{
            return Some(tree_resp::<{mod_name}::{tn}>(q, gets, stats));
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

fn get_in_types_gen_args<'a, I>(
    elems: I,
    root_type: &String,
) -> Option<(Vec<syn::Type>, Vec<String>, Vec<String>)>
where
    I: Iterator<Item = &'a syn::Type>,
{
    //this knows way too much and does a _lot_ of magic
    //also presumes here:
    let mut gen_args: Vec<syn::Type> = vec![parse_str(root_type).unwrap()];
    let mut in_types: Vec<String> = elems
        .map(|e| {
            gen_args.push(get_generic_arg(&e).unwrap());
            e.to_token_stream().to_string()
        })
        .collect();
    in_types.insert(0, format!("IntX<{root_type}, 0, true>"));
    in_types.extend(vec!["WorkTree".to_string(), "WT".to_string()].into_iter());
    return Some((gen_args, get_stack_type_elems(&in_types), in_types));
}

fn tree_maker_impl_extensions(imp_syn: &syn::ItemImpl, root_entity_str: String) -> Vec<ImplItem> {
    const STACK_BASIS: &str = "StackBasis";
    let (flat_types, stack_types, basis_types) = imp_syn
        .items
        .iter()
        .find_map(|e| {
            if let ImplItem::Type(it) = e {
                if it.ident.to_string() == STACK_BASIS {
                    match &it.ty {
                        syn::Type::Tuple(t) => {
                            return get_in_types_gen_args(t.elems.iter(), &root_entity_str);
                        }
                        syn::Type::Paren(t) => {
                            return get_in_types_gen_args(
                                vec![t.elem.as_ref()].into_iter(),
                                &root_entity_str,
                            );
                        }
                        syn::Type::Path(t) => {
                            let syn_t: syn::Type =
                                parse_str(&t.path.segments.last().to_token_stream().to_string())
                                    .unwrap();
                            return get_in_types_gen_args(vec![syn_t].iter(), &root_entity_str);
                        }
                        _ => (),
                    }
                }
            }
            None
        })
        .unwrap();
    let n = stack_types.len() - 1;
    //WARN: WorkTree is discusting magic
    let bd_specs = cjoin(
        basis_types
            .iter()
            .skip(1)
            .take(n - 2)
            .map(|e| format!("to_bds::<{e}, WorkTree>()",)),
        // .map(|e| format!("<{e} as FoldStackBase<WorkTree>>::to_bd_spec()",)),
    );

    let spec_fn_txt = format!(
        "fn get_spec() -> TreeSpec {{
            let breakdowns = vec![{bd_specs}];
            let root_type = \"{root_entity_str}\".to_string();
            TreeSpec {{
                root_type,
                breakdowns,
            }}
        }}"
    );

    let spec_fn = parse_str(&spec_fn_txt).expect("spec fn");

    let fold_fn_txt = "fn get_root_tree<I>(id: ET<Self::Root>, it: I) -> Self::RootTree
    where
        I: Iterator<Item = Self::SortedRec>,
    {
        let stack: Self::Stack = Self::SortedRec::fold(it, id);
        stack.0.collapse()
    }";

    let fold_fn = parse_str(&fold_fn_txt).expect("fold fn");

    //TODO: ET is known and used here +WTs at the end of flat rec
    let mut ent_iters = flat_types.into_iter().map(|e| e.to_token_stream());

    let root_id = parse_str(&format!("type Root = {};", ent_iters.next().unwrap())).unwrap();
    let root_tree = parse_str(&format!(
        "type RootTree = CollT<{}>;",
        stack_types.iter().last().unwrap()
    ))
    .unwrap();

    // let tree_id_set_nested_type = get_id_set_nested_type(ent_iters.clone().rev());
    // let tis_type = parse_str(&format!("type NestedIds = ({tree_id_set_nested_type});")).unwrap();

    let rest_ets = cjoin(
        ent_iters
            .map(|e| format!("ET<{e}>").to_string())
            .chain(vec!["WT".to_string(), "WT".to_string()].into_iter()),
    );
    let sr_type = parse_str(&format!(
        "type SortedRec = {SRECORD_PREFIX}::{SRECORD_ENUM}{n}<{rest_ets}>;"
    ))
    .unwrap();
    let stack_type = parse_str(&format!(
        "type Stack = ({});",
        cjoin(stack_types.into_iter().rev())
    ))
    .unwrap();
    vec![sr_type, stack_type, root_id, root_tree, spec_fn, fold_fn]
}

fn get_stack_type_elems(in_types: &Vec<String>) -> Vec<String> {
    let mut type_iter = in_types.iter().rev();
    let mut child = type_iter.next().expect("at least one type needed").clone();
    let mut stack_type_elems: Vec<String> = vec![child.clone()];
    for t in type_iter {
        let as_trait = format!("<{t} as {BASIS_TRAIT}<{child}>>");
        child = format!("{}::StackElement", as_trait);
        stack_type_elems.push(child.clone());
    }
    stack_type_elems
}

fn get_id_set_nested_type<I, E>(mut et_it: I) -> String
where
    I: Iterator<Item = E>,
    E: Display + Clone,
{
    let first = et_it.next().expect("at least one type needed").clone();
    //TODO: IdSet name is known
    let mut child = format!("TreeIdSet<{first}, ()>");
    for t in et_it {
        child = format!("TreeIdSet<{t}, {child}>");
    }
    child
}

fn get_generic_arg(syn_type: &syn::Type) -> Option<syn::Type> {
    if let syn::Type::Path(syn::TypePath { path, .. }) = &syn_type {
        let last_segment = path.segments.last().unwrap();
        if let syn::PathArguments::AngleBracketed(args) = &last_segment.arguments {
            if let syn::GenericArgument::Type(arg_type) = &args.args.iter().next().unwrap() {
                return Some(arg_type.clone());
            }
        }
    }
    None
}

fn repler(stack_i: usize, rec_i: usize) -> String {
    format!("std::mem::replace(&mut stack.{stack_i}, rec.{rec_i}.into())")
}

fn spec_match(i: usize, last_si: usize) -> String {
    let mut lines = Vec::new();
    for j in 0..i {
        let able_expr = if i == 1 {
            format!("std::mem::replace(&mut stack.{last_si}, rec.into())").to_string()
        } else if j == 0 {
            repler(last_si, 0)
        } else {
            "consumer".to_string()
        };
        lines.push(format!("let consumable = {able_expr}"));

        let parent_stack_i = last_si - (j + 1);
        let c_expr = if j < (i - 1) {
            format!("let mut consumer = {}", repler(parent_stack_i, j + 1))
        } else {
            format!("let consumer = &mut stack.{parent_stack_i}")
        };
        lines.push(c_expr);
        lines.push("consumer.consume(consumable)".to_string());
    }
    join(lines.iter().map(|e| format!("            {}", e)), ";\n")
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
