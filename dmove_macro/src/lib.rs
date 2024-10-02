use std::str::FromStr;

use proc_macro::{TokenStream, TokenTree::Group};

const TYPE_ARG_NAME: &str = "entity_name";
const IMPL_FIELD: &str = "impl_str";
const IMPORT_FIELD: &str = "importables";
const ME_STRUCT: &str = "MetaElem";

#[proc_macro]
pub fn def_me_struct(_: TokenStream) -> TokenStream {
    TokenStream::from_str(&format!(
        "
pub struct {} {{
    pub {}: String,
    pub {}: Vec<String>,
}}
",
        ME_STRUCT, IMPL_FIELD, IMPORT_FIELD
    ))
    .unwrap()
}

fn sarg(arg: &str) -> String {
    format!("{}: &str", arg)
}

#[proc_macro_attribute]
pub fn derive_meta_trait(_attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut iclone = item.clone().into_iter();
    let _pub_trait = iclone.by_ref().take(2).last();
    let trait_name: String = iclone.next().expect("trait name").to_string();

    let mut arg_names = Vec::new();
    let mut signature_elems = vec![sarg(TYPE_ARG_NAME)];

    let mut fmt_trait_name = trait_name.clone();

    let mut in_iter = loop {
        let mut nt = iclone.next().unwrap();
        if "where" == nt.to_string() {
            nt = iclone.clone().last().unwrap();
        }
        if "<" == nt.to_string() {
            fmt_trait_name.extend("<".chars());
            let mut phs = vec![];
            loop {
                let snt = iclone.next().unwrap().to_string();
                if snt == ">" {
                    break;
                }
                if snt == "," {
                    continue;
                }
                phs.push("{}".to_string());
                let ma_arg_name = snt.to_lowercase();
                signature_elems.push(sarg(&ma_arg_name));
                arg_names.push(ma_arg_name);
            }
            fmt_trait_name.extend(phs.join(", ").chars());
            fmt_trait_name.extend(">".chars());
            continue;
        }
        if let Group(g) = nt {
            break g.stream().into_iter().map(|e| e.to_string());
        }
    };

    let mut meta_function_fmtstr = vec![
        "impl".to_string(),
        fmt_trait_name,
        "for {}".to_string(),
        "{{".to_string(),
    ];
    arg_names.push(TYPE_ARG_NAME.to_string());

    while let Some(inel) = in_iter.next() {
        meta_function_fmtstr.push(inel.clone());
        if inel == "type" {
            let type_name = in_iter.next().unwrap();
            meta_function_fmtstr.push(type_name.clone());
            let type_arg_name = type_name.to_lowercase();
            signature_elems.push(format!("{}: &str", type_arg_name));
            arg_names.push(type_arg_name);
            meta_function_fmtstr.push("= {};".to_string());
            while in_iter.next().unwrap() != ";" {}
        } else if inel == "const" {
            let var_name = in_iter.next().unwrap();
            let var_arg_name = var_name.to_lowercase();
            meta_function_fmtstr.push(var_name);
            arg_names.push(var_arg_name.clone());
            let mut const_def = vec![var_arg_name];
            loop {
                let mut next_elem = in_iter.next().unwrap();
                if next_elem == ";" {
                    if const_def.last().unwrap() == "str" {
                        meta_function_fmtstr.push("= \\\"{}\\\"".to_string());
                    } else {
                        meta_function_fmtstr.push("= {}".to_string());
                    }
                    meta_function_fmtstr.push(next_elem);
                    break;
                }
                if next_elem == "'" {
                    next_elem.extend(in_iter.next().unwrap().chars());
                    meta_function_fmtstr.push(next_elem);
                    continue;
                }
                meta_function_fmtstr.push(next_elem.clone());
                const_def.push(next_elem);
            }
            signature_elems.push(const_def.join(" "));
        } else {
            panic!("start with either const or type, not {}", inel)
        }
    }
    meta_function_fmtstr.push("}}".to_string());

    let meta_fun_signature = signature_elems.join(", ");
    let meta_fun_format_stmt = format!(
        "format!(\"{}\", {})",
        meta_function_fmtstr.join(" "),
        arg_names.join(",")
    );

    let format_line = format!("let {} = {}", IMPL_FIELD, meta_fun_format_stmt);
    let imps_line = format!(
        "let {} = vec![\"{}\".to_string()]",
        IMPORT_FIELD, trait_name
    );
    let return_line = format!("{} {{ {}, {} }}", ME_STRUCT, IMPL_FIELD, IMPORT_FIELD);

    let meta_fun_body = vec![format_line, imps_line, return_line].join(";\n");

    let struct_name = format!("{}TraitMeta", trait_name);
    let struct_def = format!("pub struct {} {{}}", struct_name);
    let struct_impls = format!(
        "impl {} {{ pub fn meta({}) -> {} {{ {} }} }}",
        struct_name, meta_fun_signature, ME_STRUCT, meta_fun_body
    );

    let lines = vec![struct_def, struct_impls];

    let post = TokenStream::from_str(&lines.join("\n\n"));

    let mut out = item.clone();
    out.extend(post);
    out
}
