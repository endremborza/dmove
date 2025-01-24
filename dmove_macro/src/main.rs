use std::{fmt::Display, fs::create_dir_all, path::Path};

use clap::{Parser, Subcommand};

const CLI_PATH: &str = "./target/release/dmove-macro";
const LIB_MACRO: &str = "mods_as_comms";
const MOD_STEM: &str = "mod";
const STEPS_MODULE: &str = "steps";
const GEN_MODULE: &str = "gen";

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long)]
    package: Option<String>,

    #[command(subcommand)]
    command: Option<Commands>,
}

#[derive(Subcommand)]
enum Commands {
    PreBuild {
        #[arg(short, long)]
        step: String,
    },
    PostRun {
        #[arg(short, long)]
        step: String,
    },
    MakeSetup,
}

fn main() {
    use Commands::*;
    let cli = Args::parse();

    let pack = cli.package.unwrap_or(".".to_string());
    let pname = get_mname(&pack);
    let cargo_param = if &pack == "." {
        "".to_string()
    } else {
        format!("-p {}", pname)
    };
    let pack_path = std::path::Path::new(&pack);
    let src_path = pack_path.join("src");
    let steps_mod_dir = src_path.join(STEPS_MODULE);
    let gen_mod_dir = src_path.join(GEN_MODULE);
    let make_path = pack_path.join("Makefile");

    let mut steps: Vec<String> = steps_mod_dir
        .read_dir()
        .unwrap()
        .map(|e| {
            e.unwrap()
                .path()
                .file_stem()
                .to_owned()
                .unwrap()
                .to_str()
                .unwrap()
                .to_owned()
        })
        .filter(|e| e != MOD_STEM)
        .collect();
    steps.sort();

    match &cli.command.unwrap() {
        PreBuild { step } => build_ends(&steps, step, &src_path, true),
        PostRun { step } => build_ends(&steps, step, &src_path, false),
        MakeSetup => {
            create_dir_all(&gen_mod_dir).unwrap();
            let mut last_gen = "".to_string();
            let mut make_comms = Vec::new();
            for step in steps.iter() {
                let step_mod = rs_file_name(&steps_mod_dir, step);
                let gen_mod = rs_file_name(&gen_mod_dir, step);
                make_comms.push(format!(
                    "{gen_mod}: {step_mod} {last_gen}
{}
\tcargo build {cargo_param} --release
\tcargo run {cargo_param} --release -- {step}
{}
",
                    comm_line("pre-build", &pack, step),
                    comm_line("post-run", &pack, step)
                ));
                last_gen = gen_mod;
            }
            let make_content = make_comms.join("\n");
            std::fs::write(&make_path, make_content).unwrap();
        }
    }
}

fn build_ends(steps: &Vec<String>, step: &String, src_path: &Path, skip_last_gen: bool) {
    let mut upto_steps: Vec<&String> = steps.iter().take_while(|e| e != &step).collect();
    upto_steps.push(step);
    let lib_path = rs_file_name(src_path, "lib");
    let lib_string = std::fs::read_to_string(&lib_path).unwrap();
    let clean_inner = clean_of_macro(lib_string, LIB_MACRO, &upto_steps);
    pub_mods_to_file(&src_path.join(STEPS_MODULE), &upto_steps);
    if skip_last_gen {
        upto_steps.truncate(upto_steps.len() - 1)
    }
    pub_mods_to_file(&src_path.join(GEN_MODULE), &upto_steps);
    std::fs::write(&lib_path, clean_inner).unwrap();
}

fn rs_file_name(src_path: &Path, name: &str) -> String {
    src_path
        .join(format!("{name}.rs"))
        .to_str()
        .unwrap()
        .to_string()
}

fn pub_mods_to_file(mod_dir: &Path, steps: &Vec<&String>) {
    std::fs::write(
        &rs_file_name(mod_dir, MOD_STEM),
        get_pub_mod_lines(steps.iter()).join("\n"),
    )
    .unwrap();
}

fn comm_line(comm: &str, pack: &str, step: &str) -> String {
    format!("\t{CLI_PATH} -p {pack} {comm} -s {step}")
}

fn get_mname(pack_name: &str) -> String {
    let pack_path = std::path::Path::new(pack_name).join("Cargo.toml");
    for lib_line in std::fs::read_to_string(&pack_path).unwrap().split("\n") {
        if lib_line.starts_with("name = ") {
            return lib_line.split(" = ").last().unwrap().replace('"', "");
        }
    }
    panic!("no name found");
}

fn get_pub_mod_lines<I: Iterator<Item = E>, E: Display>(mods: I) -> Vec<String> {
    mods.map(|e| format!("pub mod {};", e)).collect()
}

fn clean_of_macro(mut in_str: String, macro_name: &str, new_params: &Vec<&String>) -> String {
    let macro_prefix = format!("{}!(", macro_name);
    let new_lib_macro_call = format!(
        "{macro_prefix}{});\n",
        new_params
            .iter()
            .map(|e| e.to_owned().clone())
            .collect::<Vec<String>>()
            .join(", ")
    );
    let mpl = macro_prefix.len();
    for i in 0..(in_str.len() - mpl) {
        if in_str[i..i + mpl] == macro_prefix {
            let mut e = i + 1;
            for (ci, c) in in_str[i..].char_indices() {
                if c == ';' {
                    e = i + ci + 1;
                    break;
                }
            }
            in_str = vec![in_str[..i].trim().to_string(), in_str[e..].to_string()]
                .join("")
                .trim()
                .to_string();
            break;
        }
    }
    vec![in_str.trim().to_string(), new_lib_macro_call].join("\n")
}
