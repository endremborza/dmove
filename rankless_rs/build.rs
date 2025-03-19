use std::{env, ops::AddAssign};

fn main() {
    // println!("cargo:rerun-if-changed=build.rs");
    // println!("cargo:rerun-if-changed=data.txt");
    let path = std::path::Path::new("src").join("env_consts.rs");
    // std::fs::write(&path, "pub const {}: {} = {}").unwrap();

    let rankless_env = env::var_os("RANKLESS_ENV").unwrap_or("full".into());
    let envs = vec!["nano", "micro", "mini"];

    let mut e_ind = 0;
    for e in envs.iter() {
        if *e == rankless_env {
            break;
        }
        e_ind.add_assign(1);
    }

    let year = 2025;
    let env_dependent_vars = vec![
        ("FINAL_YEAR", [year, year, year, year]),
        ("START_YEAR", [1990, 1990, 1980, 1950]),
        ("MIN_PAPERS_FOR_INST", [40, 20, 30, 350]),
        ("MIN_PAPERS_FOR_SOURCE", [10, 20, 50, 200]),
        ("MIN_AUTHOR_WORK_COUNT", [10, 10, 10, 8]),
        ("MIN_AUTHOR_CITE_COUNT", [500, 500, 500, 400]),
        // ("MIN_AUTHOR_H_INDEX", [3, 3, 3, 5]),
        // ("MIN_AUTHOR_I10_INDEX", [2, 2, 2, 3]),
    ];

    let mut env_lines = Vec::new();
    for e_var in env_dependent_vars.iter() {
        env_lines.push(format!("pub const {}: u16 = {};", e_var.0, e_var.1[e_ind]))
    }
    std::fs::write(&path, env_lines.join("\n")).unwrap();
}
