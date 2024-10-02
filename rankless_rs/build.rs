use std::env;

fn main() {
    let rankless_env = env::var_os("RANKLESS_ENV").unwrap_or("full".into());
    if rankless_env == "nano" {
        println!("cargo::rustc-env=START_YEAR=1990");
        println!("cargo::rustc-env=MIN_PAPERS_FOR_INST=50");
        println!("cargo::rustc-env=MIN_PAPERS_FOR_SOURCE=30");
    } else {
        println!("cargo::rustc-env=START_YEAR=1950");
        println!("cargo::rustc-env=MIN_PAPERS_FOR_INST=700");
        println!("cargo::rustc-env=MIN_PAPERS_FOR_SOURCE=200");
    }
}
