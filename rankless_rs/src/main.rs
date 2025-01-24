use std::env;

fn main() -> std::io::Result<()> {
    let mut args = std::env::args().skip(1);
    if let Some(comm) = args.next() {
        let root_str = args.next().unwrap_or_else(|| {
            env::var_os("OA_ROOT")
                .unwrap()
                .to_str()
                .unwrap()
                .to_string()
        });
        let in_root_str = args.next();
        rankless_rs::runner(&comm, &root_str, in_root_str)?;
    }
    Ok(())
}
