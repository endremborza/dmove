fn main() -> std::io::Result<()> {
    let mut args = std::env::args();
    args.next();
    if let (Some(comm), Some(root_str)) = (args.next(), args.next()) {
        let in_root_str = args.next();
        rankless_rs::runner(&comm, &root_str, in_root_str)?;
    }
    Ok(())
}
