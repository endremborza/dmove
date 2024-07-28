use dmove::runner;
use std::io;

fn main() -> io::Result<()> {
    let mut args = std::env::args();
    args.next();
    if let (Some(comm), Some(root_str)) = (args.next(), args.next()) {
        let in_root_str = args.next();
        let n: Option<usize> = match args.next() {
            Some(sn) => Some(sn.parse::<usize>().unwrap()),
            None => None,
        };
        runner(&comm, &root_str, in_root_str, n)?
    }
    Ok(())
}
