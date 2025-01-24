use std::{fs::File, path::Path};

use dmove::UniqueMap;

#[test]
fn un_map() {
    let p = Path::new("testmap");
    let mut map = UniqueMap::<u16, u8>::new(p);
    let items = vec![(20, 1), (30, 2), (20, 3), (40, 10), (0, 7)];
    let il = items.len() - 1; //one key is there twice
    let n = il * 3;
    for (k, v) in items.into_iter() {
        map.push((k, v));
    }
    map.extend();
    assert_eq!(map.get(&20), Some(1));
    assert_eq!(map.get(&30), Some(2));
    assert_eq!(map.get(&40), Some(10));
    let nf = File::open(p).unwrap().metadata().unwrap().len();
    assert_eq!(n as u64, nf);
    assert_eq!(map.to_map().len(), il);
    std::fs::remove_file(p).unwrap();
}
