use rankless_rs::agg_tree::{HeapIterator, MinHeap, SRecord3};

#[test]
fn srecords() {
    let tups = vec![
        (0, 1, 2),
        (0, 1, 1),
        (0, 1, 2),
        (0, 1, 1),
        (1, 2, 3),
        (1, 2, 3),
        (0, 2, 3),
    ];

    // let heap = BinaryHeap::from(tups);

    let mut heap = MinHeap::new();
    for e in tups.into_iter() {
        heap.push(e)
    }

    type SR = SRecord3<u8, u8, u8>;
    let hip: Option<HeapIterator<SR>> = heap.into();

    let v: Vec<SR> = hip.unwrap().collect();
    println!("{:?}", v);
    assert_eq!(v.len(), 4);
    match v[0] {
        // SRecord3::Rec3(rec) => assert_eq!(rec, (3, 2, 1)),
        SRecord3::Rec3(rec) => assert_eq!(rec, (1, 1, 0)),
        _ => panic!("wrong"),
    }
}
