use rankless_rs::agg_tree::{merge_sorted_vecs, HeapIterator, MinHeap, SRecord3};

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

#[derive(Debug, Clone)]
struct PartCmp(u8, u8);

impl PartialEq for PartCmp {
    fn eq(&self, other: &Self) -> bool {
        self.0.eq(&other.0)
    }
}

impl PartialOrd for PartCmp {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

#[test]
fn merger() {
    let left = vec![PartCmp(0, 1), PartCmp(1, 1), PartCmp(3, 1), PartCmp(5, 1)];
    let right = vec![PartCmp(2, 1), PartCmp(3, 1), PartCmp(4, 3)];

    test_m1(left.clone(), right.clone());
    test_m1(right, left);
}

fn test_m1(left: Vec<PartCmp>, right: Vec<PartCmp>) {
    let out = merge_sorted_vecs(left, right);

    assert_eq!(out.len(), 6);
    for (i, pc) in out.iter().enumerate() {
        assert_eq!(pc.0, i as u8);
    }

    assert_eq!(out[0].1, 1);
    assert_eq!(out[3].1, 1);
    assert_eq!(out[4].1, 3);
}
