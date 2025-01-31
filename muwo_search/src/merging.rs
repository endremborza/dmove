pub struct ArrExtender<'a, T> {
    v: &'a mut T,
}

pub trait OrderedMapper<T> {
    type Elem;
    fn left_map(&mut self, e: &Self::Elem);
    fn right_map(&mut self, e: &Self::Elem);
    fn common_map(&mut self, l: &Self::Elem, r: &Self::Elem);
}

pub trait ExtendableArr<T> {
    fn add(&mut self, e: T);
}

impl<T> ExtendableArr<T> for Vec<T> {
    fn add(&mut self, e: T) {
        self.push(e)
    }
}

impl<'a, T, V> OrderedMapper<T> for ArrExtender<'a, V>
where
    T: Clone,
    V: ExtendableArr<T>,
{
    type Elem = T;
    fn left_map(&mut self, e: &Self::Elem) {
        self.v.add(e.clone())
    }
    fn right_map(&mut self, e: &Self::Elem) {
        self.v.add(e.clone())
    }
    fn common_map(&mut self, l: &Self::Elem, _r: &Self::Elem) {
        self.v.add(l.clone())
    }
}

fn _merge_sorted_vecs<T>(left_vec: Vec<T>, right_vec: Vec<T>) -> Vec<T>
where
    T: PartialOrd + Clone,
{
    let mut v = Vec::new();
    sorted_iters_to_arr(&mut v, left_vec.iter(), right_vec.iter());
    v
}

pub fn merge_box_into_sorted_vec<T>(left_vec: &mut Vec<T>, right_barr: &Box<[T]>)
where
    T: PartialOrd + Copy,
{
    merge_into_sorted_vec(left_vec, right_barr.clone().to_vec());
}

pub fn merge_into_sorted_vec<T>(left_vec: &mut Vec<T>, mut right_vec: Vec<T>)
where
    T: PartialOrd + Copy,
{
    if left_vec.len() == 0 {
        let _ = std::mem::replace(left_vec, right_vec);
        return;
    }
    if right_vec.len() == 0 {
        return;
    }
    let mut li = 0;
    let mut ri = 0;

    loop {
        if left_vec[li] == right_vec[ri] {
            //maybe the merging thing
            ri += 1;
            li += 1;
        } else if right_vec[ri] < left_vec[li] {
            std::mem::swap(&mut left_vec[li], &mut right_vec[ri]);
            movin_on_up(&mut right_vec[ri..]);
        } else {
            li += 1;
        }

        if (li == left_vec.len()) || (ri == right_vec.len()) {
            break;
        }
    }

    if ri < right_vec.len() {
        if left_vec[li - 1] == right_vec[ri] {
            ri += 1;
        }
    }

    for i in ri..right_vec.len() {
        let nval = right_vec[i];
        if nval == left_vec[left_vec.len() - 1] {
            //maybe merging thing
        } else {
            left_vec.push(nval);
        }
    }
}

fn movin_on_up<T: PartialOrd>(arr: &mut [T]) {
    let mut i = 0;
    while (i + 1) < arr.len() {
        if arr[i] > arr[i + 1] {
            arr.swap(i, i + 1);
            i += 1;
        } else {
            break;
        }
    }
}

pub fn sorted_iters_to_arr<'a, 'b, T, IL, IR, V>(out: &mut V, left_it: IL, right_it: IR)
where
    T: PartialOrd + Clone + 'a + 'b,
    IL: Iterator<Item = &'a T>,
    IR: Iterator<Item = &'b T>,
    V: ExtendableArr<T>,
{
    let mut vadd = ArrExtender { v: out };
    ordered_calls(left_it, right_it, &mut vadd);
}

pub fn ordered_calls<'a, 'b, T, IL, IR, M>(mut left_it: IL, mut right_it: IR, merger: &mut M)
where
    T: PartialOrd + 'a + 'b,
    IL: Iterator<Item = &'a T>,
    IR: Iterator<Item = &'b T>,
    M: OrderedMapper<T, Elem = T> + ?Sized,
{
    let mut left_elem = match left_it.next() {
        Some(v) => v,
        None => return right_it.for_each(|e| merger.right_map(e)),
    };

    let mut right_elem = match right_it.next() {
        Some(v) => v,
        None => {
            merger.left_map(left_elem);
            left_it.for_each(|e| merger.left_map(e));
            return;
        }
    };

    loop {
        if left_elem == right_elem {
            merger.common_map(left_elem, right_elem);
            match right_it.next() {
                Some(el) => right_elem = el,
                None => break,
            }
            match left_it.next() {
                Some(el) => left_elem = el,
                None => {
                    merger.right_map(right_elem);
                    break;
                }
            }
        } else if right_elem < left_elem {
            merger.right_map(right_elem);
            match right_it.next() {
                Some(el) => right_elem = el,
                None => {
                    merger.left_map(left_elem);
                    break;
                }
            }
        } else {
            merger.left_map(left_elem);
            match left_it.next() {
                Some(el) => left_elem = el,
                None => {
                    merger.right_map(right_elem);
                    break;
                }
            }
        }
    }
    left_it.for_each(|e| merger.left_map(e));
    right_it.for_each(|e| merger.right_map(e));
}

/// gets i so that arr\[..i\] all true, arr\[i..\] all false
pub fn log_search<T, F: Fn(&T) -> bool>(arr: &[T], f: F) -> usize {
    if arr.len() == 0 {
        return 0;
    }
    let (mut l, mut r) = (0, arr.len());
    loop {
        let m = (l + r) / 2;
        if f(&arr[m]) {
            l = m + 1;
        } else {
            r = m;
        }
        if l >= r {
            break;
        }
    }
    l
}

pub fn logfound<T>(v: &Vec<T>, val: T) -> bool
where
    T: PartialOrd,
{
    let li = log_search(&v[0..v.len()], |e| *e < val);
    if li < v.len() {
        return v[li] == val;
    }
    false
}

#[cfg(test)]
mod tests {

    use std::collections::HashSet;

    use super::*;
    use rand::{rngs::StdRng, Rng, SeedableRng};

    #[derive(Debug, Clone, Copy)]
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
        let mut left = vec![PartCmp(0, 1), PartCmp(1, 1), PartCmp(3, 1), PartCmp(5, 1)];
        let right = vec![PartCmp(2, 1), PartCmp(3, 2), PartCmp(4, 3)];

        println!("lr");
        test_m1_out(_merge_sorted_vecs(left.clone(), right.clone()), 1);
        println!("rl");
        test_m1_out(_merge_sorted_vecs(right.clone(), left.clone()), 2);

        merge_into_sorted_vec(&mut left, right);
        println!("lin");
        test_m1_out(left, 1);
    }

    #[test]
    fn compare_rnd() {
        let mut rng = StdRng::seed_from_u64(42);
        for (ll, rl) in vec![
            (0, 4),
            (4, 0),
            (3, 2),
            (2, 3),
            (10, 30),
            (30, 10),
            (50, 50),
            (100, 10),
            (10, 100),
        ] {
            let mut ls: HashSet<u8> = HashSet::new();
            let mut rs: HashSet<u8> = HashSet::new();
            (0..ll).for_each(|_| {
                ls.insert(rng.gen());
            });
            (0..rl).for_each(|_| {
                rs.insert(rng.gen());
            });
            let mut lv: Vec<u8> = ls.into_iter().collect();
            let mut rv: Vec<u8> = rs.into_iter().collect();
            lv.sort();
            rv.sort();

            let ov = _merge_sorted_vecs(lv.clone(), rv.clone());
            merge_into_sorted_vec(&mut lv, rv);
            assert_eq!(lv, ov, "(new vs old) {ll} - {rl}");
        }
    }

    fn test_m1_out(out: Vec<PartCmp>, tc: u8) {
        println!("{out:?}");
        assert_eq!(out.len(), 6);
        for (i, pc) in out.iter().enumerate() {
            assert_eq!(pc.0, i as u8, "inds");
        }
        assert_eq!(out[0].1, 1, "e0");
        assert_eq!(out[3].1, tc, "e3");
        assert_eq!(out[4].1, 3);
    }
}
