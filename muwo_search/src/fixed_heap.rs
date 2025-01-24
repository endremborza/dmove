use std::cmp::Reverse;

pub struct FixedHeap<T, const S: usize> {
    pub arr: [T; S],
}

pub trait Maxed {
    fn max_value() -> Self;
}

pub trait Mined {
    fn min_value() -> Self;
}

impl<T, const S: usize> FixedHeap<T, S>
where
    T: PartialOrd + Maxed,
{
    pub fn new() -> Self {
        let arr = core::array::from_fn(|_| T::max_value());
        Self { arr }
    }

    pub fn push_unique(&mut self, e: T) {
        if self.arr[0] > e {
            if self.find_in(&e, 0, S) {
                return;
            }
            self.arr[0] = e;
            self.reorganize_limited(0, S)
        }
    }

    pub fn into_iter(mut self) -> std::array::IntoIter<T, S> {
        self.sort();
        self.arr.into_iter()
    }

    fn find_in(&self, e: &T, i: usize, limit: usize) -> bool {
        if &self.arr[i] == e {
            return true;
        }
        if &self.arr[i] < e {
            return false;
        }
        for child_side in 1..3 {
            let child_ind = i * 2 + child_side;
            if child_ind >= limit {
                return false;
            };
            if self.find_in(e, child_ind, limit) {
                return true;
            }
        }
        false
    }

    fn reorganize_limited(&mut self, i: usize, limit: usize) {
        for child_side in 1..3 {
            let child_ind = i * 2 + child_side;
            if child_ind >= limit {
                return;
            };
            if self.arr[child_ind] > self.arr[i] {
                self.arr.swap(child_ind, i);
                self.reorganize_limited(child_ind, limit);
            }
        }
    }

    pub fn sort(&mut self) {
        for e in (1..S).rev() {
            self.arr.swap(0, e);
            self.reorganize_limited(0, e);
        }
    }
}

macro_rules! max_impl {
    ($($T: ty),*) => {
        $(
            impl Maxed for $T {
                fn max_value() -> Self {
                    Self::MAX
                }
            }

            impl Mined for $T {
                fn min_value() -> Self {
                    Self::MIN
                }
            }

        )*

    };
}

max_impl!(u8, u16, u32, u64, u128, i8, i16, i32, i64, i128, f32, f64);

impl<T1, T2> Maxed for (T1, T2)
where
    T1: Maxed,
    T2: Maxed,
{
    fn max_value() -> Self {
        (T1::max_value(), T2::max_value())
    }
}

impl<T1, T2> Mined for (T1, T2)
where
    T1: Mined,
    T2: Mined,
{
    fn min_value() -> Self {
        (T1::min_value(), T2::min_value())
    }
}

impl<T> Maxed for Reverse<T>
where
    T: Mined,
{
    fn max_value() -> Self {
        Reverse(T::min_value())
    }
}
