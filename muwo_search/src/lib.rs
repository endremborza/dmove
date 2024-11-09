use deunicode::deunicode;
use std::{
    ops::{Add, AddAssign},
    sync::Arc,
    usize,
};
use triple_accel::{levenshtein_search, rdamerau};

pub type IndType = u32;

#[allow(dead_code)]
const ASCII_LC: [u8; ASCII_COUNT] = [
    97, 98, 99, 100, 101, 102, 103, 104, 105, 106, 107, 108, 109, 110, 111, 112, 113, 114, 115,
    116, 117, 118, 119, 120, 121, 122,
];
const SPLIT_CHAR: u8 = 32;
const ASCII_LC_MIN: u8 = 97;
const ASCII_LC_MAX: u8 = 122;
const ASCII_COUNT: usize = 26;
const CHAR_COUNT: usize = ASCII_COUNT;

pub const MAX_HEAP_SIZE: usize = 16;
const MAX_QUERY_CHARS: usize = 256;
const MAX_QUERY_WORDS: usize = 16;

pub struct SearchEngine {
    char_array: Arc<[u8]>,
    words: Arc<[(u32, u16)]>,
    inds: Arc<[ByteInd]>,
}

// diff, starting point
#[derive(PartialEq, Ord, Eq, PartialOrd, Copy, Clone)]
struct OrdererBase(u8, u8);

#[derive(PartialEq, Ord, Eq, PartialOrd)]
struct OrdererIndexed(OrdererBase, IndType);

struct FixedHeap<T> {
    arr: [T; MAX_HEAP_SIZE],
}

struct StackWordSet {
    char_array: [u8; MAX_QUERY_CHARS],
    break_array: [u8; MAX_QUERY_WORDS],
    breaks_count: usize,
}

struct ByteInd {
    word_idx: u32,
    skipped_n: u16,
}

struct CustomTrie {
    tree: [[TrieNode; (CHAR_COUNT + 1)]; CHAR_COUNT],
}

struct TrieNode {
    leaves: Box<[TrieLeaf]>,
}

struct TrieLeaf {
    suffix: Box<[u8]>,
    ids: Box<[IndType]>,
}

struct MultiWordMatcher<'a> {
    query_arr: [u8; MAX_QUERY_CHARS],
    breaks: &'a [u8],
    matching_basis: [OrdererBase; MAX_QUERY_WORDS],
    mathing_inds: [u16; MAX_QUERY_WORDS],
}

struct SingleWordMatcher<'a> {
    query: &'a [u8],
}

struct OuterWordIter<'a> {
    siter: std::slice::Iter<'a, ByteInd>,
    current_b_ind: &'a ByteInd,
    keeps_going: bool,
    new_inner: bool,
    skipped_n: u16,
}

struct IndexedWord {
    word: Vec<u8>,
    outer_idx: usize,
    inner_idx: usize,
}

trait Maxed {
    fn max_value() -> Self;
}

trait WordMatcher
where
    Self: Sized,
{
    fn order_from_words(
        &mut self,
        words: &mut OuterWordIter<'_>,
        state: &SearchEngine,
        cache: &mut Vec<Option<OrdererBase>>,
    ) -> OrdererBase;

    fn get_order_heap(mut self, state: &SearchEngine) -> FixedHeap<OrdererIndexed> {
        let mut heap: FixedHeap<OrdererIndexed> = FixedHeap::new();
        let mut outer_ind = 0;
        let mut owi = OuterWordIter::new(state.inds.iter()).unwrap();
        let mut cache = vec![None; state.words.len()];
        while owi.keeps_going {
            outer_ind.add_assign(owi.skipped_n as u32);
            let order = self.order_from_words(&mut owi, state, &mut cache);
            heap.push(OrdererIndexed(order, outer_ind));
        }
        heap
    }
}

impl<T> FixedHeap<T>
where
    T: PartialOrd + Maxed,
{
    fn new() -> Self {
        let arr = core::array::from_fn(|_| T::max_value());
        Self { arr }
    }

    fn push(&mut self, e: T) {
        if self.arr[0] > e {
            self.arr[0] = e;
            self.reorganize_limited(0, MAX_HEAP_SIZE)
        }
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

    fn sort(&mut self) {
        for e in (1..MAX_HEAP_SIZE).rev() {
            self.arr.swap(0, e);
            self.reorganize_limited(0, e);
        }
    }
}

impl StackWordSet {
    fn new(words: &str) -> Self {
        let mut out = Self {
            char_array: [0; MAX_QUERY_CHARS],
            break_array: [0; MAX_QUERY_WORDS],
            breaks_count: 0,
        };
        let mut i: u8 = 0;
        for c in deunicode(&words.to_lowercase()).chars() {
            if (c == ' ') && (i > 0) {
                out.new_break(i);
            } else {
                c.encode_utf8(&mut out.char_array[(i as usize)..(i.add(1) as usize)]);
                i.add_assign(1);
            }
        }
        out.new_break(i);
        out
    }

    fn new_break(&mut self, i: u8) {
        self.break_array[self.breaks_count as usize] = i as u8;
        self.breaks_count.add_assign(1);
    }

    fn get_heap(self, engine: &SearchEngine) -> FixedHeap<OrdererIndexed> {
        if self.breaks_count == 1 {
            let be = self.break_array[0];
            if be == 0 {
                let mut h = FixedHeap::new();
                for i in 0..MAX_HEAP_SIZE {
                    h.push(OrdererIndexed(OrdererBase(0, 0), i as u32));
                }
                return h;
            } else if be == 1 {
                let mut h = FixedHeap::new();
                let c = self.char_array[0];
                let (mut i, mut e, mut wi) = (0, 0, 0);
                for idxs in engine.inds.iter() {
                    i += idxs.skipped_n;
                    if idxs.skipped_n != 0 {
                        wi = 0
                    }
                    let start_idx = engine.words[idxs.word_idx as usize].0;
                    if engine.char_array[start_idx as usize] == c {
                        h.push(OrdererIndexed(OrdererBase(wi, 0), i as u32));
                        e += 1;
                        if e == MAX_HEAP_SIZE {
                            return h;
                        }
                    }
                    wi += 1;
                }
                return h;
            }
            let matcher = SingleWordMatcher {
                query: &self.char_array[..be as usize],
            };
            matcher.get_order_heap(&engine)
        } else {
            let matcher =
                MultiWordMatcher::new(self.char_array, &self.break_array[..self.breaks_count]);
            matcher.get_order_heap(&engine)
        }
    }
}

impl SearchEngine {
    pub fn new<I: Iterator<Item = String>>(haystacks: I) -> Self {
        //TODO:
        // involve sizetype (authors-names is only u8 max len!)
        // maybe if small enough precompile the whole whing with the data - store on stack
        // prefix-tree for single word queries
        // 26, 676, 17576, 456976
        let mut idxed_words = Vec::new();
        for (hi, haystack) in haystacks.enumerate() {
            let mut word = Vec::new();
            let mut inner_idx = 0;
            for c in deunicode(&haystack.to_lowercase()).into_bytes().iter() {
                if c == &SPLIT_CHAR {
                    let mut old_word: Vec<u8> = Vec::new();
                    std::mem::swap(&mut old_word, &mut word);
                    idxed_words.push(IndexedWord {
                        word: old_word,
                        inner_idx,
                        outer_idx: hi,
                    });
                    inner_idx += 1;
                } else if (c >= &ASCII_LC_MIN) && (c <= &ASCII_LC_MAX) {
                    word.push(*c);
                }
            }
            idxed_words.push(IndexedWord {
                word,
                inner_idx,
                outer_idx: hi,
            });
        }
        idxed_words.sort_by(|l, r| l.word.cmp(&r.word));
        let mut char_array = Vec::new();
        let mut last_word: Vec<u8> = Vec::new();
        let mut prep_idx_sets = Vec::new();
        for idxed_word in idxed_words.into_iter() {
            if idxed_word.word.len() == 0 {
                continue;
            }
            let overlap = if idxed_word.word.starts_with(&last_word) {
                last_word.len()
            } else {
                0
            };
            char_array.extend(idxed_word.word[overlap..].iter());
            last_word = idxed_word.word;
            prep_idx_sets.push((
                char_array.len() - last_word.len(),
                last_word.len(),
                idxed_word.outer_idx,
                idxed_word.inner_idx,
            ))
        }
        prep_idx_sets.sort();

        let mut prep_round2 = Vec::new();
        let mut words = Vec::new();
        let mut last_word = (0, 0);
        for (w0, w1, hi, ii) in prep_idx_sets.into_iter() {
            let this_word = (w0 as u32, w1 as u16);
            if last_word != this_word {
                words.push(this_word.clone());
                last_word = this_word;
            }
            prep_round2.push((hi, ii, words.len() - 1));
        }
        prep_round2.sort();

        let mut inds = Vec::new();
        let mut last_hi = 0;
        for (hi, _, wind) in prep_round2.into_iter() {
            let val = ByteInd {
                word_idx: wind as u32,
                skipped_n: (hi - last_hi) as u16,
            };
            inds.push(val);
            last_hi = hi;
        }

        Self {
            char_array: char_array.into(),
            inds: inds.into(),
            words: words.into(),
        }
    }

    pub fn query(&self, query: &str) -> [IndType; MAX_HEAP_SIZE] {
        let mut heap = StackWordSet::new(query).get_heap(self);
        heap.sort();
        heap.arr.map(|e| e.1)
    }
}

impl<'a> MultiWordMatcher<'a> {
    fn new(query_arr: [u8; MAX_QUERY_CHARS], breaks: &'a [u8]) -> Self {
        Self {
            query_arr,
            breaks,
            matching_basis: [OrdererBase::max_value(); MAX_QUERY_WORDS],
            mathing_inds: [0; MAX_QUERY_WORDS],
        }
    }

    fn reset(&mut self) {
        for i in 0..self.breaks.len() {
            self.matching_basis[i] = OrdererBase::max_value();
        }
    }
}

impl<'a> OuterWordIter<'a> {
    fn new(mut siter: std::slice::Iter<'a, ByteInd>) -> Option<Self> {
        if let Some(current_b_ind) = siter.next() {
            return Some(Self {
                siter,
                current_b_ind,
                keeps_going: true,
                new_inner: true,
                skipped_n: current_b_ind.skipped_n,
            });
        }
        None
    }
}

impl Maxed for OrdererBase {
    fn max_value() -> Self {
        Self(u8::MAX, u8::MAX)
    }
}

impl Maxed for OrdererIndexed {
    fn max_value() -> Self {
        Self(OrdererBase::max_value(), IndType::MAX)
    }
}

impl WordMatcher for MultiWordMatcher<'_> {
    fn order_from_words(
        &mut self,
        words: &mut OuterWordIter<'_>,
        state: &SearchEngine,
        cache: &mut Vec<Option<OrdererBase>>,
    ) -> OrdererBase {
        self.reset();
        for (word_idx, b_ind) in words.enumerate() {
            let mut start_p = 0;
            for (break_idx, end_p) in self.breaks.iter().enumerate() {
                let end_u = *end_p as usize;
                let new_order = get_score(&self.query_arr[start_p..end_u], cache, b_ind, state);
                start_p = end_u;

                if new_order < self.matching_basis[break_idx] {
                    self.matching_basis[break_idx] = new_order;
                    self.mathing_inds[break_idx] = word_idx as u16;
                };
            }
        }
        let n_i = n_unique(&mut self.mathing_inds[..self.breaks.len()]);
        // TODO: incorporate this, also the fei fei li problem
        // also, incorporate matching order of matched words
        self.matching_basis[..self.breaks.len()]
            .iter()
            .max()
            .unwrap()
            .to_owned()
    }
}

impl WordMatcher for SingleWordMatcher<'_> {
    fn order_from_words(
        &mut self,
        words: &mut OuterWordIter<'_>,
        state: &SearchEngine,
        cache: &mut Vec<Option<OrdererBase>>,
    ) -> OrdererBase {
        let mut order = OrdererBase::max_value();
        for b_ind in words {
            let new_order = get_score(self.query, cache, b_ind, state);
            if new_order < order {
                order = new_order;
            }
        }
        order
    }
}

impl<'a> Iterator for OuterWordIter<'a> {
    type Item = &'a ByteInd;

    fn next(&mut self) -> Option<Self::Item> {
        if self.new_inner {
            self.new_inner = false;
            return Some(self.current_b_ind);
        }

        if let Some(b_ind) = self.siter.next() {
            self.current_b_ind = b_ind;
            if b_ind.skipped_n > 0 {
                self.new_inner = true;
                self.skipped_n = self.current_b_ind.skipped_n;
                return None;
            }
            return Some(self.current_b_ind);
        } else {
            self.keeps_going = false;
        }
        None
    }
}

fn get_score(
    needle: &[u8],
    cache: &mut Vec<Option<OrdererBase>>,
    bo: &ByteInd,
    state: &SearchEngine,
) -> OrdererBase {
    let wuid = bo.word_idx as usize;
    match cache[wuid] {
        Some(o) => o,
        None => {
            let word = state.words[wuid];
            let haystack = &state.char_array[word.0 as usize..(word.0 + word.1 as u32) as usize];
            let o = get_score_base(needle, haystack);
            cache[wuid] = Some(o.clone());
            o
        }
    }
}

fn get_score_base(needle: &[u8], haystack: &[u8]) -> OrdererBase {
    let (hl, nl) = (haystack.len() as u8, needle.len() as u8);
    if hl > nl {
        if let Some(lmatch) = levenshtein_search(needle, haystack).next() {
            OrdererBase(lmatch.k as u8, lmatch.start as u8)
        } else {
            OrdererBase(hl, hl)
        }
    } else {
        let score = rdamerau(needle, haystack);
        OrdererBase(score as u8, 0)
    }
}

fn n_unique<T: PartialEq + Ord + std::fmt::Debug>(arr: &mut [T]) -> u8 {
    arr.sort();
    let mut o = 0;
    for (i, e) in arr.iter().enumerate().skip(1) {
        if e != &arr[i - 1] {
            o.add_assign(1);
        }
    }
    o
}
