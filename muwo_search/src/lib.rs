mod fixed_heap;
mod merging;

use deunicode::deunicode;
pub use fixed_heap::FixedHeap;
pub use merging::{
    log_search, ordered_calls, sorted_iters_to_arr, ArrExtender, ExtendableArr, OrderedMapper,
};
use merging::{logfound, merge_box_into_sorted_vec, merge_into_sorted_vec};

use std::cmp::min;
use std::convert::TryInto;
use std::fmt::Debug;
use std::{ops::AddAssign, usize};
use tqdm::Iter;

type IndType = u32;

const MAX_HEAP_SIZE: usize = 16;

const SPLIT_CHAR: u8 = 32;
const ASCII_LC_MIN: u8 = 97;
const ASCII_LC_MAX: u8 = 122;
const ASCII_COUNT: usize = 26;
const CHAR_COUNT: usize = ASCII_COUNT;

const MAX_QUERY_CHARS: usize = 256;
const MAX_QUERY_WORDS: usize = 32;

const BRANCHING_LEVELS: usize = 2;

type IndOutTrie<NL> = GenTrie<NL, FixedHeap<IndType, MAX_HEAP_SIZE>>;
type TrieNodeRoot = IndOutTrie<TrieNodeL1>;
type TrieNodeL1 = IndOutTrie<TrieLeaves>;

type PrepTrieL1 = IndOutTrie<Vec<PrepLeaf>>;
type PrepTrieRoot = IndOutTrie<PrepTrieL1>;

pub struct SearchEngine {
    tree: CustomTrie,
}

struct QueryResult {
    perfect: Vec<IndType>,
    partial: Vec<IndType>,
}

#[derive(Debug, Clone, PartialEq)]
struct WordViaCharr {
    start_idx: u32,
    len: u16,
}

pub struct StackWordSet {
    char_array: [u8; MAX_QUERY_CHARS],
    break_array: [u8; MAX_QUERY_WORDS],
    breaks_count: usize,
}

struct GenTrie<NextLevel, Out> {
    children: [NextLevel; CHAR_COUNT],
    out: Out,
}

struct CustomTrie {
    prefix_tree: TrieNodeRoot,
    inner_tree: TrieNodeRoot,
    char_array: Box<[u8]>,
}

#[derive(Debug)]
struct TrieLeaves(Box<[TrieLeaf]>);

#[derive(Debug)]
struct TrieLeaf {
    suffix: WordViaCharr,
    ids: Box<[IndType]>,
}

struct PrepLeaf {
    suffix: WordViaCharr,
    ids: Vec<IndType>,
}

struct IndexedWord {
    word: Vec<u8>,
    outer_idx: usize,
    _inner_idx: usize, //TODO/improvement: use this somehow
}

trait Construct {
    fn new() -> Self;
}

trait QueriableLevel {
    fn query(&self, word: &[u8], char_arr: &[u8]) -> QueryResult;
    fn query_prefiltered(&self, word: &[u8], char_arr: &[u8], include: &QueryResult)
        -> QueryResult;
    fn query_inners(&self, word: &[u8], char_arr: &[u8]) -> Vec<IndType>;
    fn query_inners_prefiltered(
        &self,
        word: &[u8],
        char_arr: &[u8],
        include: &QueryResult,
    ) -> Vec<IndType>;
}

impl<T> IndOutTrie<T> {
    fn rel_inds(&self) -> Vec<IndType> {
        self.out
            .arr
            .into_iter()
            .take_while(|e| *e < IndType::MAX)
            .collect()
    }

    fn rel_filt_inds(&self, include: &QueryResult) -> Vec<IndType> {
        return self
            .out
            .arr
            .into_iter()
            .filter(|e| logfound(&include.partial, *e) || logfound(&include.perfect, *e))
            .take_while(|e| *e < IndType::MAX)
            .collect();
    }
}

impl TrieLeaves {
    fn sides(&self, char_arr: &[u8], word: &[u8]) -> (usize, usize) {
        let l = log_search(&self.0, |e| e.suffix.under(char_arr, word));
        let r = log_search(&self.0[l..], |e| e.suffix.not_over(char_arr, word)) + l;
        (l, r)
    }
}

impl Into<Vec<IndType>> for QueryResult {
    fn into(mut self) -> Vec<IndType> {
        extend_sorted(&mut self.perfect, self.partial);
        self.perfect
    }
}

impl<T> Construct for Vec<T> {
    fn new() -> Self {
        Self::new()
    }
}

impl<T: Iterator<Item = TrieLeaf>> From<T> for TrieLeaves {
    fn from(value: T) -> Self {
        Self(value.collect::<Vec<TrieLeaf>>().try_into().unwrap())
    }
}

impl<T: Construct> Construct for IndOutTrie<T> {
    fn new() -> Self {
        let children = core::array::from_fn(|_| T::new());
        Self {
            children,
            out: FixedHeap::new(),
        }
    }
}

impl<T: QueriableLevel> QueriableLevel for IndOutTrie<T> {
    fn query(&self, word: &[u8], char_arr: &[u8]) -> QueryResult {
        if word.len() == 0 {
            let perfect = self.rel_inds();
            return QueryResult {
                perfect,
                partial: Vec::new(),
            };
        }
        self.children[word[0] as usize].query(&word[1..], char_arr)
    }

    fn query_inners(&self, word: &[u8], char_arr: &[u8]) -> Vec<IndType> {
        if word.len() == 0 {
            return self.rel_inds();
        }
        self.children[word[0] as usize].query_inners(&word[1..], char_arr)
    }

    fn query_prefiltered(
        &self,
        word: &[u8],
        char_arr: &[u8],
        include: &QueryResult,
    ) -> QueryResult {
        if word.len() == 0 {
            return QueryResult {
                perfect: self.rel_filt_inds(include),
                partial: Vec::new(),
            };
        }
        self.children[word[0] as usize].query_prefiltered(&word[1..], char_arr, include)
    }

    fn query_inners_prefiltered(
        &self,
        word: &[u8],
        char_arr: &[u8],
        include: &QueryResult,
    ) -> Vec<IndType> {
        if word.len() == 0 {
            return self.rel_filt_inds(include);
        }
        self.children[word[0] as usize].query_inners_prefiltered(&word[1..], char_arr, include)
    }
}

impl QueriableLevel for TrieLeaves {
    fn query(&self, word: &[u8], char_arr: &[u8]) -> QueryResult {
        let (l, r) = self.sides(char_arr, word);
        let unlen = word.len() as u16;
        let mut perfect = Vec::new();
        // let mut shorters = Vec::new();
        let mut partial = Vec::new();
        for leaf in self.0[l..r].iter() {
            if unlen == leaf.suffix.len {
                perfect = leaf.ids.to_vec();
            } else if unlen > leaf.suffix.len {
                //TODO: puzzle - how the hell can we end up here?
                // merge_box_into_sorted_vec(&mut shorters, &leaf.ids);
            } else {
                merge_box_into_sorted_vec(&mut partial, &leaf.ids);
            }
        }
        // extend_sorted(&mut shorters, longers);
        QueryResult { perfect, partial }
    }

    fn query_inners(&self, word: &[u8], char_arr: &[u8]) -> Vec<IndType> {
        let (l, r) = self.sides(char_arr, word);
        let mut out = Vec::new();
        for leaf in self.0[l..r].iter() {
            merge_box_into_sorted_vec(&mut out, &leaf.ids);
        }
        out
    }

    fn query_prefiltered(
        &self,
        word: &[u8],
        char_arr: &[u8],
        include: &QueryResult,
    ) -> QueryResult {
        let (l, r) = self.sides(char_arr, word);
        let fifu =
            |e: &&IndType| logfound(&include.partial, **e) || logfound(&include.perfect, **e);
        let unlen = word.len() as u16;
        let mut perfect = Vec::new();
        let mut partial = Vec::new();
        for leaf in self.0[l..r].iter() {
            let lids: Vec<IndType> = leaf.ids.iter().filter(fifu).map(|e| *e).collect();
            if unlen == leaf.suffix.len {
                perfect = lids;
            } else {
                merge_into_sorted_vec(&mut partial, lids);
            }
        }
        QueryResult { perfect, partial }
    }

    fn query_inners_prefiltered(
        &self,
        word: &[u8],
        char_arr: &[u8],
        include: &QueryResult,
    ) -> Vec<IndType> {
        let (l, r) = self.sides(char_arr, word);
        let fifu =
            |e: &&IndType| logfound(&include.partial, **e) || logfound(&include.perfect, **e);
        let mut out = Vec::new();
        for leaf in self.0[l..r].iter() {
            let lids: Vec<IndType> = leaf.ids.iter().filter(fifu).map(|e| *e).collect();
            merge_into_sorted_vec(&mut out, lids);
        }
        out
    }
}

impl PrepTrieRoot {
    fn finalize(mut self, char_array: &Vec<u8>) -> TrieNodeRoot {
        self.out.sort();
        let out = self.out;
        let children = child_into(self.children, |c| c.finalize(char_array));
        TrieNodeRoot { out, children }
    }

    fn extend(
        &mut self,
        idxed_word: &IndexedWord,
        char_array: &mut Vec<u8>,
        start_char: usize,
        last_suff: &[u8],
    ) -> Vec<u8> {
        let full_word = &idxed_word.word[start_char..];
        self.out.push_unique(idxed_word.outer_idx as IndType);
        if full_word.len() < 1 {
            return last_suff.into();
        }
        let l1 = &mut self.children[full_word[0] as usize];
        l1.out.push_unique(idxed_word.outer_idx as IndType);
        if full_word.len() < 2 {
            return last_suff.into();
        }
        let l2 = &mut l1.children[full_word[1] as usize];
        let suffix = &full_word[BRANCHING_LEVELS..];
        let overlap = get_overlap(suffix, last_suff);
        char_array.extend(suffix[overlap..].iter());
        let ln = suffix.len();
        let suff_by_idx = WordViaCharr {
            start_idx: (char_array.len() - ln) as u32,
            len: ln as u16,
        };
        let l2_idx = get_i(l2, suff_by_idx);
        let oind = idxed_word.outer_idx as IndType;
        //maybe just check last one?
        if !l2[l2_idx].ids.contains(&oind) {
            l2[l2_idx].ids.push(idxed_word.outer_idx as IndType);
        }
        suffix.into()
    }
}

impl PrepTrieL1 {
    fn finalize(mut self, char_array: &Vec<u8>) -> TrieNodeL1 {
        self.out.sort();
        let out = self.out;
        let children = child_into(self.children, |mut c| {
            c.sort_by_key(|tl| tl.suffix.cut(&char_array));
            c.into_iter()
                .map(|mut leaf| {
                    leaf.ids.sort();
                    TrieLeaf {
                        ids: leaf.ids.into(),
                        suffix: leaf.suffix,
                    }
                })
                .into()
        });
        TrieNodeL1 { out, children }
    }
}

impl WordViaCharr {
    fn cut<'a>(&self, char_arr: &'a [u8]) -> &'a [u8] {
        let su = self.start_idx as usize;
        &char_arr[su..(su + self.len as usize)]
    }

    fn under(&self, char_arr: &[u8], comp: &[u8]) -> bool {
        //consider empty comp
        self.cmp_meta(char_arr, comp, false)
    }

    fn not_over(&self, char_arr: &[u8], comp: &[u8]) -> bool {
        self.cmp_meta(char_arr, comp, true)
    }

    fn cmp_meta(&self, char_arr: &[u8], comp: &[u8], breaker: bool) -> bool {
        let my_size = self.len as usize;
        let my_arr = self.cut(char_arr);
        for i in 0..comp.len() {
            if i >= my_size {
                break;
            }
            if my_arr[i] > comp[i] {
                return false;
            }
            if my_arr[i] < comp[i] {
                return true;
            }
        }
        breaker
    }
}

impl CustomTrie {
    fn new(mut idxed_words: Vec<IndexedWord>) -> Self {
        idxed_words.sort_by(|l, r| get_suffix(&l.word).cmp(&get_suffix(&r.word)));
        let mut char_array = Vec::new();
        let mut last_suff: Vec<u8> = Vec::new();
        let mut prep_tree = PrepTrieRoot::new();
        let mut inner_prep = PrepTrieRoot::new();

        for idxed_word in idxed_words
            .into_iter()
            .rev()
            .tqdm()
            .desc(Some("building trie"))
            .filter(|e| e.word.len() > 0)
        {
            last_suff = prep_tree.extend(&idxed_word, &mut char_array, 0, &last_suff);
            for i in 1..(idxed_word.word.len() - 1) {
                inner_prep.extend(&idxed_word, &mut char_array, i, &last_suff);
            }
        }
        Self {
            prefix_tree: prep_tree.finalize(&char_array),
            inner_tree: inner_prep.finalize(&char_array),
            char_array: char_array.into(),
        }
    }

    fn query(&self, sword: &StackWordSet, limit: usize) -> Vec<IndType> {
        //should iterate through matches
        //return _ordered_ indeices of matches
        // I. perfect match at start of word
        // II. perfect match within the word
        // III. parital match anywhere
        //    - this might be multilevel based on partials similarity
        // should be optional to return up to a number or all - for multiword
        if sword.breaks_count <= 1 {
            let be = sword.break_array[0] as usize;
            let word = &sword.char_array[0..be];
            let mut matches = self.prefix_tree.query(word, &self.char_array);
            if (matches.perfect.len() + matches.partial.len()) < limit {
                self.extend_matches(&mut matches, word);
            }
            matches.into()
        } else {
            let be = sword.break_array[0] as usize;
            let word = &sword.char_array[0..be];
            let mut matches = self.prefix_tree.query(word, &self.char_array);
            self.extend_matches(&mut matches, word);
            let mut bs = be;
            for i in 1..(min(sword.breaks_count, 3)) {
                let be = sword.break_array[i] as usize;
                let word = &sword.char_array[bs..be];
                let mut prefs =
                    self.prefix_tree
                        .query_prefiltered(word, &self.char_array, &matches);
                let ins =
                    self.inner_tree
                        .query_inners_prefiltered(word, &self.char_array, &matches);
                merge_into_sorted_vec(&mut prefs.partial, ins);
                matches = prefs;
                bs = be;
            }
            matches.into()
        }
    }
    fn extend_matches(&self, matches: &mut QueryResult, word: &[u8]) {
        let ins = self.inner_tree.query_inners(word, &self.char_array);
        merge_into_sorted_vec(&mut matches.partial, ins);
    }
}

impl StackWordSet {
    pub fn new(words: &str) -> Self {
        let mut out = Self {
            char_array: [0; MAX_QUERY_CHARS],
            break_array: [0; MAX_QUERY_WORDS],
            breaks_count: 0,
        };
        let mut i: u8 = 0;
        for c in deunicode(&words.to_lowercase()).as_bytes().iter() {
            if (*c == SPLIT_CHAR) && (i > 0) {
                out.new_break(i);
                if out.breaks_count == MAX_QUERY_WORDS {
                    // println!(
                    //     "ran out of breaks at {} for {}",
                    //     i,
                    //     &words[..out.break_array[5] as usize]
                    // );
                    return out;
                }
            } else if (*c >= ASCII_LC_MIN) && (*c <= ASCII_LC_MAX) {
                out.char_array[i as usize] = *c - ASCII_LC_MIN;
                i.add_assign(1);
                if i == 0 {
                    //overflow
                    // println!(
                    //     "ran out of chars at {} for {}",
                    //     out.breaks_count,
                    //     &words[..out.break_array[5] as usize]
                    // );
                    return out;
                }
            }
        }
        out.new_break(i);
        out
    }

    pub fn to_words(&self) -> Vec<String> {
        let mut out = Vec::new();
        let mut si = 0;
        for i in 0..self.breaks_count {
            let ei = self.break_array[i] as usize;
            let cv = self.char_array[si..ei]
                .iter()
                .map(|e| *e + ASCII_LC_MIN)
                .collect();
            out.push(String::from_utf8(cv).expect("reading word"));
            si = ei;
        }
        out
    }

    fn new_break(&mut self, i: u8) {
        let last_break = if self.breaks_count > 0 {
            self.break_array[self.breaks_count - 1]
        } else {
            0
        };
        if last_break != i {
            self.break_array[self.breaks_count as usize] = i as u8;
            self.breaks_count.add_assign(1);
        }
    }
}

impl SearchEngine {
    pub fn new<I: Iterator<Item = String>>(haystacks: I) -> Self {
        //TODO/performance:
        // involve sizetype (authors-names is only u8 max len!)
        // maybe if small enough precompile the whole whing with the data - store on stack
        // 26, 676, 17576, 456976
        let idxed_words = get_idxed_words(haystacks);
        let trie = CustomTrie::new(idxed_words);
        Self { tree: trie.into() }
    }

    pub fn query(&self, query: &str) -> Vec<IndType> {
        let sword = StackWordSet::new(query);
        self.tree
            .query(&sword, MAX_HEAP_SIZE)
            .into_iter()
            .take(MAX_HEAP_SIZE)
            .collect()
    }
}

//adds elems that are not present
fn extend_sorted<T: PartialOrd>(int_v: &mut Vec<T>, add_v: Vec<T>) {
    let mut i = 0;
    let init_i_len = int_v.len();
    let mut last_ge = None;
    for add_e in add_v.into_iter() {
        while i < init_i_len {
            if int_v[i] < add_e {
                i += 1;
            } else {
                last_ge = Some(i);
                break;
            }
        }
        if match last_ge {
            None => true,
            Some(i) => add_e != int_v[i],
        } {
            int_v.push(add_e)
        }
    }
}

fn get_idxed_words<I: Iterator<Item = String>>(haystacks: I) -> Vec<IndexedWord> {
    let mut idxed_words = Vec::new();
    for (hi, haystack) in haystacks.enumerate() {
        let wstack = StackWordSet::new(&haystack);
        let mut last_break = 0;
        for break_n in 0..wstack.breaks_count {
            let this_break = wstack.break_array[break_n] as usize;
            idxed_words.push(IndexedWord {
                word: wstack.char_array[last_break..this_break].to_vec(),
                _inner_idx: break_n,
                outer_idx: hi,
            });
            last_break = this_break;
        }
    }
    idxed_words
}

fn _n_unique<T: PartialEq + Ord>(arr: &mut [T]) -> u8 {
    //TODO/improvement: use this for better matches
    arr.sort();
    let mut o = 0;
    for (i, e) in arr.iter().enumerate().skip(1) {
        if e != &arr[i - 1] {
            o.add_assign(1);
        }
    }
    o
}

fn get_suffix(word: &Vec<u8>) -> Vec<u8> {
    word.iter().skip(BRANCHING_LEVELS).map(|e| *e).collect()
}

fn get_overlap<T: PartialEq>(suffix: &[T], word: &[T]) -> usize {
    if word.ends_with(suffix) {
        suffix.len()
    } else {
        0
    }
}

fn get_i(v: &mut Vec<PrepLeaf>, e: WordViaCharr) -> usize {
    for (i, pl) in v.iter().enumerate() {
        if e == pl.suffix {
            return i;
        }
    }
    v.push(PrepLeaf {
        suffix: e,
        ids: Vec::new(),
    });
    return v.len() - 1;
}

fn child_into<S, T, F>(children: [S; CHAR_COUNT], f: F) -> [T; CHAR_COUNT]
where
    F: FnMut(S) -> T,
{
    match children.into_iter().map(f).collect::<Vec<T>>().try_into() {
        Ok(a) => a,
        Err(_) => panic!("cant collect to {CHAR_COUNT}"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn get_test_engine() -> SearchEngine {
        let haystacks = vec![
            "abc",
            "xyz",
            "man woo",
            "axa",
            "mewixalion",
            "bumble rumble",
        ];
        SearchEngine::new(haystacks.iter().map(|s| s.to_string()))
    }

    #[test]
    fn test_extend_sorted() {
        let mut v1 = vec![1, 2, 3];
        let v2 = vec![2, 3, 4];
        extend_sorted(&mut v1, v2);
        assert_eq!(v1, vec![1, 2, 3, 4]);
    }

    #[test]
    fn gets_empty() {
        let engine = get_test_engine();
        assert_eq!(engine.query("").len(), 6);
    }

    #[test]
    fn gets_starts() {
        let engine = get_test_engine();
        for (q, r0) in vec![
            ("a", 0),
            ("x", 1),
            ("ma", 2),
            ("w", 2),
            ("ax", 3),
            ("mewix", 4),
        ]
        .iter()
        {
            let result = engine.query(q);
            assert_eq!(result[0], *r0);
        }
        assert_eq!(engine.query("a")[1], 3);
        assert_eq!(engine.query("q").len(), 0);
    }

    #[test]
    fn gets_innards() {
        let engine = get_test_engine();
        for (q, r0) in vec![
            ("y", 1),
            ("an", 2),
            ("xa", 3),
            ("ion", 4),
            ("wix", 4),
            ("ix", 4),
        ]
        .iter()
        {
            let result = engine.query(q);
            println!("{:?} for {}", result, q);
            assert_eq!(result[0], *r0);
        }
        assert_eq!(engine.query("x")[1], 3);
        assert_eq!(engine.query("x").len(), 3);
        //cant find based on one character that is the last one
        println!("{:?}", engine.query("c"));
        assert_eq!(engine.query("c").len(), 0);
    }

    #[test]
    fn no_multiplied_result() {
        let haystacks = vec!["aba aba aba", "xxx", "zzz"];
        let engine = SearchEngine::new(haystacks.iter().map(|s| s.to_string()));
        println!("tlen: {}", engine.tree.char_array.len());
        assert_eq!(engine.query("ab").len(), 1);

        let haystacks = vec!["abas abazz abaxy", "tabaxi", "zzz"];
        let engine = SearchEngine::new(haystacks.iter().map(|s| s.to_string()));
        assert_eq!(engine.query("ab").len(), 2);
    }

    #[test]
    fn multi_word_query() {
        let haystacks = vec!["aba cdx", "aba", "cdx", "crum brabn", "udx crtasba"];
        let engine = SearchEngine::new(haystacks.iter().map(|s| s.to_string()));
        assert_eq!(engine.query("ab cd"), vec![0]);
        assert_eq!(engine.query("ru ra"), vec![3]);
        assert_eq!(engine.query("dx ba"), vec![0, 4]);
    }

    #[test]
    fn optimized_array() {
        let haystacks = vec!["ababc", "xaabc", "wuabc"];
        let engine = SearchEngine::new(haystacks.iter().map(|s| s.to_string()));
        assert_eq!(engine.tree.char_array.len(), 3);
    }

    #[test]
    fn gets_long() {
        let haystacks = vec!["Hiroyasa Hidaka", "Manuel Hidalgo", "Hisao Hidaka"];
        let engine = SearchEngine::new(haystacks.iter().map(|s| s.to_string()));
        assert_eq!(engine.query("hidalgo")[0], 1);
    }

    #[test]
    fn gets_ch() {
        let haystacks = vec!["China", "Chile", "Chad"];
        let engine = SearchEngine::new(haystacks.iter().map(|s| s.to_string()));
        assert_eq!(engine.query("ch")[0], 0);
    }

    #[test]
    fn perfect_match() {
        let mut haystacks: Vec<String> = (0..30).map(|_| "Wes".to_string()).collect();
        haystacks.push("West".to_string());
        let engine = SearchEngine::new(haystacks.into_iter());
        assert_eq!(engine.query("west")[0], 30);
    }

    #[test]
    fn lincoln() {
        //because it assumes to that results for words are all sorted
        let haystacks = vec![
            "MIT",
            "MITb",
            "MITc",
            "MIT Lincoln Laboratory",
            "GlaxoSmithKline",
            "MITc",
            "MITc",
            "MITc",
            "MITc",
        ];
        let engine = SearchEngine::new(haystacks.iter().map(|s| s.to_string()));
        println!("{:?}", engine.query("mit linc"));
        assert_eq!(engine.query("mit linc")[0], 3);
    }
}
