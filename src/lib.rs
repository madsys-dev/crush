#![no_std]

extern crate alloc;

use ahash::AHasher;
use alloc::{collections::BTreeMap, string::String, vec::Vec};
use core::hash::{Hash, Hasher};

lazy_static::lazy_static! {
    /// The ln table with value ln(x)<<44 for x in [0,65536).
    static ref LN_TABLE: Vec<u64> =
        (0..65536).map(|i| (-((i as f64 / 65536.0).ln() * ((1u64 << 44) as f64)).round()) as u64).collect();
}

/// The CRUSH algorithm.
#[derive(Default, Clone)]
pub struct Crush {
    root: Node,
}

/// A node in cluster map.
///
/// Maybe root / row / rack / host / osd.
#[derive(Default, Clone)]
struct Node {
    weight: u64,
    out: bool,
    children: BTreeMap<String, Node>,
}

impl Crush {
    /// Add weight to a node.
    pub fn add_weight(&mut self, path: &str, weight: i64) {
        self.root.add_weight(path, weight);
    }

    /// Locate a node by `pgid`.
    pub fn locate(&self, pgid: u32) -> String {
        self.select(pgid, 1).into_iter().next().unwrap()
    }

    /// Return the total weight of the cluster.
    pub fn total_weight(&self) -> u64 {
        self.root.weight
    }

    /// Get the weight of a node.
    pub fn get_weight(&self, path: &str) -> u64 {
        self.root.get(path).weight
    }

    /// Set a node IN/OUT.
    pub fn set_inout(&mut self, path: &str, out: bool) {
        self.root.get_mut(path).out = out;
    }

    /// Get IN/OUT of a node.
    pub fn get_inout(&self, path: &str) -> bool {
        self.root.get(path).out
    }

    /// Select `num` targets accoding to `pgid`.
    pub fn select(&self, pgid: u32, num: u32) -> Vec<String> {
        let mut targets = Vec::<String>::new();
        let mut failure_count = 0;
        for r in 0..num {
            let mut node = &self.root;
            let mut local_failure = 0;
            let mut fullname = String::new();
            loop {
                let name = node.choose(pgid, r + failure_count);
                if !fullname.is_empty() {
                    fullname += "/";
                }
                fullname += name;
                let child = &node.children[name];
                if !child.children.is_empty() {
                    node = child;
                    continue;
                }
                if !child.out && !targets.contains(&fullname) {
                    // found one
                    break;
                }
                failure_count += 1;
                local_failure += 1;
                if local_failure > 3 {
                    node = &self.root;
                    local_failure = 0;
                    fullname.clear();
                }
            }
            targets.push(fullname);
        }
        targets
    }
}

impl Node {
    /// Add weight to a node.
    fn add_weight(&mut self, path: &str, weight: i64) {
        self.weight = (self.weight as i64 + weight) as u64;
        if path.is_empty() {
            return;
        }
        let (name, suffix) = path.split_once('/').unwrap_or((path, ""));
        let child = self.children.entry(name.into()).or_default();
        child.add_weight(suffix, weight);
    }

    /// Get a node by path.
    fn get(&self, path: &str) -> &Self {
        if path.is_empty() {
            return self;
        }
        let (name, suffix) = path.split_once('/').unwrap_or((path, ""));
        self.children[name].get(suffix)
    }

    /// Get a mutable node by path.
    fn get_mut(&mut self, path: &str) -> &mut Self {
        if path.is_empty() {
            return self;
        }
        let (name, suffix) = path.split_once('/').unwrap_or((path, ""));
        self.children.get_mut(name).unwrap().get_mut(suffix)
    }

    /// Choose a child accroding to key and index.
    fn choose(&self, key: u32, index: u32) -> &str {
        self.children
            .iter()
            .map(|(name, child)| {
                let mut hasher = AHasher::default();
                name.hash(&mut hasher);
                key.hash(&mut hasher);
                index.hash(&mut hasher);

                let w = LN_TABLE[(hasher.finish() & 65535) as usize] / child.weight;
                (name, w)
            })
            .min_by_key(|(_, w)| *w)
            .unwrap()
            .0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use alloc::format;
    use rand::Rng;

    /// Generate a 9*9*9*10 cluster map.
    fn gen_test_map() -> Crush {
        // let mut rng = rand::thread_rng();
        let mut crush = Crush::default();
        for i in 0..9 {
            for j in 0..9 {
                for k in 0..9 {
                    for l in 0..10 {
                        let path = path_from_nums(i, j, k, l);
                        // let weight = rng.gen_range(1..5);
                        crush.add_weight(&path, 1);
                    }
                }
            }
        }
        crush
    }

    fn path_from_nums(i: usize, j: usize, k: usize, l: usize) -> String {
        let row = i;
        let rack = row * 9 + j;
        let host = rack * 9 + k;
        let osd = host * 9 + l;
        format!("row.{row}/rack.{rack}/host.{host}/osd.{osd}")
    }

    #[test]
    fn basic_balance() {
        let crush = gen_test_map();
        let mut count = BTreeMap::<String, u32>::new();
        let n = 1000000;
        for i in 0..n {
            let path = crush.locate(i);
            *count.entry(path).or_default() += 1;
        }
        let avg = n / (9 * 9 * 9 * 10);
        for (name, count) in count {
            let range = avg / 2..avg * 2;
            assert!(
                range.contains(&count),
                "path {name:?} count {count} out of range {range:?}"
            );
        }
    }

    /// test distribute on insert
    #[test]
    fn move_factor_add() {
        let mut crush = gen_test_map();
        let crush0 = crush.clone();
        let mut rng = rand::thread_rng();

        // random choose 10 OSDs, add weight to them
        for _ in 0..10 {
            let i = rng.gen_range(0..9);
            let j = rng.gen_range(0..9);
            let k = rng.gen_range(0..9);
            let l = rng.gen_range(0..10);
            let path = path_from_nums(i, j, k, l);
            let weight = rng.gen_range(1..5);
            crush.add_weight(&path, weight as i64);
        }

        let n = 1000000;
        let move_count = (0..n)
            .filter(|&i| crush0.locate(i) != crush.locate(i))
            .count();
        let shift_weight = crush.total_weight() - crush0.total_weight();
        let move_fator =
            (move_count as f32) / (n as f32 / (crush0.total_weight() / shift_weight) as f32);
        assert!(move_fator < 4.0, "move factor {move_fator} should < 4");
    }

    /// test distribute on remove
    #[test]
    fn move_factor_remove() {
        let mut crush = gen_test_map();
        let crush0 = crush.clone();
        let mut rng = rand::thread_rng();

        // shut down around 90 osds
        let mut shift_weight = 0;
        for _ in 0..90 {
            let i = rng.gen_range(0..9);
            let j = rng.gen_range(0..9);
            let k = rng.gen_range(0..9);
            let l = rng.gen_range(0..10);
            let path = path_from_nums(i, j, k, l);
            if !crush.get_inout(&path) {
                crush.set_inout(&path, true);
                shift_weight += crush.get_weight(&path);
            }
        }

        let n = 1000000;
        let move_count = (0..n)
            .filter(|&i| crush0.locate(i) != crush.locate(i))
            .count();
        let move_fator =
            (move_count as f32) / (n as f32 / (crush0.total_weight() / shift_weight) as f32);
        assert!(move_fator < 1.5, "move factor {move_fator} should < 1.5");
    }
}
