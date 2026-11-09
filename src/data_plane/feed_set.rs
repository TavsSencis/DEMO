//! [DATA PLANE] Fixed bitset over feeds, owned by one connection thread.

use crate::control_plane::identity::FeedId;

/// Fixed-size bitset over all feeds in the plan. Owned by one thread; no atomics.
#[derive(Debug, Clone)]
pub struct FeedSet {
    words: Box<[u64]>,
}

impl FeedSet {
    #[allow(clippy::disallowed_methods, clippy::disallowed_macros)] // control plane: runs once at startup
    pub fn new(feed_count: usize) -> Self {
        Self { words: vec![0u64; feed_count.div_ceil(64).max(1)].into_boxed_slice() }
    }

    #[inline(always)]
    pub fn contains(&self, f: FeedId) -> bool {
        let i = f.index();
        self.words.get(i >> 6).is_some_and(|w| w & (1 << (i & 63)) != 0)
    }

    #[inline]
    pub fn insert(&mut self, f: FeedId) {
        let i = f.index();
        if let Some(w) = self.words.get_mut(i >> 6) {
            *w |= 1 << (i & 63);
        }
    }

    #[inline]
    pub fn remove(&mut self, f: FeedId) {
        let i = f.index();
        if let Some(w) = self.words.get_mut(i >> 6) {
            *w &= !(1 << (i & 63));
        }
    }

    pub fn clear(&mut self) {
        self.words.fill(0);
    }

    pub fn iter(&self) -> impl Iterator<Item = FeedId> + '_ {
        self.words.iter().enumerate().flat_map(|(wi, &w)| {
            (0..64).filter(move |b| w & (1 << b) != 0).map(move |b| FeedId((wi * 64 + b) as u16))
        })
    }
}
