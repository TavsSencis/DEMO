//! [DATA PLANE] Bounded single-producer / single-consumer ring.
//!
//! - `Producer` and `Consumer` are separate, non-`Clone` handles, so "single" is enforced by types.
//! - `head` and `tail` live on separate 128-byte lines (Intel's adjacent-line prefetcher pulls
//!   64-byte lines in pairs, so 64-byte padding still false-shares).
//! - Each side caches the other side's index and only reloads it when the ring looks full/empty,
//!   so the common case touches no cache line the other core writes.
//! - `N` must be a power of two (index = counter & (N - 1)).

use core::cell::UnsafeCell;
use core::mem::MaybeUninit;
use core::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

#[repr(C, align(128))]
struct CachePadded<T>(T);

pub struct SpscRing<T: Copy, const N: usize> {
    /// Next slot the producer writes. Written only by the producer.
    head: CachePadded<AtomicUsize>,
    /// Next slot the consumer reads. Written only by the consumer.
    tail: CachePadded<AtomicUsize>,
    /// Heap-allocated (rings can be MBs) and prefaulted at creation.
    slots: Box<[UnsafeCell<MaybeUninit<T>>]>,
}

// SAFETY: a slot is written by the producer only while it is outside [tail, head), and read by the
// consumer only while it is inside; the Release store of `head`/`tail` publishes the slot write/read
// to the other side's Acquire load. `T: Send` because values move between threads.
unsafe impl<T: Copy + Send, const N: usize> Sync for SpscRing<T, N> {}
unsafe impl<T: Copy + Send, const N: usize> Send for SpscRing<T, N> {}

impl<T: Copy, const N: usize> SpscRing<T, N> {
    const MASK: usize = {
        assert!(N > 0 && N.is_power_of_two(), "SpscRing capacity must be a power of two");
        N - 1
    };

    /// Creates the ring and returns its only producer and consumer.
    /// Control-plane only (allocates once).
    #[allow(clippy::disallowed_methods, clippy::disallowed_macros)] // control plane: runs once at startup
    pub fn pair() -> (Producer<T, N>, Consumer<T, N>) {
        let mask = Self::MASK; // forces the compile-time capacity check
        let mut slots: Box<[UnsafeCell<MaybeUninit<T>>]> =
            (0..=mask).map(|_| UnsafeCell::new(MaybeUninit::uninit())).collect();
        // Touch every page now so the first pushes on the hot path do not page-fault.
        // SAFETY: writing bytes into MaybeUninit storage is always valid; the length is the
        // allocation size in bytes.
        unsafe {
            core::ptr::write_bytes(slots.as_mut_ptr() as *mut u8, 0, core::mem::size_of_val(&*slots));
        }
        let ring = Arc::new(Self {
            head: CachePadded(AtomicUsize::new(0)),
            tail: CachePadded(AtomicUsize::new(0)),
            slots,
        });
        (
            Producer { ring: Arc::clone(&ring), head: 0, published: 0, tail_cache: 0 },
            Consumer { ring, tail: 0, head_cache: 0 },
        )
    }
}

pub struct Producer<T: Copy, const N: usize> {
    ring: Arc<SpscRing<T, N>>,
    /// Next slot to write (may be ahead of what the consumer can see).
    head: usize,
    /// Last value stored to the shared `head` (what the consumer can see).
    published: usize,
    tail_cache: usize,
}

impl<T: Copy, const N: usize> Producer<T, N> {
    /// Writes one value without making it visible. Call `publish` to release a batch, so the
    /// shared `head` line is touched once per batch instead of once per value.
    /// Returns `Err(value)` when the ring is full.
    #[inline(always)]
    pub fn write(&mut self, value: T) -> Result<(), T> {
        if self.head.wrapping_sub(self.tail_cache) == N {
            self.tail_cache = self.ring.tail.0.load(Ordering::Acquire);
            if self.head.wrapping_sub(self.tail_cache) == N {
                return Err(value);
            }
        }
        let idx = self.head & SpscRing::<T, N>::MASK;
        // SAFETY: idx is in bounds (masked). The slot is outside [tail, published), so the
        // consumer does not access it until `publish` stores a head past it (Release).
        unsafe { (*self.ring.slots.get_unchecked(idx).get()).write(value) };
        self.head = self.head.wrapping_add(1);
        Ok(())
    }

    /// Position of the next write, for a later `rewind`.
    #[inline(always)]
    pub fn mark(&self) -> usize {
        self.head
    }

    /// Discards every value written since `mark` that is not yet published. Local only: the
    /// consumer never saw those slots (the shared `head` was not stored past them), so moving
    /// the private write position back needs no atomic and no fence. A mark from before the
    /// last `publish` is clamped to the published position.
    #[inline(always)]
    pub fn rewind(&mut self, mark: usize) {
        // Unpublished region is [published, head); keep mark inside it (wrapping arithmetic).
        if self.head.wrapping_sub(mark) <= self.head.wrapping_sub(self.published) {
            self.head = mark;
        } else {
            self.head = self.published;
        }
    }

    /// Makes every written value visible to the consumer (one Release store, skipped when
    /// nothing new was written).
    #[inline(always)]
    pub fn publish(&mut self) {
        if self.head != self.published {
            self.ring.head.0.store(self.head, Ordering::Release);
            self.published = self.head;
        }
    }
}

pub struct Consumer<T: Copy, const N: usize> {
    ring: Arc<SpscRing<T, N>>,
    tail: usize,
    head_cache: usize,
}

impl<T: Copy, const N: usize> Consumer<T, N> {
    #[inline(always)]
    pub fn pop(&mut self) -> Option<T> {
        if self.tail == self.head_cache {
            self.head_cache = self.ring.head.0.load(Ordering::Acquire);
            if self.tail == self.head_cache {
                return None;
            }
        }
        let idx = self.tail & SpscRing::<T, N>::MASK;
        // SAFETY: idx is in bounds (masked). The slot is inside [tail, head): the producer
        // initialized it before its Release store of head, which our Acquire load observed.
        let value = unsafe { (*self.ring.slots.get_unchecked(idx).get()).assume_init() };
        self.tail = self.tail.wrapping_add(1);
        self.ring.tail.0.store(self.tail, Ordering::Release);
        Some(value)
    }
}
