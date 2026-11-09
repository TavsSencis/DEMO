//! [CONTROL PLANE] Memory residency: prefaulting buffers at startup so the hot path never
//! takes a first-touch page fault.

/// Touches every page of `buf` so it is resident before the hot path uses it.
/// `vec![0; n]` maps lazily-zeroed pages; the first write to each would page-fault.
/// Volatile writes so the compiler cannot drop them as "already zero".
pub fn prefault(buf: &mut [u8]) {
    const PAGE: usize = 4096;
    let mut i = 0;
    while i < buf.len() {
        // SAFETY: i < buf.len().
        unsafe { std::ptr::write_volatile(buf.as_mut_ptr().add(i), 0) };
        i += PAGE;
    }
}
