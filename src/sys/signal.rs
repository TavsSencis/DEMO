//! [CONTROL PLANE] SIGINT/SIGTERM -> a flag the main thread polls.

use std::sync::atomic::{AtomicBool, Ordering};

static STOP: AtomicBool = AtomicBool::new(false);

extern "C" fn on_signal(_: libc::c_int) {
    // Async-signal-safe: a single atomic store.
    STOP.store(true, Ordering::Relaxed);
}

pub fn install_stop_handler() {
    // SAFETY: the handler only performs an atomic store.
    unsafe {
        libc::signal(libc::SIGINT, on_signal as *const () as libc::sighandler_t);
        libc::signal(libc::SIGTERM, on_signal as *const () as libc::sighandler_t);
    }
}

#[inline]
pub fn stop_requested() -> bool {
    STOP.load(Ordering::Relaxed)
}
