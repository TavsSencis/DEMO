//! [DATA PLANE] Cheap timestamps for the hot path (TSC on x86_64) plus a one-time calibration.

use std::time::{Duration, Instant};

/// Raw timestamp counter. ~7 ns on x86_64, no syscall. Not serializing; fine for
/// receive timestamps and timeouts, not for measuring a few instructions.
#[inline(always)]
pub fn tsc() -> u64 {
    #[cfg(target_arch = "x86_64")]
    {
        // SAFETY: rdtsc is available on every x86_64 CPU and has no memory effects.
        unsafe { core::arch::x86_64::_rdtsc() }
    }
    #[cfg(not(target_arch = "x86_64"))]
    {
        use std::sync::OnceLock;
        static START: OnceLock<Instant> = OnceLock::new();
        START.get_or_init(Instant::now).elapsed().as_nanos() as u64
    }
}

/// TSC frequency, measured once at startup.
#[derive(Debug, Clone, Copy)]
pub struct TscClock {
    pub ticks_per_sec: u64,
}

impl TscClock {
    /// Blocks ~50 ms. Control plane only.
    pub fn calibrate() -> Self {
        #[cfg(target_arch = "x86_64")]
        {
            let t0 = Instant::now();
            let c0 = tsc();
            std::thread::sleep(Duration::from_millis(50));
            let c1 = tsc();
            let ns = t0.elapsed().as_nanos().max(1);
            let ticks_per_sec = ((c1.wrapping_sub(c0)) as u128 * 1_000_000_000 / ns) as u64;
            Self { ticks_per_sec: ticks_per_sec.max(1) }
        }
        #[cfg(not(target_arch = "x86_64"))]
        {
            Self { ticks_per_sec: 1_000_000_000 }
        }
    }

    #[inline]
    pub fn ticks(&self, d: Duration) -> u64 {
        (d.as_nanos() * self.ticks_per_sec as u128 / 1_000_000_000) as u64
    }
}
