//! [STRATEGY PLACEHOLDER] Drains every event ring until the strategy layer exists, so the
//! connection threads never see a full ring. Busy-polls (never sleeps). Prints nothing.

use new_genesis::control_plane::startup::launch::MarketData;

pub fn run(md: &mut MarketData, stop: impl Fn() -> bool) {
    while !stop() {
        let mut drained = 0usize;
        for rx in md.events.iter_mut() {
            while rx.pop().is_some() {
                drained += 1;
            }
        }
        if drained == 0 {
            core::hint::spin_loop();
        }
    }
}
