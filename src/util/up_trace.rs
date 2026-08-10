//! UP-trace mode: single-tracer plain-operation fast paths.
//!
//! When EXACTLY ONE GC thread traces objects in a fully stopped world with no
//! concurrent marking, every synchronization primitive on the trace hot path
//! is redundant: there is no second thread to race, exclude, or publish to
//! before the pause-ending barrier. The OCaml binding measured 5.7 locked
//! RMW operations per copied object (forwarding claim CAS + SeqCst stores +
//! mark/unlog metadata ops), ~170-230 cycles/object of the 415-vs-80
//! cycles/object gap against stock OCaml's domain-private minor collector
//! (SHAPE.md 2026-08-10).
//!
//! The flag is set by the VM binding at stop-the-world begin when it can
//! prove the conditions (worker count == 1, mutators quiesced, no concurrent
//! marking window in flight) and cleared before any mutator resumes; the
//! pause-ending lock/unlock provides the publication barrier for the plain
//! writes. Off by default; all paths keep their atomic behaviour unless the
//! binding opts in per pause. LXR/RC paths never consult this flag.

use std::sync::atomic::{AtomicBool, Ordering};

static UP_TRACE: AtomicBool = AtomicBool::new(false);

/// Is single-tracer mode active for the current pause?
#[inline(always)]
pub fn up() -> bool {
    UP_TRACE.load(Ordering::Relaxed)
}

/// Binding-facing setter. See module docs for the soundness conditions the
/// caller must establish.
pub fn set_up_trace(enabled: bool) {
    UP_TRACE.store(enabled, Ordering::SeqCst);
}
