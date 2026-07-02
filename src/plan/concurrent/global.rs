use crate::plan::concurrent::Pause;
use crate::plan::Plan;
use crate::util::ObjectReference;

/// Trait for a concurrent plan.
pub trait ConcurrentPlan: Plan {
    /// Return `true`` if concurrent work (such as concurrent marking) is in progress.
    fn concurrent_work_in_progress(&self) -> bool;
    /// Return the current pause kind.  `None` if not in a pause.
    fn current_pause(&self) -> Option<Pause>;
    /// Return `true` if `object` must NOT be traced by the concurrent marker.
    ///
    /// A generational concurrent plan (Bactrian) overrides this to exclude its copying
    /// nursery: young objects move at every nursery pause, so a young reference held in
    /// a concurrent marking queue would dangle across the pause. Young objects are all
    /// allocated after the snapshot (the `InitialMark` pause empties the nursery), so
    /// skipping them is sound under SATB. Non-generational concurrent plans keep the
    /// default (`false`).
    fn should_skip_concurrent_trace(&self, _object: ObjectReference) -> bool {
        false
    }
    /// Return `true` if the current pause completes the marking cycle (`FinalMark` or a
    /// full STW GC), i.e. mark state is complete and may drive weak-reference clearing
    /// for the whole heap. Bindings use this to distinguish a mid-cycle nursery pause
    /// (stale mature marks) from the cycle-completing pause.
    fn current_pause_finishes_mark(&self) -> bool {
        matches!(
            self.current_pause(),
            Some(Pause::FinalMark) | Some(Pause::Full)
        )
    }
}
