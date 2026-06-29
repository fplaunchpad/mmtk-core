use super::global::LXR;
use crate::scheduler::gc_work::UnsupportedProcessEdges;
use crate::scheduler::{GCWork, GCWorker, ProcessEdgesWork};
use crate::vm::VMBinding;
use crate::{Plan, MMTK};

// ── LXR RC pause work contexts + prepare packet (P3 activation) ───────────────────────────────
// Generic-over-`E` `GCWorkContext`, mirroring the reference's `LXRGCWorkContext<E>`. The RC pause
// instantiates it with `E = RCImmixCollectRootEdges<VM>` to drive the root scan through the RC
// root→increment bridge, and with `E = UnsupportedProcessEdges<VM>` for the `Release` packet (no
// edge processing). Distinct from the const-`KIND` tracing context above (which the Immix-clone
// `schedule_immix`-style path used before RC was activated).
pub(super) struct LXRRCWorkContext<E: ProcessEdgesWork>(std::marker::PhantomData<E>);

impl<E: ProcessEdgesWork> crate::scheduler::GCWorkContext for LXRRCWorkContext<E>
where
    E::VM: VMBinding,
{
    type VM = E::VM;
    type PlanType = LXR<E::VM>;
    type DefaultProcessEdges = E;
    type PinningProcessEdges = UnsupportedProcessEdges<E::VM>;
}

/// Runs `LXR::prepare` (which calls `immix_space.prepare_rc`) as a work packet in the
/// `RCProcessIncs` bucket, because the RC pause disables the normal `Prepare` bucket. Vendored from
/// lxr-v0.32.0 gc_work.rs (the `&mut` cast-away-const pattern is the reference's — safe here because
/// the LXR `Plan::prepare` only touches interior-mutable state and the framework guarantees a single
/// prepare runs per pause).
pub(super) struct FastRCPrepare;

impl<VM: VMBinding> GCWork<VM> for FastRCPrepare {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        #[allow(invalid_reference_casting)]
        let lxr = unsafe { &mut *(lxr as *const LXR<VM> as *mut LXR<VM>) };
        lxr.prepare(worker.tls)
    }
}

/// Sentinel for the `STWRCDecsAndSweep` bucket: runs once that bucket has fully drained (all
/// `ProcessDecs` — including the recursively-spawned cascade in `Unconstrained` — are done, so
/// `possibly_dead_mature_blocks` is fully populated). It drains that queue into
/// `SweepBlocksAfterDecs` packets, which actually free the now-dead MATURE immix blocks back to the
/// page-resource free list.
///
/// In the reference LXR this is driven by the `LazySweepingJobsCounter`'s `end_of_decs` Drop
/// callback (the global lazy-sweeping registry we deferred). For the minimal STW cut a bucket
/// sentinel is the equivalent "after all decs" hook, and avoids re-introducing that registry.
pub(super) struct RCBlockSweepEpilogue;

impl<VM: VMBinding> GCWork<VM> for RCBlockSweepEpilogue {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        // Sweep the unpromoted NURSERY blocks here — AFTER all decrements have drained — not in
        // `release_rc`. A field-barrier dec carries the OLD overwritten value, which can be a YOUNG
        // object; freeing its nursery block before the decs run would make that dec dereference a
        // dangling object (heap-address SIGSEGV). By the time this epilogue runs, every inc AND dec
        // (incl. the recursive cascades) is done, so no about-to-be-decremented object's block is
        // freed underneath it.
        lxr.immix_space.rc_sweep_nursery_blocks();
        // Then sweep the now-dead MATURE blocks the decs queued into possibly_dead_mature_blocks.
        lxr.immix_space
            .schedule_rc_block_sweeping_tasks(crate::LazySweepingJobsCounter::new_decs());
        // Release-end phase-epoch bump (GC→mutator), done HERE (after the nursery sweep classified
        // blocks by the GC-phase epoch) rather than in `Plan::release` (which runs before the decs
        // and this sweep). Partner of the pause-start bump in `notify_mutators_paused`.
        crate::policy::immix::block::Block::update_global_phase_epoch(&lxr.immix_space);
    }
}
