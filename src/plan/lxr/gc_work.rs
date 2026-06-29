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
