use super::global::LXR;
use crate::policy::gc_work::TraceKind;
use crate::policy::gc_work::TRACE_KIND_TRANSITIVE_PIN;
use crate::scheduler::gc_work::PlanProcessEdges;
use crate::vm::VMBinding;

// P3.5: cloned from plan/immix/gc_work.rs (Immix -> LXR). The LXR plan currently
// traces exactly like Immix (rc_enabled = false); the RC trace contexts are layered
// on in a later P3 step.
pub(super) struct LXRGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for LXRGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = LXR<VM>;
    type DefaultProcessEdges = PlanProcessEdges<VM, LXR<VM>, KIND>;
    type PinningProcessEdges = PlanProcessEdges<VM, LXR<VM>, TRACE_KIND_TRANSITIVE_PIN>;
}
