use super::global::Bactrian;
use crate::plan::concurrent::concurrent_marking_work::ConcurrentTraceObjects;
use crate::plan::concurrent::global::ConcurrentPlan;
use crate::plan::concurrent::Pause;
use crate::plan::generational::global::GenerationalPlan;
use crate::plan::generational::global::GenerationalPlanExt;
use crate::plan::global::PlanTraceObject;
use crate::plan::VectorObjectQueue;
use crate::policy::gc_work::TraceKind;
use crate::policy::space::Space;
use crate::policy::gc_work::DEFAULT_TRACE;
use crate::policy::immix::TRACE_KIND_FAST;
use crate::scheduler::gc_work::PlanProcessEdges;
use crate::scheduler::gc_work::PlanScanObjects;
use crate::scheduler::gc_work::ProcessEdgesBase;
use crate::scheduler::gc_work::SlotOf;
use crate::scheduler::gc_work::UnsupportedProcessEdges;
use crate::scheduler::ProcessEdgesWork;
use crate::scheduler::WorkBucketStage;
use crate::util::ObjectReference;
use crate::vm::slot::Slot;
use crate::vm::VMBinding;
use crate::MMTK;
use std::ops::{Deref, DerefMut};

/// Work context for every nursery-anchored pause (`Nursery`, `InitialMark`,
/// `FinalMark`); the trace type is pause-aware.
pub(in crate::plan) struct BactrianNurseryGCWorkContext<VM: VMBinding>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding> crate::scheduler::GCWorkContext for BactrianNurseryGCWorkContext<VM> {
    type VM = VM;
    type PlanType = Bactrian<VM>;
    type DefaultProcessEdges = BactrianNurseryProcessEdges<VM>;
    type PinningProcessEdges = UnsupportedProcessEdges<VM>;
}

/// Work context for `Pause::Full`: GenImmix's STW full-heap collection.
pub(in crate::plan) struct BactrianSTWGCWorkContext<VM: VMBinding, const KIND: TraceKind>(
    std::marker::PhantomData<VM>,
);
impl<VM: VMBinding, const KIND: TraceKind> crate::scheduler::GCWorkContext
    for BactrianSTWGCWorkContext<VM, KIND>
{
    type VM = VM;
    type PlanType = Bactrian<VM>;
    type DefaultProcessEdges = PlanProcessEdges<VM, Bactrian<VM>, KIND>;
    type PinningProcessEdges = UnsupportedProcessEdges<VM>;
}

/// The trace for all nursery-anchored Bactrian pauses. It is the generational
/// nursery trace (young objects are promoted, transitively), extended per pause:
///
/// - `Pause::Nursery`: exactly the generational nursery trace; mature objects are
///   left to the concurrent marker (mid-cycle) or the next cycle.
///
/// - `Pause::InitialMark`: additionally *seeds* the concurrent marking queue with
///   every mature object the trace encounters (root targets, remembered-set
///   targets, and the mature children of promoted objects). Together with
///   emptying the nursery this establishes the SATB snapshot; the mature-to-
///   mature closure then runs concurrently.
///
/// - `Pause::FinalMark`: additionally *marks* (non-moving, with transitive
///   scanning via this same trace) any mature object it reaches that concurrent
///   marking missed. This makes FinalMark a true remark pause: marking is
///   complete at its end regardless of concurrent coverage — the same safety
///   structure as other SATB collectors' final remark. In the common case
///   everything reachable is already marked and this degenerates to mark-bit
///   checks. It is also what lets weak-reference processing at FinalMark
///   resurrect ("retain") mature objects correctly.
pub(in crate::plan) struct BactrianNurseryProcessEdges<VM: VMBinding> {
    plan: &'static Bactrian<VM>,
    base: ProcessEdgesBase<VM>,
    pause: Pause,
    /// InitialMark only: mature objects encountered by this trace, handed to the
    /// concurrent marker as marking roots.
    mark_seed: Vec<ObjectReference>,
}

/// Slot visitor that just collects slots into a scratch buffer, for the UP
/// direct-trace drain below.
struct SlotCollector<'a, S: Slot>(&'a mut Vec<S>);
impl<S: Slot> crate::vm::SlotVisitor<S> for SlotCollector<'_, S> {
    fn visit_slot(&mut self, slot: S) {
        self.0.push(slot);
    }
}

impl<VM: VMBinding> BactrianNurseryProcessEdges<VM> {
    /// Match ProcessEdgesWork's own buffer sizing for the seed packets.
    const SEED_CAPACITY: usize = 4096;

    /// UP direct-trace closure: with a single tracer inside a stopped-world
    /// pause, consume the whole transitive closure inside THIS packet with an
    /// explicit work list — stock oldify's todo-list discipline — instead of
    /// bouncing every generation of the BFS through packet creation, bucket
    /// scheduling and a fresh ProcessEdges instance. Per object this performs
    /// exactly the packet path's protocol (support_slot_enqueuing → scan_object
    /// → post_scan_object → process each slot), so trace semantics, line
    /// marking at scan time, InitialMark seed collection and the FinalMark
    /// remark all behave identically; only the scheduling round-trips go away.
    fn drain_closure_locally(&mut self) {
        use crate::vm::Scanning;
        let tls = self.worker().tls;
        let mut scratch: Vec<SlotOf<Self>> = Vec::new();
        loop {
            let nodes = self.pop_nodes();
            if nodes.is_empty() {
                break;
            }
            for object in nodes {
                // The OCaml binding always supports slot enqueuing (trait
                // default). The packet path would fall back to
                // scan_object_and_trace_edges otherwise; this drain does not.
                debug_assert!(<VM as VMBinding>::VMScanning::support_slot_enqueuing(
                    tls, object
                ));
                {
                    let mut collector = SlotCollector(&mut scratch);
                    <VM as VMBinding>::VMScanning::scan_object(tls, object, &mut collector);
                }
                self.plan.post_scan_object(object);
                for i in 0..scratch.len() {
                    self.process_slot(scratch[i]);
                }
                scratch.clear();
            }
        }
    }

    fn flush_mark_seed(&mut self) {
        if !self.mark_seed.is_empty() {
            let objects = std::mem::take(&mut self.mark_seed);
            let w = ConcurrentTraceObjects::<VM, Bactrian<VM>, TRACE_KIND_FAST>::new(
                objects,
                self.base.mmtk(),
            );
            // Like ProcessRootSlots: park the packets in the Concurrent bucket
            // without notifying — the scheduler opens the bucket (and wakes workers)
            // when the pause ends.
            self.base.mmtk().scheduler.work_buckets[WorkBucketStage::Concurrent].add_no_notify(w);
        }
    }
}

impl<VM: VMBinding> ProcessEdgesWork for BactrianNurseryProcessEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = PlanScanObjects<Self, Bactrian<VM>>;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        let plan = base.plan().downcast_ref::<Bactrian<VM>>().unwrap();
        // The pause kind is fixed for the duration of a collection; latch it here.
        // (Packets of this type only ever run inside a pause.)
        let pause = plan.current_pause().unwrap_or(Pause::Nursery);
        Self {
            plan,
            base,
            pause,
            mark_seed: Vec::new(),
        }
    }

    fn trace_object(&mut self, object: ObjectReference) -> ObjectReference {
        // We cannot borrow `self` twice in a call, so extract `worker` first.
        let worker = self.worker();
        let new_object = self
            .plan
            .trace_object_nursery::<VectorObjectQueue, DEFAULT_TRACE>(
                &mut self.base.nodes,
                object,
                worker,
            );
        // `new_object == object` means the object was already mature (promotion
        // returns the new copy, which post_copy born-black-marked and which this
        // trace scans transitively; an LOS "promotion" is in place and is idempotent
        // under both treatments below).
        if new_object == object && !self.plan.is_object_in_nursery(object) {
            match self.pause {
                Pause::InitialMark => {
                    crate::plan::concurrent::diag::SEEDED
                        .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    self.mark_seed.push(object);
                    if self.mark_seed.len() >= Self::SEED_CAPACITY {
                        self.flush_mark_seed();
                    }
                }
                Pause::FinalMark => {
                    // Remark: mark (non-moving) anything concurrent marking missed;
                    // newly-marked objects are enqueued to base.nodes and scanned
                    // with this same trace. Already-marked objects are a mark-bit
                    // check only.
                    let marked = self.plan.trace_object::<VectorObjectQueue, TRACE_KIND_FAST>(
                        &mut self.base.nodes,
                        object,
                        worker,
                    );
                    debug_assert_eq!(marked, object, "FinalMark remark must not move");
                }
                _ => (),
            }
        }
        new_object
    }

    fn process_slot(&mut self, slot: SlotOf<Self>) {
        let Some(object) = slot.load() else {
            return;
        };
        let new_object = self.trace_object(object);
        // With survivor aging, a trace result may legitimately be YOUNG (in the
        // aged to-space); it must only never remain in the nursery proper.
        debug_assert!(!self.plan.gen.nursery.in_space(new_object));
        if new_object != object {
            slot.store(new_object);
        }
    }

    fn flush(&mut self) {
        // Single tracer: finish the whole closure here (see drain_closure_locally).
        // Gated off when live-bytes stats are requested — the packet path is the
        // one that accounts them.
        if crate::util::up_trace::up()
            && !*self.base.mmtk().get_options().count_live_bytes_in_gc
        {
            self.drain_closure_locally();
        }
        self.flush_mark_seed();
        // Default flush behaviour: hand accumulated nodes to a scan-objects packet.
        let nodes = self.pop_nodes();
        if !nodes.is_empty() {
            self.start_or_dispatch_scan_work(self.create_scan_work(nodes));
        }
    }

    fn create_scan_work(&self, nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType {
        PlanScanObjects::new(self.plan, nodes, false, self.bucket)
    }
}

impl<VM: VMBinding> Drop for BactrianNurseryProcessEdges<VM> {
    fn drop(&mut self) {
        // Safety net: flush any buffered marking seeds when the instance is dropped.
        // Most callers go through the blanket `GCWork for E` do_work (which flushes),
        // but a few construct a ProcessEdgesWork and drop it without calling flush()
        // — e.g. `ProcessEdgesWorkTracerContext::with_tracer` (weak-ref "retain"
        // processing). Losing seeds means live mature objects escape the snapshot and
        // get swept at FinalMark.
        self.flush_mark_seed();
    }
}

impl<VM: VMBinding> Deref for BactrianNurseryProcessEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for BactrianNurseryProcessEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
