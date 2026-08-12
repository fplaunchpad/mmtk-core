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

/// Sliced-STW mark quantum: pops parked marking packets (see
/// `Bactrian::parked_marking`) and executes them on this worker, world
/// stopped, until the queue empties or the budget expires. Scheduled in the
/// Release stage of mid-cycle Nursery pauses (budgeted — stock OCaml's
/// allocation-paced mark slice, one per minor) and in the Closure stage of
/// FinalMark (unbudgeted — drain everything, including SATB flushes parked
/// during StopMutators).
pub(in crate::plan) struct BactrianMarkQuantum<VM: VMBinding> {
    plan: &'static Bactrian<VM>,
    budget: Option<std::time::Duration>,
}

/// Per-quantum budget. MMTK_MARK_SLICE_MS overrides (fractional ok);
/// default 2ms — comparable to a nursery pause at the stock-parity 2 MiB
/// nursery, so mid-cycle pauses stay in vanilla's slice-pause class.
fn mark_slice_budget() -> std::time::Duration {
    static V: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        let ms = std::env::var("MMTK_MARK_SLICE_MS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|ms| *ms > 0.0 && *ms <= 1000.0)
            .unwrap_or(2.0);
        std::time::Duration::from_secs_f64(ms / 1e3)
    })
}

impl<VM: VMBinding> BactrianMarkQuantum<VM> {
    pub(in crate::plan) fn budgeted(plan: &'static Bactrian<VM>) -> Self {
        Self {
            plan,
            budget: Some(mark_slice_budget()),
        }
    }
    pub(in crate::plan) fn unbudgeted(plan: &'static Bactrian<VM>) -> Self {
        Self { plan, budget: None }
    }
}

impl<VM: VMBinding> crate::scheduler::GCWork<VM> for BactrianMarkQuantum<VM> {
    fn do_work(
        &mut self,
        worker: &mut crate::scheduler::GCWorker<VM>,
        mmtk: &'static MMTK<VM>,
    ) {
        let deadline = self.budget.map(|b| std::time::Instant::now() + b);
        let mut packets = 0usize;
        // Mid-cycle marking marks MATURE objects; the enclosing pause is a
        // NURSERY pause whose prepare latched the LOS space to nursery
        // semantics, under which los.trace_object SKIPS mature objects — the
        // quantum would silently drop them from the cycle and FinalMark's
        // sweep would free them live (NOTES 2026-08-12). Scope full-heap LOS
        // semantics over the drain; restore after. Treadmill ops are
        // internally locked, and the pause's own LOS work (nursery sweep in
        // Release) touches the disjoint nursery lists.
        let was_full = mmtk
            .get_plan()
            .common()
            .los
            .set_marking_full_semantics(true);
        while let Some(mut w) = self.plan.pop_marking_packet() {
            w.do_work(worker, mmtk);
            packets += 1;
            if let Some(d) = deadline {
                if std::time::Instant::now() >= d {
                    break;
                }
            }
        }
        mmtk.get_plan()
            .common()
            .los
            .set_marking_full_semantics(was_full);
        probe!(mmtk, bactrian_mark_quantum, packets);
    }
}

/// Incremental-sweep quantum: pops deferred chunk-sweep packets (see
/// `Bactrian::parked_sweep`) and executes them, world stopped, until the
/// queue empties or the budget expires — stock OCaml's sweep slices,
/// scheduled in the Release stage of nursery pauses after FinalMark.
/// Unbudgeted when the pacing wants the next cycle (drain-to-completion).
/// The freed blocks flow to the page resource per packet, so RSS falls
/// incrementally across the minors instead of at one FinalMark cliff.
pub(in crate::plan) struct BactrianSweepQuantum<VM: VMBinding> {
    plan: &'static Bactrian<VM>,
    budget: Option<std::time::Duration>,
}

/// Per-quantum sweep budget. MMTK_SWEEP_SLICE_MS overrides; default 2ms
/// (a chunk-sweep packet is ~fast: line-mark scans over 4MB of blocks).
fn sweep_slice_budget() -> std::time::Duration {
    static V: std::sync::OnceLock<std::time::Duration> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        let ms = std::env::var("MMTK_SWEEP_SLICE_MS")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .filter(|ms| *ms > 0.0 && *ms <= 1000.0)
            .unwrap_or(2.0);
        std::time::Duration::from_secs_f64(ms / 1e3)
    })
}

impl<VM: VMBinding> BactrianSweepQuantum<VM> {
    pub(in crate::plan) fn budgeted(plan: &'static Bactrian<VM>) -> Self {
        Self { plan, budget: Some(sweep_slice_budget()) }
    }
    pub(in crate::plan) fn unbudgeted(plan: &'static Bactrian<VM>) -> Self {
        Self { plan, budget: None }
    }
}

impl<VM: VMBinding> crate::scheduler::GCWork<VM> for BactrianSweepQuantum<VM> {
    fn do_work(
        &mut self,
        worker: &mut crate::scheduler::GCWorker<VM>,
        mmtk: &'static MMTK<VM>,
    ) {
        let deadline = self.budget.map(|b| std::time::Instant::now() + b);
        let mut packets = 0usize;
        loop {
            let Some(mut w) = self.plan.pop_sweep_packet() else {
                // Queue empty: the cycle's sweep is COMPLETE. (Single quantum
                // per pause and quanta only run world-stopped, so this edge
                // cannot race a concurrent producer — packets are only parked
                // by FinalMark, which is gated on the previous drain.)
                self.plan.sweep_queue_emptied();
                break;
            };
            w.do_work(worker, mmtk);
            packets += 1;
            if let Some(d) = deadline {
                if std::time::Instant::now() >= d {
                    // Budget expired with the queue possibly non-empty: check
                    // emptiness so a drained-on-the-last-packet quantum still
                    // flips the flag this pause.
                    if self.plan.pop_sweep_packet().map(|w2| {
                        // put it back semantics unavailable on Injector; run it —
                        // one packet of overrun keeps the logic simple.
                        let mut w2 = w2; w2.do_work(worker, mmtk); packets += 1;
                    }).is_none() {
                        self.plan.sweep_queue_emptied();
                    }
                    break;
                }
            }
        }
        probe!(mmtk, bactrian_sweep_quantum, packets);
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
            // Route via the plan: worker-concurrent mode parks in the Concurrent
            // bucket without notifying (the scheduler opens it when the pause
            // ends); sliced mode parks in the plan queue for in-pause quanta.
            self.plan.schedule_marking_packet(Box::new(w));
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
