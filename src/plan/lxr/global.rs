use super::gc_work::FastRCPrepare;
use super::gc_work::LXRRCWorkContext;
use super::mutator::ALLOCATOR_MAPPING;
use super::rc::ProcessDecs;
use super::rc::RCImmixCollectRootEdges;
use crate::policy::immix::block::Block;
use crate::scheduler::gc_work::Release;
use crate::scheduler::gc_work::StopMutators;
use crate::scheduler::gc_work::UnsupportedProcessEdges;
use crate::LazySweepingJobsCounter;
use crossbeam::queue::SegQueue;
use std::sync::RwLock;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::immix::ImmixSpaceArgs;
use crate::policy::space::Space;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::heap::VMRequest;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::rc::RefCountHelper;
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use crate::util::ObjectReference;
use crate::{policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use std::sync::atomic::AtomicBool;

use super::Pause;
use atomic::Atomic;
use atomic::Ordering;
use enum_map::EnumMap;

use mmtk_macros::{HasSpaces, PlanTraceObject};

// LXR (reference counting on a hierarchical Immix heap) — P3.5 skeleton.
//
// Built UP from our base Immix plan (plan/immix/global.rs), NOT down from the
// reference's 1268-line global.rs (see LXR_PORT_PLAN.md). At this stage the LXR
// plan is a structural clone of Immix with `rc_enabled = false`, so selecting
// `MMTK_PLAN=LXR` runs identically to Immix and trips none of the P2
// `debug_assert(!rc_enabled)` guards. The reference-counting behaviour (field
// barrier install, RefCountHelper field, Pause::RefCount, ProcessIncs/Decs, the
// RC sweep machinery) is layered on incrementally in later P3 steps, only flipping
// `rc_enabled = true` once the machinery the asserts guard is actually present.
#[derive(HasSpaces, PlanTraceObject)]
pub struct LXR<VM: VMBinding> {
    #[post_scan]
    #[space]
    #[copy_semantics(CopySemantics::DefaultCopy)]
    pub immix_space: ImmixSpace<VM>,
    #[parent]
    pub common: CommonPlan<VM>,
    last_gc_was_defrag: AtomicBool,
    /// Reference-counting helper (RC_TABLE access + promote/dead bookkeeping). Inert
    /// until the RC trace (`ProcessIncs`/`ProcessDecs`) and the field barrier are wired
    /// and `rc_enabled` is flipped on; present now as the foundation those steps build on.
    #[allow(dead_code)] // read by the RC trace (ProcessIncs/ProcessDecs), wired in a later P3 step
    pub rc: RefCountHelper<VM>,
    /// The kind of the in-progress GC pause (`None` outside a pause). Set in `schedule_collection`,
    /// read by the RC trace + scheduling. Mirrors the same field on the ConcurrentImmix plan.
    current_pause: Atomic<Option<Pause>>,
    /// The kind of the previous GC pause. Deferred / always `None` in the minimal RC cut.
    #[allow(dead_code)]
    previous_pause: Atomic<Option<Pause>>,
    /// Roots collected in the PREVIOUS GC, kept alive by an extra reference count, to be
    /// decremented at the start of THIS GC (`process_prev_roots`). Each entry is one
    /// root-edge-scan's worth of root targets. `RwLock<SegQueue<..>>` matches the reference so the
    /// `release`-time swap (`mem::swap(prev, curr)`) is a cheap pointer swap.
    pub prev_roots: RwLock<SegQueue<Vec<ObjectReference>>>,
    /// Roots collected in THIS GC (pushed by the RC root scan); become `prev_roots` at release.
    pub curr_roots: RwLock<SegQueue<Vec<ObjectReference>>>,
}

/// The plan constraints for the LXR plan. **RC ACTIVATED**: `rc_enabled = true`,
/// `needs_field_log_bit = true`, `barrier = FieldBarrier`. `needs_log_bit` is also true (the
/// per-field unlog metadata the barrier indexes lives in the global field-unlog spec, which the
/// log-bit machinery maps).
///
/// `moves_objects = false`: the minimal single-domain RC cut promotes nursery objects **in place**
/// and never evacuates (mature evac / nursery copying are deferred), so the GC is non-moving. This
/// is intentionally stricter than the reference LXR (a moving GC) and keeps `sanity` honest (no
/// forwarding to validate).
pub const LXR_CONSTRAINTS: PlanConstraints = PlanConstraints {
    moves_objects: false,
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
    needs_log_bit: true,
    needs_field_log_bit: true,
    rc_enabled: true,
    barrier: crate::plan::barriers::BarrierSelector::FieldBarrier,
    ..PlanConstraints::default()
};

impl<VM: VMBinding> Plan for LXR<VM> {
    fn collection_required(&self, space_full: bool, _space: Option<SpaceStats<Self::VM>>) -> bool {
        self.base().collection_required(self, space_full)
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        self.immix_space
            .is_last_gc_exhaustive(self.last_gc_was_defrag.load(Ordering::Relaxed))
    }

    fn constraints(&self) -> &'static PlanConstraints {
        &LXR_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        use enum_map::enum_map;
        CopyConfig {
            copy_mapping: enum_map! {
                CopySemantics::DefaultCopy => CopySelector::Immix(0),
                _ => CopySelector::Unused,
            },
            space_mapping: vec![(CopySelector::Immix(0), &self.immix_space)],
            constraints: &LXR_CONSTRAINTS,
        }
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        // Lazily wire `block_allocation`'s self-reference (idempotent).
        self.ensure_block_allocation_initialized();
        // Minimal RC cut: the only pause kind is RefCount (no CM, no emergency full GC, no defrag).
        let pause = self.select_collection_kind();
        self.current_pause.store(Some(pause), Ordering::SeqCst);
        match pause {
            Pause::RefCount => self.schedule_rc_collection(scheduler),
            // The minimal cut never schedules these (select_collection_kind only returns RefCount).
            _ => unreachable!("minimal LXR cut only schedules RefCount pauses, got {:?}", pause),
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        // RC pause prepare (run from FastRCPrepare in the RCProcessIncs bucket — the normal Prepare
        // bucket is disabled for a RefCount pause). The minimal cut is RefCount-only.
        let pause = self.current_pause().unwrap();
        debug_assert_eq!(pause, Pause::RefCount);
        // `false` = not a full/major heap prepare (no mark-based tracing in the RC pause).
        self.common.prepare(tls, false);
        self.immix_space.prepare_rc(pause);
    }

    fn release(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        debug_assert_eq!(pause, Pause::RefCount);
        self.common.release(tls, false);
        self.immix_space.release_rc(pause);
        // Swap roots: this GC's collected roots become next GC's prev_roots (to be decremented).
        {
            let mut prev_roots = self.prev_roots.write().unwrap();
            let mut curr_roots = self.curr_roots.write().unwrap();
            std::mem::swap::<SegQueue<_>>(&mut prev_roots, &mut curr_roots);
            debug_assert!(curr_roots.is_empty());
        }
        // Bump the global phase epoch at the end of the pause.
        Block::update_global_phase_epoch(&self.immix_space);
    }

    fn end_of_gc(&mut self, tls: VMWorkerThread) {
        self.previous_pause
            .store(self.current_pause(), Ordering::SeqCst);
        self.current_pause.store(None, Ordering::SeqCst);
        self.last_gc_was_defrag.store(false, Ordering::Relaxed);
        self.common.end_of_gc(tls);
    }

    fn current_gc_may_move_object(&self) -> bool {
        // Minimal RC cut: in-place promotion only, never moves objects.
        false
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.immix_space.defrag_headroom_pages()
    }

    fn get_used_pages(&self) -> usize {
        self.immix_space.reserved_pages() + self.common.get_used_pages()
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.common.base
    }

    fn base_mut(&mut self) -> &mut BasePlan<Self::VM> {
        &mut self.common.base
    }

    fn common(&self) -> &CommonPlan<VM> {
        &self.common
    }
}

impl<VM: VMBinding> LXR<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        // P4: map the VM-side global log bit (needs_log_bit) + the per-field unlog bit
        // (needs_field_log_bit) the coalescing field barrier uses. The binding lays the
        // field-unlog spec `side_after` the log bit, so they occupy disjoint regions.
        let mut spec = crate::util::metadata::extract_side_metadata(&[
            *VM::VMObjectModel::GLOBAL_LOG_BIT_SPEC,
            *VM::VMObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC,
        ]);
        // RC_TABLE is a global side-metadata spec (whole-heap reference counts), but it is only
        // DEFINED in spec_defs — no plan/space registered it in its global metadata context, so its
        // per-chunk pages were never mapped/committed. Without this, rc.count/rc.inc fault reading
        // uncommitted metadata on the first RC pause (the object is valid; only its RC metadata is
        // uncommitted). Register it so the whole heap's RC counts are backed.
        spec.push(crate::util::rc::RC_TABLE);
        let mut plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &LXR_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(&spec),
        };
        let lxr = LXR {
            immix_space: ImmixSpace::new(
                plan_args.get_normal_space_args(
                    "immix",
                    true,
                    false,
                    VMRequest::discontiguous(),
                ),
                ImmixSpaceArgs {
                    mixed_age: false,
                    // Minimal RC cut: in-place promotion only, never evacuate. Matches
                    // `LXR_CONSTRAINTS.moves_objects = false`.
                    never_move_objects: true,
                },
            ),
            common: CommonPlan::new(plan_args),
            last_gc_was_defrag: AtomicBool::new(false),
            rc: RefCountHelper::NEW,
            current_pause: Atomic::new(None),
            previous_pause: Atomic::new(None),
            prev_roots: RwLock::new(SegQueue::new()),
            curr_roots: RwLock::new(SegQueue::new()),
        };

        lxr.verify_side_metadata_sanity();

        lxr
    }

    /// One-time hook to give `block_allocation` its self-reference + space pointer. Idempotent;
    /// run lazily at the top of `schedule_collection` (the first `&'static self` entry point). The
    /// reference does this in a `gc_init` hook, which our base plan-creation flow lacks.
    fn ensure_block_allocation_initialized(&'static self) {
        if self.immix_space.block_allocation.lxr.is_none() {
            // # Safety: `self` is `&'static` (the plan is boxed-and-leaked into MMTK), and the
            // `block_allocation` lives inside `self.immix_space`, so these pointers are stable for
            // the plan's whole lifetime. We mutate through a shared ref because `block_allocation`'s
            // `lxr`/`space` are set-once; no concurrent writer (this runs at the start of a STW
            // collection, single-threaded).
            #[allow(invalid_reference_casting)]
            let ba = unsafe {
                &mut *(&self.immix_space.block_allocation
                    as *const crate::policy::immix::block_allocation::BlockAllocation<VM>
                    as *mut crate::policy::immix::block_allocation::BlockAllocation<VM>)
            };
            ba.init(&self.immix_space);
            ba.lxr = Some(self);
        }
    }
}

// ── LXR RC-trace support (P3) ─────────────────────────────────────────────────────────────────
// The reference-counting work packets (`ProcessIncs`/`ProcessDecs`/`RCImmixCollectRootEdges`),
// the field barrier, and `block_allocation.rs` query the plan through this small surface. In the
// minimal single-domain RC cut the CM/defrag-flavoured queries are hard `false`/`None` (concurrent
// marking, mature evacuation, and lazy decrements are deferred). RC is now ACTIVE (`rc_enabled =
// true`): the RefCount pause runs these on `MMTK_PLAN=LXR`.
#[allow(dead_code)]
impl<VM: VMBinding> LXR<VM> {
    /// The large-object space (RC dec'd objects that overflow immix live here). Returns the shared
    /// CommonPlan LOS — note the *RC-aware* LOS nursery tracking / `rc_free` is deferred, so this is
    /// the standard mark-swept LOS for now.
    pub fn los(&self) -> &crate::policy::largeobjectspace::LargeObjectSpace<VM> {
        self.common.get_los()
    }

    /// Is a concurrent-marking (SATB) cycle in progress? Deferred under `lxr_no_cm` → always false.
    pub fn cm_in_progress(&self) -> bool {
        false
    }

    /// Is concurrent marking enabled at all? Deferred (`lxr_no_cm`) → always false.
    pub fn cm_enabled(&self) -> bool {
        false
    }

    /// Convenience used by the reference's `block_allocation` cm-gate. Deferred → false.
    pub fn cm_in_progress_or_final_mark(&self) -> bool {
        self.cm_in_progress() || self.current_pause() == Some(Pause::FinalMark)
    }

    /// The kind of the in-progress pause, or `None` outside a pause. Set in `schedule_collection`,
    /// cleared in `end_of_gc`.
    pub fn current_pause(&self) -> Option<Pause> {
        self.current_pause.load(Ordering::Relaxed)
    }

    /// The kind of the previous pause. Deferred / always `None` in the minimal RC cut.
    pub fn previous_pause(&self) -> Option<Pause> {
        self.previous_pause.load(Ordering::Relaxed)
    }

    /// Is `object` marked (live) by the mark bit? Forwarded to the immix space's RC read-side.
    pub fn is_marked(&self, object: ObjectReference) -> bool {
        self.immix_space.is_marked(object)
    }

    /// Atomically mark `object` (0→1); returns true iff this call did the marking. Only used by the
    /// CM/SATB dec path (deferred), so inert in the minimal cut.
    pub fn mark(&self, object: ObjectReference) -> bool {
        self.immix_space.attempt_mark_rc(object)
    }

    /// Is `object` in a block selected for mature defrag-evacuation? Deferred (no mature evac) → false.
    pub fn in_defrag(&self, _object: ObjectReference) -> bool {
        false
    }

    /// Is `addr` in a defrag-source block? Deferred (no mature evac) → false.
    pub fn address_in_defrag(&self, _addr: crate::util::Address) -> bool {
        false
    }
}

// ── LXR RC pause scheduling (P3 activation) ───────────────────────────────────────────────────
impl<VM: VMBinding> LXR<VM> {
    /// Decide the pause kind. Minimal cut: always `RefCount` — emergency full GC, defrag, and the
    /// SATB initial/final-mark pauses are all DEFERRED (no CM, no mature evac). The heap is sized so
    /// OOM (which would force a `Full` fallback) does not fire; if it ever does, the
    /// `schedule_collection` `unreachable!` will surface it loudly.
    fn select_collection_kind(&self) -> Pause {
        Pause::RefCount
    }

    /// Disable the work buckets a RefCount pause does not use, so stray packets never run. Adapted
    /// from lxr-v0.32.0; trimmed to the stages that exist in our base. The RC pause keeps
    /// `Unconstrained`, `Initial` (= `RCProcessIncs`), `STWRCDecsAndSweep`, `Release`, `Final` (and
    /// `ClearVOBits` under `vo_bit`) enabled. NOTE: unlike the reference, we DO NOT disable `Prepare`
    /// or `Closure` — our base root-scan plumbing routes `ScanMutatorRoots`/`ScanVMSpecificRoots`
    /// through `Prepare`, and the RC root packets are routed to `RCProcessIncs` via `RC_ROOTS`
    /// (so `Closure` carries no RC work but is harmless if left enabled). Disabling the strictly-
    /// unused ref-closure / forwarding / compact stages keeps the pause clean.
    fn disable_unnecessary_buckets(&self, scheduler: &GCWorkScheduler<VM>, pause: Pause) {
        debug_assert_eq!(pause, Pause::RefCount);
        use WorkBucketStage::*;
        for stage in [
            Closure,
            SoftRefClosure,
            WeakRefClosure,
            FinalRefClosure,
            PhantomRefClosure,
            CalculateForwarding,
            SecondRoots,
            RefForwarding,
            FinalizableForwarding,
            Compact,
        ] {
            scheduler.work_buckets[stage].set_enabled(false);
        }
    }

    /// Wrap the previous GC's roots into `ProcessDecs` packets (their extra root reference count is
    /// dropped now). The minimal cut runs decs stop-the-world in `STWRCDecsAndSweep` (lazy decrements
    /// are deferred). Always schedules at least one (possibly empty) packet so the bucket opens.
    fn process_prev_roots(&self, scheduler: &GCWorkScheduler<VM>) {
        let prev_roots = self.prev_roots.write().unwrap();
        let mut work_packets: Vec<Box<dyn GCWork<VM>>> = Vec::with_capacity(prev_roots.len());
        while let Some(decs) = prev_roots.pop() {
            work_packets.push(Box::new(ProcessDecs::new(
                decs,
                LazySweepingJobsCounter::new_decs(),
            )));
        }
        if work_packets.is_empty() {
            work_packets.push(Box::new(ProcessDecs::new(
                vec![],
                LazySweepingJobsCounter::new_decs(),
            )));
        }
        scheduler.work_buckets[WorkBucketStage::STWRCDecsAndSweep].bulk_add(work_packets);
    }

    /// Schedule a RefCount pause. Adapted from lxr-v0.32.0 `schedule_rc_collection`:
    /// (1) disable unused buckets, (2) decrement the previous GC's roots, (3) StopMutators (whose
    /// root scan flows through `RCImmixCollectRootEdges` → `ProcessIncs<ROOT>`), (4) `FastRCPrepare`
    /// (runs `prepare_rc`) in `RCProcessIncs`, (5) `Release` in `Release`. CM/mature-evac packets are
    /// omitted (deferred).
    fn schedule_rc_collection(&'static self, scheduler: &GCWorkScheduler<VM>) {
        self.disable_unnecessary_buckets(scheduler, Pause::RefCount);
        self.process_prev_roots(scheduler);
        type RootEdges<VM> = RCImmixCollectRootEdges<VM>;
        // Plain `add` (not `add_prioritized`): our base's `Unconstrained` bucket has no
        // prioritized queue (the reference's does), and the base schedules StopMutators the
        // same way (scheduler.rs). The prioritization was only a latency optimisation.
        scheduler.work_buckets[WorkBucketStage::Unconstrained]
            .add(StopMutators::<LXRRCWorkContext<RootEdges<VM>>>::new());
        scheduler.work_buckets[WorkBucketStage::RCProcessIncs].add(FastRCPrepare);
        scheduler.work_buckets[WorkBucketStage::Release]
            .add(Release::<LXRRCWorkContext<UnsupportedProcessEdges<VM>>>::new(self));
    }
}
