use crate::plan::concurrent::bactrian::gc_work::BactrianNurseryGCWorkContext;
use crate::plan::concurrent::bactrian::gc_work::BactrianSTWGCWorkContext;
use crate::plan::concurrent::global::ConcurrentPlan;
use crate::plan::concurrent::Pause;
use crate::plan::generational::global::CommonGenPlan;
use crate::plan::generational::global::GenerationalPlan;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::gc_work::TraceKind;
use crate::policy::immix::defrag::StatsForDefrag;
use crate::policy::immix::ImmixSpace;
use crate::policy::immix::ImmixSpaceArgs;
use crate::policy::immix::{TRACE_KIND_DEFRAG, TRACE_KIND_FAST};
use crate::policy::space::Space;
use crate::scheduler::GCWorkScheduler;
use crate::scheduler::GCWorker;
use crate::scheduler::WorkBucketStage;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::heap::VMRequest;
use crate::util::metadata::log_bit::UnlogBitsOperation;
use crate::util::Address;
use crate::util::ObjectReference;
use crate::util::VMWorkerThread;
use crate::vm::ObjectModel;
use crate::vm::VMBinding;
use crate::ObjectQueue;

use atomic::Atomic;
use enum_map::EnumMap;
use std::sync::atomic::AtomicBool;
use std::sync::atomic::Ordering;

use mmtk_macros::{HasSpaces, PlanTraceObject};

/// Bactrian: copying nursery + concurrently-marked, STW-evacuated Immix mature space
/// with an SATB deletion barrier — the faithful MMTk realization of OCaml 5's GC.
/// See the module documentation for the design.
#[derive(HasSpaces, PlanTraceObject)]
pub struct Bactrian<VM: VMBinding> {
    /// Generational plan (the copying nursery + common spaces).
    #[parent]
    pub gen: CommonGenPlan<VM>,
    /// The mature space: concurrently marked, evacuated only at STW `Full` pauses.
    #[post_scan]
    #[space]
    #[copy_semantics(CopySemantics::Mature)]
    pub immix_space: ImmixSpace<VM>,
    /// Whether the last GC was a defrag GC for the immix space.
    last_gc_was_defrag: AtomicBool,
    current_pause: Atomic<Option<Pause>>,
    previous_pause: Atomic<Option<Pause>>,
    concurrent_marking_active: AtomicBool,
}

/// The plan constraints for the Bactrian plan.
pub const BACTRIAN_CONSTRAINTS: PlanConstraints = PlanConstraints {
    // Copying nursery always moves; the mature space moves only at STW Full pauses.
    moves_objects: true,
    // Nursery promotion copies into the mature Immix space, so nursery objects must
    // also respect the max immix object size (same reasoning as GenImmix).
    max_non_los_default_alloc_bytes: crate::util::rust_util::min_of_usize(
        crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
        crate::plan::plan_constraints::MAX_NON_LOS_ALLOC_BYTES_COPYING_PLAN,
    ),
    generational: true,
    // The unlog bit is owned exclusively by the generational (object/region
    // remembering) half of the barrier; the SATB half is slot-granular and bit-free
    // (like stock OCaml's deletion barrier).
    needs_log_bit: true,
    // The barrier selector is an indicator for VM fast paths; Bactrian's combined
    // barrier subsumes SATB (pre) + object remembering (post).
    barrier: crate::BarrierSelector::SATBBarrier,
    // The object-remembering half may enqueue the same slot/object more than once.
    may_trace_duplicate_edges: true,
    needs_prepare_mutator: true,
    ..PlanConstraints::default()
};

impl<VM: VMBinding> Plan for Bactrian<VM> {
    fn constraints(&self) -> &'static PlanConstraints {
        &BACTRIAN_CONSTRAINTS
    }

    fn create_copy_config(&'static self) -> CopyConfig<Self::VM> {
        use enum_map::enum_map;
        CopyConfig {
            copy_mapping: enum_map! {
                CopySemantics::PromoteToMature => CopySelector::ImmixHybrid(0),
                CopySemantics::Mature => CopySelector::ImmixHybrid(0),
                _ => CopySelector::Unused,
            },
            space_mapping: vec![(CopySelector::ImmixHybrid(0), &self.immix_space)],
            constraints: &BACTRIAN_CONSTRAINTS,
        }
    }

    fn collection_required(&self, space_full: bool, space: Option<SpaceStats<Self::VM>>) -> bool
    where
        Self: Sized,
    {
        // Concurrent marking finished all its work: transition to FinalMark at the
        // next poll site. (The GC-worker side self-trigger in the scheduler covers
        // the case where no mutator polls; see Scheduler::concurrent_marking_drained.)
        if self.concurrent_marking_in_progress()
            && self.gen.common.base.scheduler.work_buckets[WorkBucketStage::Concurrent]
                .is_drained()
        {
            return true;
        }
        self.gen.collection_required(self, space_full, space)
    }

    fn last_collection_was_exhaustive(&self) -> bool {
        self.previous_pause() == Some(Pause::Full)
            && self
                .immix_space
                .is_last_gc_exhaustive(self.last_gc_was_defrag.load(Ordering::Relaxed))
    }

    fn schedule_collection(&'static self, scheduler: &GCWorkScheduler<Self::VM>) {
        let pause = self.decide_pause();
        self.trace_pause("schedule", pause);
        self.current_pause.store(Some(pause), Ordering::SeqCst);
        // `gc_full_heap` drives `is_current_gc_nursery()`: every pause except Full is
        // a nursery-collecting pause (ProcessModBuf/weak-processing rely on this).
        self.gen
            .gc_full_heap
            .store(pause == Pause::Full, Ordering::SeqCst);

        probe!(mmtk, concurrent_pause_determined, pause as usize);

        match pause {
            Pause::Full => {
                crate::plan::immix::global::Immix::schedule_immix_full_heap_collection::<
                    Bactrian<VM>,
                    BactrianSTWGCWorkContext<VM, TRACE_KIND_FAST>,
                    BactrianSTWGCWorkContext<VM, TRACE_KIND_DEFRAG>,
                >(self, &self.immix_space, scheduler);
            }
            Pause::InitialMark | Pause::FinalMark | Pause::Nursery => {
                scheduler.schedule_common_work::<BactrianNurseryGCWorkContext<VM>>(self);
            }
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &crate::plan::generational::ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        match pause {
            Pause::Full => {
                // GenImmix's full-heap protocol: bulk-clear unlog bits; the full trace
                // reconstructs them (post_copy / unlog_object_if_needed).
                self.gen.prepare(tls);
                self.immix_space.prepare(
                    true,
                    Some(StatsForDefrag::new(self)),
                    UnlogBitsOperation::BulkClear,
                );
            }
            Pause::InitialMark => {
                // A nursery collection fused with the start of a marking cycle. The
                // nursery prepares as in a minor GC, while the mature/common spaces
                // prepare for a new (full) mark cycle. Unlike ConcurrentImmix we do
                // NOT bulk-set unlog bits: the SATB barrier is slot-granular and
                // bit-free; the unlog bit stays owned by the generational barrier.
                self.gen.full_heap_gc_count.lock().unwrap().inc();
                self.gen.nursery.prepare(true);
                self.gen
                    .nursery
                    .set_copy_for_sft_trace(Some(CopySemantics::PromoteToMature));
                self.gen.common.prepare(tls, true);
                self.immix_space.prepare(
                    true,
                    Some(StatsForDefrag::new(self)),
                    UnlogBitsOperation::NoOp,
                );
                // The marking cycle starts NOW, not at end_of_gc: this pause's own
                // Closure stage promotes the whole live nursery into the (just
                // prepared) mature space, and those promotions must be born live —
                // post_copy gives them the new cycle's object mark bit, but with
                // MARK_LINE_AT_SCAN_TIME their LINES are only marked by the eager
                // allocate_as_live path. Arming it here (Prepare stage, strictly
                // before Closure opens) makes InitialMark promotions survive the
                // FinalMark sweep. (ConcurrentImmix flips this at end_of_gc, but it
                // has no in-pause promotions.)
                self.set_concurrent_marking_state(true);
            }
            Pause::Nursery => {
                // Plain minor collection (GenImmix's nursery prepare).
                self.gen.prepare(tls);
            }
            Pause::FinalMark => {
                // A nursery collection that completes the marking cycle. Only the
                // nursery needs preparing; the mature/common spaces were prepared at
                // InitialMark and have been marked concurrently since.
                self.gen.nursery.prepare(true);
                self.gen
                    .nursery
                    .set_copy_for_sft_trace(Some(CopySemantics::PromoteToMature));
            }
        }
    }

    fn release(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();
        match pause {
            Pause::Full => {
                self.gen.release(tls);
                // Unlog bits were reconstructed during tracing; keep them.
                self.immix_space.release(true, UnlogBitsOperation::NoOp);
            }
            Pause::InitialMark | Pause::Nursery => {
                // Minor collection: release the nursery (and common spaces at nursery
                // level). The mature space is untouched — for InitialMark its sweep
                // happens at FinalMark, after marking completes.
                self.gen.release(tls);
            }
            Pause::FinalMark => {
                // Nursery release + mature/common sweep over the completed mark state.
                // Unlog bits stay owned by the generational protocol: remembered
                // objects were re-unlogged by ProcessModBuf during this (nursery)
                // pause; everything else is untouched.
                self.gen.nursery.release();
                self.gen.common.release(tls, true);
                self.immix_space.release(true, UnlogBitsOperation::NoOp);
            }
        }
    }

    fn end_of_gc(&mut self, tls: VMWorkerThread) {
        let pause = self.current_pause().unwrap();

        let next_gc_full_heap = CommonGenPlan::should_next_gc_be_full_heap(self);
        self.gen.end_of_gc(tls, next_gc_full_heap);

        let did_defrag = self.immix_space.end_of_gc();
        self.last_gc_was_defrag.store(did_defrag, Ordering::Relaxed);

        match pause {
            Pause::InitialMark => {
                // Marking state was already armed in prepare() (this pause's own
                // promotions must be born live); nothing further to do here.
                debug_assert!(self.concurrent_marking_in_progress());
            }
            Pause::FinalMark => {
                // The sweep (Release) has completed; end the cycle. This is
                // deliberately later than ConcurrentImmix's notify_mutators_paused:
                // promotions during the FinalMark pause itself must still be born
                // live (eager line marks) to survive this pause's sweep.
                self.set_concurrent_marking_state(false);
            }
            Pause::Full | Pause::Nursery => (),
        }

        self.previous_pause.store(Some(pause), Ordering::SeqCst);
        self.current_pause.store(None, Ordering::SeqCst);
        self.trace_pause("end", pause);
        info!("{:?} end", pause);
    }

    fn current_gc_may_move_object(&self) -> bool {
        // Every pause moves young objects (nursery evacuation); Full may also defrag.
        true
    }

    fn get_collection_reserved_pages(&self) -> usize {
        self.gen.get_collection_reserved_pages() + self.immix_space.defrag_headroom_pages()
    }

    fn get_used_pages(&self) -> usize {
        self.gen.get_used_pages() + self.immix_space.reserved_pages()
    }

    /// Return the number of pages available for allocation. Assuming all future
    /// allocations go to the nursery (same as GenImmix).
    fn get_available_pages(&self) -> usize {
        (self
            .get_total_pages()
            .saturating_sub(self.get_reserved_pages()))
            >> 1
    }

    fn base(&self) -> &BasePlan<VM> {
        &self.gen.common.base
    }

    fn base_mut(&mut self) -> &mut BasePlan<Self::VM> {
        &mut self.gen.common.base
    }

    fn common(&self) -> &CommonPlan<VM> {
        &self.gen.common
    }

    fn generational(&self) -> Option<&dyn GenerationalPlan<VM = VM>> {
        Some(self)
    }

    fn concurrent(&self) -> Option<&dyn ConcurrentPlan<VM = VM>> {
        Some(self)
    }

    fn notify_mutators_paused(&self, _scheduler: &GCWorkScheduler<VM>) {
        let pause = self.current_pause().unwrap();
        match pause {
            Pause::Full | Pause::Nursery => {
                debug_assert!(
                    pause == Pause::Nursery || !self.concurrent_marking_in_progress(),
                    "Full pause scheduled while marking is in progress"
                );
            }
            Pause::InitialMark => {
                debug_assert!(
                    !self.concurrent_marking_in_progress(),
                    "prev pause: {:?}",
                    self.previous_pause()
                );
            }
            Pause::FinalMark => {
                debug_assert!(self.concurrent_marking_in_progress());
                // Mutator SATB buffers were flushed by StopMutators (flush_mutator).
                // Marking stays "active" through this pause: the SATB packets flushed
                // above still route to the (open) Concurrent bucket, which drains
                // before the STW stages advance, and promotions in this pause must be
                // born live. The state is cleared in end_of_gc, after the sweep.
            }
        }
        info!("{:?} start", pause);
    }
}

impl<VM: VMBinding> GenerationalPlan for Bactrian<VM> {
    fn is_current_gc_nursery(&self) -> bool {
        self.gen.is_current_gc_nursery()
    }

    fn is_object_in_nursery(&self, object: ObjectReference) -> bool {
        self.gen.nursery.in_space(object)
    }

    fn is_address_in_nursery(&self, addr: Address) -> bool {
        self.gen.nursery.address_in_space(addr)
    }

    fn get_mature_physical_pages_available(&self) -> usize {
        self.immix_space.available_physical_pages()
    }

    fn get_mature_reserved_pages(&self) -> usize {
        self.immix_space.reserved_pages()
    }

    fn force_full_heap_collection(&self) {
        self.gen.force_full_heap_collection()
    }

    /// For Bactrian, a "full heap collection" in the generational sense is any pause
    /// that completed a whole-heap reclamation: a STW `Full` GC *or* the `FinalMark`
    /// of a concurrent cycle (which swept the mature space over complete marks).
    /// The binding's mature-pressure pacing and `Gc.major_collections` accounting key
    /// on this — a completed concurrent cycle counts as a major collection, exactly
    /// as in stock OCaml.
    fn last_collection_full_heap(&self) -> bool {
        matches!(
            self.previous_pause(),
            Some(Pause::Full) | Some(Pause::FinalMark)
        )
    }
}

impl<VM: VMBinding> crate::plan::generational::global::GenerationalPlanExt<VM> for Bactrian<VM> {
    fn trace_object_nursery<Q: ObjectQueue, const KIND: TraceKind>(
        &self,
        queue: &mut Q,
        object: ObjectReference,
        worker: &mut GCWorker<VM>,
    ) -> ObjectReference {
        self.gen
            .trace_object_nursery::<Q, KIND>(queue, object, worker)
    }
}

impl<VM: VMBinding> ConcurrentPlan for Bactrian<VM> {
    fn current_pause(&self) -> Option<Pause> {
        self.current_pause.load(Ordering::SeqCst)
    }

    fn concurrent_work_in_progress(&self) -> bool {
        self.concurrent_marking_in_progress()
    }

    fn should_skip_concurrent_trace(&self, object: ObjectReference) -> bool {
        // The copying nursery is outside the snapshot: young objects are all
        // post-snapshot (InitialMark empties the nursery) and move at every
        // nursery pause, so the concurrent marker must never see them.
        self.gen.nursery.in_space(object)
    }
}

impl<VM: VMBinding> Bactrian<VM> {
    pub fn new(args: CreateGeneralPlanArgs<VM>) -> Self {
        let mut plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &BACTRIAN_CONSTRAINTS,
            global_side_metadata_specs:
                crate::plan::generational::new_generational_global_metadata_specs::<VM>(),
        };

        let immix_space = ImmixSpace::new(
            plan_args.get_mature_space_args(
                "immix_mature",
                true,
                false,
                VMRequest::discontiguous(),
            ),
            ImmixSpaceArgs {
                // Young objects are never allocated in the ImmixSpace directly.
                mixed_age: false,
                never_move_objects: false,
            },
        );

        // These buckets are never used by this plan (no compaction/forwarding stages).
        let scheduler = &plan_args.global_args.scheduler;
        scheduler.work_buckets[WorkBucketStage::VMRefForwarding].set_enabled(false);
        scheduler.work_buckets[WorkBucketStage::CalculateForwarding].set_enabled(false);
        scheduler.work_buckets[WorkBucketStage::SecondRoots].set_enabled(false);
        scheduler.work_buckets[WorkBucketStage::RefForwarding].set_enabled(false);
        scheduler.work_buckets[WorkBucketStage::FinalizableForwarding].set_enabled(false);
        scheduler.work_buckets[WorkBucketStage::Compact].set_enabled(false);

        let bactrian = Bactrian {
            gen: CommonGenPlan::new(plan_args),
            immix_space,
            last_gc_was_defrag: AtomicBool::new(false),
            current_pause: Atomic::new(None),
            previous_pause: Atomic::new(None),
            concurrent_marking_active: AtomicBool::new(false),
        };

        bactrian.verify_side_metadata_sanity();

        bactrian
    }

    /// Decide what kind of pause this collection is. Called once per collection from
    /// `schedule_collection`, with mutators about to be (or being) stopped.
    fn decide_pause(&self) -> Pause {
        if self.concurrent_marking_in_progress() {
            // While a cycle is in flight the only legal pauses are Nursery and
            // FinalMark (upgrading to Full mid-cycle is unsafe w.r.t. defrag — same
            // restriction as ConcurrentImmix). Any pending full-heap request stays
            // set (next_gc_full_heap) and is honoured after the cycle completes.
            if self.gen.common.base.scheduler.work_buckets[WorkBucketStage::Concurrent]
                .is_drained()
            {
                Pause::FinalMark
            } else {
                Pause::Nursery
            }
        } else {
            // Consume the "next GC should be full heap" request. Its meaning depends
            // on who set it:
            //  - a USER request (Gc.major/full_major/compact routes through
            //    handle_user_collection_request(exhaustive=true), which sets both
            //    user_triggered_collection and next_gc_full_heap) is a full-heap
            //    collection by contract → STW Full;
            //  - a mature-pressure request (the binding's GH#5 pacing, or the
            //    available-pages heuristic at end_of_gc) starts a *concurrent* major
            //    cycle — that is what a major collection IS in this design, as in
            //    stock OCaml → InitialMark.
            let cycle_requested = self.gen.next_gc_full_heap.swap(false, Ordering::SeqCst);
            let user_triggered = self
                .gen
                .common
                .base
                .global_state
                .user_triggered_collection
                .load(Ordering::SeqCst);
            let user_full = user_triggered
                && (cycle_requested || *self.gen.common.base.options.full_heap_system_gc);
            let emergency = self
                .gen
                .common
                .base
                .global_state
                .cur_collection_attempts
                .load(Ordering::SeqCst)
                > 1;
            let vm_exhausted = ((self.get_collection_reserved_pages() as f64
                * VM::VMObjectModel::VM_WORST_CASE_COPY_EXPANSION)
                as usize)
                > self.get_mature_physical_pages_available();
            let full = crate::plan::generational::FULL_NURSERY_GC
                || user_full
                || emergency
                || vm_exhausted;
            if full {
                Pause::Full
            } else if cycle_requested {
                // Debug bisection knob: BACTRIAN_NO_CONCURRENT=1 degrades every
                // major-cycle request to a STW Full GC (GenImmix-equivalent
                // behaviour), isolating the concurrent machinery when debugging.
                if std::env::var_os("BACTRIAN_NO_CONCURRENT").is_some() {
                    Pause::Full
                } else {
                    Pause::InitialMark
                }
            } else {
                Pause::Nursery
            }
        }
    }

    /// Temporary bring-up tracing (release builds strip `log`); gated on
    /// BACTRIAN_TRACE=1.
    fn trace_pause(&self, what: &str, pause: Pause) {
        if std::env::var_os("BACTRIAN_TRACE").is_some() {
            use crate::plan::concurrent::diag;
            use std::sync::atomic::Ordering as O;
            eprintln!(
                "[bactrian] {} {:?} (marking={}, mature_pages={}, seeded={}, enq={}, traced={}, skipped_young={}, satb={}, satb_young_drop={})",
                what,
                pause,
                self.concurrent_marking_in_progress(),
                self.immix_space.reserved_pages(),
                diag::SEEDED.load(O::Relaxed),
                diag::ENQUEUED.load(O::Relaxed),
                diag::TRACED.load(O::Relaxed),
                diag::SKIPPED_YOUNG.load(O::Relaxed),
                diag::SATB_ENQ.load(O::Relaxed),
                diag::SATB_YOUNG_DROP.load(O::Relaxed),
            );
        }
    }

    pub fn concurrent_marking_in_progress(&self) -> bool {
        self.concurrent_marking_active.load(Ordering::Acquire)
    }

    fn set_concurrent_marking_state(&self, active: bool) {
        use crate::plan::global::HasSpaces;

        // Tell the spaces to allocate new objects as live (eager line marks in the
        // Immix copy allocator; mature+marked LOS allocations).
        self.for_each_space(&mut |space: &dyn Space<VM>| {
            space.set_allocate_as_live(active);
        });

        self.concurrent_marking_active
            .store(active, Ordering::SeqCst);
    }

    pub(crate) fn is_concurrent_marking_active(&self) -> bool {
        self.concurrent_marking_active.load(Ordering::SeqCst)
    }

    fn previous_pause(&self) -> Option<Pause> {
        self.previous_pause.load(Ordering::SeqCst)
    }
}
