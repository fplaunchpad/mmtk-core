use super::gc_work::LXRGCWorkContext;
use super::mutator::ALLOCATOR_MAPPING;
use crate::plan::global::BasePlan;
use crate::plan::global::CommonPlan;
use crate::plan::global::CreateGeneralPlanArgs;
use crate::plan::global::CreateSpecificPlanArgs;
use crate::plan::AllocationSemantics;
use crate::plan::Plan;
use crate::plan::PlanConstraints;
use crate::policy::immix::ImmixSpaceArgs;
use crate::policy::immix::{TRACE_KIND_DEFRAG, TRACE_KIND_FAST};
use crate::policy::space::Space;
use crate::scheduler::*;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::copy::*;
use crate::util::heap::gc_trigger::SpaceStats;
use crate::util::heap::VMRequest;
use crate::util::metadata::log_bit::UnlogBitsOperation;
use crate::util::metadata::side_metadata::SideMetadataContext;
use crate::util::rc::RefCountHelper;
use crate::vm::VMBinding;
use crate::{policy::immix::ImmixSpace, util::opaque_pointer::VMWorkerThread};
use std::sync::atomic::AtomicBool;

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
}

/// The plan constraints for the LXR plan. Currently identical to the Immix
/// constraints (`rc_enabled = false`); the RC constraints (`rc_enabled`,
/// `needs_field_log_bit`, `BarrierSelector::FieldBarrier`) are turned on in a later
/// P3 step once the RC machinery is wired.
pub const LXR_CONSTRAINTS: PlanConstraints = PlanConstraints {
    moves_objects: !cfg!(feature = "immix_non_moving"),
    max_non_los_default_alloc_bytes: crate::policy::immix::MAX_IMMIX_OBJECT_SIZE,
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
        // Inlined from Immix::schedule_immix_full_heap_collection (the shared helper
        // lives on the private Immix type). LXR will replace this with RC-pause
        // scheduling in a later P3 step.
        let in_defrag = self.immix_space.decide_whether_to_defrag(
            self.base().global_state.is_emergency_collection(),
            true,
            self.base()
                .global_state
                .cur_collection_attempts
                .load(Ordering::SeqCst),
            self.base().global_state.is_user_triggered_collection(),
            *self.base().options.full_heap_system_gc,
        );
        if in_defrag {
            scheduler.schedule_common_work::<LXRGCWorkContext<VM, TRACE_KIND_DEFRAG>>(self);
        } else {
            scheduler.schedule_common_work::<LXRGCWorkContext<VM, TRACE_KIND_FAST>>(self);
        }
    }

    fn get_allocator_mapping(&self) -> &'static EnumMap<AllocationSemantics, AllocatorSelector> {
        &ALLOCATOR_MAPPING
    }

    fn prepare(&mut self, tls: VMWorkerThread) {
        self.common.prepare(tls, true);
        self.immix_space.prepare(
            true,
            Some(crate::policy::immix::defrag::StatsForDefrag::new(self)),
            UnlogBitsOperation::NoOp,
        );
    }

    fn release(&mut self, tls: VMWorkerThread) {
        self.common.release(tls, true);
        self.immix_space.release(true, UnlogBitsOperation::NoOp);
    }

    fn end_of_gc(&mut self, tls: VMWorkerThread) {
        self.last_gc_was_defrag
            .store(self.immix_space.end_of_gc(), Ordering::Relaxed);
        self.common.end_of_gc(tls);
    }

    fn current_gc_may_move_object(&self) -> bool {
        self.immix_space.in_defrag()
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
        let mut plan_args = CreateSpecificPlanArgs {
            global_args: args,
            constraints: &LXR_CONSTRAINTS,
            global_side_metadata_specs: SideMetadataContext::new_global_specs(&[]),
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
                    never_move_objects: false,
                },
            ),
            common: CommonPlan::new(plan_args),
            last_gc_was_defrag: AtomicBool::new(false),
            rc: RefCountHelper::NEW,
        };

        lxr.verify_side_metadata_sanity();

        lxr
    }
}
