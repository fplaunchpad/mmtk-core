use super::barrier::BactrianBarrier;
use super::global::Bactrian;
use crate::plan::concurrent::Pause;
use crate::plan::mutator_context::create_allocator_mapping;
use crate::plan::mutator_context::ReservedAllocators;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::alloc::ImmixAllocator;
use enum_map::EnumMap;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorBuilder;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::AllocationSemantics;
use crate::util::alloc::BumpAllocator;
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use crate::MMTK;

// NB: we deliberately do NOT call common_prepare_func/common_release_func here.
// Under the `marksweep_as_nonmoving` feature they do a typed FreeListAllocator
// downcast keyed by AllocationSemantics::NonMoving — but Bactrian remaps
// NonMoving to Immix(0) (pretenuring), so that downcast panics. The common
// mark-sweep nonmoving space's FreeList allocator still exists and still
// participates in the release-packet handshake (MarkSweepSpace::release arms
// pending_release_packets = num_mutators + 1; the per-mutator decrement lives
// in FreeListAllocator::release), so we reach it BY SELECTOR instead:
// BACTRIAN_RESERVED reserves no free-list allocators, so the common space owns
// FreeList(0).
#[cfg(feature = "marksweep_as_nonmoving")]
fn common_nonmoving_prepare<VM: VMBinding>(mutator: &mut Mutator<VM>) {
    unsafe {
        mutator
            .allocators
            .get_typed_allocator_mut::<crate::util::alloc::FreeListAllocator<VM>>(
                AllocatorSelector::FreeList(0),
            )
    }
    .prepare();
}

#[cfg(feature = "marksweep_as_nonmoving")]
fn common_nonmoving_release<VM: VMBinding>(mutator: &mut Mutator<VM>) {
    unsafe {
        mutator
            .allocators
            .get_typed_allocator_mut::<crate::util::alloc::FreeListAllocator<VM>>(
                AllocatorSelector::FreeList(0),
            )
    }
    .release();
}

// Reset the pretenure ImmixAllocator in both prepare and release
// (ConcurrentImmix precedent: InitialMark schedules no mutator release and
// FinalMark no prepare, so both hooks must invalidate the stale bump cursor).
fn reset_pretenure_allocator<VM: VMBinding>(mutator: &mut Mutator<VM>) {
    let immix_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::NonMoving])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.reset();
}

pub fn bactrian_mutator_prepare<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    #[cfg(feature = "marksweep_as_nonmoving")]
    common_nonmoving_prepare(mutator);
    reset_pretenure_allocator(mutator);
    let current_pause = mutator.plan.concurrent().unwrap().current_pause().unwrap();
    // Arm the SATB half of the barrier for the marking cycle that starts when this
    // pause ends. (Concurrent marking state proper is armed in the plan's prepare.)
    if current_pause == Pause::InitialMark {
        mutator
            .barrier
            .downcast_mut::<BactrianBarrier<VM>>()
            .unwrap()
            .set_satb_enabled(true);
    }
}

pub fn bactrian_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, _tls: VMWorkerThread) {
    // Reset the nursery allocator: the nursery was evacuated (every pause except
    // Full is nursery-anchored; Full collects the nursery too).
    let bump_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<BumpAllocator<VM>>()
    .unwrap();
    bump_allocator.reset();

    #[cfg(feature = "marksweep_as_nonmoving")]
    common_nonmoving_release(mutator);
    reset_pretenure_allocator(mutator);

    let current_pause = mutator.plan.concurrent().unwrap().current_pause().unwrap();
    // Disarm the SATB half when the marking cycle ends.
    if current_pause == Pause::FinalMark || current_pause == Pause::Full {
        mutator
            .barrier
            .downcast_mut::<BactrianBarrier<VM>>()
            .unwrap()
            .set_satb_enabled(false);
    }
}

/// Bactrian reserves one bump pointer (the nursery TLAB) and one Immix
/// allocator: the MATURE space, exposed to mutators under
/// AllocationSemantics::NonMoving to implement stock OCaml's
/// Max_young_wosize pretenuring — blocks above the boundary are born in the
/// major heap (never transiting the minor heap), exactly as
/// shared_heap.c:504/515 does via pools/malloc. The runtime routes the
/// >=2056B band here under MMTK_MEDIUM_NONMOVING (see caml_mmtk_semantics).
/// Born-mature objects are unlogged at birth (binding, post-alloc) so the
/// generational barrier remembers their young stores; during concurrent
/// marking the ImmixAllocator's allocate-as-live path keeps them from the
/// FinalMark sweep, same as InitialMark promotions.
const BACTRIAN_RESERVED: ReservedAllocators = ReservedAllocators {
    n_bump_pointer: 1,
    n_immix: 1,
    ..ReservedAllocators::DEFAULT
};

lazy_static::lazy_static! {
    static ref ALLOCATOR_MAPPING: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(BACTRIAN_RESERVED, true);
        map[AllocationSemantics::Default] = AllocatorSelector::BumpPointer(0);
        map[AllocationSemantics::NonMoving] = AllocatorSelector::Immix(0);
        map
    };
}

pub fn create_bactrian_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let bactrian = mmtk.get_plan().downcast_ref::<Bactrian<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            // Must be built with the SAME reserved set as ALLOCATOR_MAPPING:
            // reserving an extra Immix allocator shifts the common spaces'
            // selector indices, so reusing the generational space mapping
            // here leaves a mapped selector with no space (worker copy-context
            // construction unwraps None).
            let mut vec = crate::plan::mutator_context::create_space_mapping(
                BACTRIAN_RESERVED,
                true,
                mmtk.get_plan(),
            );
            vec.push((AllocatorSelector::BumpPointer(0), &bactrian.gen.nursery));
            vec.push((AllocatorSelector::Immix(0), &bactrian.immix_space));
            vec
        }),
        prepare_func: &bactrian_mutator_prepare,
        release_func: &bactrian_mutator_release,
    };

    let builder = MutatorBuilder::new(mutator_tls, mmtk, config);
    let mut mutator = builder
        .barrier(Box::new(BactrianBarrier::new(mmtk, mutator_tls)))
        .build();

    // A mutator created mid-cycle (e.g. a new domain spawned during concurrent
    // marking) must start with the SATB barrier armed.
    mutator
        .barrier
        .downcast_mut::<BactrianBarrier<VM>>()
        .unwrap()
        .set_satb_enabled(bactrian.is_concurrent_marking_active());

    mutator
}
