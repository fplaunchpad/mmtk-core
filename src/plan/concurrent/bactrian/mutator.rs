use super::barrier::BactrianBarrier;
use super::global::Bactrian;
use crate::plan::concurrent::Pause;
use crate::plan::generational::create_gen_space_mapping;
use crate::plan::generational::ALLOCATOR_MAPPING;
use crate::plan::mutator_context::common_prepare_func;
use crate::plan::mutator_context::common_release_func;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorBuilder;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::AllocationSemantics;
use crate::util::alloc::BumpAllocator;
use crate::util::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use crate::MMTK;

pub fn bactrian_mutator_prepare<VM: VMBinding>(mutator: &mut Mutator<VM>, tls: VMWorkerThread) {
    common_prepare_func(mutator, tls);
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

pub fn bactrian_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, tls: VMWorkerThread) {
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

    common_release_func(mutator, tls);

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

pub fn create_bactrian_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let bactrian = mmtk.get_plan().downcast_ref::<Bactrian<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new(create_gen_space_mapping(
            mmtk.get_plan(),
            &bactrian.gen.nursery,
        )),
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
