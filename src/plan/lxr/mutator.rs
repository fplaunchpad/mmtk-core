use super::barrier::LXRFieldBarrierSemantics;
#[cfg(feature = "marksweep_as_nonmoving")]
use super::Pause;
use super::LXR;
use crate::plan::barriers::FieldBarrier;
use crate::plan::mutator_context::common_prepare_func;
use crate::plan::mutator_context::common_release_func;
use crate::plan::mutator_context::create_allocator_mapping;
use crate::plan::mutator_context::create_space_mapping;
use crate::plan::mutator_context::Mutator;
use crate::plan::mutator_context::MutatorBuilder;
use crate::plan::mutator_context::MutatorConfig;
use crate::plan::mutator_context::ReservedAllocators;
use crate::plan::AllocationSemantics;
use crate::util::alloc::allocators::AllocatorSelector;
use crate::util::alloc::ImmixAllocator;
use crate::util::opaque_pointer::{VMMutatorThread, VMWorkerThread};
use crate::vm::VMBinding;
use crate::MMTK;
use enum_map::EnumMap;

// P3.5: cloned from plan/immix/mutator.rs (Immix -> LXR). One Immix allocator at
// AllocationSemantics::Default; the LXR field barrier is installed in a later P3 step.
pub fn lxr_mutator_release<VM: VMBinding>(mutator: &mut Mutator<VM>, tls: VMWorkerThread) {
    let immix_allocator = unsafe {
        mutator
            .allocators
            .get_allocator_mut(mutator.config.allocator_mapping[AllocationSemantics::Default])
    }
    .downcast_mut::<ImmixAllocator<VM>>()
    .unwrap();
    immix_allocator.reset();

    // marksweep_as_nonmoving: common_release_func releases the NonMoving free-list allocator,
    // which ends in MarkSweepSpace::release_packet_done. That pairs with the SPACE-side
    // MarkSweepSpace::release handshake (pending_release_packets = num_mutators + 1), which
    // LXR::release arms only at a Full pause (the only pause whose backup trace marks the space).
    // is_nursery_gc() cannot gate it here -- LXR is not generational -- so running it at a
    // RefCount pause both frees blocks against marks no trace rebuilt and underflows the unarmed
    // counter (the `pending_release_packets is still 18446744073709551615` abort,
    // https://github.com/fplaunchpad/ocaml-mmtk/issues/25).
    #[cfg(feature = "marksweep_as_nonmoving")]
    let release_common = mutator
        .plan
        .downcast_ref::<LXR<VM>>()
        .unwrap()
        .current_pause()
        == Some(Pause::Full);
    #[cfg(not(feature = "marksweep_as_nonmoving"))]
    let release_common = true;
    if release_common {
        common_release_func(mutator, tls);
    }
}

pub(in crate::plan) const RESERVED_ALLOCATORS: ReservedAllocators = ReservedAllocators {
    n_immix: 1,
    ..ReservedAllocators::DEFAULT
};

lazy_static! {
    pub static ref ALLOCATOR_MAPPING: EnumMap<AllocationSemantics, AllocatorSelector> = {
        let mut map = create_allocator_mapping(RESERVED_ALLOCATORS, true);
        map[AllocationSemantics::Default] = AllocatorSelector::Immix(0);
        map
    };
}

pub fn create_lxr_mutator<VM: VMBinding>(
    mutator_tls: VMMutatorThread,
    mmtk: &'static MMTK<VM>,
) -> Mutator<VM> {
    let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
    let config = MutatorConfig {
        allocator_mapping: &ALLOCATOR_MAPPING,
        space_mapping: Box::new({
            let mut vec = create_space_mapping(RESERVED_ALLOCATORS, true, lxr);
            vec.push((AllocatorSelector::Immix(0), &lxr.immix_space));
            vec
        }),
        prepare_func: &common_prepare_func,
        release_func: &lxr_mutator_release,
    };

    // Install the LXR coalescing field-logging write barrier (per-field unlog bit + inc/dec
    // buffering). Mirrors how GenImmix/ConcurrentImmix install theirs. `LXR_CONSTRAINTS.barrier`
    // = `FieldBarrier`, so the framework drives the barrier on `object_reference_write`.
    MutatorBuilder::new(mutator_tls, mmtk, config)
        .barrier(Box::new(FieldBarrier::new(LXRFieldBarrierSemantics::new(
            mmtk,
        ))))
        .build()
}
