use std::sync::Arc;

use crate::policy::largeobjectspace::LargeObjectSpace;
use crate::policy::space::Space;
use crate::util::alloc::{allocator, Allocator};
use crate::util::opaque_pointer::*;
use crate::util::Address;
use crate::vm::VMBinding;

use super::allocator::AllocatorContext;

/// An allocator that only allocates at page granularity.
/// This is intended for large objects.
#[repr(C)]
pub struct LargeObjectAllocator<VM: VMBinding> {
    /// [`VMThread`] associated with this allocator instance
    pub tls: VMThread,
    /// [`Space`](src/policy/space/Space) instance associated with this allocator instance.
    space: &'static LargeObjectSpace<VM>,
    context: Arc<AllocatorContext<VM>>,
    /// Rotating start-phase counter (see `alloc`).
    phase: usize,
}

/// LOS start-phase rotation range in lines (0 or 1 disables). Default 16
/// (max 960B skipped inside page one of an already page-rounded request).
/// Override with MMTK_LOS_PHASE_LINES (2..=64; capped so the offset stays
/// within the first page).
fn los_phase_range() -> usize {
    static V: std::sync::OnceLock<usize> = std::sync::OnceLock::new();
    *V.get_or_init(|| {
        std::env::var("MMTK_LOS_PHASE_LINES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n <= 64)
            .unwrap_or(16)
    })
}

impl<VM: VMBinding> Allocator<VM> for LargeObjectAllocator<VM> {
    fn get_tls(&self) -> VMThread {
        self.tls
    }

    fn get_context(&self) -> &AllocatorContext<VM> {
        &self.context
    }

    fn get_space(&self) -> &'static dyn Space<VM> {
        // Casting the interior of the Option: from &LargeObjectSpace to &dyn Space
        self.space as &'static dyn Space<VM>
    }

    fn does_thread_local_allocation(&self) -> bool {
        false
    }

    fn alloc(&mut self, size: usize, align: usize, offset: usize) -> Address {
        // Rotating line-phase for large objects: allocate_pages returns
        // page-aligned cells, so every LOS object starts at an identical
        // cache-set (and 4K) phase — a stream of same-shaped large arrays
        // then loads/stores at fully correlated offsets, which a malloc'd
        // large object (stock OCaml's placement for this band) never does.
        // Start successive objects a rotating number of cache lines into the
        // first page instead. The pad is accounted in the request size; the
        // free path is unaffected (sweep recovers the region via
        // align_down(BYTES_IN_PAGE), and the offset stays inside page one).
        let phase = self.next_phase(align);
        let cell: Address = self.alloc_slow(size + phase, align, offset);
        // We may get a null ptr from alloc due to the VM being OOM
        if !cell.is_zero() {
            allocator::align_allocation::<VM>(cell + phase, align, offset)
        } else {
            cell
        }
    }

    fn alloc_slow_once(&mut self, size: usize, align: usize, _offset: usize) -> Address {
        if self.space.handle_obvious_oom_request(
            self.tls,
            size,
            self.get_context().get_alloc_options(),
        ) {
            return Address::ZERO;
        }

        let maxbytes = allocator::get_maximum_aligned_size::<VM>(size, align);
        let pages = crate::util::conversions::bytes_to_pages_up(maxbytes);
        self.space
            .allocate_pages(self.tls, pages, self.get_context().get_alloc_options())
    }
}

impl<VM: VMBinding> LargeObjectAllocator<VM> {
    pub(crate) fn new(
        tls: VMThread,
        space: &'static LargeObjectSpace<VM>,
        context: Arc<AllocatorContext<VM>>,
    ) -> Self {
        LargeObjectAllocator {
            tls,
            space,
            context,
            phase: 0,
        }
    }

    /// Next start-phase pad in bytes: a rotating number of cache lines,
    /// rounded up to the object alignment, always < one page.
    fn next_phase(&mut self, align: usize) -> usize {
        let range = los_phase_range();
        if range <= 1 {
            return 0;
        }
        let k = self.phase;
        self.phase = (self.phase + 1) % range;
        let pad = k * 64;
        // Keep the object's alignment: round up to `align` (a power of two).
        (pad + align - 1) & !(align - 1)
    }
}
