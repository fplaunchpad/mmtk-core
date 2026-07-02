//! LXR clean/reusable block allocation bookkeeping (P3, additive).
//!
//! Vendored and adapted from the LXR research fork's `policy/immix/block_allocation.rs`. Tracks the
//! clean nursery blocks handed out during a mutator phase and sweeps the unpromoted ones at the
//! next RC pause. Inert until the LXR plan runs (`rc_enabled` stays false), so it is fully
//! `#[allow(dead_code)]` and never touched by the 10 shipping plans.
//!
//! ## Adaptation notes (LXR `lxr/lxr-v0.32.0`  vs  our base)
//!
//! * `crate::plan::immix::Pause` → `crate::plan::lxr::Pause` (our LXR pause type is distinct from
//!   the ConcurrentImmix one; the reference used the immix-plan pause).
//! * `self.space().pr` is reached via the new `pub(super)` `ImmixSpace::block_page_resource()`
//!   accessor (the field is private to `immixspace.rs`).
//! * `block.init(copy, false, self.space())` → `block.init_rc(copy, false, self.space())` — our
//!   base keeps the original 1-arg `Block::init(copy)` for the shipping plans and adds the 3-arg RC
//!   variant additively (so the other plans stay byte-identical).
//! * `block.{initialize_field_unlog_table_as_unlogged, initialize_mark_table_as_marked,
//!   clear_mark_table, clear_field_unlog_table}` take an explicit `<VM>` here.

use super::{block::Block, ImmixSpace};
use crate::plan::lxr::Pause;
use crate::plan::lxr::LXR;
use crate::util::constants::LOG_BYTES_IN_PAGE;
use crate::util::linear_scan::Region;
use crate::{policy::space::Space, vm::*};
use atomic::Ordering;
use std::cell::UnsafeCell;
use std::sync::atomic::AtomicUsize;
use std::sync::Mutex;

#[allow(dead_code)]
pub struct BlockAllocation<VM: VMBinding> {
    space: UnsafeCell<*const ImmixSpace<VM>>,
    pub(crate) lxr: Option<&'static LXR<VM>>,
    num_nursery_blocks: AtomicUsize,
    pub(crate) in_place_promoted_nursery_blocks: AtomicUsize,
    /// The CLEAN nursery blocks actually handed out this mutator phase (one `push` per
    /// `initialize_new_clean_block(copy=false)`). The RC nursery sweep frees ONLY blocks from this
    /// list — never a chunk-scan — so it can never touch a block that was not explicitly handed out
    /// as a nursery allocation (a block still in the page-resource free queue, or a not-yet-mapped /
    /// not-yet-stopped allocator block), which is what caused the chunk-scan sweep to fault.
    nursery_blocks: Mutex<Vec<Block>>,
}

// Safety: matches the reference; the `*const ImmixSpace` is only set once at init and read
// thereafter, and the space outlives the allocation bookkeeping.
unsafe impl<VM: VMBinding> Sync for BlockAllocation<VM> {}

#[allow(dead_code)]
impl<VM: VMBinding> BlockAllocation<VM> {
    pub fn new() -> Self {
        Self {
            space: UnsafeCell::new(std::ptr::null()),
            lxr: None,
            num_nursery_blocks: AtomicUsize::new(0),
            in_place_promoted_nursery_blocks: Default::default(),
            nursery_blocks: Mutex::new(Vec::new()),
        }
    }

    fn space(&self) -> &'static ImmixSpace<VM> {
        unsafe { &**self.space.get() }
    }

    pub fn clean_nursery_blocks(&self) -> usize {
        self.num_nursery_blocks.load(Ordering::Relaxed)
    }

    pub fn clean_nursery_mb(&self) -> usize {
        self.clean_nursery_blocks() << Block::LOG_BYTES >> 20
    }

    pub fn total_young_allocation_in_bytes(&self) -> usize {
        (self.clean_nursery_blocks() << Block::LOG_BYTES)
            + (self.space().get_mutator_recycled_lines_in_pages() << LOG_BYTES_IN_PAGE)
    }

    pub fn init(&self, space: &ImmixSpace<VM>) {
        unsafe { *self.space.get() = space as *const ImmixSpace<VM> }
    }

    /// Sweep the clean nursery blocks handed out this phase: free (to the page-resource free list)
    /// every block from `nursery_blocks` that was NOT promoted in place (`is_nursery()` is still
    /// true — i.e. it was not flipped to `Unmarked` by `set_as_in_place_promoted`) and whose RC
    /// table is all-zero (`rc_dead()`). Drains the list each phase.
    ///
    /// This REPLACES the `ImmixSpace::rc_sweep_nursery_blocks` chunk-scan, which faulted by touching
    /// blocks never handed out as nursery allocations (free-queue blocks, mid-spawn allocator blocks
    /// whose per-block metadata is not in a sweepable state). By iterating only the explicit list,
    /// every block here is provably a real, mapped, this-phase nursery allocation. Returns the
    /// number of blocks freed.
    pub fn sweep_nursery_blocks(&self) -> usize {
        use super::block::BlockState;
        let blocks = std::mem::take(&mut *self.nursery_blocks.lock().unwrap());
        let mut released = 0usize;
        for block in blocks {
            // Every block here was handed out as a clean nursery block this phase. The promote path
            // (`set_as_in_place_promoted`) flips a block that received a SURVIVING object to
            // `Unmarked`. So an UNPROMOTED nursery block is still in `BlockState::Unallocated` (the
            // clean-nursery state `init_rc` left it in) and, having received no survivor, is
            // all-RC-zero. We key on the STATE (Unallocated = unpromoted) — NOT `is_nursery()`,
            // whose epoch comparison is unreliable under the single-bump scheme — and use `rc_dead()`
            // as the authoritative safety guard (never free a block with a live object).
            if block.get_state() == BlockState::Unallocated && block.rc_dead() {
                self.space().release_block(block);
                released += 1;
            }
        }
        self.num_nursery_blocks.store(0, Ordering::SeqCst);
        self.in_place_promoted_nursery_blocks
            .store(0, Ordering::SeqCst);
        released
    }

    /// Drain the nursery-block list + reset the counters WITHOUT freeing anything (the
    /// `MMTK_RC_NO_FREE` / `MMTK_RC_NO_NURSERY_SWEEP` bisect path).
    pub fn reset_nursery_counters(&self) {
        self.nursery_blocks.lock().unwrap().clear();
        self.num_nursery_blocks.store(0, Ordering::SeqCst);
        self.in_place_promoted_nursery_blocks
            .store(0, Ordering::SeqCst);
    }

    /// Notify a GC phase has started/ended.
    pub fn notify_mutator_phase_end(&self) {}

    pub fn cm_in_progress_or_final_mark(&self) -> bool {
        // `lxr` is wired lazily at the first GC, but clean-block allocation (which reaches here
        // via initialize_new_clean_block) happens earlier at mutator startup. CM is deferred
        // (cm_in_progress is always false), so treat the not-yet-wired state as "no CM".
        match self.lxr {
            Some(lxr) => lxr.cm_in_progress() || lxr.current_pause() == Some(Pause::FinalMark),
            None => false,
        }
    }

    pub(super) fn initialize_new_clean_block(&self, block: Block, copy: bool, cm_enabled: bool) {
        if self.space().in_defrag() {
            self.space().notify_new_clean_block(copy);
        }
        if cm_enabled && !super::BLOCK_ONLY && !self.space().rc_enabled {
            let current_state = self.space().line_mark_state.load(Ordering::Acquire);
            for line in block.lines() {
                line.mark(current_state);
            }
        }
        // Initialize unlog table
        if (self.space().rc_enabled
            || (crate::args::BARRIER_MEASUREMENT && !crate::args::BARRIER_MEASUREMENT_NO_SLOW))
            && copy
        {
            block.initialize_field_unlog_table_as_unlogged::<VM>();
        }
        // Initialize mark table
        if self.space().rc_enabled {
            if self.cm_in_progress_or_final_mark() {
                block.initialize_mark_table_as_marked::<VM>();
            } else {
                block.clear_mark_table::<VM>();
            }
            if !copy {
                self.num_nursery_blocks.fetch_add(1, Ordering::Relaxed);
                block.clear_field_unlog_table::<VM>();
                // Record the block as a clean nursery block handed out this phase. The RC nursery
                // sweep frees only blocks from this list, so it never touches a block that was not
                // explicitly allocated as a nursery block (free-queue blocks, mid-spawn allocator
                // blocks) — the source of the chunk-scan sweep's fault.
                self.nursery_blocks.lock().unwrap().push(block);
            }
        }
        block.init_rc(copy, false, self.space());
        if self.space().common().zeroed && !copy && cfg!(feature = "force_zeroing") {
            crate::util::memory::zero(block.start(), Block::BYTES);
        }
    }
}
