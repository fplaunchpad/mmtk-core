//! LXR lazy mature-block sweeping work packet (P3, additive).
//!
//! Vendored and adapted from the LXR research fork's `policy/immix/rc_work.rs`. Only the minimal
//! single-domain RC subset is ported: `SweepBlocksAfterDecs`, the packet that sweeps mature blocks
//! flagged as possibly-dead by a batch of decrements. The reference's defrag/mature-evac selection
//! (`SelectDefragBlocks`, `MatureEvacuationSet`), the dead-cycle sweeper (`SweepDeadCycles`), and
//! the concurrent mark-table zeroing (`ConcurrentChunkMetadataZeroing`, `PrepareChunksForFullGC`)
//! are **deferred** (CM / mature evac are out of the minimal cut). Inert until the LXR plan runs
//! (`rc_enabled` stays false).
//!
//! ## Adaptation notes (LXR `lxr/lxr-v0.32.0`  vs  our base)
//!
//! * `rc_sweep_mature` here is the 3-arg `rc_sweep_mature::<VM>(space, defrag, rc_dead)` we added to
//!   `Block`.
//! * The reference also bumps `num_clean_blocks_released_*` stats inside a `current_pause().is_none()
//!   || STWRCDecsAndSweep.is_open()` gate; kept verbatim (the stats fields exist on `ImmixSpace`).

use atomic::Ordering;

use crate::{
    scheduler::{GCWork, GCWorker, WorkBucketStage},
    vm::VMBinding,
    LazySweepingJobsCounter, MMTK,
};

use super::block::Block;
use crate::plan::lxr::LXR;

/// Sweep the mature blocks that a batch of decrements flagged as possibly-dead. Any block whose RC
/// table is now all-zero is deinitialised and its pages bulk-released.
pub(crate) struct SweepBlocksAfterDecs {
    blocks: Vec<(Block, bool)>,
    _counter: LazySweepingJobsCounter,
}

impl SweepBlocksAfterDecs {
    pub fn new(blocks: Vec<(Block, bool)>, counter: LazySweepingJobsCounter) -> Self {
        Self {
            blocks,
            _counter: counter,
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for SweepBlocksAfterDecs {
    fn do_work(&mut self, _worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        if self.blocks.is_empty() {
            return;
        }
        let mut count = 0;
        for (block, defrag) in &self.blocks {
            block.unlog();
            if block.rc_sweep_mature::<VM>(&lxr.immix_space, *defrag, false) {
                count += 1;
            } else {
                assert!(
                    !*defrag,
                    "defrag block is freed? {:?} {:?} {}",
                    block,
                    block.get_state(),
                    block.is_defrag_source()
                );
            }
        }
        if count != 0 {
            lxr.immix_space.block_page_resource().bulk_release_blocks(count);
        }
        if count != 0
            && (lxr.current_pause().is_none()
                || mmtk.scheduler.work_buckets[WorkBucketStage::STWRCDecsAndSweep].is_open())
        {
            lxr.immix_space
                .num_clean_blocks_released_mature
                .fetch_add(count, Ordering::Relaxed);
            lxr.immix_space
                .num_clean_blocks_released_lazy
                .fetch_add(count, Ordering::Relaxed);
        }
    }
}
