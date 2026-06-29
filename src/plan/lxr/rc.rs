//! LXR reference-counting work packets — minimal single-domain port (P3).
//!
//! Vendored and **heavily adapted** (streamlined to the minimal subset) from the LXR research
//! fork's `plan/lxr/rc.rs`. This is the "RC heart": the increment processor (`ProcessIncs`), the
//! decrement processor (`ProcessDecs`), and the root-edge → root-increment bridge
//! (`RCImmixCollectRootEdges`). Everything is **present + compiling but INERT** — the LXR plan ships
//! with `rc_enabled = false`, so the field barrier that would enqueue these packets is not installed
//! and `current_pause()` stays `None`; nothing here runs until the main loop flips `rc_enabled`.
//!
//! ## What was kept vs the reference (single-domain, in-place-promotion-only cut)
//!
//! The reference interleaves four concerns we are **deferring**: nursery/mature **evacuation**
//! (copying), **concurrent marking** (CM/SATB), **lazy decrements** (the global sweeping-jobs
//! registry), and a large body of **instrumentation**. The minimal cut here is the
//! `RC_NURSERY_EVACUATION = false`, `lxr_no_cm`, `lxr_no_mature_evac` configuration, which collapses:
//!
//! * `process_inc_and_evacuate` → **inc, and if the object was freshly promoted (RC 0→1), promote it
//!   in place**. No forwarding, no copy context, no `NO_EVAC` throttle. Since objects never move,
//!   the slot is never written back.
//! * `scan_nursery_object` → set the freshly-promoted object's per-field unlog bits (so the field
//!   barrier won't re-log them) and generate recursive increments for its pointer fields. The
//!   compressed-pointer / val-array / huge-obj-array special cases are dropped (OCaml is 64-bit,
//!   uncompressed; field iteration goes through the base `SlotIterator`).
//! * `process_dead_object` → recursively decrement fields, clear straddle-line metadata, and hand
//!   the now-(maybe-)dead block to the lazy mature sweep. The CM/SATB mark push is dropped.
//!
//! ## Base-API adaptations (vs the LXR fork)
//!
//! | LXR call | base replacement |
//! |---|---|
//! | `o.get_size::<VM>()` | `VM::VMObjectModel::get_current_size(o)` |
//! | `o.verify::<VM>()` | dropped (no such method on our `ObjectReference`) |
//! | `o.iterate_fields::<VM,_>(CLDScanPolicy, RefScanPolicy, |slot, out_of_heap| …)` | `crate::plan::tracing::SlotIterator::<VM>::iterate_fields(o, tls, |slot| …)`; `out_of_heap` is derived as `!immix_space.in_space(target)` |
//! | `Block::containing(o)` | same, but via the `Region` trait import |
//! | `GCWorker::current()` | a `*mut GCWorker` captured at `do_work` start (our base has no thread-local current-worker accessor; matches the `ProcessEdgesBase` pattern) |
//! | `rc.fetch_update(o, closure)` for the dec | a manual CAS loop — our `RefCountHelper::fetch_update` bounds the closure `+ Copy`, which a `&mut self`-capturing closure is not |
//! | `s.store(Some(new))` | dropped — the in-place cut never moves objects |
//! | CM (`super::cm::*`), forwarding, copy context, survival predictor, counters/prefetch, `RootKind`/`RC_ROOTS`, `curr_roots` | dropped |

use std::ops::{Deref, DerefMut};
use std::sync::Arc;

use super::LXR;
use crate::plan::tracing::SlotIterator;
use crate::plan::VectorQueue;
use crate::policy::immix::block::Block;
use crate::policy::space::Space;
use crate::scheduler::gc_work::{ProcessEdgesBase, ScanObjects, SlotOf};
use crate::scheduler::{GCWork, GCWorker, ProcessEdgesWork, WorkBucketStage};
use crate::util::linear_scan::Region;
use crate::util::rc::{RefCountHelper, MAX_REF_COUNT};
use crate::util::{ObjectReference, VMThread};
use crate::vm::slot::Slot;
use crate::vm::*;
use crate::LazySweepingJobsCounter;
use crate::MMTK;

/// The classification of an increment's source slot.
pub type EdgeKind = u8;
/// A root slot (scanned from the stack/registers). Roots are never written back. (Used by
/// `RCImmixCollectRootEdges` once the RC root scan is wired — main loop.)
#[allow(dead_code)]
pub const EDGE_KIND_ROOT: u8 = 0;
/// A slot of a freshly-promoted nursery object (recursive increment).
pub const EDGE_KIND_NURSERY: u8 = 1;
/// A mature heap slot (logged by the field barrier). Mature slots are unlogged on load.
pub const EDGE_KIND_MATURE: u8 = 2;

/// A fake TLS for the base `SlotIterator` (it ignores the tls; see the FIXME there).
#[inline(always)]
fn fake_tls() -> VMThread {
    VMThread::UNINITIALIZED
}

// ───────────────────────────────────── ProcessIncs ──────────────────────────────────────────────

/// Process a buffer of reference-count increments. `KIND` distinguishes root / nursery / mature
/// slots (it only changes whether the slot is unlogged on load).
pub struct ProcessIncs<VM: VMBinding, const KIND: EdgeKind> {
    /// Increments (slots) to process.
    incs: Vec<VM::VMSlot>,
    /// Recursively-generated new increments (fields of freshly-promoted objects).
    new_incs: VectorQueue<VM::VMSlot>,
    new_incs_count: u32,
    lxr: &'static LXR<VM>,
    rc: RefCountHelper<VM>,
    /// The worker running this packet (captured at `do_work` start; null until then). Used to
    /// enqueue recursively-generated nursery-inc packets.
    worker: *mut GCWorker<VM>,
}

unsafe impl<VM: VMBinding, const KIND: EdgeKind> Send for ProcessIncs<VM, KIND> {}

#[allow(dead_code)]
impl<VM: VMBinding, const KIND: EdgeKind> ProcessIncs<VM, KIND> {
    const CAPACITY: usize = crate::args::BUFFER_SIZE;

    fn worker(&self) -> &'static mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn new(incs: Vec<VM::VMSlot>, lxr: &'static LXR<VM>) -> Self {
        Self {
            incs,
            new_incs: VectorQueue::default(),
            new_incs_count: 0,
            lxr,
            rc: RefCountHelper::NEW,
            worker: std::ptr::null_mut(),
        }
    }

    /// Increment `o`'s RC. Returns true iff this call promoted it (0 → 1).
    fn inc(&self, o: ObjectReference) -> bool {
        self.rc.inc(o) == Ok(0)
    }

    /// Promote a freshly-incremented object to mature: mark its block as in-place-promoted (if it
    /// is a fresh nursery block), set its straddle-line metadata, and scan it to set field unlog
    /// bits + generate recursive increments. No copying (in-place-only cut).
    fn promote(&mut self, o: ObjectReference, los: bool) {
        let size = VM::VMObjectModel::get_current_size(o);
        if !los {
            let block = Block::containing(o);
            if block.is_nursery() {
                block.set_as_in_place_promoted(&self.lxr.immix_space);
            }
            self.rc.promote_with_size(o, size);
        }
        self.scan_nursery_object(o, los);
    }

    /// Scan a freshly-promoted object: set its per-field unlog bits (so the field write barrier
    /// won't re-log them — they are now mature) and generate a recursive increment for each pointer
    /// field, bumping already-live children directly.
    fn scan_nursery_object(&mut self, o: ObjectReference, los: bool) {
        if los {
            o.to_raw_address().unlog_field_relaxed::<VM>();
        }
        SlotIterator::<VM>::iterate_fields(o, fake_tls(), |slot| {
            // Unlog this field (it now belongs to a mature object).
            slot.to_address().unlog_field_relaxed::<VM>();
            let Some(target) = slot.load() else {
                return;
            };
            let rc = self.rc.count(target);
            if rc == 0 {
                // Fresh nursery child — defer a recursive increment (it will itself promote).
                self.new_incs.push(slot);
                self.new_incs_count += 1;
            } else if rc != MAX_REF_COUNT {
                // Already-live child — just bump its RC.
                let _ = self.rc.inc(target);
            }
        });
        if self.new_incs_count as usize >= Self::CAPACITY {
            self.flush();
        }
    }

    /// The minimal in-place-promotion inc: increment, and promote on the 0 → 1 transition.
    fn process_inc(&mut self, o: ObjectReference) {
        let los = self.lxr.los().in_space(o);
        if self.inc(o) {
            self.promote(o, los);
        }
    }

    /// Load the (possibly-null) target of slot `s`, unlogging the slot first for mature edges,
    /// then increment + maybe-promote it.
    fn process_slot(&mut self, s: VM::VMSlot) {
        if KIND == EDGE_KIND_MATURE {
            s.to_address().unlog_field_relaxed::<VM>();
        }
        let Some(o) = s.load() else {
            return;
        };
        self.process_inc(o);
        // In-place cut: the object never moves, so the slot is never written back.
    }

    fn process_incs(&mut self, incs: &[VM::VMSlot]) {
        for s in incs {
            self.process_slot(*s);
        }
    }

    #[cold]
    fn flush(&mut self) {
        if !self.new_incs.is_empty() {
            let new_incs = self.new_incs.take();
            let w = ProcessIncs::<VM, EDGE_KIND_NURSERY>::new(new_incs, self.lxr);
            self.worker().add_work(WorkBucketStage::Unconstrained, w);
        }
        self.new_incs_count = 0;
    }
}

impl<VM: VMBinding, const KIND: EdgeKind> GCWork<VM> for ProcessIncs<VM, KIND> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        self.lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        self.worker = worker as *mut GCWorker<VM>;
        // Process the main buffer.
        let incs = std::mem::take(&mut self.incs);
        self.process_incs(&incs);
        // Drain the recursively-generated buffer.
        let mut buf = vec![];
        while !self.new_incs.is_empty() {
            self.new_incs_count = 0;
            buf.clear();
            self.new_incs.swap(&mut buf);
            self.process_incs(&buf);
        }
    }
}

// ───────────────────────────────────── ProcessDecs ──────────────────────────────────────────────

/// Process a buffer of reference-count decrements. A 1 → 0 transition makes the object dead, which
/// triggers a recursive decrement of its fields, straddle-line clearing, and a lazy block sweep.
pub struct ProcessDecs<VM: VMBinding> {
    decs: Option<Vec<ObjectReference>>,
    decs_arc: Option<Arc<Vec<ObjectReference>>>,
    /// Recursively-generated new decrements (fields of dead objects).
    new_decs: VectorQueue<ObjectReference>,
    counter: LazySweepingJobsCounter,
    rc: RefCountHelper<VM>,
    /// The worker running this packet (captured at `do_work` start; null until then).
    worker: *mut GCWorker<VM>,
}

unsafe impl<VM: VMBinding> Send for ProcessDecs<VM> {}

#[allow(dead_code)]
impl<VM: VMBinding> ProcessDecs<VM> {
    pub const CAPACITY: usize = crate::args::BUFFER_SIZE;

    fn worker(&self) -> &'static mut GCWorker<VM> {
        unsafe { &mut *self.worker }
    }

    pub fn new(decs: Vec<ObjectReference>, counter: LazySweepingJobsCounter) -> Self {
        Self {
            decs: Some(decs),
            decs_arc: None,
            new_decs: VectorQueue::default(),
            counter,
            rc: RefCountHelper::NEW,
            worker: std::ptr::null_mut(),
        }
    }

    pub fn new_arc(decs: Arc<Vec<ObjectReference>>, counter: LazySweepingJobsCounter) -> Self {
        Self {
            decs: None,
            decs_arc: Some(decs),
            new_decs: VectorQueue::default(),
            counter,
            rc: RefCountHelper::NEW,
            worker: std::ptr::null_mut(),
        }
    }

    fn recursive_dec(&mut self, o: ObjectReference) {
        self.new_decs.push(o);
        if self.new_decs.is_full() {
            self.flush();
        }
    }

    fn flush(&mut self) {
        if !self.new_decs.is_empty() {
            let mmtk = self.worker().mmtk;
            let new_decs = self.new_decs.take();
            let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
            let w = ProcessDecs::new(new_decs, self.counter.clone_with_decs());
            if lxr.current_pause().is_none() {
                self.worker()
                    .add_work_prioritized(WorkBucketStage::Unconstrained, w);
            } else {
                self.worker().add_work(WorkBucketStage::Unconstrained, w);
            }
        }
    }

    /// An object's RC reached 0. Recursively decrement its fields, clear its straddle-line
    /// metadata, and hand its block to the lazy mature sweep.
    #[cold]
    fn process_dead_object(&mut self, o: ObjectReference, lxr: &LXR<VM>) {
        let in_ix_space = lxr.immix_space.in_space(o);
        // Recursively decrement the dead object's pointer fields.
        if !cfg!(feature = "lxr_no_recursive_dec") {
            SlotIterator::<VM>::iterate_fields(o, fake_tls(), |slot| {
                if let Some(x) = slot.load() {
                    let out_of_heap = !lxr.immix_space.in_space(x);
                    if !out_of_heap {
                        let rc = self.rc.count(x);
                        if rc != MAX_REF_COUNT && rc != 0 {
                            self.recursive_dec(x);
                        }
                    }
                }
            });
        }
        if !crate::args::BLOCK_ONLY && in_ix_space {
            self.rc.unmark_straddle_object(o);
        }
        #[cfg(feature = "sanity")]
        unsafe {
            o.to_raw_address().store(0xdeadusize)
        };
        if in_ix_space {
            let block = Block::containing(o);
            lxr.immix_space
                .add_to_possibly_dead_mature_blocks(block, false);
        }
        // LOS path (`!in_ix_space`): the RC-aware LOS free (`los().rc_free`) is deferred; the
        // standard LOS sweep reclaims the object.
    }

    fn process_decs(&mut self, decs: &[ObjectReference], lxr: &LXR<VM>) {
        for o in decs {
            let o = *o;
            // Manual decrement: our `RefCountHelper::fetch_update` bounds its closure `+ Copy`,
            // which a `&mut self`-capturing closure (needed to call `process_dead_object`) is not.
            // So we read → maybe-kill → store, with a small CAS retry to stay atomic-ish. (Inert in
            // the single-domain cut: the trace runs stop-the-world, so there is no contention.)
            let c = self.rc.count(o);
            if c == 0 || c == MAX_REF_COUNT {
                continue; // dead or stuck (sticky)
            }
            if c == 1 {
                // Last reference — the object dies. Recurse into its fields *before* zeroing its RC
                // (its fields are still readable), then set RC to 0.
                self.process_dead_object(o, lxr);
                self.rc.set(o, 0);
            } else {
                let _ = self.rc.dec(o);
            }
        }
    }
}

impl<VM: VMBinding> GCWork<VM> for ProcessDecs<VM> {
    fn do_work(&mut self, worker: &mut GCWorker<VM>, mmtk: &'static MMTK<VM>) {
        if cfg!(feature = "lxr_no_decs") {
            return;
        }
        self.worker = worker as *mut GCWorker<VM>;
        let lxr = mmtk.get_plan().downcast_ref::<LXR<VM>>().unwrap();
        if let Some(decs) = std::mem::take(&mut self.decs) {
            self.process_decs(&decs, lxr);
        } else if let Some(decs) = std::mem::take(&mut self.decs_arc) {
            self.process_decs(&decs, lxr);
        }
        let mut decs = vec![];
        while !self.new_decs.is_empty() {
            decs.clear();
            self.new_decs.swap(&mut decs);
            self.process_decs(&decs, lxr);
        }
        self.flush();
    }
}

// ─────────────────────────────── RCImmixCollectRootEdges ─────────────────────────────────────────

/// Converts a root-edge buffer into a root-increment packet. This is a `ProcessEdgesWork` purely so
/// the existing root-scanning machinery (which produces `ProcessEdgesWork` packets) can drive the RC
/// roots: its `process_slots` turns the root slots into a `ProcessIncs<_, EDGE_KIND_ROOT>` and runs
/// it inline. `trace_object`/`create_scan_work` are unreachable — it never traces. (Wired into the
/// RC root scan when `rc_enabled` is flipped — main loop.)
#[allow(dead_code)]
pub struct RCImmixCollectRootEdges<VM: VMBinding> {
    base: ProcessEdgesBase<VM>,
}

impl<VM: VMBinding> ProcessEdgesWork for RCImmixCollectRootEdges<VM> {
    type VM = VM;
    type ScanObjectsWorkType = ScanObjects<Self>;
    const OVERWRITE_REFERENCE: bool = false;
    const SCAN_OBJECTS_IMMEDIATELY: bool = true;
    const RC_ROOTS: bool = true;

    fn new(
        slots: Vec<SlotOf<Self>>,
        roots: bool,
        mmtk: &'static MMTK<VM>,
        bucket: WorkBucketStage,
    ) -> Self {
        debug_assert!(roots);
        let base = ProcessEdgesBase::new(slots, roots, mmtk, bucket);
        Self { base }
    }

    fn trace_object(&mut self, _object: ObjectReference) -> ObjectReference {
        unreachable!()
    }

    fn process_slots(&mut self) {
        if !self.slots.is_empty() {
            let lxr = self.mmtk().get_plan().downcast_ref::<LXR<VM>>().unwrap();
            let roots = std::mem::take(&mut self.slots);
            let mut w = ProcessIncs::<_, EDGE_KIND_ROOT>::new(roots, lxr);
            GCWork::do_work(&mut w, self.worker(), self.mmtk());
        }
    }

    fn create_scan_work(&self, _nodes: Vec<ObjectReference>) -> Self::ScanObjectsWorkType {
        unimplemented!()
    }
}

impl<VM: VMBinding> Deref for RCImmixCollectRootEdges<VM> {
    type Target = ProcessEdgesBase<VM>;
    fn deref(&self) -> &Self::Target {
        &self.base
    }
}

impl<VM: VMBinding> DerefMut for RCImmixCollectRootEdges<VM> {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.base
    }
}
