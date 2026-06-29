# LXR P3 port plan (minimal single-domain RC for RQ1)

Working roadmap for porting the LXR plan into this fork (`gc/mmtk-core`), branch
**`lxr-p3`** (atop the merged P1/P2 read-side scaffolding at `0.32-ocaml`). Goal: the
**minimal single-domain LXR that runs well enough to MEASURE RQ1** — (a) the field-logging
write-barrier overhead on OCaml's mostly-initializing-write heap, and (b) RC pause/tail
latency vs GenImmix at memory parity. Reference: `lxr/lxr-v0.32.0` (version-matched to our
0.32.0 base) — read with `git show lxr/lxr-v0.32.0:<path>`.

## Discipline
- **Additive-and-gated**: every non-additive LXR signature change is added as a *parallel*
  method (e.g. `initialize_object_metadata_bytes`, `attempt_mark_rc`) so the 10 shipping
  plans stay **byte-identical**. No plan sets `rc_enabled`/`needs_field_log_bit` until the
  LXR plan lands, so the gated overlays stay inert.
- Build the first measurable cut with cargo features **`lxr_no_cm`, `lxr_no_mature_evac`,
  `lxr_no_lazy`** (≈ the `lxr_stw` profile). Cycle collection is **deferrable** (RC-only is
  memory-safe; only leaks cyclic garbage — rare in OCaml's immutable-by-default heap). Size
  the heap (always pass `MMTK_HEAP_SIZE_MB` at GenImmix parity) so the `Pause::Full` OOM
  fallback never fires; optionally `lxr_abort_on_trace` to *prove* no trace ran.

## Pause model
Minimal subset needs only **`Pause::RefCount`** (inc/dec + nursery sweep + lazy block
sweep) + **`Pause::Full`** (OOM fallback, never entered if the heap is sized right).
`FullDefrag` is `unreachable!()` even in LXR; `InitialMark`/`FinalMark` are the SATB
cycle-collection pauses — deferred (`lxr_no_cm`).

## Checklist (R = required for minimal, D = deferrable)

- [x] **P3.0** — `PlanConstraints.needs_field_log_bit` (default false) +
      `SFT::initialize_object_metadata_bytes` (default forwards). `1b1a43fe63`
- [x] **P3.1** — vendor `Address::{is_field_logged,attempt_log_field,log_field,unlog_field,
      unlog_field_relaxed}` (the per-field unlog-bit primitives). `88afa7d997`
- [x] **P3.2** — vendor `FieldBarrier<S>` (the pre-write delegating wrapper). `a3cc3f02a3`
- [ ] **P3.3 (R)** — `Pause` enum (in a new `plan/lxr/` module, NOT `immix/mod.rs`, to
      avoid colliding with the base concurrent `Pause`).
- [ ] **P3.4 (R)** — port the immix RC machinery absent from base (**highest risk** — the
      moving/sweep core; lean on the `sanity` feature at small heap): new
      `policy/immix/rc_work.rs` (`SweepBlocksAfterDecs`, `SweepDeadCycles`); a trimmed
      `policy/immix/block_allocation.rs` (`BlockAllocation`, nursery block list,
      `sweep_nursery_blocks`); `Block::{log,unlog,rc_dead,rc_sweep_mature,attempt_dealloc,
      is_nursery,set_as_in_place_promoted,...}` + `&ImmixSpace` `init/deinit`; `Line`
      align/containing/RC-array; `ImmixSpace::{possibly_dead_mature_blocks,
      add_to_possibly_dead_mature_blocks,schedule_rc_block_sweeping_tasks,prepare_rc,
      release_rc,rc_eager_prepare,trace_object_without_moving_rc,attempt_mark_rc,unmark_rc,
      copy_alloc_bytes,block_allocation}`.
- [ ] **P3.5 (R)** — port LOS RC write/free side (`rc_free`, `release_rc_nursery_objects`,
      `sweep_rc_mature_objects_after_satb`, `attempt_mark`, `trace_object_rc`,
      `RCSweepMatureAfterSATBLOS`, `initialize_object_metadata_bytes` RC branch).
- [ ] **P3.6 (R)** — vendor `plan/lxr/` (`mod.rs` trim predictors→consts, `global.rs`,
      `mutator.rs`, `barrier.rs`=`LXRFieldBarrierSemantics`, `gc_work.rs`, **`rc.rs`** =
      `ProcessIncs`/`ProcessDecs` [the heart]); vendor `cm.rs`/`remset.rs`/`mature_evac.rs`
      **intact for compilation** (dead under `lxr_no_cm`/`lxr_no_mature_evac`).
- [ ] **P3.7 (R)** — scheduler/lib.rs runtime support (`LazySweepingJobs*`,
      `postpone*`, lib.rs globals `NO_EVAC`/`REMSET_RECORDING`/`RC_STAT`/...). The
      `WorkBucketStage` RC aliases already exist in base.
- [ ] **P3.8 (R)** — plan wiring: `PlanSelector::LXR` in `options.rs` + `create_plan`/
      `create_mutator` arms; `LXR_CONSTRAINTS` (`rc_enabled=true`, `needs_field_log_bit=true`,
      `barrier: FieldBarrier`).
- [ ] **P3.9 (R)** — fixed-heap enforcement: require `FixedHeapSize` for LXR (reject the
      SpaceOverhead dynamic trigger for this plan); LXR `gc_init` panics otherwise.
- [ ] **P4 (R, binding side, separate `gc/mmtk` crate + runtime)** — override
      `ObjectModel::GLOBAL_FIELD_UNLOG_BIT_SPEC` to `side_after` the log bit; route
      `caml_modify`/array-blit barriers to the field-barrier slot path for LXR (a
      `caml_mmtk_field_log` gate parallel to `caml_mmtk_generational`/`_concurrent`). **The
      RQ1 crux: do NOT log `caml_initialize` (initializing writes) as mutations** — OCaml's
      heap is initializing-write-dominated, which is exactly why the barrier is nearly free.

## P4 contract (good news: the runtime already has the shape)
`runtime/memory.c`'s `caml_modify` already issues a **pre-store, slot-granular** barrier
(`caml_mmtk_satb_barrier(Op_val(obj)+field, 1)` → `memory_region_copy_pre` →
`BarrierSemantics::memory_region_copy_slow`). LXR's `LXRFieldBarrierSemantics::
memory_region_copy_slow` is exactly that per-slot path. So P4 is mostly a routing gate +
the field-unlog-bit spec override + the initializing-write policy decision.

## Conflicts vs our deltas
- **Fixed heap**: LXR requires `FixedHeapSize`; our default is the SpaceOverhead dynamic
  trigger. Always run RQ1 with `MMTK_HEAP_SIZE_MB` at GenImmix parity (P3.9).
- **no-zero alloc**: orthogonal (unlog-bit bulk writes target side metadata, not the heap);
  low risk — verify the debug-runtime `caml_initialize` zero-init relaxation still holds.
- **side-metadata budget**: already solved — `RC_TABLE`/`RC_STRADDLE_LINES`/P2 specs are
  append-only; `GLOBAL_FIELD_UNLOG_BIT_SPEC` is a VM-side spec (`side_after` the log bit),
  no core-global overflow.

## End-to-end minimal path
alloc via Immix (RC=0) → mutating store fires the field barrier → unlog bit + push slot to
`ProcessIncs` + old value to `ProcessDecs` → at an RC pause, `RCImmixCollectRootEdges` →
`ProcessIncs` promotes roots (0→1) + recursively incs the reachable subgraph + sweeps
unpromoted nursery blocks; `ProcessDecs` decrements old referents, and on 0 runs
`process_dead_object` → recursive decs + `add_to_possibly_dead_mature_blocks` →
`SweepBlocksAfterDecs` frees empty blocks (LOS via `rc_free`). No trace, no cycle collection.
