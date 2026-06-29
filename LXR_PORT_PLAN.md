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
- [x] **P3.3** — `plan/lxr` module + `Pause` enum (distinct from `concurrent::Pause`). `c3dfa801d9`
- [~] **P3.4 (R)** — port the immix RC machinery absent from base (**highest risk** — the
      connected core; comes in as a chunk + cargo-fixes, not isolated commits). **P3.4a done**
      (`Line::{of,containing}`, `ce093eefb5`). Remaining is the connected bulk below:
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

## P3.4 connected-core structure (corrected against lxr-v0.32.0, 2026-06-29)

The version-matched reference is laid out differently than the initial map (which mixed in
`lxr/lxr` file names). Verified locations at `lxr/lxr-v0.32.0`:
- **`reset_nursery_state` + the RC nursery page-resource API live in `src/util/heap/blockpageresource_nosweep.rs`** — a *separate* "no-sweep" BlockPageResource variant that LXR's ImmixSpace uses. Our base ImmixSpace `pr` is the standard `BlockPageResource` (415 lines; the nosweep variant is 349). **Decision needed:** add the RC/nursery methods (`reset_nursery_state`, nursery-block tracking) to our `BlockPageResource` **additively/gated** (keep other plans byte-identical) rather than swap the `pr` type per-plan (ImmixSpace `pr` is shared across plans). This is the heaviest sub-piece.
- **`block_allocation.rs` is `src/policy/immix/block_allocation.rs`** (a *policy*-level file), not `plan/lxr/`. Holds `BlockAllocation` + the nursery-block list + `sweep_nursery_blocks`.
- `nursery_blocks`/sweep also touched in `src/args.rs` (consts) + `src/plan/lxr/global.rs`.

So the connected P3.4 batch order is: (1) additive RC/nursery methods on `BlockPageResource` +
`src/policy/immix/block_allocation.rs`; (2) `ImmixSpace` RC fields (`possibly_dead_mature_blocks`,
`block_allocation`, `copy_alloc_bytes`) + methods (`add_to_possibly_dead_mature_blocks`,
`schedule_rc_block_sweeping_tasks`, `prepare_rc`/`release_rc`/`rc_eager_prepare`, `update_global_phase_epoch`);
(3) `src/policy/immix/rc_work.rs` (`SweepBlocksAfterDecs`) + `Block::{init,deinit,rc_sweep_mature,
set_as_in_place_promoted,rc_dead}`; (4) `plan/lxr/rc.rs` (`ProcessIncs`/`ProcessDecs`). Each step
cargo-builds against the prior; the batch is not byte-identical-decomposable below this granularity.

## Strategy refinement: build LXR UP from our base Immix plan, not DOWN from the reference (2026-06-29)

The reference `plan/lxr/global.rs` is **1268 lines / 49 Plan methods / 26 struct fields**, laden with
deferred machinery (cycle collection, mature evac, defrag, unloading, survival predictors) and forward
deps. Porting it wholesale is a non-starter. **Our base `plan/immix/` is only 318 lines** (global.rs 233 /
15 Plan methods, mutator.rs 62, gc_work.rs 17, mod.rs 6) — a clean, working, minimal Immix Plan impl.

**Progress (this construction):**
- **P3.5 DONE** (`84d266b139`): LXR plan = Immix clone, `MMTK_PLAN=LXR` wired. **VALIDATED** — world.opt
  green against lxr-p3, `MMTK_PLAN=LXR` runs, `par_binarytrees`=355319636 (identical to Immix/GenImmix).
- **P3.6 DONE** (`859f82644e`): added the `rc: RefCountHelper<VM>` field (RC foundation; allow(dead_code)).
- **P3.7+ (next — the connected RC heart, NOT byte-identical-decomposable):** the field barrier needs
  `LXRFieldBarrierSemantics` whose `flush` enqueues to `ProcessIncs`/`ProcessDecs`, which need the
  ImmixSpace RC methods (`rc.promote`, `scan_nursery_object`, `add_to_possibly_dead_mature_blocks`,
  `rc_sweep_mature`) + the `Block` RC methods (`rc_dead`, `init`/`deinit`) + `block_allocation.rs` +
  `rc_work.rs` — so the barrier, `rc.rs`, and the policy RC machinery come in as one cargo-fixed batch,
  after which `rc_enabled`/`needs_field_log_bit`/`BarrierSelector::FieldBarrier` flip on in LXR_CONSTRAINTS.

**Original P3.5 plan (now done):**
- **P3.5:** clone `plan/immix/{global,mutator,gc_work,mod}.rs` → `plan/lxr/`, rename Immix→LXR,
  `IMMIX_CONSTRAINTS`→`LXR_CONSTRAINTS` with **`rc_enabled=false`** initially (so the P2
  `debug_assert(!rc_enabled)` guards in immixspace prepare/release don't trip), add `PlanSelector::LXR`
  + the `create_plan`/`create_mutator` arms + options parsing. Result: `MMTK_PLAN=LXR` exists and **runs
  identically to Immix** — compiles, committable, byte-identical to the other plans (nothing else selects it).
- **P3.6+:** layer RC on incrementally, each step built+tested: install `BarrierSelector::FieldBarrier`
  (P3.2) on the LXR mutator; add the `RefCountHelper` field; wire `Pause::RefCount` into
  `schedule_collection`; bring in `ProcessIncs`/`ProcessDecs` (rc.rs) + the sweep machinery; only flip
  `rc_enabled=true` once the RC behavior the asserts guard is actually in place. This keeps `lxr-p3`
  green at every step instead of a big-bang non-compiling batch.

This supersedes the "vendor the reference plan/lxr wholesale" framing of P3.6 above.

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
