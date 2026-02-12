# Planned Improvements Plan (Feasibility + Order)

Assessed: 2026-02-12
Repo: /home/sprite/incoming
Current version: 0.1.0

## Corrections vs planned_improvements.md (drift)

- Item 6 "Missing policy phases" is partially incorrect.
- Implemented today: `:connect`, `:helo`, `:message_complete` are already enforced in `lib/incoming/session.ex`.
- Not implementable with current `gen_smtp` callback surface: `:data_chunk` policy phase (no chunk callback; DATA is accumulated and delivered as a single binary).
- Item 17 "RFC 5322 header folding missing" is incorrect: header unfolding exists in `lib/incoming/message.ex`.

## Feasibility Matrix (by item number)

Legend:
- DONE: already implemented
- FEASIBLE: implementable cleanly in this repo
- PARTIAL: implementable only partially due to `gen_smtp` constraints
- LARGE: significant scope / separate milestone
- BLOCKED: not realistically implementable without upstream/forking `gen_smtp`

1. Backpressure / max_depth: FEASIBLE
2. Per-IP connection limits: FEASIBLE (needs ETS + correct session teardown)
3. Max commands / max errors per session: FEASIBLE (disconnect via self-stop message; see plan)
4. Slowloris / data rate protection: PARTIAL/BLOCKED (can't enforce bytes/sec minimum from callbacks)
5. Incoming.Connection struct: PARTIAL (no peer port, TLS cipher/version not available from callbacks)
6. Missing policy phases: DONE for connect/helo/message_complete; BLOCKED for data_chunk
7. Policy metadata accumulation: FEASIBLE (policy interface expansion)
8. Postgres queue backend: OUT OF SCOPE (core); optional external backend package if desired
9. Graceful shutdown / drain: FEASIBLE (stop listeners + drain sessions; delivery drain semantics need definition)
10. Queue cleanup / GC: FEASIBLE
11. Configurable telemetry prefix: FEASIBLE
12. Fuzz testing: FEASIBLE (tooling work)
13. Corpus testing: FEASIBLE (ops + storage; not unit-test friendly)
14. Load/stress testing: FEASIBLE (bench harness)
15. Chaos engineering: FEASIBLE (harness)
16. Client compatibility testing: FEASIBLE (harness + scripts)
17. RFC 5322 header folding: DONE (already supported)
18. Remove tracked erl_crash.dump: FEASIBLE
19. CI/CD: FEASIBLE
20. Lower Elixir version requirement: FEASIBLE
21. Split test file: FEASIBLE
22. Add config examples: FEASIBLE
23. Publish Hex docs: FEASIBLE
24. Move recover() out of Supervisor.init/1: FEASIBLE
25. Fix queue path inconsistency: FEASIBLE
26. Remove unnecessary Code.ensure_loaded?(:telemetry) guard: FEASIBLE
27. Fix fire-and-forget in Delivery.Dispatcher: FEASIBLE
28. Rate limiter ETS unbounded growth: FEASIBLE (cap + aggressive cleanup)

## Recommended Execution Order (phased)

Phase 0: Document + correctness
- Update planned docs to stop drifting (this file becomes source of truth).
- Optional: patch `planned_improvements.md` to reflect DONE/BLOCKED items.

Phase 1: Production-safety, minimal surface area
- (1) Backpressure `max_depth` with `421 Try again later`
- (2) Per-IP connection limits with `421` reject at connect time
- (3) Max commands / max errors with `421` and forced disconnect
- (11) Configurable telemetry prefix
- (24) Move `recover()` out of `Supervisor.init/1`
- (26) Remove telemetry guard
- (27) Fix dispatcher async error visibility
- (25) Fix queue path inconsistency (needed to keep limits correct)

Phase 2: Operational hardening
- (10) Queue cleanup / GC (dead-letter retention)
- (9) Graceful shutdown / drain
- (28) Rate limiter ETS cap / tighter cleanup

Phase 3: Policy API expansion
- (5) Incoming.Connection struct (best-effort metadata)
- (7) Policy metadata accumulation

Phase 4: Big bets / harnesses
- (12-16) fuzz/corpus/load/chaos/interop test harnesses
- (19) CI matrix and runtime coverage enforcement

Postgres note:
- Do not add Postgres/Ecto dependencies to `incoming` core.
- If a Postgres-backed queue is ever needed, ship it as a separate optional package (e.g. `incoming_queue_postgres`) that implements `Incoming.Queue`.

Phase 5: Polish
- (18) remove tracked `erl_crash.dump`
- (20-23) version floor, test split, config examples, published docs

