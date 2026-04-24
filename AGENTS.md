# AGENTS.md

## Purpose

This repository is a serious end-to-end systems prototype, not a toy app.

TickDB is a small analytical database for OHLCV market data. The project exists to demonstrate:

- write-ahead logging
- chunked columnar storage
- column-aware query execution
- metadata-driven pruning
- mmap-based reads
- a small native scan path for hot numeric filters

The core thesis is:

> physical layout plus chunk metadata should materially reduce market-data query cost.

## Project Guardrails

- Keep the scope narrow and finishable within the submission window.
- Prefer a clean, defensible implementation over extra features.
- Optimize for correctness, clarity, benchmarks, and repo quality.
- Treat the repository as something that will be evaluated directly, including commit history.

## Non-Goals

Do not drift into:

- full SQL parsing
- joins
- concurrent writers
- transactions
- distributed execution
- streaming ingestion
- production-grade crash recovery
- speculative optimization work that is not part of the benchmark story

## Storage and Query Rules

- The write path is row-oriented WAL first, then compaction.
- The read path is per-chunk columnar storage, not one global file per column.
- Use fixed-size chunks with explicit `meta.json` per chunk.
- `symbol` should use dictionary encoding as the default implementation.
- `symbol` dictionary + RLE is optional and should only be added if it materially helps the `symbol_time` layout story.
- `timestamp` should use `base + offsets`, not chained deltas.
- Queries should use a small CLI query surface, not a SQL parser.
- Query execution should always inspect chunk metadata before reading heavy column files.
- Benchmarks should focus on reduced work, not inflated absolute performance claims.

## Commit Discipline

- Do not commit unnecessarily.
- Do not make cosmetic-only commits unless they complete a real milestone.
- Do not create `wip`, `misc`, or vague commits.
- Each commit should represent one coherent step in the project.
- Each commit should leave the repo in a runnable, reviewable state.
- Run relevant tests before committing.
- Update docs when the storage format or execution model changes materially.
- Prefer commits that align to milestone completion.
- A milestone commit should include the implementation, relevant tests, and any doc updates needed to explain the change.
- If a milestone grows too large to stay reviewable, split it into a small number of coherent commits rather than forcing one oversized commit.

## Milestone Tracking

- Keep detailed, granular milestone tracking in a local `.internal/` folder.
- The `.internal/` folder is for working notes and progress tracking only and should stay out of version control.
- The detailed tracker can be more granular than the public repo documentation.
- The internal tracker should reflect the actual execution order and completion status of the work.
- Do not let the internal tracker replace code quality, tests, or commit discipline.

## Expected Commit Shape

Commits should generally follow milestone boundaries such as:

1. project skeleton and baseline docs
2. synthetic OHLCV generator
3. WAL ingestion and table metadata
4. columnar compaction and chunk layout
5. symbol and timestamp encodings
6. mmap-based column reads
7. query parsing and query plan construction
8. query execution, aggregation, and projection
9. chunk pruning and execution metrics
10. native scan kernel, benchmarks, and submission polish

These are planning boundaries, not rigid rules. If two adjacent milestones land more cleanly together, that is acceptable. What matters is that each commit stays coherent and reviewable.

## Documentation Rules

- Keep `README.md` repository-facing and concise.
- Keep `docs/design.md` as the engineering spec.
- Keep `docs/architecture.md` aligned with the actual implementation.
- Do not let diagrams drift from the code.
- Final benchmark claims in the README must be backed by runnable code in the repo.

## Testing Rules

- Add tests for every core storage or query feature.
- Prefer deterministic synthetic data in tests.
- Verify correctness before discussing performance.
- If a native path exists, keep a Python fallback and test both behaviors when practical.

## Python Engineering Rules

- Write the minimum amount of code needed to solve the actual problem.
- Do not introduce abstractions before they are justified by the implementation.
- Prefer simple, explicit modules over clever frameworks or generic infrastructure.
- Keep the codebase standard-library-first unless a dependency materially improves the project.
- Separate I/O-heavy code from pure logic whenever practical.

## Python Module Design

- Each module should have one clear responsibility.
- Keep files small and focused; split modules when they start carrying multiple concerns.
- Prefer boundaries like `storage`, `encoding`, `query`, `data`, and `native`, and keep logic inside the correct layer.
- Do not mix CLI parsing, storage format logic, and aggregation logic in the same module.
- Keep the native C boundary isolated behind a small Python wrapper.

## Functions and Classes

- Prefer plain functions when behavior is straightforward.
- Use classes only when they model a real concept with state or lifecycle.
- Do not create classes just to group helper methods.
- Use `@dataclass` for simple structured records such as query specs, chunk metadata, or plans.
- Prefer composition over inheritance.
- Avoid deep class hierarchies entirely unless there is a strong reason.

## Naming Rules

- Use `snake_case` for functions, variables, and module names.
- Use `PascalCase` for class names.
- Use `UPPER_SNAKE_CASE` for constants.
- Choose names that reflect the storage or query concept directly.
- Avoid vague names like `Manager`, `Processor`, `Helper`, `Util`, or `Thing` unless the role is genuinely precise.

## Code Structure Rules

- Keep core data flow explicit: parse -> plan -> prune -> read -> filter -> aggregate.
- Prefer small functions with clear inputs and outputs.
- Keep side effects near the edges of the system.
- Avoid hidden global state.
- Avoid long chains of conditionals when a small explicit dispatcher or table is clearer.
- Keep encoding and decoding logic symmetric and easy to test.
- Keep on-disk format logic centralized rather than duplicated across modules.

## Typing and Validation

- Add type hints to public functions and important internal functions.
- Normalize and validate data at system boundaries.
- Fail loudly on invalid on-disk formats or malformed query inputs.
- Use explicit value conversions instead of relying on implicit coercion.

## Error Handling

- Raise clear, specific errors with actionable messages.
- Do not silently swallow exceptions.
- Handle missing optional native code explicitly and fall back to Python cleanly.
- Keep error handling close to the boundary where the failure can occur.

## Readability Rules

- Optimize for code that can be reviewed quickly.
- Add comments only when they explain non-obvious intent or a tricky invariant.
- Do not add redundant comments that restate the code.
- Prefer straightforward control flow over compact but opaque code.
- Remove dead code and unused helpers instead of keeping them around "just in case."

## Query and Storage Implementation Rules

- Keep query planning separate from query execution.
- Keep chunk pruning logic separate from row-level filtering logic.
- Make required-column calculation explicit in the planner.
- Keep storage layout decisions visible in code and reflected in metadata structures.
- Do not hide important performance behavior behind opaque helper layers.

## Decision Rule

When choosing between two directions, prefer the one that strengthens the final demonstration:

- cleaner architecture
- clearer benchmarks
- tighter scope
- better evidence for the core thesis
