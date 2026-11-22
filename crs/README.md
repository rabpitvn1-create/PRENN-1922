# PRENN CRS v3.0 foundation

This package is isolated from the existing game code. It uses only Python standard-library modules and local SQLite. Python 3.10+ is required.

## Included

- Deterministic `CDX.SEC.CHKCC` codes, Base36 weighted checksum modulo 1296, with immutable identifier records.
- Local central registry schema and per-codex SQLite shard schema.
- Metadata tables: codex_registry, shard_catalog, entry_registry, entity_aliases, cross_codex_relations, chunk_locator, section_toc, title_index, release_manifest, release_shards, game_sessions, session_intents, retrieval_ledger, world_state, event_log.
- Immutable published chunks with UPDATE, DELETE, and REPLACE guards; `PRAGMA recursive_triggers=ON`.
- Offline initial publication to a read-only registry snapshot.
- Local session state with pinned intent IDs, not caller-provided intent objects.
- Exact GET with checksum, release, anchor, scope, title, SHA-256 verification; paged TOC root/group and IDX.

## Test

From repository root:

```sh
python -m unittest discover -s crs/tests -v
```

Python import:

```python
from crs import CodexStore, CodexReader, make_code
```

## Strict limitations

**Not production-ready.** The initial `publish()` is NOT crash-atomic and does not implement immutable-shard reuse/delta releases. The offline indexer must run only against unpublished data. The `pin_intent()` method is a **trusted coordinator operation** and must never be exposed as an LLM-controlled tool. The foundation currently does not include automatic code allocation/tombstones, a repair-round state machine, GET_BATCH token budgeting, a network HTTP facade, token-aware pagination, content ingestion/chunking, FTS5 indexing, shard archive recovery, or model/scaling benchmarks. There is no public HTTP listener.

Do not commit generated `.db` files or expose mutable session state to untrusted callers.

## Offline ingestion (new)

Use `crs.ingest.OfflineIndexer(root).ingest(cdx, codex_title, markdown, release_id)` to parse Markdown headings first, then chunk by semantic paragraph/sentence. Explicit type hints are supported using `<!-- CRS:FACT -->`, `RULE`, `LORE`, or `NARRATIVE` immediately inside a section. Without a hint, classification uses conservative heuristics, so editorial review is essential for authoritative rules. Token estimates count regex lexical units, **not actual LLM tokens**. Oversized indivisible RULE paragraphs are rejected rather than cut across conditions/exceptions. Short semantic units may be below a target minimum.

Chunk identifiers come from persistent `code_allocations`; retired codes are never reclaimed. Section 000 reserves entry anchors. The indexer populates NFC/casefold `title_exact`, accent-folded `title_folded`, and SQLite FTS5 with `unicode61 remove_diacritics 0` for **candidate lookup only**. GET remains exact and intent-pinned.

After ingestion, call `crs.ingest.publish_atomic(root, "R001")`. This validates chunk content and SQLite integrity, constructs a frozen release registry snapshot under `releases/R001/registry.db`, then atomically replaces the small `CURRENT` text pointer. Reader usage stays `CodexReader(root, "R001")`; `CURRENT` is not an authorization mechanism for an LLM caller. The previous published release remains usable after a failed pre-commit activation. Newly built releases reuse unchanged, immutable shard files; new source material is placed in a distinct shard. Returned report provides chunk counts, approximate logical metadata/index/FTS bytes, database bytes, shard hashes, and average size. Logical byte estimates **are not physical SQLite page attribution**. All shards are reported as hot until archive-tier placement is implemented.

Convenience: `crs.ingest.ingest_file(root, "KAE", "Kael", source_path, "R001")` performs ingestion and publication. Run the focused tests with `python -m unittest discover -s crs/tests -v`.

## Remaining limitations

This is an offline single-writer foundation, **not production certification**. The new publisher does not retrofit or replace the older, non-atomic `CodexStore.publish()`. No cross-process write lock, automatic failed-build cleanup/retry, physical page-level storage accounting, human-reviewed semantic classification, real model tokenizer, automated section rollover, archive restoration, or benchmark of 100k-word corpora has been established. The `CURRENT` pointer is atomic on the local filesystem, but crash durability across sudden power loss and multi-platform filesystems still needs fault-injection verification. Restrict inputs to trusted offline codex material and keep this root on a local filesystem.
