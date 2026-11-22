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
