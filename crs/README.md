# PRENN CRS v3.0 foundation and offline ingestion

CRS is a local-only Codex Retrieval System. The package uses Python 3.10+ and the Python standard library, with SQLite (`sqlite3`) as its only database. It does not require a database server, hosted database, vector database, Redis, or network service.

## Core guarantees

- Deterministic `CDX.SEC.CHKCC` identifiers with a weighted Base36 checksum modulo 1296.
- Central, persistent code and section reservations. Failed builds leave identifier tombstones; allocated codes and section numbers are never reused.
- Per-codex SQLite shards. A shard is rejected above 2,000,000,000 physical bytes.
- Published chunks are immutable. UPDATE, DELETE, and REPLACE guards are active with `PRAGMA recursive_triggers=ON`.
- Release-pinned GET checks syntax/checksum, release membership, entity anchor, intent scope, shard hash, chunk metadata, and content SHA-256 before returning content.
- `title_exact` is NFC plus casefold and retains diacritics. `title_folded` is NFD with combining marks removed and `đ` folded to `d`.
- FTS5 uses `unicode61 remove_diacritics 0` and is candidate search only. It never authorizes or supplies GET content.

## Offline ingestion

```python
from crs.ingest import OfflineIndexer, publish_atomic

indexer = OfflineIndexer("./codex-data")
try:
    report = indexer.ingest("KAE", "Kael", markdown_text, "R001")
finally:
    indexer.close()
release_report = publish_atomic("./codex-data", "R001")
```

A file convenience function is also available:

```python
from crs.ingest import ingest_file
report = ingest_file("./codex-data", "KAE", "Kael", "./Kael.md", "R001")
```

The indexer parses Markdown headings/sections before size-based segmentation. Explicit type hints use `<!-- CRS:FACT -->`, `RULE`, `LORE`, or `NARRATIVE` immediately inside a section. Otherwise, conservative heuristics classify the section. Semantic paragraph and sentence units are packed to these CRS lexical-token targets:

| Type | Target lexical tokens |
|---|---:|
| FACT | 50–150 |
| RULE | 100–250 |
| LORE | 200–400 |
| NARRATIVE | 300–500 |

A semantic unit may remain below the lower bound when combining it would break meaning. RULE paragraphs are atomic; a condition and a separately formatted exception are kept together. Oversized atomic RULEs are rejected rather than split. Breadcrumbs are capped at 60 lexical tokens and context-prepends at 40.

The tokenizer is deterministic and stdlib-only (`\\w+` plus punctuation units). These are CRS lexical tokens, not a claim of exact compatibility with a particular model tokenizer. Authoritative rule classification still requires editorial review.

### Staging and rollback

Every indexer build writes its shard database under `staging/build_<id>/`. Manual `CodexStore.new_shard()` authoring also writes under `staging/`. No new shard file is written directly into the live `shards/` directory during BUILD.

A successful ingestion marks the staging artifact READY. Failed ingestion removes its staging directory and pending build metadata. Allocated identifiers remain reserved as tombstones. A process crash during BUILD is recovered under the writer lock; incomplete BUILDING directories are discarded, while READY builds remain available for publish/retry.

### Publish protocol

`publish_atomic()` is the only publisher. `CodexStore.publish()` is retained only as a compatibility delegate to it.

1. **BUILD:** create new shard data in staging.
2. **VERIFY:** check SQLite integrity, foreign keys, chunk hashes, code allocations, anchors, title coverage, and shard size.
3. **PREPARE:** promote only verified complete shard files, create a frozen registry snapshot, and write `manifest.json` plus `manifest.sha256`.
4. **COMMIT:** verify snapshot and manifest hashes, then atomically replace the small `CURRENT` pointer with `os.replace()`.
5. **RETAIN:** keep all earlier release snapshots and immutable shards. Delta releases reuse unchanged shards and add only new immutable shards.

A failed or interrupted pre-COMMIT attempt does not change `CURRENT`; the prior release remains readable. A retry can recover a fully promoted shard or discard an unactivated prepared snapshot. A kernel advisory writer lock serializes ingestion and publication and is released automatically when a process exits.

Readers are explicitly pinned: `CodexReader(root, "R001")`. `CURRENT` is an activation pointer, not an authorization mechanism. `pin_intent()` is a trusted coordinator operation and must never be exposed as an LLM-controlled tool.

## Physical indexer report

The report measures every SQLite database using `PRAGMA page_count * PRAGMA page_size`:

- registry snapshot database;
- each hot shard and each archive shard separately;
- FTS5, title-index, and manifest page allocations using SQLite `dbstat` attribution inside the registry database;
- manifest sidecar file bytes;
- remaining staging databases and bytes promoted from staging.

FTS/title/manifest component bytes are sub-allocations within the registry database and are not added a second time to the total database bytes. Shards are hot by default; the archive category is reported separately and remains zero until a shard is assigned an archive path.

## Verification and benchmark

Run the complete CRS test suite from the repository root:

```sh
python -m unittest discover -s crs/tests -v
```

Run the reproducible 100,000-word synthetic corpus benchmark:

```sh
python -m crs.benchmarks.benchmark_large_codex --words 100000
```

The benchmark reports elapsed time, chunk counts, and the same physical storage breakdown as a normal release.

## Scope and operational limits

This is an offline single-writer foundation, not production certification. Keep the data root on a local filesystem. FTS is candidate-only. Classification is heuristic unless source sections carry explicit type hints. The lexical tokenizer is not model-specific. Archive tier placement is reported but not automatically managed. There is no public HTTP listener, hosted service, vector index, or external database.
