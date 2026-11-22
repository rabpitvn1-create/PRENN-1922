"""SQLite schema for an initial local CRS store."""

REGISTRY_SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS codex_registry (
    cdx TEXT PRIMARY KEY, title TEXT NOT NULL, retired INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS shard_catalog (
    shard_id TEXT PRIMARY KEY, cdx TEXT NOT NULL REFERENCES codex_registry(cdx),
    relative_path TEXT NOT NULL UNIQUE, frozen INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS entry_registry (
    anchor TEXT PRIMARY KEY, cdx TEXT NOT NULL REFERENCES codex_registry(cdx),
    title TEXT NOT NULL, entity_type TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS entity_aliases (
    alias_id INTEGER PRIMARY KEY, anchor TEXT NOT NULL REFERENCES entry_registry(anchor),
    display_name TEXT NOT NULL, exact_key TEXT NOT NULL, folded_key TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_alias_exact ON entity_aliases(exact_key);
CREATE TABLE IF NOT EXISTS cross_codex_relations (
    id INTEGER PRIMARY KEY, source_anchor TEXT NOT NULL, target_anchor TEXT NOT NULL,
    relation TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS release_manifest (
    release_id TEXT PRIMARY KEY, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
    registry_sha256 TEXT, published INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS release_shards (
    release_id TEXT NOT NULL REFERENCES release_manifest(release_id),
    shard_id TEXT NOT NULL REFERENCES shard_catalog(shard_id),
    sha256 TEXT NOT NULL, PRIMARY KEY(release_id, shard_id)
);
CREATE TABLE IF NOT EXISTS chunk_locator (
    code TEXT PRIMARY KEY, shard_id TEXT NOT NULL REFERENCES shard_catalog(shard_id),
    cdx TEXT NOT NULL, section INTEGER NOT NULL, entity_anchor TEXT NOT NULL,
    title TEXT NOT NULL, topic TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS section_toc (
    cdx TEXT NOT NULL, section INTEGER NOT NULL, group_name TEXT NOT NULL,
    title TEXT NOT NULL, PRIMARY KEY(cdx, section)
);
CREATE TABLE IF NOT EXISTS title_index (
    code TEXT PRIMARY KEY REFERENCES chunk_locator(code), title_key TEXT NOT NULL,
    breadcrumb TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS idx_titles ON title_index(title_key);
CREATE TABLE IF NOT EXISTS game_sessions (
    session_id TEXT PRIMARY KEY, release_id TEXT NOT NULL REFERENCES release_manifest(release_id)
);
CREATE TABLE IF NOT EXISTS session_intents (
    intent_id TEXT PRIMARY KEY, session_id TEXT NOT NULL REFERENCES game_sessions(session_id),
    entity_anchor TEXT NOT NULL, cdx TEXT NOT NULL, section INTEGER NOT NULL,
    topic TEXT NOT NULL, expected_title TEXT NOT NULL, state TEXT NOT NULL DEFAULT 'PINNED',
    repair_count INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS retrieval_ledger (
    id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, intent_id TEXT NOT NULL,
    code TEXT NOT NULL, status TEXT NOT NULL, created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
CREATE TABLE IF NOT EXISTS world_state (
    session_id TEXT NOT NULL, key TEXT NOT NULL, value_json TEXT NOT NULL,
    PRIMARY KEY(session_id, key)
);
CREATE TABLE IF NOT EXISTS event_log (
    id INTEGER PRIMARY KEY, session_id TEXT NOT NULL, event_json TEXT NOT NULL,
    created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
);
"""

SHARD_SCHEMA = """
PRAGMA foreign_keys=ON;
CREATE TABLE IF NOT EXISTS chunks (
    code TEXT PRIMARY KEY, cdx TEXT NOT NULL, section INTEGER NOT NULL,
    entity_anchor TEXT NOT NULL, title TEXT NOT NULL, topic TEXT NOT NULL,
    breadcrumb TEXT NOT NULL, content TEXT NOT NULL, content_sha256 TEXT NOT NULL,
    published INTEGER NOT NULL DEFAULT 0
);
CREATE TABLE IF NOT EXISTS chunk_relations (
    source_code TEXT NOT NULL, target_code TEXT NOT NULL,
    relation TEXT NOT NULL, release_id TEXT NOT NULL,
    PRIMARY KEY(source_code, target_code, relation, release_id)
);
CREATE TRIGGER IF NOT EXISTS block_published_update
BEFORE UPDATE ON chunks WHEN OLD.published = 1
BEGIN SELECT RAISE(ABORT, 'PUBLISHED_CHUNK_IMMUTABLE'); END;
CREATE TRIGGER IF NOT EXISTS block_published_delete
BEFORE DELETE ON chunks WHEN OLD.published = 1
BEGIN SELECT RAISE(ABORT, 'PUBLISHED_CHUNK_IMMUTABLE'); END;
CREATE TRIGGER IF NOT EXISTS block_published_replace
BEFORE INSERT ON chunks WHEN EXISTS (
    SELECT 1 FROM chunks WHERE code=NEW.code AND published=1
)
BEGIN SELECT RAISE(ABORT, 'PUBLISHED_CHUNK_IMMUTABLE'); END;
"""
