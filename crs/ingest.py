"""Offline, stdlib-only CRS ingestion and atomic release publication.

Token counts are deterministic lexical-token estimates, not model-specific tokens.
All new shard databases are built under staging/ and promoted only after verification.
"""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import sqlite3
import unicodedata
import uuid

from .ids import make_code, parse_code
from .schema import SHARD_SCHEMA
from .store import CodexStore, connect, database, digest, file_hash

TARGETS = {"FACT": (50, 150), "RULE": (100, 250),
           "LORE": (200, 400), "NARRATIVE": (300, 500)}
HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
TYPE = re.compile(r"^\s*<!--\s*CRS:(FACT|RULE|LORE|NARRATIVE)\s*-->\s*$")
SENTENCES = re.compile(r"(?<=[.!?。！？])\s+(?=[^\s])")
WORDS = re.compile(r"\w+|[^\w\s]", re.UNICODE)
DANGLING = re.compile(r"^(?:he|she|they|it|this|that|these|those|"
                      r"hắn|cô ấy|người này|điều này|nó|ông ấy)\b", re.I)
RULE_CONDITION = re.compile(r"\b(if|when|whenever|provided that|nếu|khi|trong trường hợp)\b", re.I)
RULE_EXCEPTION = re.compile(r"\b(unless|except|exception|ngoại lệ|trừ khi)\b", re.I)


def tokens(text: str) -> int:
    """Count CRS lexical tokens using the documented stdlib-only tokenizer."""
    return len(WORDS.findall(text))


def title_keys(title: str) -> tuple[str, str]:
    """Exact NFC/casefold key and accent-folded NFD key (including đ -> d)."""
    exact = unicodedata.normalize("NFC", title).casefold()
    folded = "".join(
        c for c in unicodedata.normalize("NFD", exact)
        if unicodedata.category(c) != "Mn"
    ).replace("đ", "d")
    return exact, unicodedata.normalize("NFC", folded)


def _category(title: str, text: str) -> str:
    sample = (title + " " + text[:400]).casefold()
    if re.search(r"\b(if|unless|must|shall|except|rule|điều kiện|ngoại lệ|phải|cấm)\b", sample):
        return "RULE"
    if re.search(r"\b(scene|dialogue|said|whispered|cảnh|đối thoại|nói|hét)\b", sample):
        return "NARRATIVE"
    if re.search(r"\b(date|born|age|height|year|ngày sinh|năm sinh|tuổi)\b", sample):
        return "FACT"
    return "LORE"


def sections(source: str):
    """Parse Markdown headings before applying any size-based segmentation."""
    title, lines, hint = None, [], None
    for raw in source.splitlines():
        marker = TYPE.fullmatch(raw)
        if marker:
            if title is not None and any(line.strip() for line in lines):
                yield title, "\n".join(lines).strip(), hint
                lines = []
            hint = marker.group(1)
            continue
        match = HEADING.fullmatch(raw)
        if match:
            if title is not None and any(line.strip() for line in lines):
                yield title, "\n".join(lines).strip(), hint
            title, lines, hint = match.group(2).strip(), [], None
        else:
            if title is None:
                title = "Introduction"
            lines.append(raw)
    if title is not None and any(line.strip() for line in lines):
        yield title, "\n".join(lines).strip(), hint


def _units(text: str, kind: str, maximum: int) -> list[str]:
    """Keep paragraphs atomic; RULE paragraphs include their conditions/exceptions."""
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    if kind == "RULE":
        # A condition followed by a separately formatted exception remains one rule.
        linked: list[str] = []
        index = 0
        while index < len(paragraphs):
            paragraph = paragraphs[index]
            if (RULE_CONDITION.search(paragraph) and not RULE_EXCEPTION.search(paragraph)
                    and index + 1 < len(paragraphs)
                    and RULE_EXCEPTION.search(paragraphs[index + 1])):
                bundle = [paragraph]
                index += 1
                while index < len(paragraphs):
                    bundle.append(paragraphs[index])
                    if RULE_EXCEPTION.search(paragraphs[index]):
                        break
                    index += 1
                linked.append("\n\n".join(bundle))
            else:
                linked.append(paragraph)
            index += 1
        paragraphs = linked
    output: list[str] = []
    for paragraph in paragraphs:
        if tokens(paragraph) <= maximum:
            output.append(paragraph)
            continue
        if kind == "RULE":
            raise ValueError("OVERSIZED_ATOMIC_RULE")
        sentences = [s.strip() for s in SENTENCES.split(paragraph) if s.strip()]
        if len(sentences) == 1:
            raise ValueError("OVERSIZED_ATOMIC_UNIT")
        for sentence in sentences:
            if tokens(sentence) > maximum:
                raise ValueError("OVERSIZED_ATOMIC_UNIT")
        output.extend(sentences)
    return output


def chunk_text(text: str, kind: str) -> list[str]:
    """Pack semantic units within the per-kind ceiling without splitting atomic units."""
    if kind not in TARGETS:
        raise ValueError("INVALID_CHUNK_TYPE")
    minimum, maximum = TARGETS[kind]
    units = _units(text, kind, maximum)
    groups: list[list[str]] = []
    current: list[str] = []
    for unit in units:
        if current and tokens("\n\n".join(current + [unit])) > maximum:
            groups.append(current)
            current = []
        current.append(unit)
    if current:
        groups.append(current)
    # Rebalance the tail while preserving each semantic unit.
    if len(groups) > 1 and tokens("\n\n".join(groups[-1])) < minimum:
        previous, tail = groups[-2], groups[-1]
        while len(previous) > 1 and tokens("\n\n".join(tail)) < minimum:
            tail.insert(0, previous.pop())
        if (tokens("\n\n".join(tail)) < minimum
                and tokens("\n\n".join(previous + tail)) <= maximum):
            groups[-2:] = [previous + tail]
    return ["\n\n".join(group) for group in groups]


@contextmanager
def _writer_lock(root: Path):
    """Kernel-released advisory lock; safe against stale lock files after a crash."""
    root.mkdir(parents=True, exist_ok=True)
    lock_path = root / ".crs-writer.lock"
    handle = lock_path.open("a+b")
    try:
        if os.name == "nt":
            import msvcrt
            handle.seek(0, os.SEEK_END)
            if handle.tell() == 0:
                handle.write(b"\0")
                handle.flush()
            handle.seek(0)
            msvcrt.locking(handle.fileno(), msvcrt.LK_LOCK, 1)
        else:
            import fcntl
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX)
        yield
    finally:
        try:
            if os.name == "nt":
                import msvcrt
                handle.seek(0)
                msvcrt.locking(handle.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(handle.fileno(), fcntl.LOCK_UN)
        finally:
            handle.close()


def _schema(db: sqlite3.Connection):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS code_allocations (
            code TEXT PRIMARY KEY, cdx TEXT NOT NULL,
            section INTEGER NOT NULL, chunk_no INTEGER NOT NULL,
            UNIQUE(cdx,section,chunk_no)
        );
        CREATE TABLE IF NOT EXISTS section_allocations (
            cdx TEXT NOT NULL, section INTEGER NOT NULL, build_id TEXT,
            PRIMARY KEY(cdx,section)
        );
        CREATE TABLE IF NOT EXISTS ingestion_sources (
            source_sha256 TEXT PRIMARY KEY, cdx TEXT NOT NULL,
            release_id TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingestion_builds (
            build_id TEXT PRIMARY KEY, release_id TEXT NOT NULL, cdx TEXT NOT NULL,
            codex_title TEXT NOT NULL, shard_id TEXT NOT NULL UNIQUE,
            stage_relpath TEXT NOT NULL UNIQUE, source_sha256 TEXT NOT NULL UNIQUE,
            state TEXT NOT NULL CHECK(state IN ('BUILDING','READY','PROMOTED')),
            chunk_count INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );
    """)
    code_cols = {r[1] for r in db.execute("PRAGMA table_info(code_allocations)")}
    if "build_id" not in code_cols:
        db.execute("ALTER TABLE code_allocations ADD COLUMN build_id TEXT")
    source_cols = {r[1] for r in db.execute("PRAGMA table_info(ingestion_sources)")}
    if "build_id" not in source_cols:
        db.execute("ALTER TABLE ingestion_sources ADD COLUMN build_id TEXT")
    title_cols = {r[1] for r in db.execute("PRAGMA table_info(title_index)")}
    if "title_exact" not in title_cols:
        db.execute("ALTER TABLE title_index ADD COLUMN title_exact TEXT")
    if "title_folded" not in title_cols:
        db.execute("ALTER TABLE title_index ADD COLUMN title_folded TEXT")
    manifest_cols = {r[1] for r in db.execute("PRAGMA table_info(release_manifest)")}
    if "activated" not in manifest_cols:
        db.execute("ALTER TABLE release_manifest ADD COLUMN activated INTEGER NOT NULL DEFAULT 0")
        db.execute("UPDATE release_manifest SET activated=1")
    db.execute("CREATE INDEX IF NOT EXISTS idx_title_exact ON title_index(title_exact)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_title_folded ON title_index(title_folded)")
    db.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS title_fts USING fts5(
        code UNINDEXED, title, breadcrumb, tokenize='unicode61 remove_diacritics 0'
    )""")
    db.commit()


def _allocate(db: sqlite3.Connection, cdx: str, section: int,
              build_id: str | None = None) -> str:
    row = db.execute(
        "SELECT COALESCE(MAX(chunk_no),0)+1 FROM code_allocations WHERE cdx=? AND section=?",
        (cdx, section),
    ).fetchone()
    number = row[0]
    if number > 9999:
        raise ValueError("CHUNK_CODE_EXHAUSTED")
    code = make_code(cdx, section, number)
    with db:
        db.execute(
            "INSERT INTO code_allocations(code,cdx,section,chunk_no,build_id) VALUES(?,?,?,?,?)",
            (code, cdx, section, number, build_id),
        )
    return code


def _next_section(db: sqlite3.Connection, cdx: str, build_id: str) -> int:
    reserved = db.execute(
        "SELECT COALESCE(MAX(section),0) FROM section_allocations WHERE cdx=?", (cdx,)
    ).fetchone()[0]
    published = db.execute(
        "SELECT COALESCE(MAX(section),0) FROM section_toc WHERE cdx=?", (cdx,)
    ).fetchone()[0]
    number = max(reserved, published) + 1
    if number > 999:
        raise ValueError("SECTION_CODE_EXHAUSTED")
    with db:
        db.execute("INSERT INTO section_allocations VALUES(?,?,?)", (cdx, number, build_id))
    return number


def _remove_stage_path(root: Path, relative: str):
    stage_root = (root / "staging").resolve()
    path = (root / relative).resolve()
    if not path.is_relative_to(stage_root):
        raise ValueError("STAGING_PATH_INVALID")
    if path.is_dir():
        shutil.rmtree(path, ignore_errors=True)
    elif path.exists():
        path.unlink()
    parent = path.parent
    if parent != stage_root and parent.exists() and not any(parent.iterdir()):
        parent.rmdir()


def _recover_abandoned_builds(root: Path, db: sqlite3.Connection):
    """Discard incomplete BUILDING work; keep READY builds and all ID tombstones."""
    rows = list(db.execute(
        "SELECT build_id,stage_relpath FROM ingestion_builds WHERE state='BUILDING'"
    ))
    for row in rows:
        _remove_stage_path(root, row["stage_relpath"])
        with db:
            db.execute("DELETE FROM ingestion_builds WHERE build_id=?", (row["build_id"],))
    ready_paths = [
        row[0] for row in db.execute(
            "SELECT stage_relpath FROM ingestion_builds WHERE state='READY'"
        )
    ]
    stage_root = root / "staging"
    if stage_root.exists():
        for candidate in stage_root.glob("build_*"):
            rel = candidate.relative_to(root).as_posix().rstrip("/") + "/"
            if not any(path.startswith(rel) for path in ready_paths):
                shutil.rmtree(candidate, ignore_errors=True)


def _verify_shard(path: Path, expected_cdx: str | None = None,
                  require_published: bool = False) -> tuple[int, int, int]:
    if not path.is_file():
        raise ValueError("SHARD_MISSING")
    with database(path, readonly=True) as db:
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("SHARD_INTEGRITY_FAILURE")
        if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ValueError("SHARD_FOREIGN_KEY_FAILURE")
        page_count = db.execute("PRAGMA page_count").fetchone()[0]
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
        physical = page_count * page_size
        if physical > 2_000_000_000:
            raise ValueError("SHARD_MISSING_OR_TOO_LARGE")
        for row in db.execute(
            "SELECT code,cdx,section,content,content_sha256,published,breadcrumb FROM chunks"
        ):
            cdx, section, _ = parse_code(row["code"])
            if cdx != row["cdx"] or section != row["section"]:
                raise ValueError("CHUNK_CODE_METADATA_MISMATCH")
            if expected_cdx is not None and cdx != expected_cdx:
                raise ValueError("SHARD_CDX_MISMATCH")
            if digest(row["content"]) != row["content_sha256"]:
                raise ValueError("CHUNK_SHA256_MISMATCH")
            if tokens(row["breadcrumb"]) > 60:
                raise ValueError("BREADCRUMB_TOO_LONG")
            if require_published and row["published"] != 1:
                raise ValueError("UNPUBLISHED_CHUNK_IN_RELEASE")
    return page_count, page_size, physical


def _promote_manual_staged_shards(root: Path, db: sqlite3.Connection) -> int:
    """Promote manual CodexStore shards that were also authored under staging/."""
    promoted_bytes = 0
    rows = list(db.execute(
        "SELECT * FROM shard_catalog WHERE relative_path LIKE 'staging/%' ORDER BY shard_id"
    ))
    for shard in rows:
        stage_path = root / shard["relative_path"]
        final_rel = f"shards/{shard['shard_id']}.db"
        final_path = root / final_rel
        source = stage_path if stage_path.is_file() else final_path
        if not source.is_file():
            raise ValueError("STAGED_SHARD_MISSING")
        if source == stage_path:
            with database(stage_path) as sdb:
                sdb.execute("UPDATE chunks SET published=1 WHERE published=0")
                sdb.execute("PRAGMA optimize")
            _, _, physical = _verify_shard(stage_path, shard["cdx"], True)
            os.chmod(stage_path, 0o444)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            os.replace(stage_path, final_path)
            promoted_bytes += physical
        else:
            _, _, physical = _verify_shard(final_path, shard["cdx"], True)
            promoted_bytes += physical
        _verify_shard(final_path, shard["cdx"], True)
        with db:
            db.execute(
                "UPDATE shard_catalog SET relative_path=?,frozen=1 WHERE shard_id=?",
                (final_rel, shard["shard_id"]),
            )
    return promoted_bytes


def _promote_ingestion_builds(root: Path, db: sqlite3.Connection,
                              release_id: str) -> int:
    """Verify staged indexer artifacts, then atomically move complete DBs into shards/."""
    promoted_bytes = 0
    builds = list(db.execute(
        "SELECT * FROM ingestion_builds WHERE release_id=? AND state IN ('READY','PROMOTED') "
        "ORDER BY created_at,build_id", (release_id,)
    ))
    for build in builds:
        if build["state"] == "PROMOTED":
            continue
        stage_path = root / build["stage_relpath"]
        final_rel = f"shards/{build['shard_id']}.db"
        final_path = root / final_rel
        if stage_path.is_file():
            with database(stage_path) as sdb:
                sdb.execute("UPDATE chunks SET published=1 WHERE published=0")
                sdb.execute("PRAGMA optimize")
            _, _, physical = _verify_shard(stage_path, build["cdx"], True)
            os.chmod(stage_path, 0o444)
            final_path.parent.mkdir(parents=True, exist_ok=True)
            if final_path.exists():
                raise ValueError("SHARD_DESTINATION_EXISTS")
            os.replace(stage_path, final_path)
            promoted_bytes += physical
        elif final_path.is_file():
            _, _, physical = _verify_shard(final_path, build["cdx"], True)
            promoted_bytes += physical
        else:
            raise ValueError("STAGED_SHARD_MISSING")

        _verify_shard(final_path, build["cdx"], True)
        with database(final_path, readonly=True) as sdb:
            chunk_rows = [dict(row) for row in sdb.execute(
                "SELECT code,cdx,section,entity_anchor,title,topic,breadcrumb FROM chunks ORDER BY code"
            )]
            section_rows = [dict(row) for row in sdb.execute(
                "SELECT cdx,section,group_name,title FROM staged_sections ORDER BY section"
            )]
        if len(chunk_rows) != build["chunk_count"]:
            raise ValueError("STAGED_CHUNK_COUNT_MISMATCH")
        for row in chunk_rows:
            allocation = db.execute(
                "SELECT cdx,section,build_id FROM code_allocations WHERE code=?", (row["code"],)
            ).fetchone()
            anchor = db.execute(
                "SELECT 1 FROM entry_registry WHERE anchor=? AND cdx=?",
                (row["entity_anchor"], row["cdx"]),
            ).fetchone()
            section_allocation = db.execute(
                "SELECT cdx,build_id FROM section_allocations WHERE cdx=? AND section=?",
                (row["cdx"], row["section"]),
            ).fetchone()
            if (allocation is None or allocation["cdx"] != row["cdx"]
                    or allocation["section"] != row["section"]
                    or allocation["build_id"] != build["build_id"]
                    or section_allocation is None
                    or section_allocation["build_id"] != build["build_id"]
                    or anchor is None):
                raise ValueError("REGISTRY_ALLOCATION_OR_ANCHOR_MISMATCH")
        with db:
            db.execute(
                "INSERT OR IGNORE INTO shard_catalog(shard_id,cdx,relative_path,frozen) "
                "VALUES(?,?,?,1)",
                (build["shard_id"], build["cdx"], final_rel),
            )
            catalog = db.execute(
                "SELECT cdx,relative_path FROM shard_catalog WHERE shard_id=?",
                (build["shard_id"],),
            ).fetchone()
            if catalog["cdx"] != build["cdx"] or catalog["relative_path"] != final_rel:
                raise ValueError("SHARD_CATALOG_CONFLICT")
            for section in section_rows:
                existing = db.execute(
                    "SELECT group_name,title FROM section_toc WHERE cdx=? AND section=?",
                    (section["cdx"], section["section"]),
                ).fetchone()
                if existing is None:
                    db.execute("INSERT INTO section_toc VALUES(?,?,?,?)", tuple(section.values()))
                elif (existing["group_name"], existing["title"]) != (
                    section["group_name"], section["title"]
                ):
                    raise ValueError("SECTION_TOC_CONFLICT")
            for row in chunk_rows:
                existing = db.execute(
                    "SELECT shard_id,cdx,section,entity_anchor,title,topic "
                    "FROM chunk_locator WHERE code=?", (row["code"],)
                ).fetchone()
                expected = (build["shard_id"], row["cdx"], row["section"],
                            row["entity_anchor"], row["title"], row["topic"])
                if existing is None:
                    db.execute(
                        "INSERT INTO chunk_locator VALUES(?,?,?,?,?,?,?)",
                        (row["code"],) + expected,
                    )
                elif tuple(existing) != expected:
                    raise ValueError("CHUNK_LOCATOR_CONFLICT")
                exact, folded = title_keys(row["title"])
                title_row = db.execute(
                    "SELECT breadcrumb FROM title_index WHERE code=?", (row["code"],)
                ).fetchone()
                if title_row is None:
                    db.execute(
                        "INSERT INTO title_index(code,title_key,breadcrumb,title_exact,title_folded) "
                        "VALUES(?,?,?,?,?)",
                        (row["code"], exact, row["breadcrumb"], exact, folded),
                    )
                elif title_row["breadcrumb"] != row["breadcrumb"]:
                    raise ValueError("TITLE_INDEX_CONFLICT")
                db.execute("DELETE FROM title_fts WHERE code=?", (row["code"],))
                db.execute(
                    "INSERT INTO title_fts(code,title,breadcrumb) VALUES(?,?,?)",
                    (row["code"], row["title"], row["breadcrumb"]),
                )
            db.execute(
                "UPDATE ingestion_builds SET state='PROMOTED' WHERE build_id=?",
                (build["build_id"],),
            )
        if stage_path.parent.exists():
            shutil.rmtree(stage_path.parent, ignore_errors=True)
    return promoted_bytes


def _normalize_title_index(db: sqlite3.Connection):
    rows = list(db.execute(
        "SELECT l.code,l.title,t.breadcrumb,t.title_exact,t.title_folded "
        "FROM chunk_locator l JOIN title_index t ON t.code=l.code"
    ))
    for row in rows:
        exact, folded = title_keys(row["title"])
        if row["title_exact"] != exact or row["title_folded"] != folded:
            with db:
                db.execute(
                    "UPDATE title_index SET title_exact=?,title_folded=?,title_key=? WHERE code=?",
                    (exact, folded, exact, row["code"]),
                )
        if not db.execute("SELECT 1 FROM title_fts WHERE code=? LIMIT 1", (row["code"],)).fetchone():
            with db:
                db.execute(
                    "INSERT INTO title_fts(code,title,breadcrumb) VALUES(?,?,?)",
                    (row["code"], row["title"], row["breadcrumb"]),
                )


def _validate_registry(root: Path, db: sqlite3.Connection):
    if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
        raise ValueError("REGISTRY_INTEGRITY_FAILURE")
    if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
        raise ValueError("REGISTRY_FOREIGN_KEY_FAILURE")
    orphan_title = db.execute(
        """SELECT COUNT(*) FROM chunk_locator l LEFT JOIN title_index t ON t.code=l.code
           WHERE t.code IS NULL"""
    ).fetchone()[0]
    orphan_locator = db.execute(
        """SELECT COUNT(*) FROM title_index t LEFT JOIN chunk_locator l ON l.code=t.code
           WHERE l.code IS NULL"""
    ).fetchone()[0]
    if orphan_title or orphan_locator:
        raise ValueError("ORPHAN_TITLE")
    for shard in db.execute("SELECT * FROM shard_catalog ORDER BY shard_id"):
        path = root / shard["relative_path"]
        _verify_shard(path, shard["cdx"], True)
        with database(path, readonly=True) as sdb:
            for chunk in sdb.execute(
                "SELECT code,cdx,section,entity_anchor,title,topic FROM chunks"
            ):
                loc = db.execute(
                    "SELECT shard_id,cdx,section,entity_anchor,title,topic "
                    "FROM chunk_locator WHERE code=?", (chunk["code"],)
                ).fetchone()
                if loc is None or tuple(loc) != (
                    shard["shard_id"], chunk["cdx"], chunk["section"],
                    chunk["entity_anchor"], chunk["title"], chunk["topic"]
                ):
                    raise ValueError("CHUNK_LOCATOR_MISMATCH")


def _page_stats(path: Path) -> dict:
    with database(path, readonly=True) as db:
        pages = db.execute("PRAGMA page_count").fetchone()[0]
        page_size = db.execute("PRAGMA page_size").fetchone()[0]
    return {"path": str(path), "page_count": pages, "page_size": page_size,
            "physical_bytes": pages * page_size}


def _dbstat_objects(db: sqlite3.Connection) -> dict[str, int]:
    return {
        row["name"]: row["bytes"]
        for row in db.execute(
            "SELECT name,SUM(pgsize) AS bytes FROM dbstat GROUP BY name"
        )
    }


def _report(root: Path, release_id: str, shards: list,
            staging_promoted_bytes: int = 0) -> dict:
    release_dir = root / "releases" / release_id
    registry_path = release_dir / "registry.db"
    registry_stats = _page_stats(registry_path)
    hot, archive = [], []
    for shard in shards:
        path = root / shard["relative_path"]
        item = {"shard_id": shard["shard_id"], **_page_stats(path),
                "sha256": file_hash(path)}
        if shard["relative_path"].startswith("archive/"):
            archive.append(item)
        else:
            hot.append(item)
    with database(registry_path, readonly=True) as db:
        total = db.execute("SELECT COUNT(*) FROM chunk_locator").fetchone()[0]
        content_bytes = 0
        for shard in shards:
            with database(root / shard["relative_path"], readonly=True) as sdb:
                content_bytes += sum(
                    len(row[0].encode("utf-8"))
                    for row in sdb.execute("SELECT content FROM chunks")
                )
        objects = _dbstat_objects(db)
    fts_objects = {k: v for k, v in objects.items() if k.startswith("title_fts")}
    manifest_names = {
        "release_manifest", "release_shards", "sqlite_autoindex_release_manifest_1",
        "sqlite_autoindex_release_shards_1",
    }
    manifest_objects = {k: v for k, v in objects.items() if k in manifest_names}
    title_objects = {
        k: v for k, v in objects.items()
        if k == "title_index" or k.startswith("idx_title")
        or k == "sqlite_autoindex_title_index_1"
    }
    manifest_file_bytes = sum(
        (release_dir / name).stat().st_size
        for name in ("manifest.json", "manifest.sha256")
        if (release_dir / name).is_file()
    )
    staging_files = []
    staging_root = root / "staging"
    if staging_root.exists():
        for path in sorted(staging_root.rglob("*.db")):
            if path.is_file():
                staging_files.append(_page_stats(path))
    hot_bytes = sum(x["physical_bytes"] for x in hot)
    archive_bytes = sum(x["physical_bytes"] for x in archive)
    database_bytes = registry_stats["physical_bytes"] + hot_bytes + archive_bytes
    return {
        "release_id": release_id,
        "total_chunks": total,
        "content_bytes": content_bytes,
        "average_bytes_per_chunk": round(database_bytes / total, 2) if total else 0,
        "database_bytes": database_bytes,
        "physical_storage": {
            "registry_database": registry_stats,
            "hot_shards": hot,
            "hot_shard_bytes": hot_bytes,
            "archive_shards": archive,
            "archive_shard_bytes": archive_bytes,
            "fts": {"stored_in": "registry.db", "objects": fts_objects,
                    "physical_bytes": sum(fts_objects.values())},
            "title_index": {"stored_in": "registry.db", "objects": title_objects,
                            "physical_bytes": sum(title_objects.values())},
            "manifest": {"stored_in": "registry.db + release sidecars",
                         "objects": manifest_objects,
                         "database_physical_bytes": sum(manifest_objects.values()),
                         "sidecar_file_bytes": manifest_file_bytes},
            "staging": {"remaining_databases": staging_files,
                        "remaining_physical_bytes": sum(x["physical_bytes"] for x in staging_files),
                        "promoted_database_bytes": staging_promoted_bytes},
            "accounting": "SQLite page_count multiplied by page_size; FTS/title/manifest use dbstat page attribution.",
        },
        "shard_sha256": {x["shard_id"]: x["sha256"] for x in hot + archive},
    }


def _fsync_directory(path: Path):
    if os.name == "posix":
        fd = os.open(path, os.O_RDONLY)
        try:
            os.fsync(fd)
        finally:
            os.close(fd)


def _write_fsynced(path: Path, data: bytes):
    with path.open("wb") as handle:
        handle.write(data)
        handle.flush()
        os.fsync(handle.fileno())


def _verify_prepared_release(release_dir: Path, release_id: str,
                             expected_shards: dict[str, dict]):
    manifest_path = release_dir / "manifest.json"
    checksum_path = release_dir / "manifest.sha256"
    snapshot_path = release_dir / "registry.db"
    raw = manifest_path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != checksum_path.read_text(encoding="ascii").strip():
        raise ValueError("MANIFEST_SHA256_MISMATCH")
    manifest = json.loads(raw.decode("utf-8"))
    if manifest.get("release_id") != release_id:
        raise ValueError("MANIFEST_RELEASE_MISMATCH")
    if file_hash(snapshot_path) != manifest.get("registry_sha256"):
        raise ValueError("SNAPSHOT_SHA256_MISMATCH")
    if manifest.get("shards") != expected_shards:
        raise ValueError("MANIFEST_SHARD_SET_MISMATCH")
    with database(snapshot_path, readonly=True) as db:
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("SNAPSHOT_INTEGRITY_FAILURE")
        if db.execute("PRAGMA foreign_key_check").fetchone() is not None:
            raise ValueError("SNAPSHOT_FOREIGN_KEY_FAILURE")
        row = db.execute(
            "SELECT published FROM release_manifest WHERE release_id=?", (release_id,)
        ).fetchone()
        if row is None or row["published"] != 1:
            raise ValueError("SNAPSHOT_RELEASE_MISSING")
        snapshot_shards = {
            r["shard_id"]: r["sha256"]
            for r in db.execute(
                "SELECT shard_id,sha256 FROM release_shards WHERE release_id=?", (release_id,)
            )
        }
    if snapshot_shards != {k: v["sha256"] for k, v in expected_shards.items()}:
        raise ValueError("SNAPSHOT_SHARD_SET_MISMATCH")
    for value in expected_shards.values():
        shard_path = release_dir.parent.parent / value["relative_path"]
        if not shard_path.is_file() or file_hash(shard_path) != value["sha256"]:
            raise ValueError("SHARD_HASH_MISMATCH")


def publish_atomic(root: str | Path, release_id: str,
                   crash_at: str | None = None) -> dict:
    """VERIFY -> PREPARE -> atomic CURRENT switch -> RETAIN previous releases.

    The only public publish path. Shards are immutable, staged first, then atomically
    renamed into shards/ after validation. CURRENT changes only after the full snapshot
    and external manifest have passed integrity and hash verification.
    """
    root = Path(root).resolve()
    if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", release_id):
        raise ValueError("INVALID_RELEASE_ID")
    store = CodexStore(root)
    db = store.registry
    target = root / "releases" / release_id
    tmp = root / "releases" / (".prepare_" + uuid.uuid4().hex)
    staging_promoted_bytes = 0
    try:
        with _writer_lock(root):
            _schema(db)
            _recover_abandoned_builds(root, db)
            current_path = root / "CURRENT"
            current_release = (
                current_path.read_text(encoding="ascii").strip()
                if current_path.exists() else None
            )
            existing = db.execute(
                "SELECT activated FROM release_manifest WHERE release_id=?", (release_id,)
            ).fetchone()
            if existing is not None:
                if current_release == release_id or existing["activated"]:
                    raise ValueError("RELEASE_EXISTS")
                # A previous attempt prepared this release but did not activate CURRENT.
                if target.exists():
                    shutil.rmtree(target)
                with db:
                    db.execute("DELETE FROM release_shards WHERE release_id=?", (release_id,))
                    db.execute("DELETE FROM release_manifest WHERE release_id=?", (release_id,))
            elif target.exists():
                raise ValueError("RELEASE_EXISTS")

            staging_promoted_bytes += _promote_ingestion_builds(root, db, release_id)
            staging_promoted_bytes += _promote_manual_staged_shards(root, db)
            shards = list(db.execute("SELECT * FROM shard_catalog ORDER BY shard_id"))
            if not shards:
                raise ValueError("NO_SHARDS")

            shard_hashes: dict[str, dict] = {}
            for shard in shards:
                path = root / shard["relative_path"]
                if not path.is_file() or path.stat().st_size > 2_000_000_000:
                    raise ValueError("SHARD_MISSING_OR_TOO_LARGE")
                if not shard["frozen"]:
                    with database(path) as sdb:
                        sdb.execute("UPDATE chunks SET published=1 WHERE published=0")
                        sdb.execute("PRAGMA optimize")
                    os.chmod(path, 0o444)
                    with db:
                        db.execute(
                            "UPDATE shard_catalog SET frozen=1 WHERE shard_id=?",
                            (shard["shard_id"],),
                        )
                _verify_shard(path, shard["cdx"], True)
                shard_hashes[shard["shard_id"]] = {
                    "relative_path": shard["relative_path"],
                    "sha256": file_hash(path),
                    "bytes": _page_stats(path)["physical_bytes"],
                }

            _normalize_title_index(db)
            _validate_registry(root, db)
            # dbstat is required for exact physical attribution of FTS/title/manifest pages.
            _dbstat_objects(db)
            with db:
                db.execute(
                    "INSERT INTO release_manifest(release_id,published,activated) VALUES(?,1,0)",
                    (release_id,),
                )
                for shard_id, value in shard_hashes.items():
                    db.execute(
                        "INSERT INTO release_shards(release_id,shard_id,sha256) VALUES(?,?,?)",
                        (release_id, shard_id, value["sha256"]),
                    )

            tmp.mkdir(parents=True)
            snapshot = connect(tmp / "registry.db")
            db.backup(snapshot)
            snapshot.close()
            os.chmod(tmp / "registry.db", 0o444)
            with (tmp / "registry.db").open("rb") as snapshot_file:
                os.fsync(snapshot_file.fileno())
            manifest = {
                "format_version": 1,
                "release_id": release_id,
                "registry_sha256": file_hash(tmp / "registry.db"),
                "shards": shard_hashes,
                "total_chunks": db.execute("SELECT COUNT(*) FROM chunk_locator").fetchone()[0],
            }
            manifest_bytes = json.dumps(
                manifest, ensure_ascii=False, sort_keys=True, separators=(",", ":")
            ).encode("utf-8")
            _write_fsynced(tmp / "manifest.json", manifest_bytes)
            _write_fsynced(
                tmp / "manifest.sha256",
                (hashlib.sha256(manifest_bytes).hexdigest() + "\n").encode("ascii"),
            )
            _verify_prepared_release(tmp, release_id, shard_hashes)
            _fsync_directory(tmp)
            if crash_at == "before_prepare":
                raise RuntimeError("INJECTED_CRASH_BEFORE_PREPARE")
            os.replace(tmp, target)
            _fsync_directory(root / "releases")

            pending = root / (".CURRENT_" + uuid.uuid4().hex)
            try:
                _write_fsynced(pending, (release_id + "\n").encode("ascii"))
                if crash_at == "before_commit":
                    raise RuntimeError("INJECTED_CRASH_BEFORE_COMMIT")
                os.replace(pending, current_path)
                with db:
                    db.execute(
                        "UPDATE release_manifest SET activated=1 WHERE release_id=?",
                        (release_id,),
                    )
                _fsync_directory(root)
            finally:
                pending.unlink(missing_ok=True)

            # Retain all previous releases; no shard referenced by them is removed.
            return _report(root, release_id, shards, staging_promoted_bytes)
    finally:
        store.close()
        if tmp.exists():
            shutil.rmtree(tmp, ignore_errors=True)


class OfflineIndexer:
    """Build one immutable shard in a private staging directory, then mark it READY."""

    def __init__(self, root: str | Path):
        self.root = Path(root).resolve()
        self.store = CodexStore(self.root)
        (self.root / "staging").mkdir(exist_ok=True)
        _schema(self.store.registry)

    def close(self):
        self.store.close()

    def ingest(self, cdx: str, codex_title: str, source: str,
               release_id: str, entity_type: str = "LORE") -> dict:
        if not source.strip():
            raise ValueError("EMPTY_SOURCE")
        if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", release_id):
            raise ValueError("INVALID_RELEASE_ID")
        make_code(cdx, 0, 0)
        source_sha = digest(source)
        db = self.store.registry
        with _writer_lock(self.root):
            _schema(db)
            _recover_abandoned_builds(self.root, db)
            if db.execute(
                "SELECT 1 FROM ingestion_sources WHERE source_sha256=?", (source_sha,)
            ).fetchone():
                raise ValueError("SOURCE_ALREADY_INGESTED")
            registered = db.execute(
                "SELECT title,retired FROM codex_registry WHERE cdx=?", (cdx,)
            ).fetchone()
            if registered is None:
                self.store.register_codex(cdx, codex_title)
            elif registered["retired"] or registered["title"] != codex_title:
                raise ValueError("CODEX_REGISTRY_CONFLICT")

            # Section 000 contains permanent entity-anchor identifiers. Failed builds
            # retain allocated codes as tombstones; identifiers are never reused.
            row = db.execute(
                "SELECT anchor FROM entry_registry WHERE cdx=? ORDER BY anchor LIMIT 1", (cdx,)
            ).fetchone()
            if row:
                anchor = row["anchor"]
            else:
                anchor = _allocate(db, cdx, 0)
                self.store.add_entry(anchor, cdx, codex_title, entity_type)

            build_id = uuid.uuid4().hex
            shard_id = "crs_" + build_id
            stage_dir = self.root / "staging" / ("build_" + build_id)
            stage_path = stage_dir / "shard.db"
            stage_relpath = stage_path.relative_to(self.root).as_posix()
            stage_dir.mkdir(parents=True, exist_ok=False)
            with db:
                db.execute(
                    """INSERT INTO ingestion_builds(
                        build_id,release_id,cdx,codex_title,shard_id,stage_relpath,
                        source_sha256,state,chunk_count)
                        VALUES(?,?,?,?,?,?,?,'BUILDING',0)""",
                    (build_id, release_id, cdx, codex_title, shard_id,
                     stage_relpath, source_sha),
                )
            stage_db = None
            try:
                stage_db = connect(stage_path)
                stage_db.executescript(SHARD_SCHEMA)
                count = 0
                short = 0
                kinds = dict.fromkeys(TARGETS, 0)
                for heading, body, explicit in sections(source):
                    section = _next_section(db, cdx, build_id)
                    kind = explicit or _category(heading, body)
                    parts = chunk_text(body, kind)
                    stage_db.execute(
                        "INSERT INTO staged_sections VALUES(?,?,?,?)",
                        (cdx, section, codex_title, heading),
                    )
                    for part in parts:
                        code = _allocate(db, cdx, section, build_id)
                        title = f"{heading} [{code}]"
                        breadcrumb = f"{codex_title} / {heading}"
                        if tokens(breadcrumb) > 60:
                            raise ValueError("BREADCRUMB_TOO_LONG")
                        context = f"{heading}: " if DANGLING.search(part) else ""
                        if tokens(context) > 40:
                            raise ValueError("CONTEXT_TOO_LONG")
                        full_content = context + part
                        if tokens(full_content) > TARGETS[kind][1]:
                            raise ValueError("CONTEXT_PUSHES_CHUNK_OVER_TARGET")
                        stage_db.execute(
                            "INSERT INTO chunks VALUES(?,?,?,?,?,?,?,?,?,0)",
                            (code, cdx, section, anchor, title, kind + "." + str(section),
                             breadcrumb, full_content, digest(full_content)),
                        )
                        count += 1
                        kinds[kind] += 1
                        if tokens(part) < TARGETS[kind][0]:
                            short += 1
                if count == 0:
                    raise ValueError("NO_CHUNKS")
                stage_db.commit()
                if stage_db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ValueError("STAGING_INTEGRITY_FAILURE")
                stage_db.close()
                stage_db = None
                with db:
                    db.execute(
                        "INSERT INTO ingestion_sources(source_sha256,cdx,release_id,build_id) "
                        "VALUES(?,?,?,?)", (source_sha, cdx, release_id, build_id),
                    )
                    db.execute(
                        "UPDATE ingestion_builds SET state='READY',chunk_count=? WHERE build_id=?",
                        (count, build_id),
                    )
                stage_stats = _page_stats(stage_path)
                return {
                    "build_id": build_id, "total_chunks": count, "by_type": kinds,
                    "short_semantic_units": short, "shard_id": shard_id,
                    "anchor": anchor, "source_sha256": source_sha,
                    "staging_path": stage_relpath,
                    "staging_physical_bytes": stage_stats["physical_bytes"],
                }
            except Exception:
                if stage_db is not None:
                    stage_db.close()
                with db:
                    db.execute("DELETE FROM ingestion_sources WHERE source_sha256=?", (source_sha,))
                    db.execute("DELETE FROM ingestion_builds WHERE build_id=?", (build_id,))
                shutil.rmtree(stage_dir, ignore_errors=True)
                # code_allocations and section_allocations intentionally remain reserved.
                raise


def ingest_file(root: str | Path, cdx: str, title: str,
                source_path: str | Path, release_id: str) -> dict:
    indexer = OfflineIndexer(root)
    try:
        result = indexer.ingest(
            cdx, title, Path(source_path).read_text(encoding="utf-8"), release_id
        )
    finally:
        indexer.close()
    result["publish"] = publish_atomic(root, release_id)
    return result
