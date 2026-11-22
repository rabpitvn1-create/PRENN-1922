"""Offline, stdlib-only CRS ingestion and crash-safe release activation.

Token counts are a deterministic approximation, not a model tokenizer.
"""
from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path
import re
import sqlite3
import unicodedata
import uuid

from .ids import make_code
from .store import CodexStore, connect, digest, file_hash

TARGETS = {"FACT": (50, 150), "RULE": (100, 250),
           "LORE": (200, 400), "NARRATIVE": (300, 500)}
HEADING = re.compile(r"^(#{1,6})\s+(.+?)\s*$")
TYPE = re.compile(r"^\s*<!--\s*CRS:(FACT|RULE|LORE|NARRATIVE)\s*-->\s*$")
SENTENCES = re.compile(r"(?<=[.!?。！？])\s+(?=[^\s])")
WORDS = re.compile(r"\w+|[^\w\s]", re.UNICODE)
DANGLING = re.compile(r"^(?:he|she|they|it|this|that|these|those|"
                      r"hắn|cô ấy|người này|điều này|nó|ông ấy)\b", re.I)


def tokens(text: str) -> int:
    return len(WORDS.findall(text))


def title_keys(title: str) -> tuple[str, str]:
    exact = unicodedata.normalize("NFC", title).casefold()
    folded = "".join(c for c in unicodedata.normalize("NFD", exact)
                     if unicodedata.category(c) != "Mn").replace("đ", "d")
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
    """Heading-first segmentation; marker selects category for following section."""
    title, lines, hint = None, [], None
    for raw in source.splitlines():
        marker = TYPE.fullmatch(raw)
        if marker:
            if title is not None and any(s.strip() for s in lines):
                yield title, "\n".join(lines).strip(), hint
                lines = []
            hint = marker.group(1)
            continue
        match = HEADING.fullmatch(raw)
        if match:
            if title is not None and any(s.strip() for s in lines):
                yield title, "\n".join(lines).strip(), hint
            title, lines, hint = match.group(2).strip(), [], None
        else:
            if title is None:
                title = "Introduction"
            lines.append(raw)
    if title is not None and any(s.strip() for s in lines):
        yield title, "\n".join(lines).strip(), hint


def _units(text: str, kind: str, maximum: int) -> list[str]:
    """Never split an individual RULE paragraph, including its exceptions."""
    paragraphs = [p.strip() for p in re.split(r"\n\s*\n", text) if p.strip()]
    output = []
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
    if kind not in TARGETS:
        raise ValueError("INVALID_CHUNK_TYPE")
    minimum, maximum = TARGETS[kind]
    units = _units(text, kind, maximum)
    result, current = [], []
    for unit in units:
        n = tokens(unit)
        if current and tokens("\n\n".join(current)) + n > maximum:
            result.append("\n\n".join(current))
            current = []
        current.append(unit)
        if tokens("\n\n".join(current)) >= minimum:
            result.append("\n\n".join(current))
            current = []
    if current:
        tail = "\n\n".join(current)
        if result and tokens(result[-1]) + tokens(tail) <= maximum:
            result[-1] += "\n\n" + tail
        else:
            result.append(tail)
    return result


def _schema(db: sqlite3.Connection):
    db.executescript("""
        CREATE TABLE IF NOT EXISTS code_allocations (
            code TEXT PRIMARY KEY, cdx TEXT NOT NULL,
            section INTEGER NOT NULL, chunk_no INTEGER NOT NULL,
            UNIQUE(cdx,section,chunk_no)
        );
        CREATE TABLE IF NOT EXISTS ingestion_sources (
            source_sha256 TEXT PRIMARY KEY, cdx TEXT NOT NULL,
            release_id TEXT NOT NULL
        );
    """)
    cols = {r[1] for r in db.execute("PRAGMA table_info(title_index)")}
    if "title_exact" not in cols:
        db.execute("ALTER TABLE title_index ADD COLUMN title_exact TEXT")
    if "title_folded" not in cols:
        db.execute("ALTER TABLE title_index ADD COLUMN title_folded TEXT")
    db.execute("CREATE INDEX IF NOT EXISTS idx_title_exact ON title_index(title_exact)")
    db.execute("CREATE INDEX IF NOT EXISTS idx_title_folded ON title_index(title_folded)")
    db.execute("""CREATE VIRTUAL TABLE IF NOT EXISTS title_fts USING fts5(
        code UNINDEXED, title, breadcrumb, tokenize='unicode61 remove_diacritics 0'
    )""")
    db.commit()


def _allocate(db: sqlite3.Connection, cdx: str, section: int) -> str:
    number = db.execute(
        "SELECT COALESCE(MAX(chunk_no),0)+1 FROM code_allocations WHERE cdx=? AND section=?",
        (cdx, section)).fetchone()[0]
    if number > 9999:
        raise ValueError("CHUNK_CODE_EXHAUSTED")
    code = make_code(cdx, section, number)
    with db:
        db.execute("INSERT INTO code_allocations VALUES(?,?,?,?)",
                   (code, cdx, section, number))
    return code


def _next_section(db, cdx):
    number = db.execute(
        "SELECT COALESCE(MAX(section),0)+1 FROM section_toc WHERE cdx=?",
        (cdx,)).fetchone()[0]
    if number > 999:
        raise ValueError("SECTION_CODE_EXHAUSTED")
    return number


class OfflineIndexer:
    """Use a single offline writer per root; never ingest into a served snapshot."""

    def __init__(self, root: str | Path):
        self.store = CodexStore(root)
        self.root = Path(root)
        _schema(self.store.registry)

    def close(self):
        self.store.close()

    def ingest(self, cdx: str, codex_title: str, source: str,
               release_id: str, entity_type: str = "LORE") -> dict:
        if not source.strip():
            raise ValueError("EMPTY_SOURCE")
        db = self.store.registry
        source_sha = digest(source)
        if db.execute("SELECT 1 FROM ingestion_sources WHERE source_sha256=?",
                      (source_sha,)).fetchone():
            raise ValueError("SOURCE_ALREADY_INGESTED")
        registered = db.execute("SELECT title,retired FROM codex_registry WHERE cdx=?",
                                (cdx,)).fetchone()
        if registered is None:
            self.store.register_codex(cdx, codex_title)
        elif registered["retired"] or registered["title"] != codex_title:
            raise ValueError("CODEX_REGISTRY_CONFLICT")
        # Section 000 is reserved for permanent entity-anchor identifiers.
        row = db.execute("SELECT anchor FROM entry_registry WHERE cdx=? ORDER BY anchor LIMIT 1",
                         (cdx,)).fetchone()
        if row:
            anchor = row["anchor"]
        else:
            anchor = _allocate(db, cdx, 0)
            self.store.add_entry(anchor, cdx, codex_title, entity_type)
        shard_id = "crs_" + uuid.uuid4().hex
        self.store.new_shard(cdx, shard_id)
        count = 0
        short = 0
        kinds = dict.fromkeys(TARGETS, 0)
        for heading, body, explicit in sections(source):
            section = _next_section(db, cdx)
            self.store.add_section(cdx, section, codex_title, heading)
            kind = explicit or _category(heading, body)
            for part in chunk_text(body, kind):
                code = _allocate(db, cdx, section)
                ordinal = kinds[kind] + 1
                title = f"{heading} [{code}]"
                breadcrumb = f"{codex_title} / {heading}"
                if tokens(breadcrumb) > 60:
                    raise ValueError("BREADCRUMB_TOO_LONG")
                context = f"{heading}: " if DANGLING.search(part) else ""
                if tokens(context) > 40:
                    raise ValueError("CONTEXT_TOO_LONG")
                full_content = context + part
                self.store.add_chunk(shard_id, code, anchor, title,
                                     kind + "." + str(section), breadcrumb, full_content)
                exact, folded = title_keys(title)
                with db:
                    db.execute("UPDATE title_index SET title_exact=?,title_folded=? WHERE code=?",
                               (exact, folded, code))
                    db.execute("INSERT INTO title_fts(code,title,breadcrumb) VALUES(?,?,?)",
                               (code, title, breadcrumb))
                count += 1
                kinds[kind] += 1
                if tokens(part) < TARGETS[kind][0]:
                    short += 1
        if count == 0:
            raise ValueError("NO_CHUNKS")
        with db:
            db.execute("INSERT INTO ingestion_sources VALUES(?,?,?)",
                       (source_sha, cdx, release_id))
        return {"total_chunks": count, "by_type": kinds,
                "short_semantic_units": short, "shard_id": shard_id,
                "anchor": anchor, "source_sha256": source_sha}


def publish_atomic(root: str | Path, release_id: str,
                   crash_at: str | None = None) -> dict:
    """BUILD already in working registry; VERIFY -> PREPARE -> atomic CURRENT -> RETAIN.

    A pre-commit exception never changes CURRENT. Existing shard bytes stay unchanged.
    """
    root = Path(root)
    if not re.fullmatch(r"[A-Za-z0-9_]{1,64}", release_id):
        raise ValueError("INVALID_RELEASE_ID")
    store = CodexStore(root)
    db = store.registry
    target = root / "releases" / release_id
    tmp = root / "releases" / (".prepare_" + uuid.uuid4().hex)
    try:
        _schema(db)
        if target.exists() or db.execute(
            "SELECT 1 FROM release_manifest WHERE release_id=?",
            (release_id,)).fetchone():
            raise ValueError("RELEASE_EXISTS")
        shards = list(db.execute("SELECT * FROM shard_catalog ORDER BY shard_id"))
        if not shards:
            raise ValueError("NO_SHARDS")
        # Freeze only previously unpublished shards; never alter a frozen shard.
        for shard in shards:
            path = root / shard["relative_path"]
            if not path.is_file() or path.stat().st_size > 2_000_000_000:
                raise ValueError("SHARD_MISSING_OR_TOO_LARGE")
            if not shard["frozen"]:
                with connect(path) as sdb:
                    sdb.execute("UPDATE chunks SET published=1 WHERE published=0")
                    sdb.execute("PRAGMA optimize")
                with db:
                    db.execute("UPDATE shard_catalog SET frozen=1 WHERE shard_id=?",
                               (shard["shard_id"],))
            with connect(path, readonly=True) as check:
                if check.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                    raise ValueError("SHARD_INTEGRITY_FAILURE")
                for row in check.execute("SELECT code,content,content_sha256 FROM chunks"):
                    if digest(row["content"]) != row["content_sha256"]:
                        raise ValueError("CHUNK_SHA256_MISMATCH")
        if db.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
            raise ValueError("REGISTRY_INTEGRITY_FAILURE")
        if db.execute("""SELECT COUNT(*) FROM chunk_locator l LEFT JOIN title_index t
                         ON t.code=l.code WHERE t.code IS NULL""").fetchone()[0]:
            raise ValueError("ORPHAN_TITLE")
        if db.execute("""SELECT COUNT(*) FROM chunk_locator l LEFT JOIN code_allocations a
                         ON a.code=l.code WHERE a.code IS NULL""").fetchone()[0]:
            # Legacy manually-authored chunks have no allocation rows.
            # They remain readable; new ingestion is fully allocated.
            pass
        with db:
            db.execute("INSERT INTO release_manifest(release_id,published) VALUES(?,1)",
                       (release_id,))
            for shard in shards:
                db.execute("INSERT INTO release_shards VALUES(?,?,?)",
                           (release_id, shard["shard_id"],
                            file_hash(root / shard["relative_path"])))
        tmp.mkdir(parents=True)
        snap = connect(tmp / "registry.db")
        db.backup(snap)
        snap.close()
        with connect(tmp / "registry.db", readonly=True) as verify:
            if verify.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("SNAPSHOT_INTEGRITY_FAILURE")
            if not verify.execute(
                "SELECT 1 FROM release_manifest WHERE release_id=? AND published=1",
                (release_id,)).fetchone():
                raise ValueError("SNAPSHOT_RELEASE_MISSING")
        if crash_at == "before_prepare":
            raise RuntimeError("INJECTED_CRASH_BEFORE_PREPARE")
        os.replace(tmp, target)
        # Directory replace is not CURRENT activation. Readers still see old CURRENT.
        current = root / "CURRENT"
        pending = root / (".CURRENT_" + uuid.uuid4().hex)
        try:
            with pending.open("w", encoding="ascii") as f:
                f.write(release_id + "\n")
                f.flush()
                os.fsync(f.fileno())
            if crash_at == "before_commit":
                raise RuntimeError("INJECTED_CRASH_BEFORE_COMMIT")
            os.replace(pending, current)
            # Best-effort directory sync for POSIX power-loss durability.
            if os.name == "posix":
                fd = os.open(root, os.O_RDONLY)
                try:
                    os.fsync(fd)
                finally:
                    os.close(fd)
        finally:
            pending.unlink(missing_ok=True)
        report = _report(root, release_id, shards)
        return report
    finally:
        store.close()
        if tmp.exists():
            import shutil
            shutil.rmtree(tmp)


def _report(root: Path, release_id: str, shards: list) -> dict:
    sizes = [int((root / s["relative_path"]).stat().st_size) for s in shards]
    registry_path = root / "releases" / release_id / "registry.db"
    with connect(registry_path, readonly=True) as db:
        total = db.execute("SELECT COUNT(*) FROM chunk_locator").fetchone()[0]
        metadata = sum(len(str(tuple(r)).encode("utf-8")) for r in db.execute(
            "SELECT * FROM chunk_locator"))
        index = sum(len(str(tuple(r)).encode("utf-8")) for r in db.execute(
            "SELECT * FROM title_index"))
        fts = sum(len(str(tuple(r)).encode("utf-8")) for r in db.execute(
            "SELECT code,title,breadcrumb FROM title_fts"))
        manifest = sum(len(str(tuple(r)).encode("utf-8")) for r in db.execute(
            "SELECT * FROM release_shards WHERE release_id=?", (release_id,)))
    content = 0
    for s in shards:
        with connect(root / s["relative_path"], readonly=True) as db:
            content += sum(len(r[0].encode("utf-8")) for r in
                           db.execute("SELECT content FROM chunks"))
    database = sum(sizes) + registry_path.stat().st_size
    return {"release_id": release_id, "total_chunks": total,
            "content_bytes": content, "metadata_bytes_estimate": metadata,
            "index_bytes_estimate": index, "fts_bytes_estimate": fts,
            "manifest_bytes_estimate": manifest, "registry_bytes": registry_path.stat().st_size,
            "hot_shard_bytes": sum(sizes), "archive_shard_bytes": 0,
            "database_bytes": database,
            "average_bytes_per_chunk": round(database / total, 2) if total else 0,
            "shard_sha256": {s["shard_id"]: file_hash(root / s["relative_path"])
                             for s in shards}}


def ingest_file(root: str | Path, cdx: str, title: str,
                source_path: str | Path, release_id: str) -> dict:
    indexer = OfflineIndexer(root)
    try:
        result = indexer.ingest(cdx, title, Path(source_path).read_text(encoding="utf-8"),
                                release_id)
    finally:
        indexer.close()
    result["publish"] = publish_atomic(root, release_id)
    return result
