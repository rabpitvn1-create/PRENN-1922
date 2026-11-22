"""Offline codex authoring and release-pinned read-only retrieval."""
from __future__ import annotations

from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import unicodedata

from .ids import make_code, parse_code
from .schema import REGISTRY_SCHEMA, SHARD_SCHEMA


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def connect(path: Path, readonly: bool = False) -> sqlite3.Connection:
    path = Path(path)
    if readonly:
        db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
        db.execute("PRAGMA query_only=ON")
    else:
        db = sqlite3.connect(path)
    db.execute("PRAGMA recursive_triggers=ON")
    db.execute("PRAGMA foreign_keys=ON")
    db.row_factory = sqlite3.Row
    return db


@contextmanager
def database(path: Path, readonly: bool = False):
    """Open, commit/rollback, and always close a short-lived SQLite connection."""
    db = connect(path, readonly=readonly)
    try:
        yield db
        if not readonly:
            db.commit()
    except Exception:
        if not readonly:
            db.rollback()
        raise
    finally:
        db.close()


def _safe_relative(root: Path, relative: str) -> Path:
    path = (root / relative).resolve()
    if not path.is_relative_to(root.resolve()):
        raise ValueError("PATH_OUTSIDE_CRS_ROOT")
    return path


class CodexStore:
    """Offline authoring facade. All release activation delegates to one publisher."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "shards").mkdir(exist_ok=True)
        (self.root / "staging").mkdir(exist_ok=True)
        (self.root / "releases").mkdir(exist_ok=True)
        (self.root / "archive").mkdir(exist_ok=True)
        self.registry = connect(self.root / "registry.db")
        self.registry.executescript(REGISTRY_SCHEMA)

    def close(self):
        self.registry.close()

    def register_codex(self, cdx: str, title: str):
        make_code(cdx, 0, 0)
        with self.registry:
            self.registry.execute(
                "INSERT INTO codex_registry(cdx,title) VALUES(?,?)", (cdx, title)
            )

    def new_shard(self, cdx: str, shard_id: str):
        """Create an unpublished manual-authoring shard under staging, never live shards."""
        if not re.fullmatch(r"[A-Za-z0-9_]+", shard_id):
            raise ValueError("INVALID_SHARD_ID")
        rel = f"staging/manual_{shard_id}.db"
        path = self.root / rel
        if path.exists() or self.registry.execute(
            "SELECT 1 FROM shard_catalog WHERE shard_id=?", (shard_id,)
        ).fetchone():
            raise ValueError("SHARD_EXISTS")
        with self.registry:
            self.registry.execute(
                "INSERT INTO shard_catalog(shard_id,cdx,relative_path) VALUES(?,?,?)",
                (shard_id, cdx, rel),
            )
        try:
            db = connect(path)
            db.executescript(SHARD_SCHEMA)
            db.close()
        except Exception:
            with self.registry:
                self.registry.execute("DELETE FROM shard_catalog WHERE shard_id=?", (shard_id,))
            path.unlink(missing_ok=True)
            raise

    def add_entry(self, anchor: str, cdx: str, title: str, entity_type: str = "CHARACTER"):
        acdx, _, _ = parse_code(anchor)
        if acdx != cdx:
            raise ValueError("ANCHOR_CDX_MISMATCH")
        with self.registry:
            self.registry.execute(
                "INSERT INTO entry_registry(anchor,cdx,title,entity_type) VALUES(?,?,?,?)",
                (anchor, cdx, title, entity_type),
            )

    def add_section(self, cdx: str, section: int, group: str, title: str):
        with self.registry:
            self.registry.execute(
                "INSERT INTO section_toc VALUES(?,?,?,?)", (cdx, section, group, title)
            )

    def add_chunk(self, shard_id: str, code: str, anchor: str, title: str,
                  topic: str, breadcrumb: str, content: str):
        cdx, section, _ = parse_code(code)
        shard = self.registry.execute(
            "SELECT * FROM shard_catalog WHERE shard_id=?", (shard_id,)
        ).fetchone()
        if shard is None or shard["frozen"] or shard["cdx"] != cdx:
            raise ValueError("SHARD_NOT_WRITABLE")
        if not self.registry.execute(
            "SELECT 1 FROM entry_registry WHERE anchor=? AND cdx=?", (anchor, cdx)
        ).fetchone():
            raise ValueError("UNKNOWN_ENTITY")
        path = _safe_relative(self.root, shard["relative_path"])
        with database(path) as db:
            db.execute(
                "INSERT INTO chunks VALUES(?,?,?,?,?,?,?,?,?,0)",
                (code, cdx, section, anchor, title, topic, breadcrumb, content, digest(content)),
            )
        try:
            key = unicodedata.normalize("NFC", title).casefold()
            with self.registry:
                self.registry.execute(
                    "INSERT INTO chunk_locator VALUES(?,?,?,?,?,?,?)",
                    (code, shard_id, cdx, section, anchor, title, topic),
                )
                self.registry.execute(
                    "INSERT INTO title_index(code,title_key,breadcrumb) VALUES(?,?,?)",
                    (code, key, breadcrumb),
                )
        except Exception:
            with database(path) as db:
                db.execute("DELETE FROM chunks WHERE code=? AND published=0", (code,))
            raise

    def publish(self, release_id: str):
        """Compatibility entry point; the only publisher is publish_atomic()."""
        from .ingest import publish_atomic
        return publish_atomic(self.root, release_id)


class CodexReader:
    """Release-pinned reader with separately writable, trusted coordinator session state."""

    def __init__(self, root: str | Path, release_id: str):
        self.root = Path(root)
        self.release_id = release_id
        self._verified_shards: set[str] = set()
        release_dir = self.root / "releases" / release_id
        registry_path = release_dir / "registry.db"
        manifest_path = release_dir / "manifest.json"
        checksum_path = release_dir / "manifest.sha256"
        if not manifest_path.is_file() or not checksum_path.is_file():
            raise ValueError("RELEASE_MANIFEST_MISSING")
        manifest_bytes = manifest_path.read_bytes()
        expected_manifest_hash = checksum_path.read_text(encoding="ascii").strip()
        if hashlib.sha256(manifest_bytes).hexdigest() != expected_manifest_hash:
            raise ValueError("MANIFEST_SHA256_MISMATCH")
        try:
            self.manifest = json.loads(manifest_bytes.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ValueError("MANIFEST_INVALID") from exc
        if self.manifest.get("format_version") != 1:
            raise ValueError("MANIFEST_FORMAT_UNSUPPORTED")
        if self.manifest.get("release_id") != release_id:
            raise ValueError("MANIFEST_RELEASE_MISMATCH")
        if not registry_path.is_file() or file_hash(registry_path) != self.manifest.get("registry_sha256"):
            raise ValueError("SNAPSHOT_SHA256_MISMATCH")

        self.registry = connect(registry_path, readonly=True)
        try:
            if self.registry.execute("PRAGMA integrity_check").fetchone()[0] != "ok":
                raise ValueError("SNAPSHOT_INTEGRITY_FAILURE")
            if self.registry.execute("PRAGMA foreign_key_check").fetchone() is not None:
                raise ValueError("SNAPSHOT_FOREIGN_KEY_FAILURE")
            row = self.registry.execute(
                "SELECT 1 FROM release_manifest WHERE release_id=? AND published=1",
                (release_id,),
            ).fetchone()
            if row is None:
                raise ValueError("RELEASE_MISMATCH")
            snapshot_shards = {
                r["shard_id"]: r["sha256"]
                for r in self.registry.execute(
                    "SELECT shard_id,sha256 FROM release_shards WHERE release_id=?", (release_id,)
                )
            }
            manifest_shards = {
                key: value["sha256"] for key, value in self.manifest.get("shards", {}).items()
            }
            if snapshot_shards != manifest_shards:
                raise ValueError("MANIFEST_SHARD_SET_MISMATCH")
            catalog_paths = {
                row["shard_id"]: row["relative_path"]
                for row in self.registry.execute(
                    "SELECT shard_id,relative_path FROM shard_catalog"
                )
                if row["shard_id"] in snapshot_shards
            }
            manifest_paths = {
                key: value["relative_path"]
                for key, value in self.manifest.get("shards", {}).items()
            }
            if catalog_paths != manifest_paths:
                raise ValueError("MANIFEST_SHARD_PATH_MISMATCH")
        except Exception:
            self.registry.close()
            raise

        self.sessions = connect(self.root / "session_state.db")
        self.sessions.executescript("""
            CREATE TABLE IF NOT EXISTS active_sessions (
                session_id TEXT PRIMARY KEY, release_id TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS pinned_intents (
                intent_id TEXT PRIMARY KEY, session_id TEXT NOT NULL,
                entity_anchor TEXT NOT NULL, cdx TEXT NOT NULL,
                section INTEGER NOT NULL, topic TEXT NOT NULL,
                expected_title TEXT NOT NULL, repair_count INTEGER NOT NULL DEFAULT 0,
                FOREIGN KEY(session_id) REFERENCES active_sessions(session_id)
            );
        """)

    def close(self):
        self.registry.close()
        self.sessions.close()

    def create_session(self, session_id: str):
        with self.sessions:
            self.sessions.execute(
                "INSERT INTO active_sessions VALUES(?,?)", (session_id, self.release_id)
            )

    def pin_intent(self, session_id: str, intent_id: str, entity_anchor: str, cdx: str,
                   section: int, topic: str, expected_title: str):
        """Trusted coordinator operation; never expose as an LLM-controlled tool."""
        row = self.sessions.execute(
            "SELECT release_id FROM active_sessions WHERE session_id=?", (session_id,)
        ).fetchone()
        if row is None or row["release_id"] != self.release_id:
            raise ValueError("RELEASE_MISMATCH")
        with self.sessions:
            self.sessions.execute(
                """INSERT INTO pinned_intents(
                    intent_id,session_id,entity_anchor,cdx,section,topic,expected_title)
                    VALUES(?,?,?,?,?,?,?)""",
                (intent_id, session_id, entity_anchor, cdx, section, topic, expected_title),
            )

    def toc_root(self, cdx: str, limit: int = 10, offset: int = 0):
        self._page(limit, offset)
        rows = self.registry.execute(
            """SELECT group_name,COUNT(*) AS sections FROM section_toc
               WHERE cdx=? GROUP BY group_name ORDER BY group_name LIMIT ? OFFSET ?""",
            (cdx, limit, offset),
        )
        return [dict(x) for x in rows]

    def toc_group(self, cdx: str, group: str, limit: int = 10, offset: int = 0):
        self._page(limit, offset)
        rows = self.registry.execute(
            """SELECT section,title FROM section_toc
               WHERE cdx=? AND group_name=? ORDER BY section LIMIT ? OFFSET ?""",
            (cdx, group, limit, offset),
        )
        return [dict(x) for x in rows]

    def toc(self, cdx: str, limit: int = 10, offset: int = 0):
        self._page(limit, offset)
        rows = self.registry.execute(
            """SELECT section,group_name,title FROM section_toc
               WHERE cdx=? ORDER BY section LIMIT ? OFFSET ?""",
            (cdx, limit, offset),
        )
        return [dict(x) for x in rows]

    def idx(self, cdx: str, section: int, limit: int = 10, offset: int = 0):
        self._page(limit, offset)
        rows = self.registry.execute(
            """SELECT l.code,l.title,l.entity_anchor,t.breadcrumb
               FROM chunk_locator l JOIN title_index t ON t.code=l.code
               WHERE l.cdx=? AND l.section=?
               ORDER BY l.code LIMIT ? OFFSET ?""",
            (cdx, section, limit, offset),
        )
        return [dict(x) for x in rows]

    @staticmethod
    def _page(limit: int, offset: int):
        if not 1 <= limit <= 20 or offset < 0:
            raise ValueError("INVALID_PAGE")

    def get(self, code: str, intent_id: str, session_id: str) -> dict:
        """GET <CODE> USING <INTENT_ID>; never return mismatched lore."""
        intent = self.sessions.execute(
            """SELECT i.*,s.release_id FROM pinned_intents i
               JOIN active_sessions s ON s.session_id=i.session_id
               WHERE i.intent_id=? AND i.session_id=?""",
            (intent_id, session_id),
        ).fetchone()
        if intent is None:
            return {"status": "INTENT_NOT_FOUND", "content_returned": False}
        try:
            cdx, section, _ = parse_code(code)
        except ValueError as exc:
            return {"status": str(exc), "content_returned": False}
        if intent["release_id"] != self.release_id:
            return {"status": "RELEASE_MISMATCH", "content_returned": False}
        entry = self.registry.execute(
            "SELECT * FROM chunk_locator WHERE code=?", (code,)
        ).fetchone()
        if entry is None:
            return {"status": "NOT_IN_PINNED_RELEASE", "content_returned": False}
        if entry["entity_anchor"] != intent["entity_anchor"]:
            return {"status": "ENTITY_MISMATCH", "content_returned": False}
        if (cdx != intent["cdx"] or section != intent["section"]
                or entry["topic"] != intent["topic"]):
            return {"status": "INTENT_MISMATCH", "content_returned": False}
        if entry["title"] != intent["expected_title"]:
            return {"status": "TITLE_MISMATCH", "content_returned": False}
        shard = self.registry.execute(
            """SELECT s.shard_id,s.relative_path,r.sha256 FROM shard_catalog s
               JOIN release_shards r ON s.shard_id=r.shard_id
               WHERE s.shard_id=? AND r.release_id=?""",
            (entry["shard_id"], self.release_id),
        ).fetchone()
        if shard is None:
            return {"status": "RELEASE_MISMATCH", "content_returned": False}
        try:
            path = _safe_relative(self.root, shard["relative_path"])
        except ValueError:
            return {"status": "SHARD_PATH_INVALID", "content_returned": False}
        if not path.is_file():
            return {"status": "ARCHIVE_MISSING", "content_returned": False}
        if shard["shard_id"] not in self._verified_shards:
            try:
                if file_hash(path) != shard["sha256"]:
                    return {"status": "SHARD_HASH_MISMATCH", "content_returned": False}
            except OSError:
                return {"status": "ARCHIVE_MISSING", "content_returned": False}
            self._verified_shards.add(shard["shard_id"])
        try:
            with database(path, readonly=True) as db:
                chunk = db.execute(
                    "SELECT * FROM chunks WHERE code=? AND published=1", (code,)
                ).fetchone()
        except sqlite3.DatabaseError:
            return {"status": "SHARD_CORRUPT", "content_returned": False}
        if chunk is None or digest(chunk["content"]) != chunk["content_sha256"]:
            return {"status": "SHA256_MISMATCH", "content_returned": False}
        if (chunk["cdx"] != cdx or chunk["section"] != section
                or chunk["entity_anchor"] != entry["entity_anchor"]
                or chunk["title"] != entry["title"] or chunk["topic"] != entry["topic"]):
            return {"status": "CHUNK_METADATA_MISMATCH", "content_returned": False}
        return {
            "status": "OK", "content_returned": True, "code": code,
            "title": chunk["title"], "breadcrumb": chunk["breadcrumb"],
            "entity_anchor": chunk["entity_anchor"],
            "content_hash": chunk["content_sha256"], "content": chunk["content"],
        }
