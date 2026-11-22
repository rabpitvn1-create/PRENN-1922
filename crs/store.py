"""Offline codex authoring and release-pinned read-only retrieval."""
from __future__ import annotations

import hashlib
import os
from pathlib import Path
import sqlite3
import unicodedata

from .ids import make_code, parse_code
from .schema import REGISTRY_SCHEMA, SHARD_SCHEMA


def digest(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def connect(path: Path, readonly: bool = False) -> sqlite3.Connection:
    if readonly:
        db = sqlite3.connect(path.resolve().as_uri() + "?mode=ro", uri=True)
        db.execute("PRAGMA query_only=ON")
    else:
        db = sqlite3.connect(path)
    db.execute("PRAGMA recursive_triggers=ON")
    db.execute("PRAGMA foreign_keys=ON")
    db.row_factory = sqlite3.Row
    return db


def file_hash(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for block in iter(lambda: f.read(1048576), b""):
            h.update(block)
    return h.hexdigest()


class CodexStore:
    """Offline indexer. Initial publication only, not a delta publisher."""

    def __init__(self, root: str | Path):
        self.root = Path(root)
        self.root.mkdir(parents=True, exist_ok=True)
        (self.root / "shards").mkdir(exist_ok=True)
        (self.root / "releases").mkdir(exist_ok=True)
        self.registry = connect(self.root / "registry.db")
        self.registry.executescript(REGISTRY_SCHEMA)

    def close(self):
        self.registry.close()

    def register_codex(self, cdx: str, title: str):
        make_code(cdx, 0, 0)
        with self.registry:
            self.registry.execute("INSERT INTO codex_registry(cdx,title) VALUES(?,?)", (cdx,title))

    def new_shard(self, cdx: str, shard_id: str):
        if not shard_id.replace("_", "").isalnum():
            raise ValueError("INVALID_SHARD_ID")
        rel = f"shards/{shard_id}.db"
        path = self.root / rel
        if path.exists():
            raise ValueError("SHARD_EXISTS")
        with self.registry:
            self.registry.execute(
                "INSERT INTO shard_catalog(shard_id,cdx,relative_path) VALUES(?,?,?)",
                (shard_id,cdx,rel))
        db = connect(path)
        db.executescript(SHARD_SCHEMA)
        db.close()

    def add_entry(self, anchor: str, cdx: str, title: str, entity_type: str = "CHARACTER"):
        acdx, _, _ = parse_code(anchor)
        if acdx != cdx:
            raise ValueError("ANCHOR_CDX_MISMATCH")
        with self.registry:
            self.registry.execute(
                "INSERT INTO entry_registry(anchor,cdx,title,entity_type) VALUES(?,?,?,?)",
                (anchor,cdx,title,entity_type))

    def add_section(self, cdx: str, section: int, group: str, title: str):
        with self.registry:
            self.registry.execute(
                "INSERT INTO section_toc VALUES(?,?,?,?)", (cdx,section,group,title))

    def add_chunk(self, shard_id: str, code: str, anchor: str, title: str,
                  topic: str, breadcrumb: str, content: str):
        cdx, section, _ = parse_code(code)
        shard = self.registry.execute(
            "SELECT * FROM shard_catalog WHERE shard_id=?", (shard_id,)).fetchone()
        if shard is None or shard["frozen"] or shard["cdx"] != cdx:
            raise ValueError("SHARD_NOT_WRITABLE")
        if not self.registry.execute(
            "SELECT 1 FROM entry_registry WHERE anchor=? AND cdx=?",
            (anchor,cdx)).fetchone():
            raise ValueError("UNKNOWN_ENTITY")
        path = self.root / shard["relative_path"]
        with connect(path) as db:
            db.execute("INSERT INTO chunks VALUES(?,?,?,?,?,?,?,?,?,0)",
                       (code,cdx,section,anchor,title,topic,breadcrumb,content,digest(content)))
        try:
            with self.registry:
                self.registry.execute("INSERT INTO chunk_locator VALUES(?,?,?,?,?,?,?)",
                                      (code,shard_id,cdx,section,anchor,title,topic))
                key = unicodedata.normalize("NFC", title).casefold()
                self.registry.execute("INSERT INTO title_index VALUES(?,?,?)",
                                      (code,key,breadcrumb))
        except Exception:
            with connect(path) as db:
                db.execute("DELETE FROM chunks WHERE code=? AND published=0", (code,))
            raise

    def publish(self, release_id: str):
        """Freeze shard content and create a registry snapshot for first release.

        Not yet crash-atomic or suitable for multi-release delta publication.
        """
        if self.registry.execute(
            "SELECT 1 FROM release_manifest WHERE release_id=?", (release_id,)).fetchone():
            raise ValueError("RELEASE_EXISTS")
        shards = list(self.registry.execute("SELECT * FROM shard_catalog ORDER BY shard_id"))
        for shard in shards:
            path = self.root / shard["relative_path"]
            with connect(path) as db:
                db.execute("UPDATE chunks SET published=1 WHERE published=0")
            os.chmod(path, 0o444)
        with self.registry:
            self.registry.execute(
                "INSERT INTO release_manifest(release_id,published) VALUES(?,1)", (release_id,))
            for shard in shards:
                self.registry.execute("INSERT INTO release_shards VALUES(?,?,?)",
                    (release_id,shard["shard_id"],file_hash(self.root / shard["relative_path"])))
            self.registry.execute("UPDATE shard_catalog SET frozen=1")
        target = self.root / "releases" / release_id
        target.mkdir()
        snapshot = connect(target / "registry.db")
        self.registry.backup(snapshot)
        snapshot.close()
        os.chmod(target / "registry.db", 0o444)


class CodexReader:
    """Release-pinned reader with writable *separate* session state."""

    def __init__(self, root: str | Path, release_id: str):
        self.root = Path(root)
        self.release_id = release_id
        self.registry = connect(
            self.root / "releases" / release_id / "registry.db", readonly=True)
        row = self.registry.execute(
            "SELECT 1 FROM release_manifest WHERE release_id=? AND published=1",
            (release_id,)).fetchone()
        if row is None:
            raise ValueError("RELEASE_MISMATCH")
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
                "INSERT INTO active_sessions VALUES(?,?)", (session_id,self.release_id))

    def pin_intent(self, session_id: str, intent_id: str, entity_anchor: str, cdx: str,
                   section: int, topic: str, expected_title: str):
        """Trusted coordinator only: do NOT expose to the model as a tool."""
        row = self.sessions.execute(
            "SELECT release_id FROM active_sessions WHERE session_id=?",
            (session_id,)).fetchone()
        if row is None or row["release_id"] != self.release_id:
            raise ValueError("RELEASE_MISMATCH")
        with self.sessions:
            self.sessions.execute(
                """INSERT INTO pinned_intents(
                    intent_id,session_id,entity_anchor,cdx,section,topic,expected_title)
                    VALUES(?,?,?,?,?,?,?)""",
                (intent_id,session_id,entity_anchor,cdx,section,topic,expected_title))

    def toc_root(self, cdx: str, limit: int = 10, offset: int = 0):
        self._page(limit,offset)
        rows = self.registry.execute(
            """SELECT group_name,COUNT(*) AS sections FROM section_toc
               WHERE cdx=? GROUP BY group_name ORDER BY group_name LIMIT ? OFFSET ?""",
            (cdx,limit,offset))
        return [dict(x) for x in rows]

    def toc_group(self, cdx: str, group: str, limit: int = 10, offset: int = 0):
        self._page(limit,offset)
        rows = self.registry.execute(
            """SELECT section,title FROM section_toc
               WHERE cdx=? AND group_name=? ORDER BY section LIMIT ? OFFSET ?""",
            (cdx,group,limit,offset))
        return [dict(x) for x in rows]

    def toc(self, cdx: str, limit: int = 10, offset: int = 0):
        self._page(limit,offset)
        rows = self.registry.execute(
            """SELECT section,group_name,title FROM section_toc
               WHERE cdx=? ORDER BY section LIMIT ? OFFSET ?""",
            (cdx,limit,offset))
        return [dict(x) for x in rows]

    def idx(self, cdx: str, section: int, limit: int = 10, offset: int = 0):
        self._page(limit,offset)
        rows = self.registry.execute(
            """SELECT l.code,l.title,l.entity_anchor,t.breadcrumb
               FROM chunk_locator l JOIN title_index t ON t.code=l.code
               WHERE l.cdx=? AND l.section=?
               ORDER BY l.code LIMIT ? OFFSET ?""",
            (cdx,section,limit,offset))
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
            (intent_id,session_id)).fetchone()
        if intent is None:
            return {"status":"INTENT_NOT_FOUND","content_returned":False}
        try:
            cdx,section,_ = parse_code(code)
        except ValueError as exc:
            return {"status":str(exc),"content_returned":False}
        if intent["release_id"] != self.release_id:
            return {"status":"RELEASE_MISMATCH","content_returned":False}
        entry = self.registry.execute(
            "SELECT * FROM chunk_locator WHERE code=?", (code,)).fetchone()
        if entry is None:
            return {"status":"NOT_IN_PINNED_RELEASE","content_returned":False}
        if entry["entity_anchor"] != intent["entity_anchor"]:
            return {"status":"ENTITY_MISMATCH","content_returned":False}
        if (cdx != intent["cdx"] or section != intent["section"]
                or entry["topic"] != intent["topic"]):
            return {"status":"INTENT_MISMATCH","content_returned":False}
        if entry["title"] != intent["expected_title"]:
            return {"status":"TITLE_MISMATCH","content_returned":False}
        shard = self.registry.execute(
            """SELECT s.relative_path FROM shard_catalog s
               JOIN release_shards r ON s.shard_id=r.shard_id
               WHERE s.shard_id=? AND r.release_id=?""",
            (entry["shard_id"],self.release_id)).fetchone()
        if shard is None:
            return {"status":"RELEASE_MISMATCH","content_returned":False}
        path = self.root / shard["relative_path"]
        if not path.is_file():
            return {"status":"ARCHIVE_MISSING","content_returned":False}
        try:
            with connect(path,readonly=True) as db:
                chunk = db.execute(
                    "SELECT * FROM chunks WHERE code=? AND published=1",
                    (code,)).fetchone()
        except sqlite3.DatabaseError:
            return {"status":"SHARD_CORRUPT","content_returned":False}
        if chunk is None or digest(chunk["content"]) != chunk["content_sha256"]:
            return {"status":"SHA256_MISMATCH","content_returned":False}
        return {"status":"OK","code":code,"title":chunk["title"],
                "breadcrumb":chunk["breadcrumb"],"entity_anchor":chunk["entity_anchor"],
                "content_hash":chunk["content_sha256"],"content":chunk["content"]}
