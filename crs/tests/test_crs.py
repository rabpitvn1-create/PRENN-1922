"""Focused CRS foundation tests; no third-party packages."""
import os
from pathlib import Path
import sqlite3
import tempfile
import unittest

from crs import CodexStore, CodexReader, make_code, parse_code
from crs.store import connect


class IdTests(unittest.TestCase):
    def test_known_code(self):
        self.assertEqual(make_code("KAE", 3, 17), "KAE.003.00174Z")
        self.assertEqual(parse_code("KAE.003.00174Z"), ("KAE", 3, 17))

    def test_all_single_changes_and_transpositions(self):
        code = make_code("KAE", 123, 4567)
        payload = code.replace(".", "")[:-2]
        for i, ch in enumerate(payload):
            changed = payload[:i] + ("0" if ch != "0" else "1") + payload[i + 1:]
            candidate = changed[:3] + "." + changed[3:6] + "." + changed[6:] + code[-2:]
            with self.assertRaises(ValueError):
                parse_code(candidate)
        for i in range(len(payload)):
            for j in range(i + 1, len(payload)):
                if payload[i] == payload[j]:
                    continue
                chars = list(payload)
                chars[i], chars[j] = chars[j], chars[i]
                changed = "".join(chars)
                candidate = changed[:3] + "." + changed[3:6] + "." + changed[6:] + code[-2:]
                with self.assertRaises(ValueError):
                    parse_code(candidate)


class StoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = Path(self.tmp.name)
        self.store = CodexStore(self.root)
        self.store.register_codex("KAE", "Kael")
        self.store.new_shard("KAE", "kae_001")
        self.assertTrue((self.root / "staging/manual_kae_001.db").is_file())
        self.assertFalse((self.root / "shards/kae_001.db").exists())
        self.anchor = make_code("KAE", 1, 1)
        self.good = make_code("KAE", 3, 17)
        self.other = make_code("KAE", 3, 18)
        self.store.add_entry(self.anchor, "KAE", "Kael")
        self.store.add_section("KAE", 3, "Biography", "Vatican")
        self.store.add_chunk("kae_001", self.good, self.anchor, "Vatican",
                             "BIO.VATICAN", "Kael / Biography", "Original lore.")
        self.store.add_chunk("kae_001", self.other, self.anchor, "Combat",
                             "COMBAT", "Kael / Combat", "Sword techniques.")
        self.store.publish("R001")
        self.reader = CodexReader(self.root, "R001")
        self.reader.create_session("S001")
        self.reader.pin_intent("S001", "I001", self.anchor, "KAE", 3,
                               "BIO.VATICAN", "Vatican")

    def tearDown(self):
        self.reader.close()
        self.store.close()
        for path in self.root.rglob("*.db"):
            os.chmod(path, 0o644)
        self.tmp.cleanup()

    def test_get_and_navigation(self):
        result = self.reader.get(self.good, "I001", "S001")
        self.assertEqual(result["status"], "OK")
        self.assertEqual(result["content"], "Original lore.")
        self.assertEqual(self.reader.toc_root("KAE")[0]["group_name"], "Biography")
        self.assertEqual(self.reader.toc_group("KAE", "Biography")[0]["section"], 3)
        self.assertEqual(len(self.reader.idx("KAE", 3)), 2)

    def test_reject_mismatches(self):
        self.reader.pin_intent("S001", "I_ENTITY", make_code("KAE", 1, 2), "KAE", 3,
                               "BIO.VATICAN", "Vatican")
        self.reader.pin_intent("S001", "I_TOPIC", self.anchor, "KAE", 3, "COMBAT", "Vatican")
        self.reader.pin_intent("S001", "I_TITLE", self.anchor, "KAE", 3,
                               "BIO.VATICAN", "Combat")
        cases = [
            ("KAE.003.0017ZZ", "I001", "INVALID_CHECKSUM"),
            (self.good, "MISSING", "INTENT_NOT_FOUND"),
            (self.good, "I_ENTITY", "ENTITY_MISMATCH"),
            (self.good, "I_TOPIC", "INTENT_MISMATCH"),
            (self.good, "I_TITLE", "TITLE_MISMATCH"),
            (self.other, "I001", "INTENT_MISMATCH"),
            (make_code("KAE", 3, 99), "I001", "NOT_IN_PINNED_RELEASE"),
        ]
        for code, intent, status in cases:
            with self.subTest(status=status):
                result = self.reader.get(code, intent, "S001")
                self.assertEqual(result["status"], status)
                self.assertFalse(result["content_returned"])
                self.assertNotIn("content", result)

    def test_immutable_published_chunk(self):
        path = self.root / "shards" / "kae_001.db"
        os.chmod(path, 0o644)
        statements = [
            ("UPDATE chunks SET content='bad' WHERE code=?", (self.good,)),
            ("DELETE FROM chunks WHERE code=?", (self.good,)),
            ("UPDATE chunks SET content_sha256='bad' WHERE code=?", (self.good,)),
            ("INSERT OR REPLACE INTO chunks SELECT * FROM chunks WHERE code=?", (self.good,)),
            ("INSERT INTO chunks SELECT * FROM chunks WHERE code=? ON CONFLICT(code) DO UPDATE SET content='bad'", (self.good,)),
        ]
        db = connect(path)
        try:
            for sql, args in statements:
                with self.subTest(sql=sql), self.assertRaises(sqlite3.DatabaseError):
                    db.execute(sql, args)
        finally:
            db.close()
        self.assertEqual(self.reader.get(self.good, "I001", "S001")["content"], "Original lore.")

    def test_manifest_and_snapshot_are_verified(self):
        self.reader.close()
        manifest = self.root / "releases/R001/manifest.json"
        original = manifest.read_bytes()
        manifest.write_bytes(original + b" ")
        with self.assertRaisesRegex(ValueError, "MANIFEST_SHA256_MISMATCH"):
            CodexReader(self.root, "R001")
        manifest.write_bytes(original)
        snapshot = self.root / "releases/R001/registry.db"
        os.chmod(snapshot, 0o644)
        with snapshot.open("ab") as handle:
            handle.write(b"tamper")
        with self.assertRaisesRegex(ValueError, "SNAPSHOT_SHA256_MISMATCH"):
            CodexReader(self.root, "R001")


if __name__ == "__main__":
    unittest.main()
