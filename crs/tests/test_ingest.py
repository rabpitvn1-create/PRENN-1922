"""Offline ingestion, publication recovery, and exact GET integration tests."""
import os
from pathlib import Path
import tempfile
import unittest

from crs.ingest import OfflineIndexer, chunk_text, title_keys, tokens, publish_atomic
from crs.store import CodexReader, connect


def sample():
    blocks = []
    examples = {
        "FACT": "Kael was born in Saigon in 1901. " * 11,
        "RULE": ("If the gate is locked, the party must find the key; "
                 "unless the guardian approves, entry remains forbidden. ") * 11,
        "LORE": "The ancient library preserves a memory of the old city. " * 28,
        "NARRATIVE": "The lantern trembled. Kael said the road was clear. " * 45,
    }
    for kind, text in examples.items():
        blocks.append(f"# {kind} chapter\n<!-- CRS:{kind} -->\n\n{text}\n")
    return "\n".join(blocks)


class ChunkTests(unittest.TestCase):
    def test_type_boundaries(self):
        cases = {
            "FACT": ("An item is recorded in the ledger. " * 36, 50, 150),
            "RULE": ("If the door opens, proceed; unless it is guarded, wait. " * 14, 100, 250),
            "LORE": ("History survives in the library under the mountain. " * 55, 200, 400),
            "NARRATIVE": ("The traveler listened and turned toward the fire. " * 85, 300, 500),
        }
        for kind, (source, minimum, maximum) in cases.items():
            with self.subTest(kind=kind):
                parts = chunk_text(source, kind)
                self.assertTrue(parts)
                self.assertTrue(all(minimum <= tokens(x) <= maximum for x in parts))

    def test_rule_condition_and_exception_are_atomic(self):
        clause = ("If the door opens, proceed; unless it is guarded, wait. " * 12).strip()
        self.assertEqual(chunk_text(clause, "RULE"), [clause])
        with self.assertRaisesRegex(ValueError, "OVERSIZED_ATOMIC_RULE"):
            chunk_text(clause * 8, "RULE")

    def test_nfc_folded_and_vietnamese_d(self):
        a, b = title_keys("Đặng Văn")
        self.assertEqual(a, "đặng văn")
        self.assertEqual(b, "dang van")


class IngestionTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name)

    def tearDown(self):
        for db in self.root.rglob("*.db"):
            os.chmod(db, 0o644)
        self.temp.cleanup()

    def _ingest(self, source, release="R001"):
        indexer = OfflineIndexer(self.root)
        try:
            return indexer.ingest("KAE", "Kael", source, release)
        finally:
            indexer.close()

    def test_ingest_publish_get_and_indices(self):
        summary = self._ingest(sample())
        self.assertEqual(set(summary["by_type"]), {"FACT", "RULE", "LORE", "NARRATIVE"})
        self.assertTrue(all(n > 0 for n in summary["by_type"].values()))
        report = publish_atomic(self.root, "R001")
        self.assertEqual(report["total_chunks"], summary["total_chunks"])
        self.assertEqual((self.root / "CURRENT").read_text().strip(), "R001")
        with connect(self.root / "releases/R001/registry.db", readonly=True) as db:
            self.assertEqual(db.execute("""SELECT COUNT(*) FROM chunk_locator l
                LEFT JOIN title_index t ON t.code=l.code WHERE t.code IS NULL""").fetchone()[0], 0)
            self.assertEqual(db.execute("SELECT COUNT(*) FROM title_fts").fetchone()[0],
                             summary["total_chunks"])
            self.assertEqual(db.execute("SELECT COUNT(DISTINCT code) FROM code_allocations").fetchone()[0],
                             summary["total_chunks"] + 1)
            record = db.execute("""SELECT l.code,l.title,l.section,l.topic,l.entity_anchor,l.cdx
                FROM chunk_locator l LIMIT 1""").fetchone()
        reader = CodexReader(self.root, "R001")
        try:
            reader.create_session("S1")
            reader.pin_intent("S1", "I1", record["entity_anchor"], record["cdx"],
                              record["section"], record["topic"], record["title"])
            response = reader.get(record["code"], "I1", "S1")
            self.assertEqual(response["status"], "OK")
            self.assertTrue(response["content"])
        finally:
            reader.close()

    def test_crash_before_current_preserves_old_release(self):
        self._ingest(sample())
        publish_atomic(self.root, "R001")
        first = (self.root / "CURRENT").read_bytes()
        self._ingest("# Extra\n<!-- CRS:FACT -->\n" +
                     "Kael was born in Saigon in 1901. " * 12, "R002")
        with self.assertRaisesRegex(RuntimeError, "INJECTED_CRASH"):
            publish_atomic(self.root, "R002", crash_at="before_commit")
        self.assertEqual((self.root / "CURRENT").read_bytes(), first)
        reader = CodexReader(self.root, "R001")
        reader.close()
        self.assertTrue((self.root / "releases/R001/registry.db").is_file())

    def test_retry_after_crash_before_commit(self):
        self._ingest(sample())
        publish_atomic(self.root, "R001")
        first = (self.root / "CURRENT").read_bytes()
        self._ingest("# New facts\n<!-- CRS:FACT -->\n" +
                     "Kael was born in Saigon in 1901. " * 12, "R002")
        with self.assertRaisesRegex(RuntimeError, "INJECTED_CRASH"):
            publish_atomic(self.root, "R002", crash_at="before_commit")
        self.assertEqual((self.root / "CURRENT").read_bytes(), first)
        report = publish_atomic(self.root, "R002")
        self.assertEqual(report["release_id"], "R002")
        self.assertEqual((self.root / "CURRENT").read_text().strip(), "R002")
        self.assertTrue((self.root / "releases/R001/registry.db").is_file())
        with self.assertRaisesRegex(ValueError, "RELEASE_EXISTS"):
            publish_atomic(self.root, "R002")

    def test_no_code_reuse(self):
        self._ingest(sample())
        with connect(self.root / "registry.db") as db:
            codes = [r[0] for r in db.execute("SELECT code FROM code_allocations")]
            self.assertEqual(len(codes), len(set(codes)))
        with self.assertRaisesRegex(ValueError, "SOURCE_ALREADY_INGESTED"):
            self._ingest(sample())


if __name__ == "__main__":
    unittest.main()
