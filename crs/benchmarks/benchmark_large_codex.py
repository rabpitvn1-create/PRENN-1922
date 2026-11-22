"""Run a synthetic 100k-word offline ingestion benchmark.

Usage: python -m crs.benchmarks.benchmark_large_codex --words 100000
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import tempfile
import time

from crs.ingest import OfflineIndexer, publish_atomic


TEMPLATES = {
    "FACT": "The registry records Kael as a resident of Saigon during the year nineteen oh one.",
    "RULE": "If the archive door is sealed, the reader must request a key; unless the keeper approves, entry remains forbidden.",
    "LORE": "The old library preserves the memory of the river kingdom, its vanished roads, and the names of forgotten houses.",
    "NARRATIVE": "The lantern shook in the corridor while Kael listened to the rain and watched the distant doorway.",
}


def build_corpus(words: int) -> str:
    if words < 400:
        raise ValueError("BENCHMARK_WORDS_TOO_SMALL")
    per_kind = words // len(TEMPLATES)
    blocks = []
    for kind, sentence in TEMPLATES.items():
        target = per_kind
        paragraph_words = sentence.split()
        repetitions = max(1, min(5, 30 // len(paragraph_words)))
        paragraph = " ".join([sentence] * repetitions)
        count = len(paragraph.split())
        paragraphs = []
        while target > 0:
            paragraphs.append(paragraph)
            target -= count
        blocks.append(f"# {kind} benchmark corpus\n<!-- CRS:{kind} -->\n\n" +
                      "\n\n".join(paragraphs))
    return "\n\n".join(blocks)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--words", type=int, default=100_000)
    args = parser.parse_args()
    source = build_corpus(args.words)
    actual_words = len(source.split())
    with tempfile.TemporaryDirectory(prefix="crs-benchmark-") as temp:
        root = Path(temp)
        started = time.perf_counter()
        indexer = OfflineIndexer(root)
        try:
            build = indexer.ingest("BEN", "Benchmark Codex", source, "BENCH001")
        finally:
            indexer.close()
        report = publish_atomic(root, "BENCH001")
        elapsed = time.perf_counter() - started
        print(json.dumps({
            "requested_words": args.words,
            "actual_words": actual_words,
            "total_chunks": build["total_chunks"],
            "by_type": build["by_type"],
            "elapsed_seconds": round(elapsed, 3),
            "database_physical_bytes": report["database_bytes"],
            "physical_storage": report["physical_storage"],
        }, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
