"""Stable CRS v3 identifiers. No external dependencies."""
import re

ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
PATTERN = re.compile(r"^([0-9A-Z]{2,4})\\.([0-9]{3})\\.([0-9]{4})([0-9A-Z]{2})$")


def check_digits(payload: str) -> str:
    total = sum(i * ALPHABET.index(c) for i, c in enumerate(payload, 1)) % 1296
    return ALPHABET[total // 36] + ALPHABET[total % 36]


def make_code(cdx: str, section: int, chunk: int) -> str:
    if not re.fullmatch(r"[0-9A-Z]{2,4}", cdx):
        raise ValueError("INVALID_CDX")
    if not 0 <= section <= 999 or not 0 <= chunk <= 9999:
        raise ValueError("INVALID_SEQUENCE")
    payload = f"{cdx}{section:03d}{chunk:04d}"
    return f"{cdx}.{section:03d}.{chunk:04d}{check_digits(payload)}"


def parse_code(code: str) -> tuple[str, int, int]:
    match = PATTERN.fullmatch(code)
    if not match:
        raise ValueError("INVALID_FORMAT")
    cdx, sec, chk, check = match.groups()
    if check_digits(cdx + sec + chk) != check:
        raise ValueError("INVALID_CHECKSUM")
    return cdx, int(sec), int(chk)
