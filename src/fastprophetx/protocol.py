"""ProphetX protocol normalization helpers."""

from __future__ import annotations

import re
import unicodedata

_SEPARATOR_RE = re.compile(r"[\s/-]+")
_PUNCTUATION_RE = re.compile(r"[^a-z0-9_]")
_UNDERSCORE_RE = re.compile(r"_+")


def canonicalize_subtype(value: str) -> str:
    if not isinstance(value, str):
        raise TypeError("event subtype must be a string")
    ascii_value = (
        unicodedata.normalize("NFKD", value)
        .encode("ascii", "ignore")
        .decode()
        .casefold()
    )
    canonical = _SEPARATOR_RE.sub("_", ascii_value)
    canonical = _PUNCTUATION_RE.sub("", canonical)
    canonical = _UNDERSCORE_RE.sub("_", canonical).strip("_")
    if not canonical:
        raise ValueError("event subtype cannot be empty")
    return canonical
