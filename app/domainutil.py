from __future__ import annotations

import re

_LABEL_RE = re.compile(r"^[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?$")
_TLD_RE = re.compile(r"^(?:[a-z]{2,63}|xn--[a-z0-9-]{2,59})$")
_SPLIT_RE = re.compile(r"[\s,;]+")
_EDGE = " \t\r\n,;\"'()[]<>"


def domain_tokens(values: list[str]) -> list[str]:
    """Split pasted text into candidates. Commas and spaces do not glue two sites together."""
    tokens: list[str] = []
    for value in values:
        if value is None:
            continue
        for part in _SPLIT_RE.split(str(value)):
            part = part.strip(_EDGE)
            if part:
                tokens.append(part)
    return tokens


def normalize_domain(value: str) -> str:
    """Normalize a domain or URL into a stable deduplication key."""
    if value is None:
        raise ValueError("domain is required")
    raw = str(value).strip(_EDGE).lower()
    if not raw or "@" in raw:
        raise ValueError(f"invalid domain: {value!r}")
    raw = raw.replace("\\", "/")
    if "://" in raw:
        raw = raw.split("://", 1)[1]
    raw = raw.split("/")[0]
    raw = raw.split("?")[0]
    raw = raw.split("#")[0]
    raw = raw.strip(_EDGE)
    if raw.endswith("]") and raw.startswith("["):
        raw = raw[1:-1]
    elif ":" in raw:
        host, port = raw.rsplit(":", 1)
        if port.isdigit():
            raw = host
    if raw.startswith("www."):
        raw = raw[4:]
    raw = raw.strip(".")
    if not raw or "." not in raw or ".." in raw:
        raise ValueError(f"invalid domain: {value!r}")
    try:
        raw = ".".join(label.encode("idna").decode("ascii") for label in raw.split("."))
    except UnicodeError as exc:
        raise ValueError(f"invalid domain: {value!r}") from exc
    labels = raw.split(".")
    if any(not _LABEL_RE.match(label) for label in labels):
        raise ValueError(f"invalid domain: {value!r}")
    if not _TLD_RE.match(labels[-1]):
        raise ValueError(f"invalid domain: {value!r}")
    return raw
