"""Manifest and file state helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import re

import pandas as pd

MANIFEST_COLUMNS = [
    "module",
    "measure",
    "source_name",
    "source_path",
    "fingerprint",
    "size_bytes",
    "mtime_ns",
    "cache_path",
    "years",
    "min_date",
    "max_date",
    "row_count",
    "active",
    "updated_at",
]


def load_manifest(path: Path) -> pd.DataFrame:
    if not path.exists():
        return pd.DataFrame(columns=MANIFEST_COLUMNS)

    manifest = pd.read_csv(path, dtype=str).fillna("")
    for column in MANIFEST_COLUMNS:
        if column not in manifest.columns:
            manifest[column] = ""
    return manifest[MANIFEST_COLUMNS].copy()


def save_manifest(manifest: pd.DataFrame, path: Path) -> None:
    output = manifest.copy()
    output = output[MANIFEST_COLUMNS].sort_values(
        ["module", "measure", "source_name"],
        kind="stable",
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    output.to_csv(path, index=False)


def fingerprint_file(path: Path) -> tuple[str, int, int]:
    stat = path.stat()
    return f"{stat.st_size}-{stat.st_mtime_ns}", stat.st_size, stat.st_mtime_ns


def safe_file_stem(name: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9._-]+", "_", name)
    return cleaned.strip("_") or "source"


def format_years(years: set[int]) -> str:
    return "|".join(str(year) for year in sorted(years))


def parse_years(raw: str) -> set[int]:
    if not raw:
        return set()
    return {int(value) for value in raw.split("|") if value}


def utc_now_iso() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat()
