"""Helpers for working with Eurostat Excel matrix exports."""

from __future__ import annotations

import logging
import re
from pathlib import Path
from typing import Iterable

import pandas as pd

LOGGER = logging.getLogger(__name__)

METADATA_COLUMNS = [
    "country",
    "date",
    "Sheet",
    "SourceFile",
    "Frequency",
    "PRODUCT",
    "FLOW",
    "INDICATORS",
]


def resolve_existing_directory(candidates: Iterable[Path]) -> Path:
    for candidate in candidates:
        if candidate.exists() and any(
            path.suffix.lower() == ".xlsx" and not path.name.startswith("~$")
            for path in candidate.glob("*.xlsx")
        ):
            return candidate
    joined = ", ".join(str(path) for path in candidates)
    raise FileNotFoundError(f"No Excel source directory found. Checked: {joined}")


def list_excel_files(source_dir: Path) -> list[Path]:
    files = sorted(
        path
        for path in source_dir.glob("*.xlsx")
        if path.is_file() and not path.name.startswith("~$")
    )
    if not files:
        raise FileNotFoundError(f"No .xlsx files found in {source_dir}")
    return files


def clean_label(value: object) -> object:
    if pd.isna(value):
        return value
    text = str(value)
    text = re.sub(r"\s*\(.*", "", text)
    text = re.sub(r"\s*from\s+\d{4}.*?\)$", "", text)
    text = re.sub(r"\s*->\s*\d{4}\)?$", "", text)
    return text.strip()


def load_trade_exports(source_dir: Path, measure_column: str) -> pd.DataFrame:
    files = list_excel_files(source_dir)

    LOGGER.info("Using source directory: %s", source_dir)
    LOGGER.info("Found %s workbook(s)", len(files))

    frames = []
    for file_path in files:
        extracted = _extract_one_workbook(file_path)
        if extracted is not None:
            frames.append(extracted)

    if not frames:
        raise RuntimeError(f"No valid Eurostat sheets found in {source_dir}")

    wide = pd.concat(frames, ignore_index=True)
    value_columns = [column for column in wide.columns if column not in METADATA_COLUMNS]

    long = wide.melt(
        id_vars=METADATA_COLUMNS,
        value_vars=value_columns,
        var_name="partner",
        value_name=measure_column,
    )
    long[measure_column] = pd.to_numeric(
        long[measure_column].replace({":": None}),
        errors="coerce",
    )
    return long


def load_trade_export_file(file_path: Path, measure_column: str) -> pd.DataFrame:
    extracted = _extract_one_workbook(file_path)
    if extracted is None:
        raise RuntimeError(f"No valid Eurostat sheets found in {file_path}")

    value_columns = [column for column in extracted.columns if column not in METADATA_COLUMNS]
    long = extracted.melt(
        id_vars=METADATA_COLUMNS,
        value_vars=value_columns,
        var_name="partner",
        value_name=measure_column,
    )
    long[measure_column] = pd.to_numeric(
        long[measure_column].replace({":": None}),
        errors="coerce",
    )
    return long


def _extract_one_workbook(file_path: Path) -> pd.DataFrame | None:
    try:
        workbook = pd.ExcelFile(file_path, engine="openpyxl")
    except Exception as exc:  # pragma: no cover - defensive for malformed files
        LOGGER.warning("Skipping workbook %s: %s", file_path.name, exc)
        return None

    sheet_names = [sheet for sheet in workbook.sheet_names if sheet.startswith("Sheet")]
    if not sheet_names:
        LOGGER.warning("Skipping workbook without Sheet tabs: %s", file_path.name)
        return None

    frames = []
    for sheet_name in sheet_names:
        try:
            metadata = pd.read_excel(
                workbook,
                sheet_name=sheet_name,
                header=None,
                nrows=8,
                engine="openpyxl",
            )
            data = pd.read_excel(
                workbook,
                sheet_name=sheet_name,
                header=9,
                engine="openpyxl",
            )
        except Exception as exc:  # pragma: no cover - defensive for malformed tabs
            LOGGER.warning("Skipping %s / %s: %s", file_path.name, sheet_name, exc)
            continue

        if data.shape[0] < 2 or data.shape[1] < 2:
            LOGGER.warning("Skipping %s / %s because the data block is incomplete", file_path.name, sheet_name)
            continue

        data = data.iloc[1:].reset_index(drop=True)
        data = data.dropna(axis=1, how="all")

        first_column, second_column = data.columns[:2]
        data = data.rename(columns={first_column: "country", second_column: "date"})
        data["Sheet"] = sheet_name
        data["SourceFile"] = file_path.name
        data["Frequency"] = _pick_metadata_value(metadata, 4)
        data["PRODUCT"] = _pick_metadata_value(metadata, 5)
        data["FLOW"] = _pick_metadata_value(metadata, 6)
        data["INDICATORS"] = _pick_metadata_value(metadata, 7)
        frames.append(data)

    if not frames:
        return None
    return pd.concat(frames, ignore_index=True)


def _pick_metadata_value(metadata: pd.DataFrame, row_index: int) -> object:
    primary = metadata.iat[row_index, 2] if metadata.shape[1] > 2 else None
    if pd.notna(primary):
        return primary
    return metadata.iat[row_index, 0]
