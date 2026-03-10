"""Pipeline stages for tyre, vehicle and final merge datasets."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from .excel_exports import clean_label, list_excel_files, load_trade_export_file, resolve_existing_directory
from .mappings import (
    EXCLUDED_PARTNERS,
    FINAL_GENERAL_CATEGORIES,
    INVALID_REPORTERS,
    PARTNER_TO_CONTINENT,
    PARTNER_TO_SUBREGION,
    TIRE_CATEGORY_TO_GENERAL,
    TIRE_PRODUCT_TO_CATEGORY,
    TIRE_PRODUCT_TO_HS,
    VEHICLE_EXCLUDED_HS,
    VEHICLE_HS_TO_GENERAL,
    VEHICLE_PRODUCT_TO_HS,
)
from .state import (
    format_years,
    fingerprint_file,
    load_manifest,
    parse_years,
    safe_file_stem,
    save_manifest,
    utc_now_iso,
)

LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]
MANIFEST_PATH = REPO_ROOT / "data/metadata/manifest.csv"


@dataclass(frozen=True)
class StagePaths:
    source_candidates: tuple[Path, ...]
    combined_output_path: Path
    partition_root: Path
    source_cache_root: Path


@dataclass(frozen=True)
class BuildResult:
    output_path: Path
    affected_years: set[int]


TIRE_VALUE_PATHS = StagePaths(
    source_candidates=(
        REPO_ROOT / "data/raw/tire/value",
        REPO_ROOT / "Tire/raw_data/Raw_data_value",
    ),
    combined_output_path=REPO_ROOT / "data/interim/tire/tire_product_value.csv",
    partition_root=REPO_ROOT / "data/interim/tire/value/years",
    source_cache_root=REPO_ROOT / "data/interim/tire/value/sources",
)
TIRE_WEIGHT_PATHS = StagePaths(
    source_candidates=(
        REPO_ROOT / "data/raw/tire/weight",
        REPO_ROOT / "Tire/raw_data/Raw_data_weight",
    ),
    combined_output_path=REPO_ROOT / "data/interim/tire/tire_product_weight.csv",
    partition_root=REPO_ROOT / "data/interim/tire/weight/years",
    source_cache_root=REPO_ROOT / "data/interim/tire/weight/sources",
)
VEHICLE_VALUE_PATHS = StagePaths(
    source_candidates=(
        REPO_ROOT / "data/raw/vehicle/value",
        REPO_ROOT / "Vehicle/Raw_Data/Value",
    ),
    combined_output_path=REPO_ROOT / "data/interim/vehicle/vehicle_product_value.csv",
    partition_root=REPO_ROOT / "data/interim/vehicle/value/years",
    source_cache_root=REPO_ROOT / "data/interim/vehicle/value/sources",
)
VEHICLE_WEIGHT_PATHS = StagePaths(
    source_candidates=(
        REPO_ROOT / "data/raw/vehicle/weight",
        REPO_ROOT / "Vehicle/Raw_Data/Weight",
    ),
    combined_output_path=REPO_ROOT / "data/interim/vehicle/vehicle_product_weight.csv",
    partition_root=REPO_ROOT / "data/interim/vehicle/weight/years",
    source_cache_root=REPO_ROOT / "data/interim/vehicle/weight/sources",
)

MERGED_OUTPUT_PATH = REPO_ROOT / "data/processed/eurostat_tyre_vehicle.csv"
MERGED_PARTITION_ROOT = REPO_ROOT / "data/processed/eurostat_tyre_vehicle/years"


def build_tire(measure: str = "all", full_refresh: bool = False) -> list[BuildResult]:
    measure = _normalize_measure(measure)
    outputs: list[BuildResult] = []
    if measure in {"value", "all"}:
        outputs.append(_build_tire_measure("value", TIRE_VALUE_PATHS, full_refresh=full_refresh))
    if measure in {"weight", "all"}:
        outputs.append(_build_tire_measure("weight", TIRE_WEIGHT_PATHS, full_refresh=full_refresh))
    return outputs


def build_vehicle(measure: str = "all", full_refresh: bool = False) -> list[BuildResult]:
    measure = _normalize_measure(measure)
    outputs: list[BuildResult] = []
    if measure in {"value", "all"}:
        outputs.append(_build_vehicle_measure("value", VEHICLE_VALUE_PATHS, full_refresh=full_refresh))
    if measure in {"weight", "all"}:
        outputs.append(_build_vehicle_measure("weight", VEHICLE_WEIGHT_PATHS, full_refresh=full_refresh))
    return outputs


def build_merge(affected_years: set[int] | None = None, full_refresh: bool = False) -> Path:
    LOGGER.info("Building merged output")

    if affected_years is None or full_refresh or not MERGED_OUTPUT_PATH.exists():
        years_to_rebuild = _collect_interim_years()
    else:
        years_to_rebuild = set(affected_years)

    if not years_to_rebuild:
        LOGGER.info("No affected years detected for merge; keeping existing processed output")
        return MERGED_OUTPUT_PATH if MERGED_OUTPUT_PATH.exists() else _rebuild_processed_output(set())

    for year in sorted(years_to_rebuild):
        tire_value = _read_interim_year(TIRE_VALUE_PATHS, year, REPO_ROOT / "Tire/raw_data/tire_product_Value_PCR_TBR_SP.csv")
        tire_weight = _read_interim_year(TIRE_WEIGHT_PATHS, year, REPO_ROOT / "Tire/raw_data/tire_product_Weight_PCR_TBR_SP.csv")
        vehicle_value = _read_interim_year(VEHICLE_VALUE_PATHS, year, REPO_ROOT / "Vehicle/Data/vehicle_product_value.csv")
        vehicle_weight = _read_interim_year(VEHICLE_WEIGHT_PATHS, year, REPO_ROOT / "Vehicle/Data/vehicle_product_weight.csv")

        tire_final = _merge_tire(tire_value, tire_weight)
        vehicle_final = _merge_vehicle(vehicle_value, vehicle_weight)

        merged_year = pd.concat([tire_final, vehicle_final], ignore_index=True)
        merged_year["partner_continent"] = merged_year["partner"].map(PARTNER_TO_CONTINENT).fillna("Unknown")
        merged_year["partner_sub_region"] = merged_year["partner"].map(PARTNER_TO_SUBREGION).fillna("Unknown")
        merged_year["date"] = pd.to_datetime(merged_year["date"], errors="coerce")
        merged_year = merged_year.loc[merged_year["date"] >= pd.Timestamp("2017-01-01")].copy()
        merged_year = _finalize_dates(merged_year)
        merged_year = merged_year.sort_values(
            ["Type", "country", "date", "HS Code", "partner"],
            kind="stable",
        ).reset_index(drop=True)
        _write_csv(merged_year, _year_partition_path(MERGED_PARTITION_ROOT, year))

    _rebuild_processed_output(years_to_rebuild)
    LOGGER.info("Wrote merged dataset: %s", MERGED_OUTPUT_PATH)
    return MERGED_OUTPUT_PATH


def build_all(full_refresh: bool = False) -> list[Path]:
    outputs: list[Path] = []
    tyre_results = build_tire("all", full_refresh=full_refresh)
    vehicle_results = build_vehicle("all", full_refresh=full_refresh)
    affected_years = set()

    for result in [*tyre_results, *vehicle_results]:
        outputs.append(result.output_path)
        affected_years.update(result.affected_years)

    outputs.append(build_merge(affected_years=affected_years, full_refresh=full_refresh))
    return outputs


def _build_tire_measure(measure: str, paths: StagePaths, full_refresh: bool) -> BuildResult:
    return _build_incremental_stage(
        module="tire",
        measure=measure,
        paths=paths,
        transformer=_transform_tire_frame,
        dedupe_keys=["country", "date", "PRODUCT", "INDICATORS", "partner", "HS Code", "Category", "General_Category"],
        full_refresh=full_refresh,
    )


def _build_vehicle_measure(measure: str, paths: StagePaths, full_refresh: bool) -> BuildResult:
    return _build_incremental_stage(
        module="vehicle",
        measure=measure,
        paths=paths,
        transformer=_transform_vehicle_frame,
        dedupe_keys=["country", "date", "PRODUCT", "INDICATORS", "partner", "HS Code", "General_Category"],
        full_refresh=full_refresh,
    )


def _build_incremental_stage(
    module: str,
    measure: str,
    paths: StagePaths,
    transformer,
    dedupe_keys: list[str],
    full_refresh: bool,
) -> BuildResult:
    source_dir = resolve_existing_directory(paths.source_candidates)
    source_files = list_excel_files(source_dir)

    manifest = load_manifest(MANIFEST_PATH)
    existing = manifest.loc[
        (manifest["module"] == module) & (manifest["measure"] == measure)
    ].copy()
    existing_by_path = {row["source_path"]: row for _, row in existing.iterrows()}

    current_records: list[dict[str, str]] = []
    affected_years: set[int] = set()
    current_source_paths: set[str] = set()

    for file_path in source_files:
        source_path = str(file_path.resolve())
        source_name = file_path.name
        current_source_paths.add(source_path)
        fingerprint, size_bytes, mtime_ns = fingerprint_file(file_path)
        cache_path = paths.source_cache_root / f"{safe_file_stem(file_path.stem)}.csv"
        existing_row = existing_by_path.get(source_path)
        needs_refresh = (
            full_refresh
            or existing_row is None
            or existing_row["fingerprint"] != fingerprint
            or not cache_path.exists()
        )

        if needs_refresh:
            LOGGER.info("Refreshing %s %s source: %s", module, measure, source_name)
            raw_frame = load_trade_export_file(file_path, measure)
            frame = transformer(raw_frame, measure)
            frame["__source_name"] = source_name
            frame["__source_mtime_ns"] = mtime_ns
            frame = _finalize_dates(frame)
            _write_csv(frame, cache_path)
            years = {int(year) for year in frame["year"].dropna().astype(int).unique()}
            min_date = frame["date"].min()
            max_date = frame["date"].max()
            row_count = str(len(frame))
        else:
            years = parse_years(existing_row["years"])
            min_date = existing_row["min_date"]
            max_date = existing_row["max_date"]
            row_count = existing_row["row_count"]

        if full_refresh or needs_refresh:
            affected_years.update(years)
            if existing_row is not None:
                affected_years.update(parse_years(existing_row["years"]))

        current_records.append(
            {
                "module": module,
                "measure": measure,
                "source_name": source_name,
                "source_path": source_path,
                "fingerprint": fingerprint,
                "size_bytes": str(size_bytes),
                "mtime_ns": str(mtime_ns),
                "cache_path": str(cache_path),
                "years": format_years(years),
                "min_date": str(min_date),
                "max_date": str(max_date),
                "row_count": str(row_count),
                "active": "true",
                "updated_at": utc_now_iso(),
            }
        )

    removed = existing.loc[~existing["source_path"].isin(current_source_paths)].copy()
    if not removed.empty:
        for _, row in removed.iterrows():
            affected_years.update(parse_years(row["years"]))
        removed["active"] = "false"
        removed["updated_at"] = utc_now_iso()

    if full_refresh:
        for record in current_records:
            affected_years.update(parse_years(record["years"]))

    other_stages = manifest.loc[
        ~((manifest["module"] == module) & (manifest["measure"] == measure))
    ].copy()
    updated_manifest = pd.concat(
        [
            other_stages,
            pd.DataFrame(current_records),
            removed,
        ],
        ignore_index=True,
    )
    save_manifest(updated_manifest, MANIFEST_PATH)

    active_records = pd.DataFrame(current_records)
    _rebuild_stage_partitions(
        active_records=active_records,
        partition_root=paths.partition_root,
        combined_output_path=paths.combined_output_path,
        affected_years=affected_years,
        dedupe_keys=dedupe_keys,
    )

    LOGGER.info("Wrote %s %s dataset: %s", module, measure, paths.combined_output_path)
    return BuildResult(paths.combined_output_path, affected_years)


def _rebuild_stage_partitions(
    active_records: pd.DataFrame,
    partition_root: Path,
    combined_output_path: Path,
    affected_years: set[int],
    dedupe_keys: list[str],
) -> None:
    if active_records.empty:
        return

    for year in sorted(affected_years):
        frames = []
        for _, record in active_records.iterrows():
            if year not in parse_years(record["years"]):
                continue
            cache_frame = pd.read_csv(record["cache_path"], low_memory=False)
            cache_frame = cache_frame.loc[cache_frame["year"] == year].copy()
            if not cache_frame.empty:
                frames.append(cache_frame)

        year_path = _year_partition_path(partition_root, year)
        if not frames:
            year_path.unlink(missing_ok=True)
            continue

        rebuilt = pd.concat(frames, ignore_index=True)
        rebuilt = _deduplicate_snapshot_rows(rebuilt, dedupe_keys)
        rebuilt = rebuilt.drop(columns=["__source_name", "__source_mtime_ns"], errors="ignore")
        rebuilt = rebuilt.sort_values(["country", "date", "HS Code", "partner"], kind="stable").reset_index(drop=True)
        _write_csv(rebuilt, year_path)

    yearly_frames = [
        pd.read_csv(path, low_memory=False)
        for path in sorted(partition_root.glob("year=*.csv"))
        if path.is_file()
    ]
    if yearly_frames:
        combined = pd.concat(yearly_frames, ignore_index=True)
        combined = combined.sort_values(["country", "date", "HS Code", "partner"], kind="stable").reset_index(drop=True)
        _write_csv(combined, combined_output_path)


def _normalize_trade_frame(frame: pd.DataFrame, measure: str) -> pd.DataFrame:
    frame = frame[["country", "date", "PRODUCT", "INDICATORS", "partner", measure]].copy()
    frame["country"] = frame["country"].apply(clean_label)
    frame["partner"] = frame["partner"].apply(clean_label)
    frame["date"] = pd.to_datetime(frame["date"], errors="coerce", format="%Y-%m")
    frame["year"] = frame["date"].dt.year
    frame["month"] = frame["date"].dt.month

    frame = frame.dropna(subset=["country", "partner", "date", measure])
    frame = frame.loc[~frame["country"].isin(INVALID_REPORTERS)]
    frame = frame.loc[~frame["partner"].isin(EXCLUDED_PARTNERS)]
    return frame.reset_index(drop=True)


def _transform_tire_frame(frame: pd.DataFrame, measure: str) -> pd.DataFrame:
    frame = _normalize_trade_frame(frame, measure)
    frame["HS Code"] = frame["PRODUCT"].map(TIRE_PRODUCT_TO_HS)
    frame["Category"] = frame["PRODUCT"].map(TIRE_PRODUCT_TO_CATEGORY)
    frame["General_Category"] = frame["Category"].map(TIRE_CATEGORY_TO_GENERAL)

    _assert_no_missing_mapping(frame, "PRODUCT", "HS Code", f"tire {measure}")
    _assert_no_missing_mapping(frame, "PRODUCT", "Category", f"tire {measure}")
    _assert_no_missing_mapping(frame, "Category", "General_Category", f"tire {measure}")

    return frame[
        [
            "country",
            "date",
            "PRODUCT",
            "INDICATORS",
            "partner",
            measure,
            "year",
            "month",
            "HS Code",
            "Category",
            "General_Category",
        ]
    ].copy()


def _transform_vehicle_frame(frame: pd.DataFrame, measure: str) -> pd.DataFrame:
    frame = _normalize_trade_frame(frame, measure)
    frame["HS Code"] = frame["PRODUCT"].map(VEHICLE_PRODUCT_TO_HS)
    _assert_no_missing_mapping(frame, "PRODUCT", "HS Code", f"vehicle {measure}")

    frame = frame.loc[~frame["HS Code"].isin(VEHICLE_EXCLUDED_HS)].copy()
    frame["General_Category"] = frame["HS Code"].astype(str).map(VEHICLE_HS_TO_GENERAL).fillna("Other")
    frame = frame.loc[frame["General_Category"] != "Other"].copy()
    return frame[
        [
            "country",
            "date",
            "PRODUCT",
            "INDICATORS",
            "partner",
            measure,
            "year",
            "month",
            "HS Code",
            "General_Category",
        ]
    ].copy()


def _merge_tire(value: pd.DataFrame, weight: pd.DataFrame) -> pd.DataFrame:
    keys = ["country", "date", "PRODUCT", "partner", "HS Code", "Category", "General_Category"]
    value_product = value.groupby(keys, dropna=False, as_index=False)["value"].sum()
    weight_product = weight.groupby(keys, dropna=False, as_index=False)["weight"].sum()

    merged = value_product.merge(weight_product, on=keys, how="inner")
    merged["weight"] = merged["weight"] * 100
    merged["Type"] = "Tyre"
    merged = merged.loc[merged["General_Category"].isin(FINAL_GENERAL_CATEGORIES)].copy()
    return merged[
        ["date", "HS Code", "PRODUCT", "Category", "General_Category", "country", "partner", "value", "weight", "Type"]
    ]


def _merge_vehicle(value: pd.DataFrame, weight: pd.DataFrame) -> pd.DataFrame:
    keys = ["country", "date", "PRODUCT", "partner", "HS Code", "General_Category"]
    value_product = value.groupby(keys, dropna=False, as_index=False)["value"].sum()
    weight_product = weight.groupby(keys, dropna=False, as_index=False)["weight"].sum()

    merged = value_product.merge(weight_product, on=keys, how="inner")
    merged["weight"] = merged["weight"] * 100
    merged["Type"] = "Vehicle"
    merged["Category"] = pd.NA
    merged = merged.loc[merged["General_Category"].isin(FINAL_GENERAL_CATEGORIES)].copy()
    return merged[
        ["date", "HS Code", "PRODUCT", "Category", "General_Category", "country", "partner", "value", "weight", "Type"]
    ]


def _read_interim_year(paths: StagePaths, year: int, fallback: Path) -> pd.DataFrame:
    partition_path = _year_partition_path(paths.partition_root, year)
    if partition_path.exists():
        return pd.read_csv(partition_path, low_memory=False)

    combined_path = paths.combined_output_path
    source_path = combined_path if combined_path.exists() else fallback
    frame = pd.read_csv(source_path, low_memory=False)
    return frame.loc[pd.to_datetime(frame["date"], errors="coerce").dt.year == year].copy()


def _write_csv(frame: pd.DataFrame, output_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)


def _finalize_dates(frame: pd.DataFrame) -> pd.DataFrame:
    result = frame.copy()
    if not pd.api.types.is_datetime64_any_dtype(result["date"]):
        result["date"] = pd.to_datetime(result["date"], errors="coerce")
    result["date"] = result["date"].dt.strftime("%Y-%m-%d")
    return result


def _assert_no_missing_mapping(
    frame: pd.DataFrame,
    source_column: str,
    target_column: str,
    context: str,
) -> None:
    missing_values = (
        frame.loc[frame[target_column].isna(), source_column]
        .dropna()
        .astype(str)
        .sort_values()
        .unique()
        .tolist()
    )
    if missing_values:
        joined = ", ".join(missing_values)
        raise ValueError(f"Unmapped values in {context}: {source_column} -> {target_column}: {joined}")


def _normalize_measure(measure: str) -> str:
    normalized = measure.strip().lower()
    if normalized not in {"value", "weight", "all"}:
        raise ValueError(f"Unsupported measure: {measure}")
    return normalized


def _deduplicate_snapshot_rows(frame: pd.DataFrame, dedupe_keys: list[str]) -> pd.DataFrame:
    deduped = frame.sort_values(
        ["__source_mtime_ns", "__source_name"],
        kind="stable",
    ).drop_duplicates(subset=dedupe_keys, keep="last")
    return deduped.reset_index(drop=True)


def _year_partition_path(root: Path, year: int) -> Path:
    return root / f"year={year}.csv"


def _collect_interim_years() -> set[int]:
    years = set()
    for root in [
        TIRE_VALUE_PATHS.partition_root,
        TIRE_WEIGHT_PATHS.partition_root,
        VEHICLE_VALUE_PATHS.partition_root,
        VEHICLE_WEIGHT_PATHS.partition_root,
    ]:
        for path in root.glob("year=*.csv"):
            year = path.stem.split("=")[-1]
            if year.isdigit():
                years.add(int(year))
    if years:
        return years

    for candidate in [
        TIRE_VALUE_PATHS.combined_output_path,
        TIRE_WEIGHT_PATHS.combined_output_path,
        VEHICLE_VALUE_PATHS.combined_output_path,
        VEHICLE_WEIGHT_PATHS.combined_output_path,
        REPO_ROOT / "Tire/raw_data/tire_product_Value_PCR_TBR_SP.csv",
        REPO_ROOT / "Tire/raw_data/tire_product_Weight_PCR_TBR_SP.csv",
        REPO_ROOT / "Vehicle/Data/vehicle_product_value.csv",
        REPO_ROOT / "Vehicle/Data/vehicle_product_weight.csv",
    ]:
        if not candidate.exists():
            continue
        frame = pd.read_csv(candidate, usecols=["date"])
        parsed = pd.to_datetime(frame["date"], errors="coerce")
        years.update(int(year) for year in parsed.dt.year.dropna().astype(int).unique())
    return years


def _rebuild_processed_output(_affected_years: set[int]) -> Path:
    yearly_frames = [
        pd.read_csv(path, low_memory=False)
        for path in sorted(MERGED_PARTITION_ROOT.glob("year=*.csv"))
        if path.is_file()
    ]
    if yearly_frames:
        combined = pd.concat(yearly_frames, ignore_index=True)
        combined = combined.sort_values(["Type", "country", "date", "HS Code", "partner"], kind="stable").reset_index(drop=True)
        _write_csv(combined, MERGED_OUTPUT_PATH)
    return MERGED_OUTPUT_PATH
