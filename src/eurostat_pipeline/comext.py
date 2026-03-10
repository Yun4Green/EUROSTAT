"""Comext API downloader and materializer for ds-045409."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.error import HTTPError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

from .excel_exports import clean_label
from .mappings import (
    TIRE_CATEGORY_TO_GENERAL,
    TIRE_PRODUCT_TO_CATEGORY,
    TIRE_PRODUCT_TO_HS,
    VEHICLE_HS_TO_GENERAL,
    VEHICLE_PRODUCT_TO_HS,
)
from .pipeline import (
    TIRE_VALUE_PATHS,
    TIRE_WEIGHT_PATHS,
    VEHICLE_VALUE_PATHS,
    VEHICLE_WEIGHT_PATHS,
)
from .state import utc_now_iso

LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "config/comext_request_config.json"
COMEXT_ROOT = REPO_ROOT / "data/raw/comext_api"
REQUEST_MANIFEST_PATH = REPO_ROOT / "data/metadata/comext_requests_ds_045409.csv"
NORMALIZED_OUTPUT_PATH = COMEXT_ROOT / "ds-045409/normalized/ds_045409_long.csv"

REQUEST_MANIFEST_COLUMNS = [
    "dataset_code",
    "indicator_alias",
    "indicator_code",
    "product_group",
    "product_code",
    "year",
    "month",
    "reporter_chunk",
    "granularity",
    "request_url",
    "cache_path",
    "status_code",
    "success",
    "response_updated",
    "cell_count",
    "row_count",
    "requested_at",
]

TIRE_HS_TO_CATEGORY = {
    hs_code: TIRE_PRODUCT_TO_CATEGORY[product_label]
    for product_label, hs_code in TIRE_PRODUCT_TO_HS.items()
}
TIRE_HS_TO_GENERAL = {
    hs_code: TIRE_CATEGORY_TO_GENERAL[category]
    for hs_code, category in TIRE_HS_TO_CATEGORY.items()
}
DIMENSION_ALIASES = {
    "indicators": "indicator",
}


def download_comext(config_path: Path | None = None, force: bool = False) -> dict[str, Path]:
    config_file = config_path or DEFAULT_CONFIG_PATH
    config = json.loads(config_file.read_text())
    dataset_code = config["dataset_code"]

    request_rows: list[dict[str, str]] = []
    normalized_frames: list[pd.DataFrame] = []
    request_plan = _build_request_plan(config)

    for plan in request_plan:
        result_rows, request_frames = _execute_request_plan(config, plan, force=force)
        request_rows.extend(result_rows)
        normalized_frames.extend(request_frames)

    request_manifest = pd.DataFrame(request_rows, columns=REQUEST_MANIFEST_COLUMNS)
    _write_request_manifest(_normalize_request_manifest(request_manifest), REQUEST_MANIFEST_PATH)

    if normalized_frames:
        normalized = pd.concat(normalized_frames, ignore_index=True)
        normalized = normalized.sort_values(
            ["indicator_code", "product_code", "time", "reporter_code", "partner_code"],
            kind="stable",
        ).reset_index(drop=True)
    else:
        normalized = pd.DataFrame()

    NORMALIZED_OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    normalized.to_csv(NORMALIZED_OUTPUT_PATH, index=False)

    materialized = materialize_comext_dataset(normalized, dataset_code)
    materialized["normalized"] = NORMALIZED_OUTPUT_PATH
    return materialized


def materialize_comext_dataset(frame: pd.DataFrame, dataset_code: str) -> dict[str, Path]:
    if frame.empty:
        raise RuntimeError("No successful Comext request data available to materialize")

    outputs = {
        "tire_value": _write_csv(_build_tire_output(frame, "value"), TIRE_VALUE_PATHS.combined_output_path),
        "tire_weight": _write_csv(_build_tire_output(frame, "weight"), TIRE_WEIGHT_PATHS.combined_output_path),
        "vehicle_value": _write_csv(_build_vehicle_output(frame, "value"), VEHICLE_VALUE_PATHS.combined_output_path),
        "vehicle_weight": _write_csv(_build_vehicle_output(frame, "weight"), VEHICLE_WEIGHT_PATHS.combined_output_path),
    }
    LOGGER.info("Materialized Comext dataset %s into interim CSV outputs", dataset_code)
    return outputs


def jsonstat_to_frame(data: dict[str, Any], cache_path: Path) -> pd.DataFrame:
    ids = data["id"]
    sizes = data["size"]
    multipliers = _compute_multipliers(sizes)
    value_map = data.get("value", {})
    status_map = data.get("status", {})
    dimensions = data["dimension"]

    category_maps: dict[str, dict[int, tuple[str, str]]] = {}
    for dim in ids:
        category = dimensions[dim]["category"]
        index_map = category["index"]
        label_map = category["label"]
        position_map = {}
        for code, position in index_map.items():
            position_map[int(position)] = (code, label_map.get(code, code))
        category_maps[dim] = position_map

    rows = []
    for raw_index, value in value_map.items():
        flat_index = int(raw_index)
        coordinates = _decode_index(flat_index, sizes, multipliers)
        row: dict[str, Any] = {
            "dataset_code": data.get("extension", {}).get("datasetId", ""),
            "dataset_label": data.get("label", ""),
            "source_updated": data.get("updated", ""),
            "cache_path": str(cache_path),
            "value": value,
            "status": status_map.get(raw_index, ""),
        }
        for dim_name, coordinate in zip(ids, coordinates):
            code, label = category_maps[dim_name][coordinate]
            column_prefix = DIMENSION_ALIASES.get(dim_name, dim_name)
            row[f"{column_prefix}_code"] = code
            row[f"{column_prefix}_label"] = label
        rows.append(row)

    return pd.DataFrame(rows)


def _build_request_plan(config: dict[str, Any]) -> list[dict[str, Any]]:
    plan: list[dict[str, Any]] = []
    for indicator_alias, indicator_code in config["indicators"].items():
        for product_group, products in config["products"].items():
            for product_code in products:
                for year in config["years"]:
                    plan.append(
                        {
                            "indicator_alias": indicator_alias,
                            "indicator_code": indicator_code,
                            "product_group": product_group,
                            "product_code": product_code,
                            "year": int(year),
                        }
                    )
    return plan


def _execute_request_plan(
    config: dict[str, Any],
    plan: dict[str, Any],
    force: bool,
) -> tuple[list[dict[str, str]], list[pd.DataFrame]]:
    try:
        row, frame = _fetch_year_slice(config, plan, force=force)
        return [row], [frame]
    except HTTPError as exc:
        if exc.code != 413:
            raise
        LOGGER.info(
            "Year slice too large for %s %s %s; falling back to monthly requests",
            plan["indicator_alias"],
            plan["product_code"],
            plan["year"],
        )

    rows: list[dict[str, str]] = []
    frames: list[pd.DataFrame] = []
    for month in config["months"]:
        month_plan = {**plan, "month": int(month)}
        try:
            row, frame = _fetch_month_slice(config, month_plan, force=force)
            rows.append(row)
            frames.append(frame)
        except HTTPError as exc:
            if exc.code != 413:
                raise
            LOGGER.info(
                "Monthly slice still too large for %s %s %s-%02d; splitting reporter chunks",
                plan["indicator_alias"],
                plan["product_code"],
                plan["year"],
                month,
            )
            chunk_rows, chunk_frames = _fetch_month_reporter_chunks(config, month_plan, force=force)
            rows.extend(chunk_rows)
            frames.extend(chunk_frames)
    return rows, frames


def _fetch_year_slice(
    config: dict[str, Any],
    plan: dict[str, Any],
    force: bool,
) -> tuple[dict[str, str], pd.DataFrame]:
    months = [f"{plan['year']}-{int(month):02d}" for month in config["months"]]
    relative = Path(config["dataset_code"]) / plan["indicator_alias"] / plan["product_code"] / f"{plan['year']}.json"
    return _fetch_slice(
        config=config,
        plan=plan,
        times=months,
        cache_relative_path=relative,
        granularity="year",
        reporter_chunk="all",
        force=force,
    )


def _fetch_month_slice(
    config: dict[str, Any],
    plan: dict[str, Any],
    force: bool,
) -> tuple[dict[str, str], pd.DataFrame]:
    month = f"{plan['year']}-{int(plan['month']):02d}"
    relative = (
        Path(config["dataset_code"])
        / plan["indicator_alias"]
        / plan["product_code"]
        / f"{month}.json"
    )
    return _fetch_slice(
        config=config,
        plan=plan,
        times=[month],
        cache_relative_path=relative,
        granularity="month",
        reporter_chunk="all",
        force=force,
    )


def _fetch_month_reporter_chunks(
    config: dict[str, Any],
    plan: dict[str, Any],
    force: bool,
) -> tuple[list[dict[str, str]], list[pd.DataFrame]]:
    rows: list[dict[str, str]] = []
    frames: list[pd.DataFrame] = []
    month = f"{plan['year']}-{int(plan['month']):02d}"
    reporters = list(config["reporters"])
    chunk_size = int(config.get("reporter_chunk_size", len(reporters)))
    if chunk_size <= 0:
        chunk_size = len(reporters)

    for index in range(0, len(reporters), chunk_size):
        reporter_chunk = reporters[index : index + chunk_size]
        chunk_id = f"{index // chunk_size + 1:02d}"
        relative = (
            Path(config["dataset_code"])
            / plan["indicator_alias"]
            / plan["product_code"]
            / f"{month}_reporters_{chunk_id}.json"
        )
        row, frame = _fetch_slice(
            config=config,
            plan=plan,
            times=[month],
            cache_relative_path=relative,
            granularity="month_reporter_chunk",
            reporter_chunk=chunk_id,
            reporters=reporter_chunk,
            force=force,
        )
        rows.append(row)
        frames.append(frame)
    return rows, frames


def _fetch_slice(
    config: dict[str, Any],
    plan: dict[str, Any],
    times: list[str],
    cache_relative_path: Path,
    granularity: str,
    reporter_chunk: str,
    force: bool,
    reporters: list[str] | None = None,
) -> tuple[dict[str, str], pd.DataFrame]:
    cache_path = COMEXT_ROOT / cache_relative_path
    cache_path.parent.mkdir(parents=True, exist_ok=True)

    if cache_path.exists() and not force:
        data = json.loads(cache_path.read_text())
    else:
        url = _build_request_url(
            config=config,
            indicator_code=plan["indicator_code"],
            product_code=plan["product_code"],
            reporters=reporters or list(config["reporters"]),
            times=times,
        )
        data = _request_json(url)
        cache_path.write_text(json.dumps(data, ensure_ascii=False))

    frame = jsonstat_to_frame(data, cache_path)
    request_url = _build_request_url(
        config=config,
        indicator_code=plan["indicator_code"],
        product_code=plan["product_code"],
        reporters=reporters or list(config["reporters"]),
        times=times,
    )
    row = {
        "dataset_code": config["dataset_code"],
        "indicator_alias": plan["indicator_alias"],
        "indicator_code": plan["indicator_code"],
        "product_group": plan["product_group"],
        "product_code": plan["product_code"],
        "year": str(plan["year"]),
        "month": str(plan.get("month", "")),
        "reporter_chunk": reporter_chunk,
        "granularity": granularity,
        "request_url": request_url,
        "cache_path": str(cache_path),
        "status_code": "200",
        "success": "true",
        "response_updated": str(data.get("updated", "")),
        "cell_count": str(len(data.get("value", {}))),
        "row_count": str(len(frame)),
        "requested_at": utc_now_iso(),
    }
    return row, frame


def _build_request_url(
    config: dict[str, Any],
    indicator_code: str,
    product_code: str,
    reporters: list[str],
    times: list[str],
) -> str:
    params = [
        ("format", config.get("format", "JSON")),
        ("freq", config.get("frequency", "M")),
    ]
    for flow_code in config.get("flow", []):
        params.append(("flow", flow_code))
    for reporter in reporters:
        params.append(("reporter", reporter))
    params.append(("product", product_code))
    params.append(("indicators", indicator_code))
    for time_value in times:
        params.append(("time", time_value))
    query = urlencode(params, doseq=True)
    return f"{config['base_url'].rstrip('/')}/{config['dataset_code']}?{query}"


def _request_json(url: str) -> dict[str, Any]:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=60) as response:
        return json.load(response)


def _compute_multipliers(sizes: list[int]) -> list[int]:
    multipliers = [1] * len(sizes)
    running = 1
    for index in range(len(sizes) - 1, -1, -1):
        multipliers[index] = running
        running *= sizes[index]
    return multipliers


def _decode_index(flat_index: int, sizes: list[int], multipliers: list[int]) -> list[int]:
    coordinates = []
    remainder = flat_index
    for size, multiplier in zip(sizes, multipliers):
        coordinate = (remainder // multiplier) % size
        coordinates.append(coordinate)
        remainder = remainder % multiplier
    return coordinates


def _normalize_request_manifest(frame: pd.DataFrame) -> pd.DataFrame:
    if frame.empty:
        return pd.DataFrame(columns=REQUEST_MANIFEST_COLUMNS)
    output = frame.copy()
    for column in REQUEST_MANIFEST_COLUMNS:
        if column not in output.columns:
            output[column] = ""
    return output[REQUEST_MANIFEST_COLUMNS]


def _write_request_manifest(frame: pd.DataFrame, path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.sort_values(
        ["indicator_alias", "product_group", "product_code", "year", "month", "reporter_chunk"],
        kind="stable",
    ).to_csv(path, index=False)


def _build_tire_output(frame: pd.DataFrame, indicator_alias: str) -> pd.DataFrame:
    indicator_code = "VALUE_IN_EUROS" if indicator_alias == "value" else "QUANTITY_IN_100KG"
    output_column = "value" if indicator_alias == "value" else "weight"
    tire_codes = set(TIRE_HS_TO_CATEGORY.keys())

    subset = frame.loc[
        (frame["indicator_code"] == indicator_code) & (frame["product_code"].isin(tire_codes))
    ].copy()
    subset["country"] = subset["reporter_label"].apply(clean_label)
    subset["partner"] = subset["partner_label"].apply(clean_label)
    subset["date"] = pd.to_datetime(subset["time_code"], errors="coerce", format="%Y-%m")
    subset = subset.dropna(subset=["date", "country", "partner", "value"])
    subset["PRODUCT"] = subset["product_label"]
    subset["INDICATORS"] = subset["indicator_code"]
    subset["metric_value"] = pd.to_numeric(subset["value"], errors="coerce")
    subset[output_column] = subset["metric_value"]
    subset["year"] = subset["date"].dt.year
    subset["month"] = subset["date"].dt.month
    subset["HS Code"] = subset["product_code"]
    subset["Category"] = subset["HS Code"].map(TIRE_HS_TO_CATEGORY)
    subset["General_Category"] = subset["HS Code"].map(TIRE_HS_TO_GENERAL)
    subset["date"] = subset["date"].dt.strftime("%Y-%m-%d")
    columns_to_drop = ["metric_value"] if output_column == "value" else ["value", "metric_value"]
    subset = subset.drop(columns=columns_to_drop)
    return subset[
        [
            "country",
            "date",
            "PRODUCT",
            "INDICATORS",
            "partner",
            output_column,
            "year",
            "month",
            "HS Code",
            "Category",
            "General_Category",
        ]
    ].sort_values(["country", "date", "HS Code", "partner"], kind="stable").reset_index(drop=True)


def _build_vehicle_output(frame: pd.DataFrame, indicator_alias: str) -> pd.DataFrame:
    indicator_code = "VALUE_IN_EUROS" if indicator_alias == "value" else "QUANTITY_IN_100KG"
    output_column = "value" if indicator_alias == "value" else "weight"
    vehicle_codes = set(VEHICLE_HS_TO_GENERAL.keys()) - {"871610", "871680"}

    subset = frame.loc[
        (frame["indicator_code"] == indicator_code) & (frame["product_code"].isin(vehicle_codes))
    ].copy()
    subset["country"] = subset["reporter_label"].apply(clean_label)
    subset["partner"] = subset["partner_label"].apply(clean_label)
    subset["date"] = pd.to_datetime(subset["time_code"], errors="coerce", format="%Y-%m")
    subset = subset.dropna(subset=["date", "country", "partner", "value"])
    subset["PRODUCT"] = subset["product_label"]
    subset["INDICATORS"] = subset["indicator_code"]
    subset["metric_value"] = pd.to_numeric(subset["value"], errors="coerce")
    subset[output_column] = subset["metric_value"]
    subset["year"] = subset["date"].dt.year
    subset["month"] = subset["date"].dt.month
    subset["HS Code"] = subset["product_code"]
    subset["General_Category"] = subset["HS Code"].map(VEHICLE_HS_TO_GENERAL)
    subset["date"] = subset["date"].dt.strftime("%Y-%m-%d")
    columns_to_drop = ["metric_value"] if output_column == "value" else ["value", "metric_value"]
    subset = subset.drop(columns=columns_to_drop)
    return subset[
        [
            "country",
            "date",
            "PRODUCT",
            "INDICATORS",
            "partner",
            output_column,
            "year",
            "month",
            "HS Code",
            "General_Category",
        ]
    ].sort_values(["country", "date", "HS Code", "partner"], kind="stable").reset_index(drop=True)


def _write_csv(frame: pd.DataFrame, path: Path) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path
