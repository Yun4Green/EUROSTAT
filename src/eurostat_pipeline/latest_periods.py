"""Utilities for checking the latest available period on Eurostat APIs."""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Any
from urllib.parse import urlencode
from urllib.request import Request, urlopen

import pandas as pd

LOGGER = logging.getLogger(__name__)
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data/metadata/latest_periods.csv"

TRANSPORT_DATASETS = [
    "road_eqr_lormot",
    "road_eqr_busmot",
    "road_eqr_carpda",
    "road_eqr_tracmot",
    "road_eqs_lormot",
    "road_eqs_busmot",
    "road_eqs_carpda",
    "road_eqs_roaene",
]


def fetch_latest_periods(output_path: Path | None = None) -> pd.DataFrame:
    rows = [_fetch_comext_latest_period(), *[_fetch_statistics_latest_period(code) for code in TRANSPORT_DATASETS]]
    frame = pd.DataFrame(rows)
    if output_path is None:
        output_path = DEFAULT_OUTPUT_PATH
    output_path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(output_path, index=False)
    return frame


def _fetch_comext_latest_period() -> dict[str, str]:
    url = _build_comext_probe_url()
    data = _request_json(url)
    latest_period = _extract_latest_time_from_jsonstat(data)
    return {
        "dataset_code": "ds-045409",
        "api_type": "comext",
        "latest_period": latest_period,
        "updated": str(data.get("updated", "")),
        "source_url": url,
    }


def _fetch_statistics_latest_period(dataset_code: str) -> dict[str, str]:
    url = f"https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset_code}?format=JSON"
    data = _request_json(url)
    latest_period = _extract_latest_time_from_jsonstat(data)
    return {
        "dataset_code": dataset_code,
        "api_type": "statistics",
        "latest_period": latest_period,
        "updated": str(data.get("updated", "")),
        "source_url": url,
    }


def _build_comext_probe_url() -> str:
    params = [
        ("format", "JSON"),
        ("freq", "M"),
        ("flow", "1"),
        ("reporter", "DE"),
        ("partner", "CN"),
        ("product", "401110"),
        ("indicators", "VALUE_IN_EUROS"),
    ]
    return "https://ec.europa.eu/eurostat/api/comext/dissemination/statistics/1.0/data/ds-045409?" + urlencode(params, doseq=True)


def _extract_latest_time_from_jsonstat(data: dict[str, Any]) -> str:
    dimension = data["dimension"]
    time_dimension = dimension.get("time") or dimension.get("time_period")
    if time_dimension is None:
        raise KeyError("time dimension not found in dataset response")

    labels = time_dimension["category"]["label"]
    periods = [str(value) for _, value in labels.items()]
    if not periods:
        raise ValueError("time dimension contains no labels")
    return sorted(periods)[-1]


def _request_json(url: str) -> dict[str, Any]:
    LOGGER.info("Fetching latest period from %s", url)
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(request, timeout=60) as response:
        return json.load(response)
