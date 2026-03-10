from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eurostat_pipeline.excel_exports import clean_label
from eurostat_pipeline.pipeline import _deduplicate_snapshot_rows, _merge_tire, _merge_vehicle


class CleanLabelTests(unittest.TestCase):
    def test_clean_label_removes_legacy_suffix(self) -> None:
        self.assertEqual(clean_label("Belgium (incl. Luxembourg 'LU' -> 1998)"), "Belgium")


class MergeTests(unittest.TestCase):
    def test_tire_merge_scales_weight_once(self) -> None:
        value = pd.DataFrame(
            [
                {
                    "country": "Spain",
                    "date": "2024-01-01",
                    "PRODUCT": "Tyre A",
                    "partner": "China",
                    "HS Code": "401110",
                    "Category": "PCR",
                    "General_Category": "PCR",
                    "value": 10.0,
                }
            ]
        )
        weight = pd.DataFrame(
            [
                {
                    "country": "Spain",
                    "date": "2024-01-01",
                    "PRODUCT": "Tyre A",
                    "partner": "China",
                    "HS Code": "401110",
                    "Category": "PCR",
                    "General_Category": "PCR",
                    "weight": 2.0,
                }
            ]
        )
        merged = _merge_tire(value, weight)
        self.assertEqual(float(merged.iloc[0]["weight"]), 200.0)

    def test_vehicle_merge_adds_empty_category(self) -> None:
        value = pd.DataFrame(
            [
                {
                    "country": "Germany",
                    "date": "2024-01-01",
                    "PRODUCT": "Vehicle A",
                    "partner": "China",
                    "HS Code": "8703",
                    "General_Category": "PCR",
                    "value": 20.0,
                }
            ]
        )
        weight = pd.DataFrame(
            [
                {
                    "country": "Germany",
                    "date": "2024-01-01",
                    "PRODUCT": "Vehicle A",
                    "partner": "China",
                    "HS Code": "8703",
                    "General_Category": "PCR",
                    "weight": 3.0,
                }
            ]
        )
        merged = _merge_vehicle(value, weight)
        self.assertTrue(pd.isna(merged.iloc[0]["Category"]))
        self.assertEqual(float(merged.iloc[0]["weight"]), 300.0)

    def test_incremental_dedup_keeps_latest_snapshot(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "country": "Spain",
                    "date": "2024-01-01",
                    "PRODUCT": "Tyre A",
                    "INDICATORS": "VALUE_IN_EUROS",
                    "partner": "China",
                    "value": 10.0,
                    "HS Code": "401110",
                    "Category": "PCR",
                    "General_Category": "PCR",
                    "__source_name": "old.xlsx",
                    "__source_mtime_ns": 1,
                },
                {
                    "country": "Spain",
                    "date": "2024-01-01",
                    "PRODUCT": "Tyre A",
                    "INDICATORS": "VALUE_IN_EUROS",
                    "partner": "China",
                    "value": 20.0,
                    "HS Code": "401110",
                    "Category": "PCR",
                    "General_Category": "PCR",
                    "__source_name": "new.xlsx",
                    "__source_mtime_ns": 2,
                },
            ]
        )
        deduped = _deduplicate_snapshot_rows(
            frame,
            ["country", "date", "PRODUCT", "INDICATORS", "partner", "HS Code", "Category", "General_Category"],
        )
        self.assertEqual(float(deduped.iloc[0]["value"]), 20.0)


if __name__ == "__main__":
    unittest.main()
