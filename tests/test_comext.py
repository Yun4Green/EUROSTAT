from __future__ import annotations

import sys
import unittest
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eurostat_pipeline.comext import _build_tire_output, jsonstat_to_frame


class ComextJsonStatTests(unittest.TestCase):
    def test_jsonstat_to_frame_decodes_sparse_values(self) -> None:
        payload = {
            "label": "Example",
            "updated": "2026-02-13T11:00:00+0100",
            "id": ["freq", "reporter", "partner", "product", "flow", "indicators", "time"],
            "size": [1, 1, 1, 1, 1, 2, 1],
            "dimension": {
                "freq": {"category": {"index": {"M": 0}, "label": {"M": "Monthly"}}},
                "reporter": {"category": {"index": {"DE": 0}, "label": {"DE": "Germany"}}},
                "partner": {"category": {"index": {"CN": 0}, "label": {"CN": "China"}}},
                "product": {"category": {"index": {"401110": 0}, "label": {"401110": "Tyre Product"}}},
                "flow": {"category": {"index": {"1": 0}, "label": {"1": "IMPORT"}}},
                "indicators": {
                    "category": {
                        "index": {"VALUE_IN_EUROS": 0, "QUANTITY_IN_100KG": 1},
                        "label": {"VALUE_IN_EUROS": "VALUE_IN_EUROS", "QUANTITY_IN_100KG": "QUANTITY_IN_100KG"},
                    }
                },
                "time": {"category": {"index": {"2025-12": 0}, "label": {"2025-12": "2025-12"}}},
            },
            "value": {"0": 10.0, "1": 2.0},
            "extension": {"datasetId": "ds-045409"},
        }
        frame = jsonstat_to_frame(payload, Path("sample.json"))
        self.assertEqual(len(frame), 2)
        self.assertEqual(sorted(frame["indicator_code"].tolist()), ["QUANTITY_IN_100KG", "VALUE_IN_EUROS"])

    def test_build_tire_output_shapes_expected_columns(self) -> None:
        frame = pd.DataFrame(
            [
                {
                    "indicator_code": "VALUE_IN_EUROS",
                    "product_code": "401110",
                    "reporter_label": "Germany (incl. German Democratic Republic 'DD' from 1991)",
                    "partner_label": "China",
                    "time_code": "2025-12",
                    "product_label": "New pneumatic tyres, of rubber, of a kind used for motor cars, incl. station wagons and racing cars",
                    "value": 35090748.0,
                }
            ]
        )
        output = _build_tire_output(frame, "value")
        self.assertEqual(output.iloc[0]["country"], "Germany")
        self.assertEqual(output.iloc[0]["HS Code"], "401110")
        self.assertEqual(output.iloc[0]["General_Category"], "PCR")


if __name__ == "__main__":
    unittest.main()
