from __future__ import annotations

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from eurostat_pipeline.latest_periods import _extract_latest_time_from_jsonstat


class LatestPeriodTests(unittest.TestCase):
    def test_extract_latest_period_from_time_dimension(self) -> None:
        payload = {
            "dimension": {
                "time": {
                    "category": {
                        "label": {
                            "2024": "2024",
                            "2022": "2022",
                            "2023": "2023",
                        }
                    }
                }
            }
        }
        self.assertEqual(_extract_latest_time_from_jsonstat(payload), "2024")


if __name__ == "__main__":
    unittest.main()
