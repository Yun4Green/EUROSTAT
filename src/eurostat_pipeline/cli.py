"""Command line entrypoint."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from .comext import DEFAULT_CONFIG_PATH, download_comext
from .latest_periods import DEFAULT_OUTPUT_PATH, fetch_latest_periods
from .pipeline import build_all, build_merge, build_tire, build_vehicle


def main() -> None:
    parser = argparse.ArgumentParser(description="Eurostat tyre and vehicle pipeline")
    subparsers = parser.add_subparsers(dest="command", required=True)

    tire_parser = subparsers.add_parser("build-tire", help="Build tyre interim datasets")
    tire_parser.add_argument("--measure", default="all", choices=["value", "weight", "all"])
    tire_parser.add_argument("--full-refresh", action="store_true")

    vehicle_parser = subparsers.add_parser("build-vehicle", help="Build vehicle interim datasets")
    vehicle_parser.add_argument("--measure", default="all", choices=["value", "weight", "all"])
    vehicle_parser.add_argument("--full-refresh", action="store_true")

    merge_parser = subparsers.add_parser("build-merge", help="Build the final merged dataset")
    merge_parser.add_argument("--full-refresh", action="store_true")
    all_parser = subparsers.add_parser("build-all", help="Run the full pipeline")
    all_parser.add_argument("--full-refresh", action="store_true")
    comext_parser = subparsers.add_parser("download-comext", help="Download ds-045409 slices from the Comext API")
    comext_parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG_PATH)
    comext_parser.add_argument("--force", action="store_true")
    latest_parser = subparsers.add_parser("latest-periods", help="Check the latest available period for configured Eurostat datasets")
    latest_parser.add_argument("--output", type=Path, default=DEFAULT_OUTPUT_PATH)

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    if args.command == "build-tire":
        build_tire(args.measure, full_refresh=args.full_refresh)
    elif args.command == "build-vehicle":
        build_vehicle(args.measure, full_refresh=args.full_refresh)
    elif args.command == "build-merge":
        build_merge(full_refresh=args.full_refresh)
    elif args.command == "build-all":
        build_all(full_refresh=args.full_refresh)
    elif args.command == "download-comext":
        download_comext(config_path=args.config, force=args.force)
    elif args.command == "latest-periods":
        frame = fetch_latest_periods(output_path=args.output)
        print(frame.to_string(index=False))
