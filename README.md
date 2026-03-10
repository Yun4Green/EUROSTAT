# Eurostat Pipeline

Chinese version: [README.zh-CN.md](/Users/yunqiangluan/Library/Mobile%20Documents/com~apple~CloudDocs/交接/交接优化/Eurostat/README.zh-CN.md)

This repository is a reproducible Python data pipeline for the Eurostat tyre and vehicle workflow.

## Repository layout

```text
.
|-- Dashboard/
|-- Tire/                     # Legacy notebooks and legacy raw/output folders
|-- Vehicle/                  # Legacy notebooks and legacy raw/output folders
|-- Transport/                # Source-only assets, not yet automated
|-- data/
|   |-- raw/                  # Preferred location for raw inputs
|   |-- interim/              # Pipeline-generated module outputs
|   |-- processed/            # Pipeline-generated final outputs
|   |-- metadata/             # Build manifest and incremental state
|   `-- external/             # External sources such as ACEA.xlsx
|-- src/eurostat_pipeline/    # Production Python pipeline
|-- tests/                    # Lightweight regression tests
|-- pyproject.toml
`-- README.md
```

## Scope

The automated pipeline currently covers:

- `Tire/raw_data/Raw_data_value/*.xlsx` -> tyre value interim datasets
- `Tire/raw_data/Raw_data_weight/*.xlsx` -> tyre weight interim datasets
- `Vehicle/Raw_Data/Value/*.xlsx` -> vehicle value interim datasets
- `Vehicle/Raw_Data/Weight/*.xlsx` -> vehicle weight interim datasets
- tyre + vehicle interim datasets -> final `eurostat_tyre_vehicle.csv`

The preferred long-term raw layout is:

- `data/raw/tire/value`
- `data/raw/tire/weight`
- `data/raw/vehicle/value`
- `data/raw/vehicle/weight`

Legacy folders are still supported as input fallbacks.

## Install

```bash
python3 -m pip install -e .
```

## Commands

Build the full pipeline incrementally:

```bash
python3 -m eurostat_pipeline build-all
```

Force a full rebuild:

```bash
python3 -m eurostat_pipeline build-all --full-refresh
```

Run one stage only:

```bash
python3 -m eurostat_pipeline build-tire --measure value
python3 -m eurostat_pipeline build-vehicle --measure weight
python3 -m eurostat_pipeline build-merge
```

Download `ds-045409` directly from the Comext API instead of manually exporting Excel files:

```bash
python3 -m eurostat_pipeline download-comext
python3 -m eurostat_pipeline build-merge
```

Check the latest available period before downloading:

```bash
python3 -m eurostat_pipeline latest-periods
```

## Incremental update strategy

This repository no longer relies on a pure full-refresh workflow.

- Each raw workbook is normalized into its own cached CSV under `data/interim/<module>/<measure>/sources/`
- A manifest is maintained in `data/metadata/manifest.csv`
- When new or changed raw files arrive, only affected source files are reprocessed
- Only affected yearly partitions are rebuilt
- When overlapping snapshots exist, the newest source file replaces older records for the same business key
- Final combined outputs are rebuilt from yearly partitions

This design is safer than naive append and much faster than reading all historical Excel files every time.

For `ds-045409`, the repository also supports a direct Comext API workflow:

- Request slices are configured in `config/comext_request_config.json`
- Requests are sent as `product + indicator + year`
- If a yearly slice is too large (`413`), the downloader automatically falls back to monthly requests
- If a monthly slice is still too large, the downloader splits the request by reporter chunks
- Raw JSON responses are cached under `data/raw/comext_api/ds-045409/`
- A request manifest is written to `data/metadata/comext_requests_ds_045409.csv`
- Downloaded JSON responses are normalized into a long table and then materialized into the current tyre / vehicle interim CSV outputs

## Outputs

Interim outputs:

- `data/interim/tire/tire_product_value.csv`
- `data/interim/tire/tire_product_weight.csv`
- `data/interim/vehicle/vehicle_product_value.csv`
- `data/interim/vehicle/vehicle_product_weight.csv`
- `data/interim/<module>/<measure>/sources/*.csv`
- `data/interim/<module>/<measure>/years/year=YYYY.csv`

Processed outputs:

- `data/processed/eurostat_tyre_vehicle.csv`
- `data/processed/eurostat_tyre_vehicle/years/year=YYYY.csv`

Metadata:

- `data/metadata/manifest.csv`
- `data/metadata/comext_requests_ds_045409.csv`
- `data/metadata/latest_periods.csv`

## Notes

- Legacy notebooks are kept as reference, but the supported production workflow is the Python package under `src/`.
- `Transport/` raw files and the Tableau / ACEA refresh logic are not yet automated.
- The final merge applies the `QUANTITY_IN_100KG -> KG` conversion exactly once.
