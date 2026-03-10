# Eurostat Pipeline

English version: [README.md](/Users/yunqiangluan/Library/Mobile%20Documents/com~apple~CloudDocs/交接/交接优化/Eurostat/README.md)

这个仓库已经整理成一套可复现的 Python 数据流水线，用于处理 Eurostat 的轮胎和车辆贸易数据。

## 仓库结构

```text
.
|-- Dashboard/
|-- Tire/                     # 历史 notebook 和原始/产出目录
|-- Vehicle/                  # 历史 notebook 和原始/产出目录
|-- Transport/                # 仅保留原始资产，尚未自动化
|-- data/
|   |-- raw/                  # 推荐放置新的原始输入
|   |-- interim/              # 流水线生成的模块级中间结果
|   |-- processed/            # 流水线生成的最终结果
|   |-- metadata/             # manifest 和增量构建状态
|   `-- external/             # 外部数据，如 ACEA.xlsx
|-- src/eurostat_pipeline/    # 正式 Python 流水线代码
|-- tests/                    # 轻量测试
|-- pyproject.toml
`-- README.md
```

## 当前覆盖范围

当前自动化流程已经覆盖：

- `Tire/raw_data/Raw_data_value/*.xlsx` -> 轮胎 value 中间表
- `Tire/raw_data/Raw_data_weight/*.xlsx` -> 轮胎 weight 中间表
- `Vehicle/Raw_Data/Value/*.xlsx` -> 车辆 value 中间表
- `Vehicle/Raw_Data/Weight/*.xlsx` -> 车辆 weight 中间表
- 轮胎 + 车辆中间表 -> 最终总表 `eurostat_tyre_vehicle.csv`

长期建议的原始数据目录是：

- `data/raw/tire/value`
- `data/raw/tire/weight`
- `data/raw/vehicle/value`
- `data/raw/vehicle/weight`

当前仍兼容旧目录，方便平滑迁移。

## 安装

```bash
python3 -m pip install -e .
```

## 运行命令

按增量方式构建全流程：

```bash
python3 -m eurostat_pipeline build-all
```

强制全量重建：

```bash
python3 -m eurostat_pipeline build-all --full-refresh
```

只跑某个阶段：

```bash
python3 -m eurostat_pipeline build-tire --measure value
python3 -m eurostat_pipeline build-vehicle --measure weight
python3 -m eurostat_pipeline build-merge
```

如果不想再手工导出 `ds-045409` 的 Excel，可以直接走 Comext API：

```bash
python3 -m eurostat_pipeline download-comext
python3 -m eurostat_pipeline build-merge
```

如果想在更新前先检查这些数据当前有没有新时间点，可以运行：

```bash
python3 -m eurostat_pipeline latest-periods
```

## 增量更新策略

现在的更新方式不是简单重新拼接，也不是每次都全量重跑，而是：

- 每个原始 Excel 先单独标准化，缓存到 `data/interim/<module>/<measure>/sources/`
- 在 `data/metadata/manifest.csv` 中记录每个源文件的路径、指纹、修改时间、覆盖年份和缓存位置
- 当有新文件或文件内容变化时，只重跑受影响的源文件
- 只重建受影响年份的分区文件
- 如果同一业务键存在多个快照版本，则用最新源文件覆盖旧记录
- 最终总表由年份分区重新汇总生成

这种方式比直接 append 更安全，也比每次全量读取全部历史 Excel 更高效。

对于 `ds-045409`，仓库现在也支持直接走 Comext API：

- 请求切片配置放在 `config/comext_request_config.json`
- 默认按 `product + indicator + year` 发起请求
- 如果某个年度切片过大并返回 `413`，程序会自动降级成按月请求
- 如果按月仍然过大，程序会继续按 reporter 分块请求
- 原始 JSON 会缓存到 `data/raw/comext_api/ds-045409/`
- 请求 manifest 会写到 `data/metadata/comext_requests_ds_045409.csv`
- 下载后的 JSON 会先标准化成长表，再物化成当前轮胎 / 车辆流程可直接使用的 interim CSV

## 输出结果

中间结果：

- `data/interim/tire/tire_product_value.csv`
- `data/interim/tire/tire_product_weight.csv`
- `data/interim/vehicle/vehicle_product_value.csv`
- `data/interim/vehicle/vehicle_product_weight.csv`
- `data/interim/<module>/<measure>/sources/*.csv`
- `data/interim/<module>/<measure>/years/year=YYYY.csv`

最终结果：

- `data/processed/eurostat_tyre_vehicle.csv`
- `data/processed/eurostat_tyre_vehicle/years/year=YYYY.csv`

元数据：

- `data/metadata/manifest.csv`
- `data/metadata/comext_requests_ds_045409.csv`
- `data/metadata/latest_periods.csv`

## 说明

- 历史 notebook 仍然保留，主要用于追溯逻辑；正式生产流程以 `src/` 下的 Python 包为准。
- `Transport/` 原始文件，以及 Tableau / ACEA 的刷新逻辑，当前还没有并入自动化流程。
- 最终 merge 阶段只会执行一次 `QUANTITY_IN_100KG -> KG` 的单位换算。
