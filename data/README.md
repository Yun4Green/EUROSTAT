# Data directories / 数据目录

## English

- `raw/`: preferred location for raw source files
- `interim/`: generated module-level outputs and partitions
- `processed/`: generated final outputs
- `metadata/`: manifest and incremental build state
- `external/`: non-Eurostat external inputs such as ACEA workbooks

The pipeline also supports the existing legacy raw folders under `Tire/` and `Vehicle/`.

## 中文

- `raw/`：推荐放置新的原始输入文件
- `interim/`：流水线生成的模块级中间结果和年份分区
- `processed/`：流水线生成的最终结果
- `metadata/`：manifest 和增量构建状态
- `external/`：非 Eurostat 外部输入，例如 ACEA 工作簿

当前流水线仍兼容 `Tire/` 和 `Vehicle/` 下的历史原始目录。
