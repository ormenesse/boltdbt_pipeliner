from __future__ import annotations

import importlib
import types
from pathlib import Path
from typing import Iterable

from bolt_pipeliner.config import load_config

_BUILTIN_BASE_MODULES: dict[str, str] = {
    "ETLBase": "bolt_pipeliner.bases.spark_iceberg",
    "ETLBaseDelta": "bolt_pipeliner.bases.spark_delta",
    "ETLBaseParquet": "bolt_pipeliner.bases.spark_parquet",
    "ETLBaseParquetPandas": "bolt_pipeliner.bases.pandas_parquet",
    "ETLBaseParquetPolars": "bolt_pipeliner.bases.polars_parquet",
}


def _resolve_base_class(class_name: str) -> type:
    if class_name in _BUILTIN_BASE_MODULES:
        module = importlib.import_module(_BUILTIN_BASE_MODULES[class_name])
        return getattr(module, class_name)
    if "." in class_name:
        module_path, _, attr = class_name.rpartition(".")
        module = importlib.import_module(module_path)
        return getattr(module, attr)
    raise KeyError(
        f"Unknown class_name '{class_name}'. Either use one of {list(_BUILTIN_BASE_MODULES)} "
        "or provide a dotted path like 'mypkg.bases.Custom'."
    )


def _module_import_path(layer_dir: str, module_name: str) -> str:
    """Convert a filesystem layer directory (e.g. 'etl/0_bronze') into a dotted
    import path (e.g. 'etl.0_bronze') and append the module name.
    """
    parts = [p for p in Path(layer_dir).parts if p not in ("", ".")]
    return ".".join(parts + [module_name])


def run(config_path: str | Path, layers: Iterable[str] | None = None, spark=None) -> None:
    """Run ETL jobs for the requested layers in the order declared in the config.

    Args:
        config_path: path to etl_config.yaml
        layers: subset of layer names to run; defaults to every layer in `layers:`
        spark: optional SparkSession; if None, the local session is created lazily.
    """
    config = load_config(config_path)
    configs_section = config.get("configs", {})
    flatfile_bucket = configs_section.get("flatfile_bucket", "")
    output_bucket = configs_section.get("output_bucket", "")
    save_catalog = configs_section.get("catalog", "dev_catalog")
    fixed_schema = configs_section.get("schema")
    incremental_column = configs_section.get("incremental_column")
    layer_paths: dict[str, str] = config.get("layers", {}) or {}

    requested = list(layers) if layers else list(layer_paths.keys())

    if spark is None:
        from bolt_pipeliner.sessions import create_session

        spark = create_session("local")

    for stage in requested:
        layer_dir = layer_paths.get(stage)
        if layer_dir is None:
            print(f"[bolt] skipping unknown layer '{stage}'")
            continue

        for job in config.get(stage, []) or []:
            module_path = _module_import_path(layer_dir, job["module"])
            module = importlib.import_module(module_path)

            base_cls = _resolve_base_class(job.get("class_name", "ETLBase"))
            bucket = flatfile_bucket if stage == "flatfile" else output_bucket

            etl = base_cls(
                spark=spark,
                layer=stage,
                bucket=bucket,
                input_tables=job["input_tables"],
                output_table_name=job["output_table_name"],
                partition_by=job.get("partition_by", []),
                unload=job.get("unload", True),
                incremental=job.get("incremental", False),
                catalog="shared_catalog",
                save_catalog=save_catalog,
                fixed_schema=fixed_schema,
                incremental_column=incremental_column,
            )
            etl.process_data = types.MethodType(module.process_data, etl)
            etl.run()


__all__ = ["run"]
