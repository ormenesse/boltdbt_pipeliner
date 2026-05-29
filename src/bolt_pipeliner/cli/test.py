"""`bolt test` — run the `tests:` block on each job and report pass/fail."""

from __future__ import annotations

import importlib
import types
from pathlib import Path
from typing import Optional

from bolt_pipeliner.config import load_config, resolve_data_locations
from bolt_pipeliner.runner import (
    _ensure_project_import_path,
    _module_import_path,
    _resolve_base_class,
)
from bolt_pipeliner.sessions.profiles import resolve_spark_profile
from bolt_pipeliner.testing.runner import run_checks


def execute(
    config_path: Path,
    layer: Optional[str] = None,
    module: Optional[str] = None,
) -> int:
    """Run data tests for matching jobs. Returns 0 on full pass, 1 otherwise."""
    config = load_config(config_path)
    configs_section = config.get("configs", {})
    save_catalog = configs_section.get("catalog", "dev_catalog")
    fixed_schema = configs_section.get("schema")
    incremental_column = configs_section.get("incremental_column")
    incremental_type = configs_section.get("incremental_type", "int")
    incremental_unit = configs_section.get("incremental_unit", 3)
    incremental_date_grain = configs_section.get("incremental_date_grain", "monthly")
    flatfile_location, output_location = resolve_data_locations(config)
    layer_paths: dict[str, str] = config.get("layers", {}) or {}
    _ensure_project_import_path(config_path, layer_paths)

    layers_to_run = [layer] if layer else list(layer_paths.keys())
    spark_profile = resolve_spark_profile(config_path, config)

    spark = None
    total_failed = 0
    total_passed = 0

    for stage in layers_to_run:
        layer_dir = layer_paths.get(stage)
        if layer_dir is None:
            print(f"[bolt test] unknown layer '{stage}', skipping")
            continue

        for job in config.get(stage, []) or []:
            if module and job["module"] != module:
                continue
            tests = job.get("tests") or []
            if not tests:
                continue

            module_path = _module_import_path(layer_dir, job["module"])
            user_module = importlib.import_module(module_path)
            base_cls = _resolve_base_class(job.get("class_name", "ETLBase"))

            # Lazily create the Spark session only when a Spark-backed base is needed.
            if base_cls.__module__.startswith("bolt_pipeliner.bases.spark") and spark is None:
                from bolt_pipeliner.sessions import create_session

                spark = create_session(
                    spark_profile.profile,
                    spark_config=spark_profile.spark_config,
                )

            init_kwargs = dict(
                layer=stage,
                bucket=flatfile_location if stage == "flatfile" else output_location,
                input_tables=job["input_tables"],
                output_table_name=job["output_table_name"],
                partition_by=job.get("partition_by", []),
                unload=False,                  # tests should not write
                incremental=job.get("incremental", False),
                catalog="shared_catalog",
                save_catalog=save_catalog,
                fixed_schema=fixed_schema,
                incremental_column=job.get("incremental_column", incremental_column),
                incremental_type=job.get("incremental_type", incremental_type),
                incremental_unit=job.get("incremental_unit", incremental_unit),
                incremental_date_grain=job.get(
                    "incremental_date_grain",
                    incremental_date_grain,
                ),
            )
            if base_cls.__module__.startswith("bolt_pipeliner.bases.spark"):
                init_kwargs["spark"] = spark

            etl = base_cls(**init_kwargs)
            etl.process_data = types.MethodType(user_module.process_data, etl)
            etl.check_if_tables_exists_find_yearmonths()
            etl.load_data(etl.input_table_names) if hasattr(etl, "load_data") else None
            df = etl.process_data(etl.input_tables)

            results = run_checks(df, tests)
            print(f"\n=== {stage}.{job['module']} ===")
            for r in results:
                print(f"  {r!r}")
            total_passed += sum(1 for r in results if r.passed)
            total_failed += sum(1 for r in results if not r.passed)

    print(f"\nSummary: {total_passed} passed, {total_failed} failed")
    return 0 if total_failed == 0 else 1
