from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from typing import Any, Iterable

from bolt_pipeliner.config import load_config, resolve_data_locations
from bolt_pipeliner.selection import select as select_jobs
from bolt_pipeliner.sessions.profiles import resolve_spark_profile

_BUILTIN_BASE_MODULES: dict[str, str] = {
    "ETLBase": "bolt_pipeliner.bases.spark_iceberg",
    "ETLBaseDelta": "bolt_pipeliner.bases.spark_delta",
    "ETLBaseParquet": "bolt_pipeliner.bases.spark_parquet",
    "ETLBaseParquetPandas": "bolt_pipeliner.bases.pandas_parquet",
    "ETLBaseParquetPolars": "bolt_pipeliner.bases.polars_parquet",
}


def _guess_project_root(config_path: str | Path, layer_paths: dict[str, str]) -> Path:
    """Infer the project root used to import user ETL modules.

    Console-script entry points (``bolt run``) may not include the current
    directory on ``sys.path``. We infer the root from the config location and
    declared relative ``layers`` paths so imports like ``etl._flatfile.job``
    resolve reliably.
    """
    config_file = Path(config_path).resolve()
    config_dir = config_file.parent

    rel_layer_paths = [
        Path(layer_dir)
        for layer_dir in layer_paths.values()
        if not Path(layer_dir).is_absolute()
    ]

    candidates = [config_dir.parent, config_dir, Path.cwd().resolve()]
    for candidate in candidates:
        if any((candidate / layer_dir).exists() for layer_dir in rel_layer_paths):
            return candidate

    return config_dir.parent


def _ensure_project_import_path(
    config_path: str | Path,
    layer_paths: dict[str, str],
) -> None:
    project_root = _guess_project_root(config_path, layer_paths)
    root_str = str(project_root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)


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


def run(
    config_path: str | Path,
    layers: Iterable[str] | None = None,
    select: str | None = None,
    layer: str | None = None,
    verbose: bool = False,
    spark=None,
) -> None:
    """Run ETL jobs for the requested selection in declared order.

    Args:
        config_path: path to etl_config.yaml.
        layers: subset of layer names to run. Mutually exclusive with
            ``select``. Defaults to every layer in ``layers:``.
        select: dbt-style selector — ``name``, ``+name`` (upstream),
            ``name+`` (downstream), or ``+name+`` (both). ``name`` is
            either ``{layer}_{output_table_name}`` or a bare
            ``output_table_name``.
        layer: when given alongside ``select``, constrains bare-name
            resolution to this layer (use it to disambiguate when the
            same output_table_name appears in multiple layers). When
            given without ``select``, acts like ``layers=[layer]``.
        verbose: when True, prints each resolved job before execution.
        spark: optional SparkSession; if None, the local session is
            created lazily.
    """
    if select is not None and layers:
        raise ValueError(
            "`select` is mutually exclusive with `layers`. "
            "Pass one or the other, not both."
        )

    config = load_config(config_path)
    configs_section = config.get("configs", {})
    flatfile_location, output_location = resolve_data_locations(config)
    save_catalog = configs_section.get("catalog", "dev_catalog")
    fixed_schema = configs_section.get("schema")
    incremental_column = configs_section.get("incremental_column")
    layer_paths: dict[str, str] = config.get("layers", {}) or {}
    _ensure_project_import_path(config_path, layer_paths)

    plan: list[tuple[str, dict[str, Any]]]
    if select is not None:
        plan = select_jobs(config, select, layer=layer)
        if not plan:
            print(f"[bolt] selector {select!r} resolved to zero jobs.")
            return
    else:
        # Legacy path: layer-filtered (or full) run.
        if layer is not None and not layers:
            layers = [layer]
        requested = list(layers) if layers else list(layer_paths.keys())
        plan = []
        for stage in requested:
            if stage not in layer_paths:
                print(f"[bolt] skipping unknown layer '{stage}'")
                continue
            for job in config.get(stage, []) or []:
                if isinstance(job, dict):
                    plan.append((stage, job))

    if spark is None:
        from bolt_pipeliner.sessions import create_session

        profile = resolve_spark_profile(config_path, config)
        spark = create_session(profile.profile, spark_config=profile.spark_config)
        if verbose:
            print(f"[bolt] using spark profile={profile.profile!r} config={profile.path}")

    for stage, job in plan:
        layer_dir = layer_paths.get(stage)
        if layer_dir is None:
            print(f"[bolt] skipping job in unknown layer '{stage}'")
            continue

        module_path = _module_import_path(layer_dir, job["module"])
        module = importlib.import_module(module_path)

        base_cls = _resolve_base_class(job.get("class_name", "ETLBase"))
        bucket = flatfile_location if stage == "flatfile" else output_location
        if verbose:
            print(
                "[bolt] running"
                f" stage={stage}"
                f" module={job['module']}"
                f" output={stage}_{job['output_table_name']}"
                f" class={job.get('class_name', 'ETLBase')}"
                f" incremental={job.get('incremental', False)}"
            )

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
            incremental_column=job.get("incremental_column", incremental_column),
            incremental_type=job.get(
                "incremental_type",
                configs_section.get("incremental_type", "int"),
            ),
            incremental_unit=job.get(
                "incremental_unit",
                configs_section.get("incremental_unit", 3),
            ),
            incremental_date_grain=job.get(
                "incremental_date_grain",
                configs_section.get("incremental_date_grain", "monthly"),
            ),
        )
        etl.process_data = types.MethodType(module.process_data, etl)
        etl.run()
        if verbose:
            print(f"[bolt] completed {stage}.{job['module']}")


__all__ = ["run"]
