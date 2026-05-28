from __future__ import annotations

from pathlib import Path
from typing import Any, Mapping

import yaml

DEFAULT_SCHEMA = "cxdw_dm"
DEFAULT_INCREMENTAL_COLUMN = "year_month"
DEFAULT_CLASS_NAME = "ETLBase"
DEFAULT_OUTPUT_LOCATION = ""
DEFAULT_FLATFILE_LOCATION = ""


def resolve_data_locations(config: Mapping[str, Any]) -> tuple[str, str]:
    configs_section = config.get("configs", {}) if isinstance(config, Mapping) else {}
    if not isinstance(configs_section, Mapping):
        return DEFAULT_FLATFILE_LOCATION, DEFAULT_OUTPUT_LOCATION

    flatfile_location = str(
        configs_section.get("flatfile_location")
        or configs_section.get("flatfile_bucket")
        or DEFAULT_FLATFILE_LOCATION
    )
    output_location = str(
        configs_section.get("output_location")
        or configs_section.get("output_bucket")
        or DEFAULT_OUTPUT_LOCATION
    )
    return flatfile_location, output_location


def load_config(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    configs_section = config.setdefault("configs", {})
    configs_section.setdefault("schema", DEFAULT_SCHEMA)
    configs_section.setdefault("incremental_column", DEFAULT_INCREMENTAL_COLUMN)

    flatfile_location, output_location = resolve_data_locations(config)
    configs_section["flatfile_location"] = flatfile_location
    configs_section["output_location"] = output_location
    # Backwards-compat aliases used by older projects/generators.
    configs_section.setdefault("flatfile_bucket", flatfile_location)
    configs_section.setdefault("output_bucket", output_location)

    layers = config.get("layers", {}) or {}
    for layer_name in layers:
        jobs = config.get(layer_name)
        if not isinstance(jobs, list):
            config[layer_name] = []
            continue
        config[layer_name] = [normalize_job(job) for job in jobs if isinstance(job, dict)]

    return config


def normalize_job(job: dict[str, Any]) -> dict[str, Any]:
    """Normalize per-job keys. Peco-style `_input_tables` is renamed to `input_tables`."""
    if "_input_tables" in job and "input_tables" not in job:
        job["input_tables"] = job.pop("_input_tables")
    job.setdefault("class_name", DEFAULT_CLASS_NAME)
    return job
