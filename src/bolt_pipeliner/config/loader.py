from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml

DEFAULT_SCHEMA = "cxdw_dm"
DEFAULT_INCREMENTAL_COLUMN = "year_month"
DEFAULT_CLASS_NAME = "ETLBase"


def load_config(path: str | Path) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f) or {}

    configs_section = config.setdefault("configs", {})
    configs_section.setdefault("schema", DEFAULT_SCHEMA)
    configs_section.setdefault("incremental_column", DEFAULT_INCREMENTAL_COLUMN)

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
