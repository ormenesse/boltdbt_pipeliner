#!/usr/bin/env python3
"""Validate the source layout of a Bolt Pipeliner project.

This intentionally performs static checks only. It does not import project
jobs, start Spark, access cloud storage, or execute user code.
"""

from __future__ import annotations

import argparse
import ast
import sys
from pathlib import Path
from typing import Any


def _is_within(path: Path, parent: Path) -> bool:
    try:
        path.resolve().relative_to(parent.resolve())
    except ValueError:
        return False
    return True


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml
    except ImportError as exc:  # pragma: no cover - depends on target project
        raise RuntimeError("PyYAML is required to validate etl_config.yaml") from exc

    data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(data, dict):
        raise ValueError("etl_config.yaml must contain a mapping at the top level")
    return data


def _check_job_module(path: Path, errors: list[str]) -> None:
    try:
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
    except (OSError, SyntaxError) as exc:
        errors.append(f"{path}: cannot parse job module: {exc}")
        return

    functions = {
        node.name: node
        for node in ast.walk(tree)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
    }
    process_data = functions.get("process_data")
    if process_data is None:
        errors.append(f"{path}: missing process_data(self, input_tables)")
    elif len(process_data.args.args) < 2:
        errors.append(f"{path}: process_data must accept self and input_tables")


def validate(root: Path) -> tuple[list[str], list[str]]:
    errors: list[str] = []
    warnings: list[str] = []
    root = root.resolve()
    config_path = root / "configs" / "etl_config.yaml"
    if not config_path.is_file():
        return [f"missing {config_path.relative_to(root)}"], warnings

    try:
        config = _load_yaml(config_path)
    except (OSError, RuntimeError, ValueError) as exc:
        return [str(exc)], warnings

    layers = config.get("layers")
    if not isinstance(layers, dict) or not layers:
        errors.append("etl_config.yaml must declare a non-empty layers mapping")
        return errors, warnings

    declared_job_ids: set[str] = set()
    for layer_name, raw_layer_path in layers.items():
        if not isinstance(layer_name, str) or not layer_name:
            errors.append("layers contains an empty or non-string layer name")
            continue
        if not isinstance(raw_layer_path, str) or not raw_layer_path:
            errors.append(f"layer {layer_name!r} has no filesystem path")
            continue

        layer_path = (root / raw_layer_path).resolve()
        etl_root = root / "etl"
        if not _is_within(layer_path, etl_root):
            errors.append(f"layer {layer_name!r} must live below etl/")
        if not layer_path.is_dir():
            errors.append(f"layer {layer_name!r} directory does not exist: {raw_layer_path}")

        jobs = config.get(layer_name, [])
        if jobs in (None, "..."):
            warnings.append(f"layer {layer_name!r} has no job entries")
            continue
        if not isinstance(jobs, list):
            errors.append(f"layer {layer_name!r} must be a list of jobs")
            continue

        for index, job in enumerate(jobs):
            if not isinstance(job, dict):
                errors.append(f"{layer_name}[{index}] is not a job mapping")
                continue
            module_name = job.get("module")
            output_name = job.get("output_table_name")
            if not isinstance(module_name, str) or not module_name:
                errors.append(f"{layer_name}[{index}] is missing module")
                continue
            if "/" in module_name or "\\" in module_name or module_name.endswith(".py"):
                errors.append(f"{layer_name}[{index}] module must be a filename without .py")
                continue
            if not isinstance(job.get("input_tables"), dict):
                errors.append(f"{layer_name}.{module_name} is missing input_tables mapping")
            if not isinstance(output_name, str) or not output_name:
                errors.append(f"{layer_name}.{module_name} is missing output_table_name")
                continue

            job_id = f"{layer_name}_{output_name}"
            if job_id in declared_job_ids:
                errors.append(f"duplicate output table: {job_id}")
            declared_job_ids.add(job_id)

            module_path = layer_path / f"{module_name}.py"
            if not module_path.is_file():
                errors.append(f"missing job module: {module_path.relative_to(root)}")
            else:
                _check_job_module(module_path, errors)

    for source_dir_name in ("etl", "macros"):
        source_dir = root / source_dir_name
        if source_dir.is_dir():
            notebooks = sorted(source_dir.rglob("*.ipynb"))
            for notebook in notebooks:
                errors.append(
                    f"{notebook.relative_to(root)} must not contain notebooks; use the framework notebook locations"
                )

    profile_path = root / "configs" / "bolt_environment.yaml"
    if profile_path.is_file():
        try:
            profile = _load_yaml(profile_path)
        except (OSError, RuntimeError, ValueError) as exc:
            errors.append(f"invalid configs/bolt_environment.yaml: {exc}")
        else:
            if profile.get("schema_version") != 1:
                errors.append("configs/bolt_environment.yaml must use schema_version: 1")

    return errors, warnings


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("root", nargs="?", default=".", type=Path)
    args = parser.parse_args()

    errors, warnings = validate(args.root)
    for warning in warnings:
        print(f"WARNING: {warning}", file=sys.stderr)
    for error in errors:
        print(f"ERROR: {error}", file=sys.stderr)
    if errors:
        return 1
    print("Bolt Pipeliner layout is valid.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
