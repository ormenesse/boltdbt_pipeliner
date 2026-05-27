from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:  # Python 3.11+
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - exercised on Python 3.10 only
    tomllib = None  # type: ignore[assignment]


@dataclass(frozen=True)
class SparkProfile:
    profile: str
    spark_config: dict[str, str]
    path: Path | None = None


def resolve_spark_profile(
    config_path: str | Path,
    config: dict[str, Any] | None = None,
) -> SparkProfile:
    """Resolve Spark runtime settings for an ETL config.

    Priority:
    1. ``BOLT_SPARK_PROFILE``
    2. ``configs.spark_profile`` in ``etl_config.yaml``
    3. ``configs/spark/local.toml`` when present
    4. the only ``configs/spark/*.toml`` file when exactly one exists
    5. ``local`` with an empty config
    """
    config_file = Path(config_path)
    config_dir = config_file.parent
    spark_dir = config_dir / "spark"
    configs_section = (config or {}).get("configs", {}) or {}

    configured_profile = configs_section.get("spark_profile")
    profile = os.environ.get("BOLT_SPARK_PROFILE") or configured_profile
    profile_path: Path | None = None

    if profile:
        profile_path = spark_dir / f"{profile}.toml"
    elif (spark_dir / "local.toml").is_file():
        profile = "local"
        profile_path = spark_dir / "local.toml"
    elif spark_dir.is_dir():
        profiles = sorted(spark_dir.glob("*.toml"))
        if len(profiles) == 1:
            profile_path = profiles[0]
            profile = profile_path.stem

    profile = str(profile or "local")
    if profile_path is None or not profile_path.is_file():
        return SparkProfile(profile=profile, spark_config={}, path=None)

    data = _load_toml(profile_path)
    runtime = data.get("runtime", {}) if isinstance(data.get("runtime"), dict) else {}
    spark = data.get("spark", {}) if isinstance(data.get("spark"), dict) else {}

    resolved_profile = str(runtime.get("target") or profile)
    return SparkProfile(
        profile=resolved_profile,
        spark_config={str(k): _stringify_spark_value(v) for k, v in spark.items()},
        path=profile_path,
    )


def _load_toml(path: Path) -> dict[str, Any]:
    if tomllib is not None:
        with path.open("rb") as f:
            return tomllib.load(f)
    return _load_simple_toml(path)


def _load_simple_toml(path: Path) -> dict[str, Any]:
    """Tiny TOML fallback for Python 3.10 Spark profile files.

    It intentionally supports only the simple ``[section]`` and ``key = value``
    shape emitted by ``bolt init``. That keeps Python 3.10 working without adding
    a runtime TOML dependency.
    """
    result: dict[str, Any] = {}
    section: str | None = None
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.split("#", 1)[0].strip()
        if not stripped:
            continue
        if stripped.startswith("[") and stripped.endswith("]"):
            section = stripped[1:-1].strip()
            result.setdefault(section, {})
            continue
        if section and "=" in stripped:
            key, value = stripped.split("=", 1)
            result[section][key.strip().strip('"')] = _parse_simple_toml_value(value)
    return result


def _parse_simple_toml_value(raw: str) -> Any:
    value = raw.strip()
    if value.startswith('"') and value.endswith('"'):
        return value[1:-1]
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False
    try:
        return int(value)
    except ValueError:
        return value


def _stringify_spark_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)
