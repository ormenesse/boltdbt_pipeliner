from __future__ import annotations

from pathlib import Path

import yaml

from bolt_pipeliner.sessions.profiles import resolve_spark_profile


def _write_config(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(content), encoding="utf-8")


def test_resolve_spark_profile_defaults_to_local(tmp_path):
    config_path = tmp_path / "configs" / "etl_config.yaml"
    _write_config(config_path, {"configs": {}, "layers": {}, "flatfile": []})

    profile = resolve_spark_profile(config_path)

    assert profile.profile == "local"
    assert profile.spark_config == {}
    assert profile.path is None


def test_resolve_spark_profile_loads_local_toml(tmp_path):
    config_path = tmp_path / "configs" / "etl_config.yaml"
    _write_config(config_path, {"configs": {}, "layers": {}, "flatfile": []})
    spark_dir = config_path.parent / "spark"
    spark_dir.mkdir(parents=True, exist_ok=True)
    (spark_dir / "local.toml").write_text(
        "[runtime]\n"
        "target = \"local\"\n"
        "\n"
        "[spark]\n"
        "\"spark.sql.shuffle.partitions\" = 17\n"
        "\"spark.sql.adaptive.enabled\" = true\n",
        encoding="utf-8",
    )

    profile = resolve_spark_profile(config_path)

    assert profile.profile == "local"
    assert profile.spark_config["spark.sql.shuffle.partitions"] == "17"
    assert profile.spark_config["spark.sql.adaptive.enabled"] == "true"
    assert profile.path == spark_dir / "local.toml"


def test_resolve_spark_profile_honors_env_override(tmp_path, monkeypatch):
    config_path = tmp_path / "configs" / "etl_config.yaml"
    _write_config(config_path, {"configs": {"spark_profile": "local"}, "layers": {}})
    spark_dir = config_path.parent / "spark"
    spark_dir.mkdir(parents=True, exist_ok=True)
    (spark_dir / "local.toml").write_text("[runtime]\ntarget = \"local\"\n", encoding="utf-8")
    (spark_dir / "emr.toml").write_text(
        "[runtime]\n"
        "target = \"emr\"\n"
        "\n"
        "[spark]\n"
        "\"spark.sql.shuffle.partitions\" = 55\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("BOLT_SPARK_PROFILE", "emr")

    profile = resolve_spark_profile(config_path, {"configs": {"spark_profile": "local"}})

    assert profile.profile == "emr"
    assert profile.spark_config["spark.sql.shuffle.partitions"] == "55"
    assert profile.path == spark_dir / "emr.toml"


def test_resolve_spark_profile_uses_config_setting_when_present(tmp_path):
    config_path = tmp_path / "configs" / "etl_config.yaml"
    _write_config(config_path, {"configs": {"spark_profile": "k8s"}, "layers": {}})
    spark_dir = config_path.parent / "spark"
    spark_dir.mkdir(parents=True, exist_ok=True)
    (spark_dir / "k8s.toml").write_text(
        "[runtime]\n"
        "target = \"k8s\"\n"
        "\n"
        "[spark]\n"
        "\"spark.executor.instances\" = 3\n",
        encoding="utf-8",
    )

    profile = resolve_spark_profile(config_path, {"configs": {"spark_profile": "k8s"}})

    assert profile.profile == "k8s"
    assert profile.spark_config["spark.executor.instances"] == "3"


def test_resolve_spark_profile_falls_back_to_single_profile_file(tmp_path):
    config_path = tmp_path / "configs" / "etl_config.yaml"
    _write_config(config_path, {"configs": {}, "layers": {}})
    spark_dir = config_path.parent / "spark"
    spark_dir.mkdir(parents=True, exist_ok=True)
    (spark_dir / "databricks.toml").write_text(
        "[runtime]\n"
        "target = \"databricks\"\n",
        encoding="utf-8",
    )

    profile = resolve_spark_profile(config_path)

    assert profile.profile == "databricks"
    assert profile.path == spark_dir / "databricks.toml"


def test_resolve_spark_profile_keeps_explicit_missing_profile(tmp_path):
    config_path = tmp_path / "configs" / "etl_config.yaml"
    _write_config(config_path, {"configs": {"spark_profile": "emr"}, "layers": {}})

    profile = resolve_spark_profile(config_path, {"configs": {"spark_profile": "emr"}})

    assert profile.profile == "emr"
    assert profile.spark_config == {}
    assert profile.path is None
