from pathlib import Path

import pytest

from bolt_pipeliner.cli.init import _preset_answers, _scaffold, execute


def test_minimal_preset_produces_pandas_flatfile_project(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="minimal")

    assert (target / "configs" / "etl_config.yaml").is_file()
    assert (target / "etl" / "_flatfile" / "flatfile_example.py").is_file()
    assert (target / "etl" / "0_bronze" / "bronze_example.py").is_file()
    assert (target / "macros" / "__init__.py").is_file()
    assert (target / "tests" / "test_smoke.py").is_file()
    assert not (target / "configs" / "spark").exists()  # pandas → no spark profile
    assert not (target / "models").exists()  # ml disabled


def test_medallion_preset_emits_four_layers_with_spark(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="medallion")

    for layer_dir in ["_flatfile", "0_bronze", "1_silver", "2_gold"]:
        assert (target / "etl" / layer_dir).is_dir()
    assert (target / "configs" / "spark" / "local.toml").is_file()


def test_diamond_preset_includes_ml(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="diamond")
    assert (target / "models" / "train_example.py").is_file()
    assert (target / "etl" / "3_diamond").is_dir()


def test_polars_preset_uses_polars_engine(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="polars")
    config = (target / "configs" / "etl_config.yaml").read_text(encoding="utf-8")
    assert "class_name: ETLBaseParquetPolars" in config


def test_pyspark_preset_uses_etlbase(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="medallion")
    config = (target / "configs" / "etl_config.yaml").read_text(encoding="utf-8")
    assert "class_name: ETLBase" in config


def test_scaffold_refuses_non_empty_target(tmp_path):
    target = tmp_path / "demo"
    target.mkdir()
    (target / "existing").write_text("hi", encoding="utf-8")
    answers = _preset_answers("minimal", "demo", target)
    with pytest.raises(FileExistsError):
        _scaffold(answers)


def test_unknown_preset_raises(tmp_path):
    with pytest.raises(ValueError):
        _preset_answers("nope", "demo", tmp_path / "demo")
