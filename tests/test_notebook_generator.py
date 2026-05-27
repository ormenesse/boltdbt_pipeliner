from __future__ import annotations

from pathlib import Path

import pytest
import yaml

nbformat = pytest.importorskip("nbformat")

from bolt_pipeliner.generators import notebook as notebook_gen


def _write_yaml(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(content), encoding="utf-8")


def test_notebook_includes_resolved_spark_config_cell(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_path = tmp_path / "configs" / "etl_config.yaml"
    _write_yaml(
        config_path,
        {
            "configs": {
                "flatfile_bucket": "data/raw",
                "output_bucket": "outputs/tables",
            },
            "layers": {"flatfile": "etl/_flatfile"},
            "flatfile": [],
        },
    )
    spark_dir = config_path.parent / "spark"
    spark_dir.mkdir(parents=True, exist_ok=True)
    (spark_dir / "local.toml").write_text(
        "[runtime]\n"
        "target = \"local\"\n"
        "\n"
        "[spark]\n"
        "\"spark.sql.shuffle.partitions\" = 19\n",
        encoding="utf-8",
    )

    notebook_gen.create_etl_notebook(str(config_path), output_file="demo.ipynb")

    generated = tmp_path / "outputs" / "notebook" / "demo.ipynb"
    nb = nbformat.read(str(generated), as_version=4)
    cells = [cell["source"] for cell in nb.cells]

    assert any("spark_profile = 'local'" in cell for cell in cells)
    assert any("spark.sql.shuffle.partitions" in cell and "19" in cell for cell in cells)
    assert any("create_session(spark_config=spark_config)" in cell for cell in cells)
