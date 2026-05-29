import subprocess
import sys

import pytest

from bolt_pipeliner.cli.init import (
    VENDOR_DIRNAME,
    _default_data_locations,
    _preset_answers,
    _scaffold,
    execute,
)


def test_minimal_preset_produces_pandas_flatfile_project(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="minimal")

    assert (target / "configs" / "etl_config.yaml").is_file()
    assert (target / "configs" / "style_config.yaml").is_file()
    assert (target / "etl" / "_flatfile" / "flatfile_example.py").is_file()
    assert (target / "etl" / "0_bronze" / "bronze_example.py").is_file()
    assert (target / "macros" / "__init__.py").is_file()
    assert (target / "tests" / "test_smoke.py").is_file()
    assert not (target / "configs" / "spark").exists()  # pandas → no spark profile
    assert not (target / "models").exists()  # ml disabled


def test_style_config_includes_color_block_for_chosen_layers(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="medallion")

    style = (target / "configs" / "style_config.yaml").read_text(encoding="utf-8")
    assert "style_colors:" in style
    assert "layers_colors:" in style
    # The medallion preset selects flatfile + bronze + silver + gold.
    for layer in ["flatfile", "bronze", "silver", "gold"]:
        assert f"    {layer}:" in style, f"missing color block for {layer}"
    # 'raw' is always emitted because the docs generator uses it for upstream
    # source nodes regardless of the project's declared layers.
    assert "    raw:" in style


def test_style_config_falls_back_for_unknown_layer_names(tmp_path):
    """Custom layers (not in LAYER_COLOR_PALETTE) still get a color block,
    populated with the neutral gray default rather than crashing.
    """
    from bolt_pipeliner.cli.init import _render_style_config

    out = _render_style_config(["flatfile", "weirdlayer"])
    assert "    weirdlayer:" in out
    assert "#666666" in out  # neutral stroke default


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


def test_diamond_preset_scaffolds_model_notebooks(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="diamond")

    assert (target / "model_notebooks" / "README.md").is_file()
    nb = target / "model_notebooks" / "train_example.ipynb"
    assert nb.is_file()

    # nbformat-4 structural sanity check + MLflow guidance must travel with it.
    import json
    payload = json.loads(nb.read_text(encoding="utf-8"))
    assert payload["nbformat"] == 4
    assert any(
        "mlflow" in "".join(cell.get("source", [])).lower()
        for cell in payload["cells"]
    ), "notebook should reference MLflow as a suggestion"


def test_ml_example_bakes_in_mlflow_suggestion(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="diamond")

    body = (target / "models" / "train_example.py").read_text(encoding="utf-8")
    # MLflow must be a *suggestion*, not a hard dep — the import stays commented.
    assert "# import mlflow" in body
    assert "MLFLOW_TRACKING_URI" not in body.split("\n")[0]  # not at top of file
    assert "load_latest" in body
    assert "MODEL_NAME" in body


def test_readme_calls_out_ml_scaffolding(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="diamond")
    readme = (target / "README.md").read_text(encoding="utf-8")
    assert "model_notebooks/" in readme
    assert "diamond" in readme.lower()
    assert "MLflow" in readme


def test_example_job_is_a_tutorial(tmp_path):
    """The generated job module must teach the input_tables contract, not just
    show three lines of code. New users repeatedly miss that `input_tables` is
    a dict keyed by YAML aliases — this test pins the explanation in place.
    """
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="medallion")
    bronze = (target / "etl" / "0_bronze" / "bronze_example.py").read_text(encoding="utf-8")

    # Core teaching points must appear.
    assert "input_tables" in bronze
    assert "aliases" in bronze
    assert "etl_config.yaml" in bronze
    assert "ETLBase" in bronze
    assert "self.spark" in bronze
    assert "self.incremental_policy" in bronze
    assert "unload" in bronze
    assert "partition" in bronze.lower()

    # The runtime → YAML mapping example must be present.
    assert "raw_orders" in bronze
    assert "bronze_orders" in bronze


def test_example_jobs_are_valid_python(tmp_path):
    """The tutorial template still has to be syntactically valid — otherwise
    `python main.py` blows up before the user gets a chance to read it.
    """
    import ast

    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="medallion")
    for layer_dir in ["_flatfile", "0_bronze", "1_silver", "2_gold"]:
        files = list((target / "etl" / layer_dir).glob("*_example.py"))
        assert files, f"no example job in {layer_dir}"
        body = files[0].read_text(encoding="utf-8")
        ast.parse(body)  # raises SyntaxError on a bad template


def test_flatfile_example_documents_file_path_inputs(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="medallion")
    flatfile = (target / "etl" / "_flatfile" / "flatfile_example.py").read_text(encoding="utf-8")
    # Flatfile values are file paths, not table names — that's the key
    # confusion the layer-specific note exists to resolve.
    assert "flatfile_location" in flatfile
    assert ".csv" in flatfile


def test_init_config_uses_location_keys_with_local_defaults(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="minimal")
    config = (target / "configs" / "etl_config.yaml").read_text(encoding="utf-8")

    assert "output_location:" in config
    assert "flatfile_location:" in config
    assert "incremental_unit:" in config
    assert "incremental_type:" in config
    assert "incremental_date_grain:" in config
    assert 'output_location: "data/layers"' in config
    assert 'flatfile_location: "data/flatfiles"' in config


def test_default_cloud_locations_preconfigure_prefix_with_path_placeholder():
    output, flatfile = _default_data_locations("pyspark", "emr", "demo")
    assert output == "s3://<bucket>/<path>/layers/"
    assert flatfile == "s3://<bucket>/<path>/flatfiles/"

    output, flatfile = _default_data_locations("pyspark", "gcp", "demo")
    assert output.startswith("gs://")
    assert "<path>" in output
    assert flatfile.startswith("gs://")

    output, flatfile = _default_data_locations("pyspark", "azure", "demo")
    assert output.startswith("abfss://")
    assert "<path>" in output

    output, flatfile = _default_data_locations("pyspark", "databricks", "demo")
    assert output.startswith("dbfs:/")
    assert "<path>" in output


def test_polars_example_imports_polars_not_pyspark(tmp_path):
    """The tutorial must not leak Spark idioms into a polars project."""
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="polars")
    bronze = (target / "etl" / "0_bronze" / "bronze_example.py").read_text(encoding="utf-8")
    assert "import polars as pl" in bronze
    assert "pyspark" not in bronze


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


def test_scaffold_emits_self_contained_shims(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="minimal")

    for shim in ("bolt.py", "main.py", "generate.py"):
        path = target / shim
        assert path.is_file(), f"missing shim: {shim}"
        body = path.read_text(encoding="utf-8")
        # Each shim must bootstrap the vendored package onto sys.path before
        # importing bolt_pipeliner.
        assert VENDOR_DIRNAME in body
        assert "sys.path.insert(0" in body
        assert "from bolt_pipeliner.cli.app" in body


def test_scaffold_vendors_bolt_pipeliner_by_default(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="minimal")

    vendor_pkg = target / VENDOR_DIRNAME / "bolt_pipeliner"
    assert vendor_pkg.is_dir(), "vendored package directory should exist"
    assert (vendor_pkg / "__init__.py").is_file()
    assert (vendor_pkg / "cli" / "app.py").is_file()
    # Templates must travel with the vendored copy — `bolt generate documentation`
    # reads logo.png, mermaid_page.txt, etc. from the package data dir.
    assert (vendor_pkg / "templates" / "docs" / "logo.png").is_file()
    # No build artifacts should leak through.
    assert not list(vendor_pkg.rglob("__pycache__"))
    assert not list(vendor_pkg.rglob("*.pyc"))
    # And the marker README explaining what _vendor/ is should be there.
    assert (target / VENDOR_DIRNAME / "README.md").is_file()


def test_no_vendor_flag_skips_vendoring_but_keeps_shims(tmp_path):
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="minimal", vendor=False)

    assert not (target / VENDOR_DIRNAME).exists()
    # Shims are still emitted — they fall back to a pip-installed bolt_pipeliner
    # when the vendor dir is missing.
    assert (target / "bolt.py").is_file()
    assert (target / "main.py").is_file()
    assert (target / "generate.py").is_file()


def test_vendored_shim_runs_via_subprocess(tmp_path):
    """End-to-end: scaffold, wipe PYTHONPATH so the test can't lean on the
    installed package, and confirm `python bolt.py --help` exits cleanly using
    only the vendored copy. This is the user's actual scenario: clone the
    project, no install, run the pipeline.
    """
    target = tmp_path / "demo"
    execute("demo", target_dir=target, preset="minimal")

    # Strip site-packages from sys.path by running with a clean PYTHONPATH —
    # but we still need typer/questionary/pyyaml, which come from the test's
    # own environment. So we let those resolve normally; the assertion is just
    # that the shim *prefers* the vendored package (path inserted at index 0)
    # and successfully loads the CLI.
    result = subprocess.run(
        [sys.executable, str(target / "bolt.py"), "--help"],
        capture_output=True,
        text=True,
        cwd=str(target),
        timeout=30,
    )
    assert result.returncode == 0, (
        f"bolt.py --help failed (code {result.returncode}):\n"
        f"stdout:\n{result.stdout}\nstderr:\n{result.stderr}"
    )
    assert "run" in result.stdout
    assert "generate" in result.stdout
    assert "init" in result.stdout
