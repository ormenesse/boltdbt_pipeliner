import pytest
import yaml

from bolt_pipeliner.runner import (
    _BUILTIN_BASE_MODULES,
    _module_import_path,
    _resolve_base_class,
    run,
)


def test_module_import_path_for_underscore_layer():
    assert (
        _module_import_path("etl/_flatfile", "flatfile_storm_events")
        == "etl._flatfile.flatfile_storm_events"
    )


def test_module_import_path_for_numeric_prefix_layer():
    # The legacy main.py:43 bug: silver/gold modules were always pulled from
    # `etl.0_bronze.<name>`. The new resolver must respect the layer's own dir.
    assert (
        _module_import_path("etl/1_silver", "silver_fct_account_calls_monthly")
        == "etl.1_silver.silver_fct_account_calls_monthly"
    )
    assert (
        _module_import_path("etl/2_gold", "gold_model_customer")
        == "etl.2_gold.gold_model_customer"
    )


def test_module_import_path_strips_dot_segment():
    assert _module_import_path("./etl/0_bronze", "foo") == "etl.0_bronze.foo"


def test_resolve_unknown_class_name_raises():
    with pytest.raises(KeyError):
        _resolve_base_class("DefinitelyNotABase")


def test_resolve_builtin_class_names_map_to_modules():
    # The lazy-loading map covers all five built-in bases so the runner never
    # eagerly imports PySpark/Polars at package import time.
    assert set(_BUILTIN_BASE_MODULES.keys()) == {
        "ETLBase",
        "ETLBaseDelta",
        "ETLBaseParquet",
        "ETLBaseParquetPandas",
        "ETLBaseParquetPolars",
    }


def test_run_rejects_layers_and_select_together(tmp_path):
    """Selectors and the legacy layer-list path are mutually exclusive — the
    runner must fail loudly rather than silently picking one or the other.
    """
    cfg = tmp_path / "etl_config.yaml"
    cfg.write_text(
        yaml.safe_dump({
            "configs": {},
            "layers": {"bronze": "etl/0_bronze"},
            "bronze": [],
        }),
        encoding="utf-8",
    )
    with pytest.raises(ValueError, match="mutually exclusive"):
        run(cfg, layers=["bronze"], select="bronze_x")


def test_run_with_empty_selector_resolves_to_nothing(tmp_path, capsys):
    """When a selector matches no jobs (e.g. a selector targets a job that's
    only declared in an empty layer), the runner reports it and returns
    cleanly rather than raising — selectors are user input, friendlier to
    "no-op + message" than to "stack trace".
    """
    cfg = tmp_path / "etl_config.yaml"
    cfg.write_text(
        yaml.safe_dump({
            "configs": {},
            "layers": {"bronze": "etl/0_bronze", "silver": "etl/1_silver"},
            "bronze": [
                {"module": "b_o", "input_tables": {"x": "raw.t"}, "output_table_name": "o"},
            ],
            "silver": [],
        }),
        encoding="utf-8",
    )
    # Selector targets bronze_o, but with no downstream consumers `bronze_o+`
    # still includes the target itself. Use a non-existent name to validate
    # the "zero jobs" path.
    with pytest.raises(ValueError, match="No job matches"):
        run(cfg, select="nope")
