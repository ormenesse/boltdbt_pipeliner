import pytest

from bolt_pipeliner.runner import (
    _BUILTIN_BASE_MODULES,
    _module_import_path,
    _resolve_base_class,
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
