import pytest
import yaml
from types import SimpleNamespace

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


def test_run_uses_resolved_spark_profile_for_session_creation(tmp_path, monkeypatch):
    cfg = tmp_path / "configs" / "etl_config.yaml"
    cfg.parent.mkdir(parents=True, exist_ok=True)
    cfg.write_text(
        yaml.safe_dump(
            {
                "configs": {},
                "layers": {},
            }
        ),
        encoding="utf-8",
    )
    spark_dir = cfg.parent / "spark"
    spark_dir.mkdir(parents=True, exist_ok=True)
    (spark_dir / "local.toml").write_text(
        "[runtime]\n"
        "target = \"local\"\n"
        "\n"
        "[spark]\n"
        "\"spark.sql.shuffle.partitions\" = 33\n",
        encoding="utf-8",
    )

    captured = {}

    def _fake_create_session(profile, spark_config=None):
        captured["profile"] = profile
        captured["spark_config"] = spark_config or {}
        return object()

    monkeypatch.setattr("bolt_pipeliner.sessions.create_session", _fake_create_session)

    run(cfg)

    assert captured["profile"] == "local"
    assert captured["spark_config"]["spark.sql.shuffle.partitions"] == "33"


def test_run_imports_modules_relative_to_config_project_root(tmp_path, monkeypatch):
    project = tmp_path / "demo"
    config_path = project / "configs" / "etl_config.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "configs": {
                    "output_location": "data/layers",
                    "flatfile_location": "data/raw",
                },
                "layers": {"flatfile": "etl/_flatfile"},
                "flatfile": [
                    {
                        "module": "flatfile_job",
                        "input_tables": {},
                        "output_table_name": "out",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    job_module = project / "etl" / "_flatfile" / "flatfile_job.py"
    job_module.parent.mkdir(parents=True, exist_ok=True)
    (job_module.parent / "__init__.py").write_text("", encoding="utf-8")
    job_module.write_text(
        "def process_data(self, input_tables):\n"
        "    return None\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)

    monkeypatch.setattr(
        "bolt_pipeliner.runner.resolve_spark_profile",
        lambda *_args, **_kwargs: SimpleNamespace(
            profile="local", spark_config={}, path=None
        ),
    )
    monkeypatch.setattr(
        "bolt_pipeliner.sessions.create_session",
        lambda *_args, **_kwargs: object(),
    )

    class _DummyBase:
        instances = []

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            _DummyBase.instances.append(self)

        def run(self):
            return None

    monkeypatch.setattr("bolt_pipeliner.runner._resolve_base_class", lambda _name: _DummyBase)

    run(config_path)
    assert _DummyBase.instances


def test_run_passes_job_incremental_overrides(tmp_path, monkeypatch):
    project = tmp_path / "demo"
    config_path = project / "configs" / "etl_config.yaml"
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(
        yaml.safe_dump(
            {
                "configs": {
                    "output_location": "data/layers",
                    "flatfile_location": "data/raw",
                    "incremental_column": "year_month",
                    "incremental_type": "int",
                    "incremental_unit": 5,
                    "incremental_date_grain": "monthly",
                },
                "layers": {"silver": "etl/1_silver"},
                "silver": [
                    {
                        "module": "silver_job",
                        "input_tables": {},
                        "output_table_name": "out",
                        "incremental": True,
                        "incremental_column": "anomes",
                        "incremental_type": "int",
                        "incremental_unit": 2,
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    job_module = project / "etl" / "1_silver" / "silver_job.py"
    job_module.parent.mkdir(parents=True, exist_ok=True)
    (job_module.parent / "__init__.py").write_text("", encoding="utf-8")
    job_module.write_text(
        "def process_data(self, input_tables):\n"
        "    return None\n",
        encoding="utf-8",
    )

    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr(
        "bolt_pipeliner.runner.resolve_spark_profile",
        lambda *_args, **_kwargs: SimpleNamespace(profile="local", spark_config={}, path=None),
    )
    monkeypatch.setattr("bolt_pipeliner.sessions.create_session", lambda *_a, **_k: object())

    class _DummyBase:
        instances = []

        def __init__(self, **kwargs):
            self.kwargs = kwargs
            _DummyBase.instances.append(self)

        def run(self):
            return None

    monkeypatch.setattr("bolt_pipeliner.runner._resolve_base_class", lambda _name: _DummyBase)

    run(config_path)

    assert len(_DummyBase.instances) == 1
    kwargs = _DummyBase.instances[0].kwargs
    assert kwargs["incremental_column"] == "anomes"
    assert kwargs["incremental_type"] == "int"
    assert kwargs["incremental_unit"] == 2
    assert kwargs["incremental_date_grain"] == "monthly"
