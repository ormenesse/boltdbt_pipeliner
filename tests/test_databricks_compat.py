"""Regressions observed when running vendored Bolt projects on Databricks."""

from __future__ import annotations

import importlib
import sys
import types
from pathlib import Path
from types import SimpleNamespace

import pytest

from bolt_pipeliner.bases._tables import bronze_table_identifier
from bolt_pipeliner.cli.init import _preset_answers, _render_run_shim, _scaffold
from bolt_pipeliner.sessions import create_session


@pytest.mark.parametrize(
    ("reference", "expected"),
    [
        ("bronze_orders", "destination.analytics.bronze_orders"),
        ("raw.orders", "sources.raw.orders"),
        ("production.raw.orders", "production.raw.orders"),
    ],
)
def test_bronze_source_identifiers(reference, expected):
    assert bronze_table_identifier(
        reference, source_catalog="sources", save_catalog="destination", schema="analytics"
    ) == expected


@pytest.mark.parametrize("reference", ["a.b.c.d", "raw..orders", ""])
def test_invalid_bronze_identifiers_fail_clearly(reference):
    with pytest.raises(ValueError, match="Invalid bronze table reference"):
        bronze_table_identifier(
            reference, source_catalog="sources", save_catalog="destination", schema="analytics"
        )


def _fake_spark_modules(monkeypatch, active=None):
    pyspark = types.ModuleType("pyspark")
    sql = types.ModuleType("pyspark.sql")
    functions = types.ModuleType("pyspark.sql.functions")
    sql.SparkSession = SimpleNamespace(getActiveSession=lambda: active)
    sql.functions = functions
    pyspark.sql = sql
    monkeypatch.setitem(sys.modules, "pyspark", pyspark)
    monkeypatch.setitem(sys.modules, "pyspark.sql", sql)
    monkeypatch.setitem(sys.modules, "pyspark.sql.functions", functions)


def test_databricks_reuses_active_jobs_session(monkeypatch):
    active = object()
    _fake_spark_modules(monkeypatch, active)
    assert create_session("databricks") is active


def test_databricks_connect_uses_serverless_builder_and_config(monkeypatch):
    _fake_spark_modules(monkeypatch)
    calls = []
    session = object()

    class Builder:
        def serverless(self, value):
            calls.append(("serverless", value))
            return self

        def config(self, key, value):
            calls.append(("config", key, value))
            return self

        def getOrCreate(self):
            return session

    db = types.ModuleType("databricks")
    connect = types.ModuleType("databricks.connect")
    connect.DatabricksSession = SimpleNamespace(builder=Builder())
    db.connect = connect
    monkeypatch.setitem(sys.modules, "databricks", db)
    monkeypatch.setitem(sys.modules, "databricks.connect", connect)

    assert create_session("databricks", {"spark.sql.shuffle.partitions": "12"}) is session
    assert calls == [
        ("serverless", True),
        ("config", "spark.sql.shuffle.partitions", "12"),
    ]


def test_databricks_classic_job_without_connect_uses_spark_builder(monkeypatch):
    _fake_spark_modules(monkeypatch)
    monkeypatch.delenv("SPARK_REMOTE", raising=False)
    spark = object()

    class Builder:
        def config(self, key, value):
            assert (key, value) == ("spark.sql.shuffle.partitions", "12")
            return self

        def getOrCreate(self):
            return spark

    sys.modules["pyspark.sql"].SparkSession.builder = Builder()
    # A missing Connect package must not prevent a classic Databricks Job.
    monkeypatch.setitem(sys.modules, "databricks", types.ModuleType("databricks"))
    monkeypatch.delitem(sys.modules, "databricks.connect", raising=False)
    assert create_session("databricks", {"spark.sql.shuffle.partitions": "12"}) is spark


@pytest.fixture
def spark_bases(monkeypatch):
    _fake_spark_modules(monkeypatch)
    names = ("spark_iceberg", "spark_delta", "spark_parquet")
    for name in names:
        monkeypatch.delitem(sys.modules, f"bolt_pipeliner.bases.{name}", raising=False)
    modules = [importlib.import_module(f"bolt_pipeliner.bases.{name}") for name in names]
    yield modules
    for name in names:
        sys.modules.pop(f"bolt_pipeliner.bases.{name}", None)


def test_iceberg_base_reads_three_part_source_and_explicit_provider(spark_bases):
    iceberg, _, _ = spark_bases
    reads = []
    writes = []

    class Writer:
        def using(self, provider):
            writes.append(("provider", provider))
            return self

        def createOrReplace(self):
            writes.append(("created",))

    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda *_: False),
        read=SimpleNamespace(table=lambda name: reads.append(name) or object()),
    )
    base = iceberg.ETLBase(
        spark=spark, layer="bronze", bucket="", input_tables={"src": "prd.raw.orders"},
        output_table_name="orders", save_catalog="dst", fixed_schema="analytics",
        table_format="iceberg", incremental=False,
    )
    base.load_data()
    base._create_table(SimpleNamespace(writeTo=lambda name: writes.append(("target", name)) or Writer()))
    assert reads == ["prd.raw.orders"]
    assert writes == [("target", "dst.analytics.bronze_orders"), ("provider", "iceberg"), ("created",)]


def test_delta_base_uses_configured_schema_and_does_not_cache(spark_bases):
    _, delta, _ = spark_bases
    reads = []
    writes = []

    class Writer:
        def mode(self, *_):
            return self

        def option(self, *_):
            return self

        def format(self, fmt):
            writes.append(("format", fmt))
            return self

        def saveAsTable(self, name):
            writes.append(("target", name))

    spark = SimpleNamespace(
        catalog=SimpleNamespace(tableExists=lambda *_: False),
        read=SimpleNamespace(table=lambda name: reads.append(name) or object()),
        sql=lambda statement: writes.append(("sql", statement)),
    )
    base = delta.ETLBaseDelta(
        spark=spark, layer="silver", bucket="", input_tables={"orders": "bronze_orders"},
        output_table_name="orders", save_catalog="dst", fixed_schema="analytics",
        incremental=False,
    )
    base.load_data()
    frame = SimpleNamespace(write=Writer())  # no .cache(): serverless rejects it
    base.unload_data(frame)

    assert base._write_table == "dst.analytics.silver_orders"
    assert reads == ["dst.analytics.bronze_orders"]
    assert ("sql", "CREATE NAMESPACE IF NOT EXISTS dst.analytics") in writes
    assert ("format", "delta") in writes
    assert ("target", "dst.analytics.silver_orders") in writes


def test_serverless_writes_do_not_cache_iceberg_or_parquet(spark_bases):
    iceberg, _, parquet = spark_bases
    iceberg_calls = []
    iceberg_base = iceberg.ETLBase(
        spark=SimpleNamespace(
            catalog=SimpleNamespace(tableExists=lambda *_: False),
            sql=lambda statement: iceberg_calls.append(statement),
        ),
        layer="bronze", bucket="", input_tables={}, output_table_name="out",
        incremental=False,
    )
    iceberg_base._create_table = lambda frame: iceberg_calls.append(frame)
    frame = object()  # no .cache() method
    iceberg_base.unload_data(frame)
    assert frame in iceberg_calls

    class Writer:
        def mode(self, *_):
            return self

        def partitionBy(self, *_):
            return self

        def parquet(self, path):
            parquet_calls.append(path)

    parquet_calls = []
    parquet_base = parquet.ETLBaseParquet(
        spark=object(), layer="silver", bucket="/tmp/layers", input_tables={},
        output_table_name="out", partition_by=None, incremental=False,
    )
    parquet_base.unload_data(SimpleNamespace(write=Writer()))
    assert parquet_calls == ["/tmp/layers/silver_out.parquet"]


@pytest.mark.parametrize("shim", ["bolt.py", "main.py", "generate.py"])
def test_generated_job_shims_handle_missing_file_and_success_exit(tmp_path, monkeypatch, shim):
    project = tmp_path / "demo"
    _scaffold(_preset_answers("minimal", "demo", project))
    fake_cli = types.ModuleType("bolt_pipeliner.cli.app")
    fake_cli.app = lambda *_args: (_ for _ in ()).throw(SystemExit(0))
    fake_cli.main = lambda: (_ for _ in ()).throw(SystemExit(0))
    monkeypatch.setitem(sys.modules, "bolt_pipeliner.cli.app", fake_cli)
    monkeypatch.setenv("BOLT_PROJECT_ROOT", str(project))
    monkeypatch.setattr(sys, "argv", ["databricks_worker"])
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.chdir(tmp_path)

    script = (project / shim).read_text(encoding="utf-8")
    exec(compile(script, str(project / shim), "exec"), {"__name__": "__main__"})
    assert Path.cwd() == project


def test_generated_job_shim_propagates_failure(tmp_path, monkeypatch):
    project = tmp_path / "demo"
    _scaffold(_preset_answers("minimal", "demo", project))
    fake_cli = types.ModuleType("bolt_pipeliner.cli.app")
    fake_cli.app = lambda *_args: (_ for _ in ()).throw(SystemExit(2))
    monkeypatch.setitem(sys.modules, "bolt_pipeliner.cli.app", fake_cli)
    monkeypatch.setenv("BOLT_PROJECT_ROOT", str(project))
    monkeypatch.setattr(sys, "argv", ["databricks_worker"])
    monkeypatch.setattr(sys, "path", list(sys.path))
    monkeypatch.chdir(tmp_path)

    with pytest.raises(SystemExit) as exc:
        exec(compile(_render_run_shim(), str(project / "main.py"), "exec"), {"__name__": "__main__"})
    assert exc.value.code == 2
