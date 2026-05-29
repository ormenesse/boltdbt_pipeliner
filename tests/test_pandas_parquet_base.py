from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bolt_pipeliner.bases.pandas_parquet import ETLBaseParquetPandas


def _make_base(
    layer: str,
    bucket: str,
    input_tables: dict[str, str],
    **kwargs,
) -> ETLBaseParquetPandas:
    incremental = kwargs.pop("incremental", False)
    return ETLBaseParquetPandas(
        layer=layer,
        bucket=bucket,
        input_tables=input_tables,
        output_table_name="example",
        unload=False,
        incremental=incremental,
        **kwargs,
    )


def test_load_data_uses_flatfile_location_for_relative_paths(monkeypatch):
    captured: dict[str, str] = {}

    def _fake_read_csv(path, *args, **kwargs):
        captured["path"] = str(path)
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(pd, "read_csv", _fake_read_csv)
    base = _make_base("flatfile", "data/raw", {"src": "events.csv"})

    base.load_data()

    assert captured["path"] == str(Path("data/raw") / "events.csv")


def test_load_data_defaults_non_flatfile_sources_to_parquet(monkeypatch):
    captured: dict[str, str] = {}

    def _fake_read_parquet(path, *args, **kwargs):
        captured["path"] = str(path)
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(pd, "read_parquet", _fake_read_parquet)
    base = _make_base("silver", "outputs/tables", {"src": "bronze_orders"})

    base.load_data()

    assert captured["path"] == str(Path("outputs/tables") / "bronze_orders.parquet")


def test_load_data_supports_excel(monkeypatch):
    called = {"excel": False}

    def _fake_read_excel(path, *args, **kwargs):
        called["excel"] = True
        return pd.DataFrame({"a": [1]})

    monkeypatch.setattr(pd, "read_excel", _fake_read_excel)
    base = _make_base("flatfile", "data/raw", {"src": "sheet.xlsx"})

    base.load_data()

    assert called["excel"]


def test_load_data_normalizes_json_flatfiles(tmp_path):
    payload = [{"id": 1, "meta": {"country": "US"}}]
    json_path = tmp_path / "events.json"
    json_path.write_text(json.dumps(payload), encoding="utf-8")

    base = _make_base("flatfile", str(tmp_path), {"src": "events.json"})

    base.load_data()

    df = base.input_tables["src"]
    assert "meta.country" in df.columns
    assert df.iloc[0]["meta.country"] == "US"


def test_incremental_append_mode_keeps_existing_and_only_adds_new_values(monkeypatch):
    existing = pd.DataFrame({"anomes": [202401, 202402], "v": [1, 2]})
    incoming = pd.DataFrame({"anomes": [202402, 202403], "v": [20, 3]})

    base = _make_base(
        "silver",
        "outputs/tables",
        {},
        incremental=True,
        incremental_column="anomes",
        incremental_type="int",
        incremental_unit="append",
    )

    monkeypatch.setattr(base, "_load_existing_dataset", lambda: existing)
    captured: dict[str, pd.DataFrame] = {}

    def _fake_write_dataset(**kwargs):
        captured["df"] = kwargs["data"].to_pandas()

    monkeypatch.setattr("bolt_pipeliner.bases.pandas_parquet.ds.write_dataset", _fake_write_dataset)

    base.unload_data(incoming)

    written = captured["df"].sort_values(["anomes", "v"]).reset_index(drop=True)
    assert written["anomes"].tolist() == [202401, 202402, 202403]


def test_incremental_window_mode_replaces_latest_values(monkeypatch):
    existing = pd.DataFrame({"anomes": [202401, 202402, 202403], "v": [1, 2, 3]})
    incoming = pd.DataFrame({"anomes": [202402, 202403, 202404], "v": [20, 30, 40]})

    base = _make_base(
        "silver",
        "outputs/tables",
        {},
        incremental=True,
        incremental_column="anomes",
        incremental_type="int",
        incremental_unit=2,
    )

    monkeypatch.setattr(base, "_load_existing_dataset", lambda: existing)
    captured: dict[str, pd.DataFrame] = {}

    def _fake_write_dataset(**kwargs):
        captured["df"] = kwargs["data"].to_pandas()

    monkeypatch.setattr("bolt_pipeliner.bases.pandas_parquet.ds.write_dataset", _fake_write_dataset)

    base.unload_data(incoming)

    written = captured["df"].sort_values(["anomes", "v"]).reset_index(drop=True)
    assert written["anomes"].tolist() == [202401, 202402, 202403, 202404]
    assert written.loc[written["anomes"] == 202402, "v"].iloc[0] == 20
    assert written.loc[written["anomes"] == 202403, "v"].iloc[0] == 30
