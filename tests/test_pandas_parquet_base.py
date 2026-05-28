from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from bolt_pipeliner.bases.pandas_parquet import ETLBaseParquetPandas


def _make_base(layer: str, bucket: str, input_tables: dict[str, str]) -> ETLBaseParquetPandas:
    return ETLBaseParquetPandas(
        layer=layer,
        bucket=bucket,
        input_tables=input_tables,
        output_table_name="example",
        unload=False,
        incremental=False,
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
