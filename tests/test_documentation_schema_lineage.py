from __future__ import annotations

import pandas as pd

from bolt_pipeliner.generators import documentation as docs


def test_add_parent_column_for_non_id_columns_uses_previous_tables():
    all_schemas = pd.DataFrame(
        [
            {"table_name": "bronze_orders", "col_name": "order_total", "data_type": "double", "comment": ""},
            {"table_name": "silver_orders", "col_name": "order_total", "data_type": "double", "comment": ""},
            {"table_name": "gold_orders", "col_name": "order_total", "data_type": "double", "comment": ""},
            {"table_name": "gold_orders", "col_name": "order_id", "data_type": "string", "comment": ""},
        ]
    )

    table_rows = all_schemas.loc[all_schemas["table_name"] == "gold_orders"]
    enriched = docs.add_parent_column_for_table(
        table_rows,
        all_schemas,
        previous_tables=["bronze_orders", "silver_orders"],
    )

    order_total_parent = enriched.loc[enriched["col_name"] == "order_total", "parent"].iloc[0]
    assert order_total_parent == "bronze_orders.order_total, silver_orders.order_total"


def test_add_parent_column_for_high_frequency_id_columns_returns_only_id_names():
    previous_rows = [
        {"table_name": f"table_{idx}", "col_name": "customer_id", "data_type": "string", "comment": ""}
        for idx in range(5)
    ]
    current_rows = [
        {"table_name": "table_5", "col_name": "customer_id", "data_type": "string", "comment": ""},
    ]
    all_schemas = pd.DataFrame(previous_rows + current_rows)

    enriched = docs.add_parent_column_for_table(
        all_schemas.loc[all_schemas["table_name"] == "table_5"],
        all_schemas,
        previous_tables=[f"table_{idx}" for idx in range(5)],
    )

    customer_parent = enriched.loc[enriched["col_name"] == "customer_id", "parent"].iloc[0]
    assert customer_parent == "customer_id"


def test_generate_schema_csv_writes_parent_column(tmp_path, monkeypatch):
    monkeypatch.setattr(docs, "SCHEMA_DIR", str(tmp_path / "schema"))

    schemas = pd.DataFrame(
        [
            {"table_name": "bronze_orders", "col_name": "order_total", "data_type": "double", "comment": ""},
            {"table_name": "silver_orders", "col_name": "order_total", "data_type": "double", "comment": ""},
        ]
    )

    ok = docs.generate_schema_csv(
        schemas,
        table_order=["bronze_orders", "silver_orders"],
        config={"configs": {}},
    )

    assert ok is True
    generated = pd.read_csv(tmp_path / "schema" / "schema.csv")
    assert "parent" in generated.columns
    silver_parent = generated.loc[generated["table_name"] == "silver_orders", "parent"].iloc[0]
    assert silver_parent == "bronze_orders.order_total"
