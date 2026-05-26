"""Tests for bolt_pipeliner.selection — dbt-style job selection."""

from __future__ import annotations

import pytest

from bolt_pipeliner.selection import (
    build_graph,
    parse_selector,
    resolve_name,
    select,
)


# ---------------------------------------------------------------------------
# Fixture config: a small medallion graph used by every selector test.
#
#     flatfile_raw_a  ─┐
#                      ├─→ bronze_a  ─→ silver_x  ─→ gold_y
#     flatfile_raw_b ──┘                ↑
#                                       │
#                          bronze_b ────┘
# ---------------------------------------------------------------------------
@pytest.fixture
def config() -> dict:
    return {
        "configs": {},
        "layers": {
            "flatfile": "etl/_flatfile",
            "bronze": "etl/0_bronze",
            "silver": "etl/1_silver",
            "gold": "etl/2_gold",
        },
        "flatfile": [
            {
                "module": "flatfile_raw_a",
                "input_tables": {"src": "a.csv"},
                "output_table_name": "raw_a",
            },
            {
                "module": "flatfile_raw_b",
                "input_tables": {"src": "b.csv"},
                "output_table_name": "raw_b",
            },
        ],
        "bronze": [
            {
                "module": "bronze_a",
                "input_tables": {
                    "a": "flatfile_raw_a",
                    "b": "flatfile_raw_b",
                },
                "output_table_name": "a",
            },
            {
                "module": "bronze_b",
                "input_tables": {"src": "raw.shared_table"},  # external — ignored
                "output_table_name": "b",
            },
        ],
        "silver": [
            {
                "module": "silver_x",
                "input_tables": {
                    "a": "bronze_a",
                    "b": "bronze_b",
                },
                "output_table_name": "x",
            },
        ],
        "gold": [
            {
                "module": "gold_y",
                "input_tables": {"src": "silver_x"},
                "output_table_name": "y",
            },
        ],
    }


# ---------------------------------------------------------------------------
# parse_selector
# ---------------------------------------------------------------------------

class TestParseSelector:
    @pytest.mark.parametrize(
        "raw,expected",
        [
            ("silver_x",   (False, "silver_x", False)),
            ("+silver_x",  (True,  "silver_x", False)),
            ("silver_x+",  (False, "silver_x", True)),
            ("+silver_x+", (True,  "silver_x", True)),
            ("  +x+  ",    (True,  "x",        True)),  # whitespace trimmed
        ],
    )
    def test_parses_known_forms(self, raw, expected):
        assert parse_selector(raw) == expected

    @pytest.mark.parametrize("raw", ["", "+", "++", "  ", " + "])
    def test_rejects_empty_selectors(self, raw):
        with pytest.raises(ValueError):
            parse_selector(raw)


# ---------------------------------------------------------------------------
# build_graph
# ---------------------------------------------------------------------------

class TestBuildGraph:
    def test_collects_every_job(self, config):
        jobs, _edges = build_graph(config)
        assert set(jobs) == {
            "flatfile_raw_a",
            "flatfile_raw_b",
            "bronze_a",
            "bronze_b",
            "silver_x",
            "gold_y",
        }

    def test_resolves_upstream_edges(self, config):
        _jobs, edges = build_graph(config)
        assert edges["silver_x"] == {"bronze_a", "bronze_b"}
        assert edges["gold_y"] == {"silver_x"}
        assert edges["bronze_a"] == {"flatfile_raw_a", "flatfile_raw_b"}

    def test_ignores_external_sources(self, config):
        """`bronze_b` reads from `raw.shared_table` — not a job in this
        project. It should produce zero upstream edges, not a phantom one.
        """
        _jobs, edges = build_graph(config)
        assert edges["bronze_b"] == set()

    def test_handles_empty_layer_section(self):
        cfg = {
            "layers": {"bronze": "etl/0_bronze", "silver": "etl/1_silver"},
            "bronze": [],
            "silver": None,
        }
        jobs, edges = build_graph(cfg)
        assert jobs == {}
        assert edges == {}


# ---------------------------------------------------------------------------
# resolve_name
# ---------------------------------------------------------------------------

class TestResolveName:
    def test_full_job_id(self, config):
        jobs, _ = build_graph(config)
        assert resolve_name(jobs, "silver_x") == "silver_x"

    def test_bare_output_table_name_when_unique(self, config):
        jobs, _ = build_graph(config)
        # `x` is only in silver — bare form must resolve.
        assert resolve_name(jobs, "x") == "silver_x"

    def test_ambiguous_bare_name_raises(self):
        # Same output_table_name 'orders' in two layers — ambiguous.
        cfg = {
            "layers": {"bronze": "etl/0_bronze", "silver": "etl/1_silver"},
            "bronze": [{"module": "b_o", "input_tables": {}, "output_table_name": "orders"}],
            "silver": [{"module": "s_o", "input_tables": {"x": "bronze_orders"}, "output_table_name": "orders"}],
        }
        jobs, _ = build_graph(cfg)
        with pytest.raises(ValueError, match="ambiguous"):
            resolve_name(jobs, "orders")

    def test_layer_constraint_disambiguates(self):
        cfg = {
            "layers": {"bronze": "etl/0_bronze", "silver": "etl/1_silver"},
            "bronze": [{"module": "b_o", "input_tables": {}, "output_table_name": "orders"}],
            "silver": [{"module": "s_o", "input_tables": {"x": "bronze_orders"}, "output_table_name": "orders"}],
        }
        jobs, _ = build_graph(cfg)
        assert resolve_name(jobs, "orders", layer="bronze") == "bronze_orders"
        assert resolve_name(jobs, "orders", layer="silver") == "silver_orders"

    def test_unknown_name_raises(self, config):
        jobs, _ = build_graph(config)
        with pytest.raises(ValueError, match="No job matches"):
            resolve_name(jobs, "nonexistent")


# ---------------------------------------------------------------------------
# select — the full pipeline
# ---------------------------------------------------------------------------

class TestSelect:
    def _ids(self, plan):
        return [f"{layer}_{job['output_table_name']}" for layer, job in plan]

    def test_plain_name_returns_only_target(self, config):
        plan = select(config, "silver_x")
        assert self._ids(plan) == ["silver_x"]

    def test_upstream_prefix(self, config):
        plan = select(config, "+silver_x")
        ids = self._ids(plan)
        # Must include silver_x and every transitive parent, in YAML order.
        assert set(ids) == {"flatfile_raw_a", "flatfile_raw_b", "bronze_a", "bronze_b", "silver_x"}
        # Layers must appear in declaration order: flatfile → bronze → silver.
        assert ids.index("flatfile_raw_a") < ids.index("bronze_a") < ids.index("silver_x")

    def test_downstream_suffix(self, config):
        plan = select(config, "bronze_a+")
        ids = self._ids(plan)
        assert set(ids) == {"bronze_a", "silver_x", "gold_y"}
        assert ids == ["bronze_a", "silver_x", "gold_y"]  # exact order

    def test_both_directions(self, config):
        plan = select(config, "+silver_x+")
        ids = self._ids(plan)
        assert set(ids) == {
            "flatfile_raw_a", "flatfile_raw_b",
            "bronze_a", "bronze_b",
            "silver_x", "gold_y",
        }

    def test_bare_name_resolves(self, config):
        plan = select(config, "+x")
        ids = self._ids(plan)
        assert "silver_x" in ids

    def test_layer_constraint(self, config):
        # `a` only exists as bronze_a — layer hint just narrows the search,
        # the selector must still resolve unambiguously.
        plan = select(config, "a+", layer="bronze")
        ids = self._ids(plan)
        assert ids == ["bronze_a", "silver_x", "gold_y"]

    def test_unknown_table_raises(self, config):
        with pytest.raises(ValueError, match="No job matches"):
            select(config, "+nope+")
