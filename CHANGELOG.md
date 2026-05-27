# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.2] - 2026-05-27

### Removed
- Removed Snowflake DDL generation from `bolt generate`; `all` now regenerates Airflow DAGs, documentation, layer scripts, and notebooks.

## [0.2.0] - 2026-05-26

### FIXED
Notebook generation spark configuration.

## [0.2.0] - 2026-05-26

### Added
- **Self-contained scaffolded projects.** `bolt init` now vendors a copy of `bolt_pipeliner` into `<project>/_boltpipeliner/` and emits three shim scripts at the project root: `bolt.py` (full CLI), `main.py` (`bolt run`), and `generate.py` (`bolt generate`). The shims prepend the vendored copy to `sys.path` so a fresh clone runs end-to-end with no `pip install bolt_pipeliner` required.
- `bolt init --vendor/--no-vendor` flag to opt out of vendoring for users who'd rather rely on a pip-installed copy.
- **`configs/style_config.yaml` is now always scaffolded** with a per-project color palette matching the chosen layers. `bolt generate documentation` requires this file, so the scaffold flow is no longer broken on fresh projects.
- **ML scaffolding.** When the user enables the ML training layer (or uses `--preset diamond`), `bolt init` now emits a `model_notebooks/` directory alongside `models/` with an engine-aware `train_example.ipynb` and a README that documents the MLflow experiment-tracking + model-registry quickstart.
- Diamond-layer hint: when ML is enabled but the chosen architecture has no `diamond` layer, the interactive wizard offers to add one (it's the conventional home for ML jobs downstream of `gold`).
- **Tutorial-style layer examples.** `bolt init` now generates `<layer>_example.py` files with a teaching docstring rather than a three-line stub. Each example explains: `self` is the ETLBase instance and which attributes are wired on (`self.spark`, `self.year_months`, `self.partition_by`, `self._create_table`); `input_tables` is a *dict keyed by YAML aliases* (not file paths) pointing at preloaded DataFrames; the YAML → runtime mapping; and the return contract (`unload: true/false`). The "where do values come from?" note is tailored per layer (flatfile = file paths under `configs.flatfile_bucket`; bronze = shared vs. own catalog; silver/gold/diamond = upstream `<layer>_<output_table_name>`).
- **dbt-style table selection for `bolt run`.** New `--select` / `-s` flag accepts:
  - `silver_orders` — just that job;
  - `+silver_orders` — upstream + target;
  - `silver_orders+` — target + downstream;
  - `+silver_orders+` — both sides.

  Bare `output_table_name` resolves automatically when unambiguous; use `--layer` / `-l` to disambiguate when the same name lives in multiple layers. `--layer` standalone is also a synonym for `--<layer>`. `--select` is mutually exclusive with the legacy `--bronze`/`--silver`/`--gold`/`--diamond`/`--flatfile` flags, which keep working unchanged. New module `bolt_pipeliner.selection` exposes `parse_selector`, `build_graph`, `resolve_name`, and `select` for programmatic use.
- MLflow patterns baked into `models/train_example.py` as commented-out suggestions (`MODEL_NAME` constant, `train()` + `load_latest()` stubs with MLflow call sites in docstrings).
- New optional dependency extra: `bolt_pipeliner[ml]` pulling in `mlflow>=2.10,<3`.

### Changed
- README expanded with two new sections — "Self-contained projects (vendored copy + shims)" and "ML training (`models/` + `model_notebooks/`)" — plus an updated project-layout tree and CLI reference covering `--vendor/--no-vendor`.
- "Documentation flow without Spark" instructions now use `python generate.py …` so they work in vendored projects without the `bolt` script on `$PATH`.
- The scaffolder's "Next steps" message now points at `python main.py --help` / `python generate.py documentation` when vendoring is enabled.
- Added `Topic :: Scientific/Engineering :: Information Analysis` and `Intended Audience :: Information Technology` classifiers.

[Unreleased]: https://github.com/ormenesse/boltdbt_pipeliner/compare/v0.2.2...HEAD
[0.2.2]: https://github.com/ormenesse/boltdbt_pipeliner/releases/tag/v0.2.2
[0.2.0]: https://github.com/ormenesse/boltdbt_pipeliner/releases/tag/v0.2.0
[0.1.0]: https://github.com/ormenesse/boltdbt_pipeliner/releases/tag/v0.1.0

## [0.1.0] - 2026-05-26

### Added
- Initial public release on PyPI.
- Config-driven ETL framework with PySpark, Pandas, and Polars engines.
- `bolt init` interactive scaffolder with `minimal`, `medallion`, `diamond`, `polars`, and `pyspark` presets.
- `bolt test` runner for dbt-style data-quality checks (`not_null`, `unique`, `row_count`, `schema`, `freshness`).
- Code generators for Airflow DAGs, HTML documentation, standalone layer scripts, Jupyter notebooks, and Snowflake DDLs.
- Lazy engine imports — installing `bolt_pipeliner` no longer pulls PySpark / Polars / Pandas.
- Optional dependency extras: `pandas`, `polars`, `spark`, `aws`, `excel`, `notebook`, `http`, `all`, `dev`.
