"""`bolt init` — interactive project scaffolder.

Asks the user about architecture, engine, runtime, execution env, and ML,
then materializes a project tree under the given target directory.
"""

from __future__ import annotations

import shutil
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import questionary

from bolt_pipeliner.generators._paths import PACKAGE_ROOT

SCAFFOLD_DIR = PACKAGE_ROOT / "templates" / "scaffold"

# Where the vendored copy of bolt_pipeliner lands inside a scaffolded project.
# The shims at the project root prepend this to sys.path, so this copy wins
# over any pip-installed bolt_pipeliner — the project is reproducible even if
# bolt_pipeliner is later yanked from PyPI or the user's machine.
VENDOR_DIRNAME = "_boltpipeliner"

ARCHITECTURE_LAYERS: dict[str, list[str]] = {
    "flat": ["flatfile"],
    "medallion (bronze, silver, gold)": ["flatfile", "bronze", "silver", "gold"],
    "diamond (bronze, silver, gold, diamond)": [
        "flatfile",
        "bronze",
        "silver",
        "gold",
        "diamond",
    ],
    "custom": [],  # filled in interactively
}

ENGINE_CHOICES = ["pyspark", "pandas", "polars"]
SPARK_PROFILES = ["local", "databricks", "emr", "glue", "gcp", "azure", "k8s"]
EXECUTION_ENVS = ["terminal", "notebook", "airflow", "databricks-jobs"]

PROFILE_LOCATION_DEFAULTS: dict[str, tuple[str, str]] = {
    "local": ("data/layers", "data/flatfiles"),
    "databricks": (
        "dbfs:/<path>/layers",
        "dbfs:/<path>/flatfiles",
    ),
    "gcp": ("gs://<bucket>/<path>/layers/", "gs://<bucket>/<path>/flatfiles/"),
    "azure": (
        "abfss://<container>@<storage-account>.dfs.core.windows.net/<path>/layers/",
        "abfss://<container>@<storage-account>.dfs.core.windows.net/<path>/flatfiles/",
    ),
    "emr": ("s3://<bucket>/<path>/layers/", "s3://<bucket>/<path>/flatfiles/"),
    "glue": ("s3://<bucket>/<path>/layers/", "s3://<bucket>/<path>/flatfiles/"),
    "k8s": ("s3://<bucket>/<path>/layers/", "s3://<bucket>/<path>/flatfiles/"),
}

ENGINE_TO_BASE_CLASS = {
    "pyspark": "ETLBase",
    "pandas": "ETLBaseParquetPandas",
    "polars": "ETLBaseParquetPolars",
}

LAYER_DIR_NAMES = {
    "flatfile": "_flatfile",
    "bronze": "0_bronze",
    "silver": "1_silver",
    "gold": "2_gold",
    "diamond": "3_diamond",
}

# Canonical color palette for `bolt generate documentation`. Layers not listed
# fall back to a neutral gray; users override per project in style_config.yaml.
LAYER_COLOR_PALETTE: dict[str, tuple[str, str]] = {
    "raw":      ("#3D1313", "#EBA5A5"),
    "flatfile": ("#333333", "#EFEFEF"),
    "bronze":   ("#EB661F", "#FFF1E7"),
    "silver":   ("#A1AAA4", "#FCFDFD"),
    "gold":     ("#90A42D", "#F7F8E9"),
    "diamond":  ("#7B68EE", "#F0EDFA"),
}
_NEUTRAL_LAYER_COLORS = ("#666666", "#EEEEEE")


@dataclass
class InitAnswers:
    project_name: str
    target_dir: Path
    layers: list[str]
    engine: str
    spark_profile: Optional[str]
    execution_env: str
    enable_ml: bool
    output_location: str
    flatfile_location: str
    vendor: bool = True
    extra_layer_names: list[str] = field(default_factory=list)


def _default_data_locations(engine: str, spark_profile: Optional[str], project_name: str) -> tuple[str, str]:
    del project_name
    if engine != "pyspark":
        output_template, flatfile_template = PROFILE_LOCATION_DEFAULTS["local"]
    else:
        profile_key = spark_profile or "local"
        output_template, flatfile_template = PROFILE_LOCATION_DEFAULTS.get(
            profile_key,
            PROFILE_LOCATION_DEFAULTS["emr"],
        )

    return output_template, flatfile_template


def _ask_interactive(
    project_name: str, target_dir: Path, vendor: bool = True
) -> InitAnswers:
    architecture = questionary.select(
        "Project architecture?",
        choices=list(ARCHITECTURE_LAYERS.keys()),
        default="medallion (bronze, silver, gold)",
    ).unsafe_ask()

    if architecture == "custom":
        raw = questionary.text(
            "Layer names (comma-separated, in execution order). "
            "Example: flatfile, raw, curated, model",
            default="flatfile, raw, curated",
        ).unsafe_ask()
        layers = [name.strip() for name in raw.split(",") if name.strip()]
    else:
        layers = ARCHITECTURE_LAYERS[architecture]

    engine = questionary.select(
        "Default engine?",
        choices=ENGINE_CHOICES,
        default="pyspark",
    ).unsafe_ask()

    spark_profile = None
    if engine == "pyspark":
        spark_profile = questionary.select(
            "Spark runtime profile?",
            choices=SPARK_PROFILES,
            default="local",
        ).unsafe_ask()

    default_output_location, default_flatfile_location = _default_data_locations(
        engine,
        spark_profile,
        project_name,
    )

    output_location = (
        questionary.text(
            "Where should transformed datasets be stored? (URI or local path)",
            default=default_output_location,
        ).unsafe_ask()
        or default_output_location
    )

    flatfile_location = default_flatfile_location
    if "flatfile" in layers:
        flatfile_location = (
            questionary.text(
                "Where should flatfiles be read from? (URI or local path)",
                default=default_flatfile_location,
            ).unsafe_ask()
            or default_flatfile_location
        )

    execution_env = questionary.select(
        "Where will the pipeline run?",
        choices=EXECUTION_ENVS,
        default="terminal",
    ).unsafe_ask()

    enable_ml = questionary.confirm(
        "Include an ML training layer (models/ + model_notebooks/)?",
        default=False,
    ).unsafe_ask()

    # By convention ML jobs live in the diamond layer (downstream of gold),
    # because they consume curated features and emit predictions/artifacts
    # rather than analytics tables. Offer to add it if the user enabled ML
    # but picked an architecture without it.
    if enable_ml and "diamond" not in layers:
        print(
            "\nML training jobs are best placed in a `diamond` layer — it sits\n"
            "downstream of gold and is the conventional home for model artifacts\n"
            "and prediction tables.\n"
        )
        add_diamond = questionary.confirm(
            "Add a diamond layer to host ML jobs?",
            default=True,
        ).unsafe_ask()
        if add_diamond:
            layers.append("diamond")

    return InitAnswers(
        project_name=project_name,
        target_dir=target_dir,
        layers=layers,
        engine=engine,
        spark_profile=spark_profile,
        execution_env=execution_env,
        enable_ml=enable_ml,
        output_location=output_location,
        flatfile_location=flatfile_location,
        vendor=vendor,
    )


def _preset_answers(
    preset: str, project_name: str, target_dir: Path, vendor: bool = True
) -> InitAnswers:
    if preset == "minimal":
        output_location, flatfile_location = _default_data_locations(
            "pandas",
            None,
            project_name,
        )
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            vendor=vendor,
            layers=["flatfile", "bronze"],
            engine="pandas",
            spark_profile=None,
            execution_env="terminal",
            enable_ml=False,
            output_location=output_location,
            flatfile_location=flatfile_location,
        )
    if preset == "medallion":
        output_location, flatfile_location = _default_data_locations(
            "pyspark",
            "local",
            project_name,
        )
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            vendor=vendor,
            layers=["flatfile", "bronze", "silver", "gold"],
            engine="pyspark",
            spark_profile="local",
            execution_env="terminal",
            enable_ml=False,
            output_location=output_location,
            flatfile_location=flatfile_location,
        )
    if preset == "diamond":
        output_location, flatfile_location = _default_data_locations(
            "pyspark",
            "local",
            project_name,
        )
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            vendor=vendor,
            layers=["flatfile", "bronze", "silver", "gold", "diamond"],
            engine="pyspark",
            spark_profile="local",
            execution_env="airflow",
            enable_ml=True,
            output_location=output_location,
            flatfile_location=flatfile_location,
        )
    if preset == "pandas":
        output_location, flatfile_location = _default_data_locations(
            "pandas",
            None,
            project_name,
        )
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            vendor=vendor,
            layers=["flatfile", "bronze", "silver", "gold"],
            engine="pandas",
            spark_profile=None,
            execution_env="notebook",
            enable_ml=False,
            output_location=output_location,
            flatfile_location=flatfile_location,
        )
    if preset == "polars":
        output_location, flatfile_location = _default_data_locations(
            "polars",
            None,
            project_name,
        )
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            vendor=vendor,
            layers=["flatfile", "bronze", "silver", "gold"],
            engine="polars",
            spark_profile=None,
            execution_env="notebook",
            enable_ml=False,
            output_location=output_location,
            flatfile_location=flatfile_location,
        )
    raise ValueError(
        f"Unknown preset '{preset}'. Valid: minimal, medallion, diamond, pandas, polars."
    )


def _layer_dir(layer_name: str) -> str:
    return LAYER_DIR_NAMES.get(layer_name, layer_name)


def _render_etl_config(ans: InitAnswers) -> str:
    base_class = ENGINE_TO_BASE_CLASS[ans.engine]
    layers_yaml = "\n".join(
        f"  {name}: etl/{_layer_dir(name)}" for name in ans.layers
    )
    flatfile_section = ""
    if "flatfile" in ans.layers:
        flatfile_section = (
            "\nflatfile:\n"
            "  - module: flatfile_example\n"
            "    description: \"Example flatfile ingestion job — replace with yours.\"\n"
            f"    class_name: {base_class}\n"
            "    input_tables:\n"
            "      example: \"example.csv\"\n"
            "    output_table_name: example\n"
        )

    other_sections = ""
    for layer in ans.layers:
        if layer == "flatfile":
            continue
        other_sections += f"\n{layer}: []\n"

    return (
        "configs:\n"
        f"  output_location: \"{ans.output_location}\"\n"
        f"  flatfile_location: \"{ans.flatfile_location}\"\n"
        f"  schema: {ans.project_name}\n"
        "  catalog: dev_catalog\n"
        "  incremental_column: year_month\n"
        "\n"
        "layers:\n"
        f"{layers_yaml}\n"
        f"{flatfile_section}"
        f"{other_sections}"
    )


_LAYER_LOAD_NOTES = {
    "flatfile": (
        "        Flatfile values are *relative file paths* under `configs.flatfile_location`\n"
        "        (set in etl_config.yaml). Extension picks the reader: `.csv`, `.parquet`,\n"
        "        Excel (`.xlsx`/`.xls`), or JSON (`.json`/`.jsonl`).\n"
        "        Example value: `\"raw/storm_events.csv\"`.\n"
    ),
    "bronze": (
        "        Bronze values can take two shapes:\n"
        "          • `\"<schema>.<table>\"` (contains a dot) → read from the project's\n"
        "            *shared* catalog (raw ingestion zone, e.g. `raw.crm_account`).\n"
        "          • `\"<bare_table>\"` → read from the project's own catalog like a\n"
        "            silver/gold job. Useful for chained bronze refinements.\n"
    ),
}
_DEFAULT_LAYER_LOAD_NOTE = (
    "        {layer} values must match another job's `output_table_name` prefixed by\n"
    "        its layer, e.g. `bronze_orders`, `silver_fct_sales`. The runner reads\n"
    "        them from the project catalog (configs.catalog + configs.schema).\n"
)


def _layer_load_note(layer: str) -> str:
    return _LAYER_LOAD_NOTES.get(layer, _DEFAULT_LAYER_LOAD_NOTE.format(layer=layer))


def _render_example_job(layer: str, engine: str) -> str:
    """Render a tutorial-style example job for the given layer + engine.

    Most users haven't seen the ETLBase / input_tables contract before, so the
    generated file is deliberately heavy on comments. It explains:
      - what `self` is (an ETLBase instance, with the runtime attributes wired on)
      - what `input_tables` is (a dict keyed by YAML aliases → preloaded DataFrames)
      - how the YAML in etl_config.yaml maps to runtime
      - the return-value contract (DataFrame, or empty DF + unload:false)
    """
    if engine == "pyspark":
        engine_import = "from pyspark.sql import functions as F"
        df_type = "pyspark.sql.DataFrame"
        transform_body = (
            "    # 3. Transform. Below: tag every row with the run timestamp.\n"
            "    #    Real jobs do joins, aggregations, window functions, etc.\n"
            "    return df.withColumn(\"processed_at\", F.current_timestamp())\n"
        )
    elif engine == "pandas":
        engine_import = "import pandas as pd"
        df_type = "pandas.DataFrame"
        transform_body = (
            "    # 3. Transform. Below: tag every row with the run timestamp.\n"
            "    #    Real jobs do groupby/merge/assign, etc.\n"
            "    df = df.copy()\n"
            "    df[\"processed_at\"] = pd.Timestamp.utcnow()\n"
            "    return df\n"
        )
    else:  # polars
        engine_import = "import datetime as dt\nimport polars as pl"
        df_type = "polars.DataFrame"
        transform_body = (
            "    # 3. Transform. Below: tag every row with the run timestamp.\n"
            "    #    Real jobs use .group_by / .join / .with_columns chains.\n"
            "    return df.with_columns(\n"
            "        pl.lit(dt.datetime.utcnow()).alias(\"processed_at\")\n"
            "    )\n"
        )

    load_note = _layer_load_note(layer)

    return (
        f"\"\"\"Example {layer} ETL job — tutorial template.\n"
        "\n"
        "Read the comments below, then replace the body with your own logic. Once\n"
        "your YAML entry exists in configs/etl_config.yaml, run it with:\n"
        "\n"
        f"    python main.py --{layer}          # only this layer\n"
        "    python main.py                  # all layers, in dependency order\n"
        "\n"
        "For this module to actually execute, configs/etl_config.yaml must have an\n"
        f"entry under `{layer}:` whose `module:` value matches this filename (without\n"
        "`.py`) and whose `input_tables:` block declares the aliases this function reads.\n"
        "\"\"\"\n"
        f"{engine_import}\n"
        "\n"
        "\n"
        "def process_data(self, input_tables):\n"
        f"    \"\"\"Transform `input_tables` and return a {df_type}.\n"
        "\n"
        "    Parameters\n"
        "    ----------\n"
        "    self : ETLBase\n"
        "        The runtime instance — your function gets monkey-patched onto it,\n"
        "        so `self` exposes everything the base class set up. Useful bits:\n"
        "          • self.spark              SparkSession (Spark bases only)\n"
        "          • self.year_months       Incremental window (list[int] of YYYYMM)\n"
        "                                    or None on a full refresh.\n"
        "          • self.partition_by      Echo of the YAML `partition_by:` list.\n"
        "          • self.incremental       Echo of the YAML `incremental:` flag.\n"
        "          • self._create_table(df) / self._replace_table_partitions(df)\n"
        "                                    Manual write helpers — pair with\n"
        "                                    `unload: false` for memory-heavy jobs.\n"
        "\n"
        f"    input_tables : dict[str, {df_type}]\n"
        "        **THIS IS NOT YOUR JOB'S INPUT FILES.** It is a dict of\n"
        "        already-loaded DataFrames, keyed by the *aliases you declared in\n"
        "        etl_config.yaml* under this job's `input_tables:` block. ETLBase\n"
        "        loaded each value before calling you.\n"
        "\n"
        "        Example YAML:\n"
        "\n"
        f"            {layer}:\n"
        f"              - module: {('flatfile_example' if layer == 'flatfile' else f'{layer}_example')}\n"
        "                input_tables:\n"
        "                  raw_orders: bronze_orders          # ← alias: source\n"
        "                  customers:  silver_dim_customers\n"
        "                output_table_name: example\n"
        "\n"
        "        At runtime, `input_tables` becomes:\n"
        "\n"
        "            {\n"
        "                \"raw_orders\": <DataFrame loaded from bronze_orders>,\n"
        "                \"customers\":  <DataFrame loaded from silver_dim_customers>,\n"
        "            }\n"
        "\n"
        "        Where the values come from depends on the layer:\n"
        f"{load_note}"
        "\n"
        "    Returns\n"
        "    -------\n"
        f"    {df_type}\n"
        f"        Persisted by ETLBase to `{layer}_<output_table_name>` (partitioned\n"
        "        per `partition_by:`). If `unload: false` is set in the YAML, write\n"
        "        partitions yourself via `self._create_table` /\n"
        "        `self._replace_table_partitions` and return an empty DataFrame\n"
        "        instead — ETLBase will skip its own unload step.\n"
        "    \"\"\"\n"
        "    # 1. Pick a table by its declared YAML alias. The example below grabs\n"
        "    #    whichever table happens to be first — in a real job you should\n"
        "    #    reference aliases explicitly:\n"
        "    #        orders = input_tables[\"raw_orders\"]\n"
        "    #        customers = input_tables[\"customers\"]\n"
        "    df = next(iter(input_tables.values()))\n"
        "\n"
        "    # 2. (Optional) Filter to the incremental window if applicable.\n"
        "    #    `self.year_months` is None on a full refresh and a list of YYYYMM\n"
        "    #    ints on an incremental run.\n"
        "    #    if self.year_months:\n"
        "    #        df = df.filter(df[\"year_month\"].isin(self.year_months))\n"
        "\n"
        f"{transform_body}"
    )


def _render_style_config(layers: list[str]) -> str:
    """Render a `configs/style_config.yaml` consumed by `bolt generate documentation`.

    The layer color block adapts to whichever layers the user chose at
    `bolt init` time. Unknown layer names get a neutral gray; users can
    edit the hex codes later.
    """
    # `raw` is always emitted because the docs generator uses it for upstream
    # source nodes in the Mermaid diagrams, regardless of the project's layers.
    layer_names: list[str] = ["raw"] + [name for name in layers if name != "raw"]

    layer_lines: list[str] = []
    for name in layer_names:
        stroke, fill = LAYER_COLOR_PALETTE.get(name, _NEUTRAL_LAYER_COLORS)
        layer_lines.append(f"    {name}:")
        layer_lines.append(f"      stroke: \"{stroke}\"")
        layer_lines.append(f"      fill:   \"{fill}\"")
    layers_block = "\n".join(layer_lines)

    return (
        "# Colors + branding consumed by `bolt generate documentation`.\n"
        "# Edit hex codes freely. `layers_colors` keys must match the layer\n"
        "# names declared under `layers:` in etl_config.yaml.\n"
        "style_colors:\n"
        "  layers_colors:\n"
        f"{layers_block}\n"
        "\n"
        "  body_background: \"#353535ff\"\n"
        "  body_text: \"#500b3aff\"\n"
        "\n"
        "  panel_background: \"#f7f7f7\"\n"
        "  panel_border: \"#ddd\"\n"
        "  panel_shadow: \"rgba(0,0,0,.35)\"\n"
        "\n"
        "  heading_accent: \"#d54e62ff\"\n"
        "  rule_color: \"#1f2937\"\n"
        "\n"
        "  meta_text: \"#94a3b8\"\n"
        "  table_cell_text: \"#555555\"\n"
        "  link_text: \"#60a5fa\"\n"
        "\n"
        "  btn_bg: \"#d54e62ff\"\n"
        "  btn_text: \"#fff\"\n"
        "  btn_hover_bg: \"#b8122d\"\n"
        "\n"
        "  code_bg: \"#353535ff\"\n"
        "  code_border: \"#353535ff\"\n"
        "  code_inset_highlight: \"rgba(255,255,255,.03)\"\n"
        "\n"
        "  fade_top: \"rgba(11,16,33,0)\"\n"
        "  fade_mid: \"rgba(11,16,33,.9)\"\n"
        "  fade_bottom: \"rgba(11,16,33,1)\"\n"
        "\n"
        "  mermaid_text: \"#111111\"\n"
        "  mermaid_line: \"#888888\"\n"
        "  mermaid_tertiary: \"#aaaaaa\"\n"
        "\n"
        "  index_accent: \"#e31837\"\n"
        "  index_accent_hover: \"#b8122d\"\n"
        "  index_text: \"#1a1a1a\"\n"
        "  index_page_bg: \"#ffffff\"\n"
        "  index_panel_bg: \"#f7f7f7\"\n"
        "  index_border: \"#e5e5e5\"\n"
    )


def _render_spark_profile_toml(profile: str) -> str:
    if profile == "local":
        return (
            "[runtime]\n"
            "target = \"local\"\n"
            "\n"
            "[spark]\n"
            "\"spark.sql.shuffle.partitions\" = 200\n"
            "\"spark.serializer\" = \"org.apache.spark.serializer.KryoSerializer\"\n"
        )
    return (
        f"[runtime]\n"
        f"target = \"{profile}\"\n"
        "\n"
        f"# TODO: configure {profile} Spark profile. See bolt_pipeliner/sessions/{profile}.py.\n"
        "\n"
        "[spark]\n"
    )


def _render_macros_init() -> str:
    return (
        "\"\"\"Project-local reusable transforms. Import from your jobs as\n"
        "`from macros.dates import month_floor` etc.\n"
        "\"\"\"\n"
    )


def _render_ml_example(engine: str) -> str:
    """Render `models/train_example.py`.

    Bakes in MLflow load/save patterns as a *suggestion*, not a hard dep —
    `mlflow` is import-gated so the file is still valid Python without it
    installed. Users uncomment + extend as needed.
    """
    return (
        "\"\"\"Example ML training job.\n"
        "\n"
        f"Engine: {engine}. Iterate interactively in `model_notebooks/`, then\n"
        "promote the stable bits to this module.\n"
        "\n"
        "MLflow integration is optional but recommended for anything beyond\n"
        "throwaway experiments — uncomment the `mlflow.*` calls below to get\n"
        "experiment tracking and a model registry. Point MLflow at a tracking\n"
        "server via the MLFLOW_TRACKING_URI env var (local sqlite, hosted,\n"
        "Databricks, etc). See model_notebooks/README.md for details.\n"
        "\"\"\"\n"
        "from __future__ import annotations\n"
        "\n"
        "# import mlflow              # pip install mlflow\n"
        "# import mlflow.sklearn      # swap for mlflow.pytorch / .pyfunc / ...\n"
        "\n"
        "MODEL_NAME = \"example_model\"\n"
        "\n"
        "\n"
        "def train(features):\n"
        "    \"\"\"Fit a model on `features` and return it.\n"
        "\n"
        "    Suggested MLflow flow:\n"
        "        with mlflow.start_run():\n"
        "            mlflow.log_param(\"engine\", \"" + engine + "\")\n"
        "            mlflow.log_metric(\"auc\", auc)\n"
        "            mlflow.sklearn.log_model(\n"
        "                model,\n"
        "                artifact_path=\"model\",\n"
        "                registered_model_name=MODEL_NAME,\n"
        "            )\n"
        "    \"\"\"\n"
        "    raise NotImplementedError\n"
        "\n"
        "\n"
        "def load_latest(stage: str = \"Production\"):\n"
        "    \"\"\"Pull the latest registered version of MODEL_NAME from MLflow.\n"
        "\n"
        "    Example:\n"
        "        model = mlflow.pyfunc.load_model(\n"
        "            f\"models:/{MODEL_NAME}/{stage}\"\n"
        "        )\n"
        "        return model\n"
        "\n"
        "    Falls back to local disk if MLflow isn't configured.\n"
        "    \"\"\"\n"
        "    raise NotImplementedError\n"
    )


def _render_model_notebooks_readme(engine: str) -> str:
    return (
        "# Model notebooks\n\n"
        "Scratch space for ML experimentation — feature exploration, model\n"
        "selection, hyperparameter tuning, evaluation. Once a notebook flow\n"
        f"stabilizes, port it to `models/train_example.py` (engine: **{engine}**)\n"
        "and call it from a `diamond/` ETL job so it runs on a schedule.\n\n"
        "## Suggested workflow\n\n"
        "1. Pull curated features from the `gold` layer.\n"
        "2. Iterate on a model in `train_example.ipynb`.\n"
        "3. **Track experiments with MLflow** (optional but recommended):\n\n"
        "   ```bash\n"
        "   pip install mlflow\n"
        "   export MLFLOW_TRACKING_URI=sqlite:///mlflow.db   # local quickstart\n"
        "   # or point at your team's tracking server:\n"
        "   # export MLFLOW_TRACKING_URI=https://mlflow.your-org.example.com\n"
        "   mlflow ui   # open http://localhost:5000\n"
        "   ```\n\n"
        "   In your notebook:\n\n"
        "   ```python\n"
        "   import mlflow, mlflow.sklearn\n"
        "   with mlflow.start_run():\n"
        "       mlflow.log_param(\"lr\", 0.01)\n"
        "       mlflow.log_metric(\"auc\", 0.87)\n"
        "       mlflow.sklearn.log_model(model, artifact_path=\"model\",\n"
        "                                 registered_model_name=\"example_model\")\n"
        "   ```\n\n"
        "4. Promote the best run in the MLflow registry, then load it from\n"
        "   `models/train_example.py::load_latest()` so the diamond-layer ETL\n"
        "   job picks up new versions without code changes.\n\n"
        "## Without MLflow\n\n"
        "If you don't want the dep, pickle models to S3/GCS/disk under a\n"
        "versioned path and load by hash. The `load_latest()` stub is the\n"
        "right place to wire that in.\n"
    )


def _render_model_notebook_ipynb(engine: str) -> str:
    """Hand-rolled nbformat-4 JSON. Keeps the scaffold dependency-free
    (no `nbformat` import at scaffold time)."""
    import json

    engine_import = {
        "pyspark": "from pyspark.sql import SparkSession",
        "pandas": "import pandas as pd",
        "polars": "import polars as pl",
    }.get(engine, "# import your engine of choice")

    nb = {
        "cells": [
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    f"# Example training notebook ({engine})\n",
                    "\n",
                    "Scratch space for iterating on a model. Once stable,\n",
                    "promote the flow to `models/train_example.py`.\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    f"{engine_import}\n",
                    "\n",
                    "# Optional but recommended: track experiments with MLflow.\n",
                    "# import mlflow, mlflow.sklearn\n",
                    "# mlflow.set_experiment(\"example_model\")\n",
                ],
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## 1. Load curated features from the gold layer\n"],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# TODO: replace with your gold-layer feature table.\n",
                    "features = None\n",
                ],
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": ["## 2. Train + log to MLflow\n"],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# with mlflow.start_run():\n",
                    "#     model = ...   # fit\n",
                    "#     mlflow.log_metric(\"auc\", 0.0)\n",
                    "#     mlflow.sklearn.log_model(\n",
                    "#         model,\n",
                    "#         artifact_path=\"model\",\n",
                    "#         registered_model_name=\"example_model\",\n",
                    "#     )\n",
                ],
            },
            {
                "cell_type": "markdown",
                "metadata": {},
                "source": [
                    "## 3. Load the latest registered version\n",
                    "\n",
                    "Use this from the diamond-layer ETL job so it always picks\n",
                    "up the freshest registered model.\n",
                ],
            },
            {
                "cell_type": "code",
                "execution_count": None,
                "metadata": {},
                "outputs": [],
                "source": [
                    "# model = mlflow.pyfunc.load_model(\"models:/example_model/Production\")\n",
                ],
            },
        ],
        "metadata": {
            "kernelspec": {
                "display_name": "Python 3",
                "language": "python",
                "name": "python3",
            },
            "language_info": {"name": "python"},
        },
        "nbformat": 4,
        "nbformat_minor": 5,
    }
    return json.dumps(nb, indent=2) + "\n"


# ---------------------------------------------------------------------------
# Self-contained entry-point shims
# ---------------------------------------------------------------------------
#
# Every scaffolded project gets `bolt.py`, `main.py`, and `generate.py` at the
# root. They prepend `_vendor/` to sys.path before importing bolt_pipeliner, so
# the project runs even on machines where bolt_pipeliner isn't pip-installed.
# When `vendor=False`, the prepend still runs (cheaply); it just resolves to a
# missing dir and Python falls back to the installed package.

_SHIM_BOOTSTRAP = (
    "import pathlib\n"
    "import sys\n"
    "\n"
    f"_VENDOR = pathlib.Path(__file__).resolve().parent / \"{VENDOR_DIRNAME}\"\n"
    "if _VENDOR.is_dir():\n"
    "    sys.path.insert(0, str(_VENDOR))\n"
)


def _render_bolt_shim() -> str:
    """`python bolt.py <subcommand> ...` — full CLI surface."""
    return (
        "#!/usr/bin/env python\n"
        "\"\"\"Self-contained `bolt` entry point.\n"
        "\n"
        f"Uses the vendored copy of bolt_pipeliner under ./{VENDOR_DIRNAME}/ when present,\n"
        "falling back to a pip-installed copy otherwise. Forwards all CLI args.\n"
        "\"\"\"\n"
        "from __future__ import annotations\n"
        "\n"
        f"{_SHIM_BOOTSTRAP}"
        "\n"
        "from bolt_pipeliner.cli.app import main\n"
        "\n"
        "if __name__ == \"__main__\":\n"
        "    main()\n"
    )


def _render_run_shim() -> str:
    """`python main.py [--bronze ...]` — convenience for `bolt run`."""
    return (
        "#!/usr/bin/env python\n"
        "\"\"\"Run the pipeline. Thin wrapper around `bolt run`.\n"
        "\n"
        "Examples:\n"
        "    python main.py                # all layers\n"
        "    python main.py --bronze       # bronze layer only\n"
        "    python main.py --silver --gold\n"
        "\"\"\"\n"
        "from __future__ import annotations\n"
        "\n"
        f"{_SHIM_BOOTSTRAP}"
        "\n"
        "from bolt_pipeliner.cli.app import app\n"
        "\n"
        "if __name__ == \"__main__\":\n"
        "    app([\"run\", *sys.argv[1:]])\n"
    )


def _render_generate_shim() -> str:
    """`python generate.py <target>` — convenience for `bolt generate`."""
    return (
        "#!/usr/bin/env python\n"
        "\"\"\"Regenerate downstream artifacts. Thin wrapper around `bolt generate`.\n"
        "\n"
        "Examples:\n"
        "    python generate.py all\n"
        "    python generate.py documentation\n"
        "    python generate.py airflow notebook\n"
        "\"\"\"\n"
        "from __future__ import annotations\n"
        "\n"
        f"{_SHIM_BOOTSTRAP}"
        "\n"
        "from bolt_pipeliner.cli.app import app\n"
        "\n"
        "if __name__ == \"__main__\":\n"
        "    app([\"generate\", *sys.argv[1:]])\n"
    )


def _vendor_bolt_pipeliner(target_dir: Path) -> Path:
    """Copy the bolt_pipeliner package source into ``<project>/_vendor/bolt_pipeliner/``.

    Returns the destination directory. Skips ``__pycache__``, ``*.pyc``, and
    other Python build artifacts so the vendored tree stays small and clean.
    """
    dest = target_dir / VENDOR_DIRNAME / "bolt_pipeliner"
    dest.parent.mkdir(parents=True, exist_ok=True)

    shutil.copytree(
        PACKAGE_ROOT,
        dest,
        ignore=shutil.ignore_patterns(
            "__pycache__", "*.pyc", "*.pyo", ".pytest_cache", ".mypy_cache", ".DS_Store"
        ),
    )

    # Drop a marker so it's obvious to readers what _vendor/ is for and that
    # editing it is a footgun — they should edit the upstream package instead.
    readme = target_dir / VENDOR_DIRNAME / "README.md"
    readme.write_text(
        "# Vendored dependencies\n\n"
        "This directory contains a copy of `bolt_pipeliner` so the project can\n"
        "run end-to-end without `pip install bolt_pipeliner`.\n\n"
        "**Do not edit files under `bolt_pipeliner/` here.** They are overwritten\n"
        "on the next `bolt init --refresh-vendor` (or by re-running `bolt init`\n"
        "in an empty directory). Patch the upstream package instead.\n",
        encoding="utf-8",
    )
    return dest


def _write_file(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def _scaffold(ans: InitAnswers) -> list[Path]:
    """Materialize the project tree. Returns the list of written paths."""
    written: list[Path] = []
    root = ans.target_dir

    if root.exists() and any(root.iterdir()):
        raise FileExistsError(
            f"Target directory {root} exists and is not empty. "
            "Pick a fresh path or remove it first."
        )

    # etl_config.yaml
    cfg_path = root / "configs" / "etl_config.yaml"
    _write_file(cfg_path, _render_etl_config(ans))
    written.append(cfg_path)

    # style_config.yaml — required by `bolt generate documentation`
    style_path = root / "configs" / "style_config.yaml"
    _write_file(style_path, _render_style_config(ans.layers))
    written.append(style_path)

    # Per-layer directories with one example job each
    for layer in ans.layers:
        layer_dir = root / "etl" / _layer_dir(layer)
        layer_dir.mkdir(parents=True, exist_ok=True)
        (layer_dir / "__init__.py").write_text("", encoding="utf-8")
        module_name = (
            "flatfile_example" if layer == "flatfile" else f"{layer}_example"
        )
        job_path = layer_dir / f"{module_name}.py"
        _write_file(job_path, _render_example_job(layer, ans.engine))
        written.append(job_path)

    # Spark profile config (if applicable)
    if ans.spark_profile:
        spark_path = root / "configs" / "spark" / f"{ans.spark_profile}.toml"
        _write_file(spark_path, _render_spark_profile_toml(ans.spark_profile))
        written.append(spark_path)

    # Macros directory
    macros_path = root / "macros" / "__init__.py"
    _write_file(macros_path, _render_macros_init())
    written.append(macros_path)

    # Tests skeleton (pytest)
    tests_path = root / "tests" / "test_smoke.py"
    _write_file(
        tests_path,
        "def test_project_loads_config():\n"
        "    from bolt_pipeliner.config import load_config\n"
        "    config = load_config('configs/etl_config.yaml')\n"
        "    assert 'layers' in config\n",
    )
    written.append(tests_path)

    # Optional ML scaffolding — emits `models/` (production code) and
    # `model_notebooks/` (experimentation surface) side-by-side. MLflow is
    # baked in as a suggestion (commented imports + docstring patterns),
    # never as a hard dependency.
    if ans.enable_ml:
        ml_path = root / "models" / "train_example.py"
        _write_file(ml_path, _render_ml_example(ans.engine))
        written.append(ml_path)

        notebook_readme = root / "model_notebooks" / "README.md"
        _write_file(notebook_readme, _render_model_notebooks_readme(ans.engine))
        written.append(notebook_readme)

        notebook_path = root / "model_notebooks" / "train_example.ipynb"
        _write_file(notebook_path, _render_model_notebook_ipynb(ans.engine))
        written.append(notebook_path)

    # Self-contained entry-point shims (always emitted — they no-op gracefully
    # when the vendored dir is absent and bolt_pipeliner is pip-installed).
    bolt_shim = root / "bolt.py"
    _write_file(bolt_shim, _render_bolt_shim())
    written.append(bolt_shim)

    main_shim = root / "main.py"
    _write_file(main_shim, _render_run_shim())
    written.append(main_shim)

    generate_shim = root / "generate.py"
    _write_file(generate_shim, _render_generate_shim())
    written.append(generate_shim)

    # Vendor the bolt_pipeliner source so the project runs without
    # `pip install bolt_pipeliner`. Opt out via `bolt init --no-vendor`.
    if ans.vendor:
        vendor_dest = _vendor_bolt_pipeliner(root)
        written.append(vendor_dest)

    # README stub
    readme_path = root / "README.md"
    layers_str = ", ".join(ans.layers)
    vendor_note = (
        "This project ships with a vendored copy of `bolt_pipeliner` under "
        f"`{VENDOR_DIRNAME}/` so the shims work even without a pip install.\n\n"
        if ans.vendor
        else "Run `pip install bolt_pipeliner` before invoking the shims, or "
        "re-run `bolt init` without `--no-vendor` to bundle a copy.\n\n"
    )
    ml_note = ""
    if ans.enable_ml:
        ml_note = (
            "\n## ML\n\n"
            "- `models/` — production training/loading code (`train_example.py`)\n"
            "- `model_notebooks/` — experimentation surface for iterating on a model\n"
            "- Suggested home for ML ETL jobs: the **diamond** layer (downstream of gold).\n"
            "- **MLflow is recommended** for experiment tracking + a model registry.\n"
            "  See `model_notebooks/README.md` for the quickstart and how to point\n"
            "  at a tracking server.\n"
        )
    _write_file(
        readme_path,
        f"# {ans.project_name}\n\n"
        f"Bolt Pipeliner project — engine: **{ans.engine}**, layers: **{layers_str}**.\n\n"
        f"{vendor_note}"
        "## Usage\n\n"
        "```bash\n"
        "# Run the pipeline (all layers, or pass --bronze / --silver / ...)\n"
        "python main.py\n"
        "\n"
        "# Regenerate downstream artifacts (DAGs, docs, layer scripts, ...)\n"
        "python generate.py documentation\n"
        "python generate.py all\n"
        "\n"
        "# Or use the full CLI directly\n"
        "python bolt.py run --bronze\n"
        "python bolt.py test\n"
        "```\n"
        f"{ml_note}",
    )
    written.append(readme_path)

    return written


def execute(
    project_name: str,
    target_dir: Optional[Path] = None,
    preset: Optional[str] = None,
    vendor: bool = True,
) -> None:
    target = target_dir or Path(project_name)
    if preset:
        answers = _preset_answers(preset, project_name, target, vendor=vendor)
    else:
        answers = _ask_interactive(project_name, target, vendor=vendor)

    written = _scaffold(answers)

    print()
    print(f"✓ Created {len(written)} entries under {target}/")
    print()
    print("Next steps:")
    print(f"  cd {target}")
    if answers.vendor:
        print("  python main.py --help          # self-contained — no install needed")
        print("  python generate.py documentation")
    else:
        print("  pip install bolt_pipeliner")
        print("  python main.py --help")
