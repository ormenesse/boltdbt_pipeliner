"""`bolt init` — interactive project scaffolder.

Asks the user about architecture, engine, runtime, execution env, and ML,
then materializes a project tree under the given target directory.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import questionary

from bolt_pipeliner.generators._paths import PACKAGE_ROOT

SCAFFOLD_DIR = PACKAGE_ROOT / "templates" / "scaffold"

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


@dataclass
class InitAnswers:
    project_name: str
    target_dir: Path
    layers: list[str]
    engine: str
    spark_profile: Optional[str]
    execution_env: str
    enable_ml: bool
    extra_layer_names: list[str] = field(default_factory=list)


def _ask_interactive(project_name: str, target_dir: Path) -> InitAnswers:
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

    execution_env = questionary.select(
        "Where will the pipeline run?",
        choices=EXECUTION_ENVS,
        default="terminal",
    ).unsafe_ask()

    enable_ml = questionary.confirm(
        "Include an ML training layer (models/)?",
        default=False,
    ).unsafe_ask()

    return InitAnswers(
        project_name=project_name,
        target_dir=target_dir,
        layers=layers,
        engine=engine,
        spark_profile=spark_profile,
        execution_env=execution_env,
        enable_ml=enable_ml,
    )


def _preset_answers(preset: str, project_name: str, target_dir: Path) -> InitAnswers:
    if preset == "minimal":
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            layers=["flatfile", "bronze"],
            engine="pandas",
            spark_profile=None,
            execution_env="terminal",
            enable_ml=False,
        )
    if preset == "medallion":
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            layers=["flatfile", "bronze", "silver", "gold"],
            engine="pyspark",
            spark_profile="local",
            execution_env="terminal",
            enable_ml=False,
        )
    if preset == "diamond":
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            layers=["flatfile", "bronze", "silver", "gold", "diamond"],
            engine="pyspark",
            spark_profile="local",
            execution_env="airflow",
            enable_ml=True,
        )
    if preset == "pandas":
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            layers=["flatfile", "bronze", "silver", "gold"],
            engine="pandas",
            spark_profile=None,
            execution_env="notebook",
            enable_ml=False,
        )
    if preset == "polars":
        return InitAnswers(
            project_name=project_name,
            target_dir=target_dir,
            layers=["flatfile", "bronze", "silver", "gold"],
            engine="polars",
            spark_profile=None,
            execution_env="notebook",
            enable_ml=False,
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
        f"  output_bucket: \"s3://{ans.project_name}/tables/\"\n"
        f"  flatfile_bucket: \"s3://{ans.project_name}/flatfiles/\"\n"
        f"  schema: {ans.project_name}\n"
        "  catalog: dev_catalog\n"
        "  incremental_column: year_month\n"
        "\n"
        "layers:\n"
        f"{layers_yaml}\n"
        f"{flatfile_section}"
        f"{other_sections}"
    )


def _render_example_job(layer: str, engine: str) -> str:
    if engine == "pyspark":
        return (
            "from pyspark.sql import functions as F\n"
            "\n"
            "def process_data(self, input_tables):\n"
            f"    \"\"\"Example {layer} job — return a Spark DataFrame.\"\"\"\n"
            "    df = next(iter(input_tables.values()))\n"
            "    return df.withColumn(\"processed_at\", F.current_timestamp())\n"
        )
    if engine == "pandas":
        return (
            "import pandas as pd\n"
            "\n"
            "def process_data(self, input_tables):\n"
            f"    \"\"\"Example {layer} job — return a pandas DataFrame.\"\"\"\n"
            "    df = next(iter(input_tables.values()))\n"
            "    df = df.copy()\n"
            "    df[\"processed_at\"] = pd.Timestamp.utcnow()\n"
            "    return df\n"
        )
    # polars
    return (
        "import polars as pl\n"
        "import datetime as dt\n"
        "\n"
        "def process_data(self, input_tables):\n"
        f"    \"\"\"Example {layer} job — return a polars DataFrame.\"\"\"\n"
        "    df = next(iter(input_tables.values()))\n"
        "    return df.with_columns(pl.lit(dt.datetime.utcnow()).alias(\"processed_at\"))\n"
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
    return (
        "\"\"\"Example ML training job. Reuse with bolt_pipeliner.ml.MLBase later.\"\"\"\n"
        "\n"
        "def train(features):\n"
        "    # TODO: implement training\n"
        "    raise NotImplementedError\n"
    )


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

    # Optional ML scaffolding
    if ans.enable_ml:
        ml_path = root / "models" / "train_example.py"
        _write_file(ml_path, _render_ml_example(ans.engine))
        written.append(ml_path)

    # README stub
    readme_path = root / "README.md"
    layers_str = ", ".join(ans.layers)
    _write_file(
        readme_path,
        f"# {ans.project_name}\n\n"
        f"Bolt Pipeliner project — engine: **{ans.engine}**, layers: **{layers_str}**.\n\n"
        "## Usage\n\n"
        "```bash\n"
        "pip install bolt_pipeliner\n"
        f"bolt run --config configs/etl_config.yaml\n"
        "```\n",
    )
    written.append(readme_path)

    return written


def execute(
    project_name: str,
    target_dir: Optional[Path] = None,
    preset: Optional[str] = None,
) -> None:
    target = target_dir or Path(project_name)
    if preset:
        answers = _preset_answers(preset, project_name, target)
    else:
        answers = _ask_interactive(project_name, target)

    written = _scaffold(answers)

    print()
    print(f"✓ Created {len(written)} files under {target}/")
    print()
    print("Next steps:")
    print(f"  cd {target}")
    print("  pip install bolt_pipeliner")
    print("  bolt run --help")
