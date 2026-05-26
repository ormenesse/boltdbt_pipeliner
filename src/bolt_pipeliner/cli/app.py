"""bolt CLI entry point. See `bolt --help`."""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import typer

from bolt_pipeliner.cli import generate as gen_cmd
from bolt_pipeliner.cli import init as init_cmd
from bolt_pipeliner.cli import run as run_cmd
from bolt_pipeliner.cli import test as test_cmd

app = typer.Typer(
    add_completion=False,
    help="Config-driven ETL framework for Spark, Pandas, and Polars.",
    no_args_is_help=True,
)


@app.command()
def run(
    config: Path = typer.Option(
        Path("configs/etl_config.yaml"),
        "--config",
        "-c",
        help="Path to YAML config",
        exists=False,
    ),
    flatfile: bool = typer.Option(False, "--flatfile", help="Run only flatfile jobs"),
    bronze: bool = typer.Option(False, "--bronze", help="Run only bronze jobs"),
    silver: bool = typer.Option(False, "--silver", help="Run only silver jobs"),
    gold: bool = typer.Option(False, "--gold", help="Run only gold jobs"),
    diamond: bool = typer.Option(False, "--diamond", help="Run only diamond jobs"),
) -> None:
    """Run ETL jobs across the requested layers."""
    layer_flags = {
        "flatfile": flatfile,
        "bronze": bronze,
        "silver": silver,
        "gold": gold,
        "diamond": diamond,
    }
    selected = [name for name, enabled in layer_flags.items() if enabled] or None
    run_cmd.execute(config, selected)


@app.command()
def generate(
    targets: list[str] = typer.Argument(
        ..., help="One or more of: airflow, documentation, layers, notebook, snowflakeddl, all"
    ),
    config: Path = typer.Option(
        Path("configs/etl_config.yaml"),
        "--config",
        "-c",
        help="Path to YAML config",
    ),
) -> None:
    """Generate downstream artifacts (Airflow DAGs, docs, layer scripts, notebook, DDLs)."""
    gen_cmd.execute(targets, config)


@app.command()
def test(
    config: Path = typer.Option(
        Path("configs/etl_config.yaml"),
        "--config",
        "-c",
        help="Path to YAML config",
    ),
    layer: Optional[str] = typer.Option(None, "--layer", "-l", help="Run tests for a single layer"),
    module: Optional[str] = typer.Option(None, "--module", "-m", help="Run tests for a single job module"),
) -> None:
    """Run data-quality checks declared under each job's `tests:` block."""
    code = test_cmd.execute(config, layer=layer, module=module)
    raise typer.Exit(code)


@app.command()
def init(
    project_name: str = typer.Argument(..., help="Project name (also the target dir if no path given)"),
    path: Optional[Path] = typer.Option(
        None,
        "--path",
        help="Target directory (defaults to ./<project_name>)",
    ),
    preset: Optional[str] = typer.Option(
        None,
        "--preset",
        help="Skip interactive prompts. One of: minimal, medallion, diamond, pandas, polars",
    ),
) -> None:
    """Scaffold a new bolt_pipeliner project (interactive or via --preset)."""
    init_cmd.execute(project_name, target_dir=path, preset=preset)


def main() -> None:
    """Console-script entry point declared in pyproject.toml."""
    app()


if __name__ == "__main__":
    main()
