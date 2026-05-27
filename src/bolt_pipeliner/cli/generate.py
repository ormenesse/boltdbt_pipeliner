from __future__ import annotations

from pathlib import Path
from typing import Iterable

VALID_TARGETS = {"airflow", "documentation", "layers", "notebook", "all"}


def execute(targets: Iterable[str], config_path: Path) -> None:
    targets = list(targets)
    unknown = [t for t in targets if t not in VALID_TARGETS]
    if unknown:
        raise SystemExit(
            f"Unknown generate target(s): {unknown}. Valid: {sorted(VALID_TARGETS)}"
        )

    run_all = "all" in targets

    if run_all or "airflow" in targets:
        from bolt_pipeliner.generators import airflow as gen_airflow

        gen_airflow.create_layer_scripts(str(config_path))

    if run_all or "documentation" in targets:
        from bolt_pipeliner.generators import documentation as gen_docs

        gen_docs.gen_doc()

    if run_all or "layers" in targets:
        from bolt_pipeliner.generators import layers as gen_layers

        gen_layers.create_layer_scripts(str(config_path))

    if run_all or "notebook" in targets:
        from bolt_pipeliner.generators import notebook as gen_notebook

        gen_notebook.create_etl_notebook(str(config_path))

    print("\nGeneration completed!\n")
