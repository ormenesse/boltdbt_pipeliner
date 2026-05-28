from __future__ import annotations

from pathlib import Path

import yaml

from bolt_pipeliner.generators.airflow import create_layer_scripts


def _write_config(path: Path, content: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(yaml.safe_dump(content), encoding="utf-8")


def _minimal_project(tmp_path: Path, profile_name: str | None, runtime_target: str | None) -> Path:
    config_path = tmp_path / "configs" / "etl_config.yaml"
    configs = {
        "flatfile_bucket": "data/raw",
        "output_bucket": "outputs/tables",
        "schema": "demo",
    }
    if profile_name:
        configs["spark_profile"] = profile_name

    _write_config(
        config_path,
        {
            "configs": configs,
            "layers": {"bronze": "etl/0_bronze"},
            "bronze": [
                {
                    "module": "bronze_job",
                    "description": "minimal job",
                    "class_name": "ETLBase",
                    "input_tables": {},
                    "output_table_name": "out",
                }
            ],
        },
    )

    module = tmp_path / "etl" / "0_bronze" / "bronze_job.py"
    module.parent.mkdir(parents=True, exist_ok=True)
    module.write_text(
        "def process_data(self, input_tables):\n"
        "    return self.spark.createDataFrame([(1,)], ['id'])\n",
        encoding="utf-8",
    )

    if profile_name and runtime_target is not None:
        spark_profile = config_path.parent / "spark" / f"{profile_name}.toml"
        spark_profile.parent.mkdir(parents=True, exist_ok=True)
        spark_profile.write_text(
            "[runtime]\n"
            f"target = \"{runtime_target}\"\n"
            "\n"
            "[spark]\n"
            "\"spark.sql.shuffle.partitions\" = 20\n",
            encoding="utf-8",
        )

    return config_path


def _rendered_dag_text(tmp_path: Path) -> str:
    dag_path = tmp_path / "outputs" / "airflow" / "dags" / "datamart_bronze.py"
    return dag_path.read_text(encoding="utf-8")


def test_airflow_generation_defaults_to_local_bash_operator(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BOLT_SPARK_PROFILE", raising=False)
    config_path = _minimal_project(tmp_path, profile_name=None, runtime_target=None)

    create_layer_scripts(str(config_path))

    dag = _rendered_dag_text(tmp_path)
    assert "BashOperator" in dag


def test_airflow_generation_uses_emr_operator(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BOLT_SPARK_PROFILE", raising=False)
    config_path = _minimal_project(tmp_path, profile_name="emr", runtime_target="emr")

    create_layer_scripts(str(config_path))

    dag = _rendered_dag_text(tmp_path)
    assert "EmrServerlessStartJobOperator" in dag


def test_airflow_generation_uses_gcp_operator(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BOLT_SPARK_PROFILE", raising=False)
    config_path = _minimal_project(tmp_path, profile_name="gcp", runtime_target="gcp")

    create_layer_scripts(str(config_path))

    dag = _rendered_dag_text(tmp_path)
    assert "DataprocSubmitJobOperator" in dag


def test_airflow_generation_uses_azure_operator(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    monkeypatch.delenv("BOLT_SPARK_PROFILE", raising=False)
    config_path = _minimal_project(tmp_path, profile_name="azure", runtime_target="azure")

    create_layer_scripts(str(config_path))

    dag = _rendered_dag_text(tmp_path)
    assert "AzureContainerInstancesOperator" in dag


def test_airflow_generation_honors_profile_env_override(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    config_path = _minimal_project(tmp_path, profile_name="emr", runtime_target="emr")

    gcp_profile = config_path.parent / "spark" / "gcp.toml"
    gcp_profile.write_text(
        "[runtime]\n"
        "target = \"gcp\"\n",
        encoding="utf-8",
    )
    monkeypatch.setenv("BOLT_SPARK_PROFILE", "gcp")

    create_layer_scripts(str(config_path))

    dag = _rendered_dag_text(tmp_path)
    assert "DataprocSubmitJobOperator" in dag
