from bolt_pipeliner.generators._paths import (
    ETL_BASE_SOURCE,
    PACKAGE_ROOT,
    TEMPLATES_AIRFLOW,
    TEMPLATES_DOCS,
)


def test_package_root_resolves_inside_src():
    assert PACKAGE_ROOT.name == "bolt_pipeliner"
    assert PACKAGE_ROOT.parent.name == "src"


def test_etl_base_source_exists():
    assert ETL_BASE_SOURCE.is_file()


def test_templates_dirs_exist_and_carry_expected_files():
    assert TEMPLATES_DOCS.is_dir()
    assert TEMPLATES_AIRFLOW.is_dir()

    # job_script.txt is consumed by the airflow + notebook generators.
    assert (TEMPLATES_DOCS / "job_script.txt").is_file()
    # spark_config.txt feeds the airflow generator.
    assert (TEMPLATES_AIRFLOW / "spark_config.txt").is_file()
