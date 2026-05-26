"""Resolves package-bundled template + source paths for generators."""

from pathlib import Path

PACKAGE_ROOT = Path(__file__).resolve().parent.parent
TEMPLATES_DOCS = PACKAGE_ROOT / "templates" / "docs"
TEMPLATES_AIRFLOW = PACKAGE_ROOT / "templates" / "airflow"
ETL_BASE_SOURCE = PACKAGE_ROOT / "bases" / "spark_iceberg.py"
