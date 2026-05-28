from __future__ import annotations

from pathlib import Path
from urllib.parse import urlparse

CSV_EXTENSIONS = (".csv",)
PARQUET_EXTENSIONS = (".parquet",)
EXCEL_EXTENSIONS = (".xlsx", ".xls", ".xlsm", ".xlsb", ".ods", ".odf", ".odt")
JSON_EXTENSIONS = (".json",)
JSON_LINES_EXTENSIONS = (".jsonl", ".ndjson")


def _strip_query_fragment(path: str) -> str:
    return path.split("?", 1)[0].split("#", 1)[0]


def has_uri_scheme(path: str) -> bool:
    scheme = urlparse(path).scheme
    # Keep Windows drive letters (e.g. C:\\foo) out of URI detection.
    return bool(scheme) and not (len(scheme) == 1 and path[1:3] == ":\\")


def is_absolute_path(path: str) -> bool:
    if path.startswith("dbfs:/"):
        return True
    return Path(path).is_absolute()


def detect_file_format(path: str) -> str:
    lower = _strip_query_fragment(path).lower()
    if lower.endswith(CSV_EXTENSIONS):
        return "csv"
    if lower.endswith(PARQUET_EXTENSIONS):
        return "parquet"
    if lower.endswith(EXCEL_EXTENSIONS):
        return "excel"
    if lower.endswith(JSON_LINES_EXTENSIONS):
        return "jsonl"
    if lower.endswith(JSON_EXTENSIONS):
        return "json"
    return "unknown"


def has_known_extension(path: str) -> bool:
    return detect_file_format(path) != "unknown"


def resolve_data_path(
    value: str,
    root: str | None,
    *,
    default_extension: str | None = None,
) -> str:
    path = str(value)

    if default_extension and not has_known_extension(path):
        path = f"{path}{default_extension}"

    if has_uri_scheme(path) or is_absolute_path(path):
        return path

    if not root:
        return path

    root_path = str(root)
    if has_uri_scheme(root_path):
        return root_path.rstrip("/") + "/" + path.lstrip("/")

    return str(Path(root_path) / path)


def to_spark_path(path: str) -> str:
    if path.startswith("s3://"):
        return "s3a://" + path[len("s3://") :]
    return path


def to_pandas_path(path: str) -> str:
    if path.startswith("s3a://"):
        return "s3://" + path[len("s3a://") :]
    return path
