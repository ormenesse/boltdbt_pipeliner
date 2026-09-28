"""Resolve bronze source references without duplicating catalog qualifiers."""


def bronze_table_identifier(
    reference: str, *, source_catalog: str, save_catalog: str, schema: str
) -> str:
    """Resolve bare project tables, source schema.table, or catalog.schema.table."""
    parts = reference.split(".")
    if not all(parts) or len(parts) > 3:
        raise ValueError(
            f"Invalid bronze table reference {reference!r}; expected table, "
            "schema.table, or catalog.schema.table."
        )
    if len(parts) == 1:
        return f"{save_catalog}.{schema}.{reference}"
    if len(parts) == 2:
        return f"{source_catalog}.{reference}"
    return reference
