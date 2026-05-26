import logging
import os
import re
import yaml
import pandas as pd
from typing import Dict, Iterable, Optional, Any

logger = logging.getLogger(__name__)

# Constants
DEFAULT_CONFIG_PATH = "./configs/etl_config.yaml"
OUTPUTS_DIR = "./outputs/snowflake_ddls"
SCHEMA_DIR = "./outputs/schema"

_SPARK_SIMPLE_MAP = {
    # Spark SQL lowercase aliases
    "string": "VARCHAR",
    "boolean": "BOOLEAN",
    "bool": "BOOLEAN",
    "byte": "NUMBER(3,0)",
    "short": "NUMBER(5,0)",
    "int": "NUMBER(10,0)",
    "integer": "NUMBER(10,0)",
    "long": "NUMBER(38,0)",
    "bigint": "NUMBER(38,0)",
    "float": "FLOAT",
    "double": "DOUBLE",
    "binary": "BINARY",
    "date": "DATE",
    "timestamp": "TIMESTAMP_NTZ",
    "decimal": "NUMBER",   # will be refined if (p,s) present
    "array": "VARIANT",
    "map": "OBJECT",
    "struct": "OBJECT",
    "variant": "VARIANT",
}

def load_yaml_config(config_path: str) -> Dict[str, Any]:
    """
    Load and parse a YAML configuration file.
    
    Args:
        config_path: Path to the YAML configuration file
        
    Returns:
        Parsed YAML configuration as a dictionary
        
    Raises:
        FileNotFoundError: If the configuration file doesn't exist
        yaml.YAMLError: If the YAML file is malformed
    """
    try:
        with open(config_path, 'r', encoding='utf-8') as f:
            return yaml.safe_load(f)
    except FileNotFoundError:
        logger.error(f"Configuration file not found: {config_path}")
        raise
    except yaml.YAMLError as e:
        logger.error(f"Error parsing YAML file {config_path}: {e}")
        raise

def _escape_comment(txt: Optional[str]) -> Optional[str]:
    if txt is None:
        return None
    # Escape single quotes for Snowflake
    return txt.replace("'", "''")

def _quote_ident(ident: str, quote_identifiers: bool) -> str:
    if not quote_identifiers:
        # bare identifiers; you can also normalize to uppercase if desired
        return ident
    # Escape embedded double quotes by doubling them
    return '"' + ident.replace('"', '""') + '"'

def _pyspark_to_snowflake(sql_type: str) -> str:
    """
    Map a PySpark data type string to a Snowflake data type.
    Handles forms like:
      StringType()
      IntegerType()
      DecimalType(10,2)
      ArrayType(StringType(), True)
      MapType(StringType(), IntegerType(), True)
      StructType([ ... ])    -> VARIANT/OBJECT (we choose OBJECT for structs)
    Also accepts Spark SQL short names like: string, int, decimal(10,2), etc.
    """
    if not sql_type:
        return "VARCHAR"

    s = sql_type.strip()

    # Accept simple spark-sql styles like "decimal(10,2)"
    m_dec_simple = re.fullmatch(r"decimal\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", s, flags=re.I)
    if m_dec_simple:
        p, sc = m_dec_simple.groups()
        return f"NUMBER({p},{sc})"

    # Simple aliases (string, int, etc.)
    k = s.lower().strip("()").strip()
    if k in _SPARK_SIMPLE_MAP:
        return _SPARK_SIMPLE_MAP[k]

    # CamelCase PySpark types e.g., "StringType()", "IntegerType()", etc.
    # DecimalType(p,s)
    m_dec = re.fullmatch(r"DecimalType\s*\(\s*(\d+)\s*,\s*(\d+)\s*\)", s, flags=re.I)
    if m_dec:
        p, sc = m_dec.groups()
        return f"NUMBER({p},{sc})"

    # Common primitives
    if re.fullmatch(r"StringType\s*\(\s*\)", s, flags=re.I):
        return "VARCHAR"
    if re.fullmatch(r"(BooleanType|BoolType)\s*\(\s*\)", s, flags=re.I):
        return "BOOLEAN"
    if re.fullmatch(r"(ByteType)\s*\(\s*\)", s, flags=re.I):
        return "NUMBER(3,0)"
    if re.fullmatch(r"(ShortType)\s*\(\s*\)", s, flags=re.I):
        return "NUMBER(5,0)"
    if re.fullmatch(r"(IntegerType|IntType)\s*\(\s*\)", s, flags=re.I):
        return "NUMBER(10,0)"
    if re.fullmatch(r"(LongType|BigIntType)\s*\(\s*\)", s, flags=re.I):
        return "NUMBER(38,0)"
    if re.fullmatch(r"(FloatType)\s*\(\s*\)", s, flags=re.I):
        return "FLOAT"
    if re.fullmatch(r"(DoubleType)\s*\(\s*\)", s, flags=re.I):
        return "DOUBLE"
    if re.fullmatch(r"(BinaryType)\s*\(\s*\)", s, flags=re.I):
        return "BINARY"
    if re.fullmatch(r"(DateType)\s*\(\s*\)", s, flags=re.I):
        return "DATE"
    if re.fullmatch(r"(TimestampType)\s*\(\s*\)", s, flags=re.I):
        return "TIMESTAMP_NTZ"

    # Complex types -> choose Snowflake semi-structured equivalents
    if re.match(r"ArrayType\s*\(", s, flags=re.I):
        # Could be represented as ARRAY; Snowflake ARRAY is semi-structured.
        # Using VARIANT is often more flexible, but ARRAY also works.
        return "ARRAY"
    if re.match(r"MapType\s*\(", s, flags=re.I):
        return "OBJECT"
    if re.match(r"StructType\s*\(", s, flags=re.I):
        return "OBJECT"

    # Fallbacks
    if re.match(r"decimal\b", s, flags=re.I):
        return "NUMBER"
    if re.match(r"(struct|map|array)\b", s, flags=re.I):
        return {"struct": "OBJECT", "map": "OBJECT", "array": "ARRAY"}[s.split("(")[0].lower()]

    # Last resort
    return "VARCHAR"

def generate_snowflake_ddl(
    df: pd.DataFrame,
    database: Optional[str] = None,
    schema: Optional[str] = None,
    *,
    if_not_exists: bool = False,
    or_replace: bool = False,
    quote_identifiers: bool = False,
    include_fully_qualified_name: bool = True,
) -> Dict[str, str]:
    """
    Given a DataFrame with columns [table_name, col_name, data_type, comment],
    returns a dict {table_name: CREATE TABLE ... } of Snowflake DDL statements.

    Notes:
    - All columns are created as NULLable (no NULL/NOT NULL info in the input).
    - Comments are added inline in the column definition.
    - PySpark data types are mapped to Snowflake types (best effort).
    - `or_replace` and `if_not_exists` are mutually exclusive; if both set, `or_replace` wins.
    - `quote_identifiers` wraps database/schema/table/column in double quotes.
    """
    required_cols = {"table_name", "col_name", "data_type", "comment"}
    missing = required_cols - set(df.columns)
    if missing:
        raise ValueError(f"Missing required columns: {sorted(missing)}")

    # normalize table order while preserving input row order per table
    grouped = df.groupby("table_name", sort=False)

    ddls: Dict[str, str] = {}
    for raw_table_name, sub in grouped:
        # prepare qualified table name
        parts: Iterable[str] = []
        if include_fully_qualified_name and database:
            parts.append(_quote_ident(database, quote_identifiers))
        if include_fully_qualified_name and schema:
            parts.append(_quote_ident(schema, quote_identifiers))
        parts.append(_quote_ident(raw_table_name, quote_identifiers))
        qualified_table = ".".join(parts)

        # Choose CREATE prefix
        create_kw = "CREATE"
        if or_replace:
            create_kw = "CREATE OR REPLACE"
        elif if_not_exists:
            create_kw = "CREATE IF NOT EXISTS"

        column_lines = []
        for _, row in sub.iterrows():
            col = _quote_ident(str(row["col_name"]), quote_identifiers)
            sf_type = _pyspark_to_snowflake(str(row["data_type"]))
            cm = row.get("comment")
            if pd.isna(cm):
                cm = None
            cm = _escape_comment(cm) if cm is not None else None

            if cm:
                column_lines.append(f"  {col} {sf_type} COMMENT '{cm}'")
            else:
                column_lines.append(f"  {col} {sf_type}")

        cols_block = ",\n".join(column_lines)
        ddl = f"""{create_kw} TABLE {qualified_table} (
{cols_block}
);"""
        ddls[raw_table_name] = ddl

    return ddls

def create_ddls_from_schema():
    """
    Create ddls orchestrator...
    """
    print("Generating snowfalke DDL's...")
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    config = load_yaml_config(DEFAULT_CONFIG_PATH)
    database = config['configs'].get('database', None)
    schema = config['configs'].get('schema', None)

    try:
        df = pd.read_csv(f'{SCHEMA_DIR}/schema.csv')
    except Exception as e:
        print(f'Could not find schema: {e}')
        print('Please run documentation or run ' \
        'the schema script in "./outputs/schema/schema.py" and then rerun this script.')

    ddls = generate_snowflake_ddl(
        df,
        database=database,
        schema=schema,
        or_replace=True,
        quote_identifiers=True
    )

    out_path = f"{OUTPUTS_DIR}/snowflake_ddl.sql"
    with open(out_path, "w", encoding="utf-8") as f:
        for tbl, ddl in ddls.items():
            f.write(f"-- DDL for {tbl}\n{ddl}\n\n")
    
    print("Finished DDL generation!")

if __name__ == "__main__":
    create_ddls_from_schema()