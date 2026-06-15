from collections import deque
from html import escape
import datetime as dt
import pandas as pd
import shutil
import yaml
import sys
import os
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any
from dataclasses import dataclass

from bolt_pipeliner.bases._io import resolve_data_path
from bolt_pipeliner.generators._paths import ETL_BASE_SOURCE, TEMPLATES_DOCS
from bolt_pipeliner.sessions import create_session

# Constants
DEFAULT_CONFIG_PATH = "./configs/etl_config.yaml"
STYLE_CONFIG_PATH = "./configs/style_config.yaml"
OUTPUTS_DIR = "./outputs"
DOCUMENTATION_DIR = "./outputs/documentation"
TABLES_DIR = "./outputs/documentation/tables"
SCHEMA_DIR = "./outputs/schema"
TEMPLATE_DIR = str(TEMPLATES_DOCS)
SCHEMA_BASE_COLUMNS = ["table_name", "col_name", "data_type", "comment"]
SCHEMA_OUTPUT_COLUMNS = SCHEMA_BASE_COLUMNS + ["parent"]
COMMON_ID_COLUMN_THRESHOLD = 5

DEFAULT_STYLE_COLORS: Dict[str, str] = {
    "body_background": "#353535ff",
    "body_text": "#500b3aff",
    "panel_background": "#f7f7f7",
    "panel_border": "#dddddd",
    "panel_shadow": "rgba(0,0,0,.35)",
    "heading_accent": "#d54e62ff",
    "rule_color": "#1f2937",
    "meta_text": "#94a3b8",
    "table_cell_text": "#555555",
    "link_text": "#60a5fa",
    "btn_bg": "#d54e62ff",
    "btn_text": "#ffffff",
    "btn_hover_bg": "#b8122d",
    "code_bg": "#353535ff",
    "code_border": "#353535ff",
    "code_inset_highlight": "rgba(255,255,255,.03)",
    "fade_top": "rgba(11,16,33,0)",
    "fade_mid": "rgba(11,16,33,.9)",
    "fade_bottom": "rgba(11,16,33,1)",
    "mermaid_text": "#111111",
    "mermaid_line": "#888888",
    "mermaid_tertiary": "#aaaaaa",
    "index_accent": "#e31837",
    "index_accent_hover": "#b8122d",
    "index_text": "#1a1a1a",
    "index_page_bg": "#ffffff",
    "index_panel_bg": "#f7f7f7",
    "index_border": "#e5e5e5",
}


def resolve_style_colors(style_config: Dict[str, Any]) -> Dict[str, str]:
    """Return a style palette merged with sane defaults."""
    resolved = DEFAULT_STYLE_COLORS.copy()
    raw_colors: Dict[str, Any] = {}
    if isinstance(style_config, dict):
        candidate = style_config.get("style_colors", {})
        if isinstance(candidate, dict):
            raw_colors = candidate

    for key in DEFAULT_STYLE_COLORS:
        value = raw_colors.get(key)
        if isinstance(value, str) and value.strip():
            resolved[key] = value

    return resolved

@dataclass
class JobConfig:
    """Configuration for a single ETL job."""
    output_table_name: str
    module: str
    description: str = ""
    input_tables: Optional[Dict[str, str]] = None
    partition_by: Optional[str] = None
    unload: bool = False
    incremental: bool = False

@dataclass
class DocumentationConfig:
    """Configuration for documentation generation."""
    etl_config: Dict[str, Any]
    style_config: Dict[str, Any]
    target_layers: Optional[List[str]] = None

def load_yaml_config(path: str) -> Dict[str, Any]:
    """Load and parse a YAML configuration file."""
    with open(path, 'r') as f:
        return yaml.safe_load(f)

def determine_node_layer(node_name: str, config: Dict[str, Any]) -> str:
    """Determine the layer for a given node name."""
    if any(
        ext in node_name.lower()
        for ext in [
            '.csv',
            '.parquet',
            '.xlsx',
            '.xls',
            '.json',
            '.jsonl',
            '.ndjson',
        ]
    ):
        return 'flatfile'
    
    first_part = node_name.split("_")[0]
    return first_part if first_part in config['layers'].keys() else 'raw'

def extract_node_name(node_name: str, config: Dict[str, Any]) -> str:
    """Extract the clean node name from a full table name."""
    first_part = node_name.split("_")[0]
    if first_part in config['layers'].keys():
        return "_".join(node_name.split("_")[1:])
    return node_name

def get_nodes_n_edges(layer: str, name: str, config: Dict[str, Any]) -> Tuple[List[Tuple[str, str, str]], List[Tuple[str, str]]]:
    """
    Get nodes and edges to generate the mermaid graph.
    
    Returns:
        Tuple of (nodes, edges) where nodes are (id, label, layer) and edges are (source, target)
    """
    nodes = []
    edges = []
    
    # Find the job configuration
    job = next(
        (module for module in config.get(layer, []) if module['output_table_name'] == name),
        None
    )
    
    if job is None:
        return nodes, edges
    
    # Process input tables
    input_tables = list(job.get('input_tables', {}).values()) if job.get('input_tables') else []
    
    for node in input_tables:
        node_layer = determine_node_layer(node, config)
        node_name = extract_node_name(node, config)
        
        nodes.append((node, node, node_layer))
        edges.append((node, f"{layer}_{job['output_table_name']}"))
        
        # Recursively get nodes and edges for dependencies
        child_nodes, child_edges = get_nodes_n_edges(node_layer, node_name, config)
        nodes.extend(child_nodes)
        edges.extend(child_edges)
    
    return nodes, edges

def create_mermaid_graph(nodes: List[Tuple[str, str, str]], edges: List[Tuple[str, str]], group_styles: Dict[str, Any]) -> str:
    """
    Build a Mermaid 'flowchart' graph with subgraphs for groups.
    
    Args:
        nodes: List of (node_id, label, group) tuples
        edges: List of (source, target) tuples
        group_styles: Dictionary mapping group names to style configurations
        
    Returns:
        Mermaid graph definition as string
    """
    lines = ["flowchart LR", "  %% Node declarations"]
    
    # Group nodes by layer
    groups = {}
    for node_id, label, group in nodes:
        groups.setdefault(group, []).append((node_id, label))
    
    # Render subgraphs per group
    for group, group_nodes in groups.items():
        lines.append(f"  subgraph {group}")
        for node_id, label in group_nodes:
            lines.append(f"    {node_id}({label})")
        lines.append("  end")
    
    # Add edges
    lines.append("  %% Edges")
    for src, dst in edges:
        lines.append(f"  {src} --> {dst}")
    
    # Define classes per group for coloring
    lines.append("  %% Classes for groups")
    for group, group_nodes in groups.items():
        style = group_styles.get(group, {"stroke": "#999999", "fill": "#FFFFFF"})
        stroke = style["stroke"]
        fill = style["fill"]
        
        lines.append(f'  classDef {group} stroke:{stroke},fill:{fill},stroke-width:1px,color:#0F172A;')
        
        group_ids = ",".join(nid for nid, _ in group_nodes)
        lines.append(f"  class {group_ids} {group}")
    
    return "\n".join(lines)

def get_table_schema(spark, job_script_name: str, schema: str, catalog: str) -> pd.DataFrame:
    """
    Try to fetch table schema using spark.
    
    Returns:
        DataFrame with schema information or empty DataFrame if error occurs
    """
    try:
        table_schema = spark.sql(f"DESCRIBE {catalog}.{schema}.{job_script_name}")
        table_schema = table_schema.toPandas()
        
        # Filter out invalid rows
        mask = (
            table_schema["col_name"].notna()
            & (table_schema["col_name"] != "")
            & (~table_schema["col_name"].str.startswith("#"))
        )

        table_schema = table_schema.loc[mask, ["col_name", "data_type", "comment"]].copy()
        table_schema.loc[:, "table_name"] = job_script_name

    except Exception as e:
        print(f"Error fetching schema: {e}")
        table_schema = pd.DataFrame([], columns=SCHEMA_OUTPUT_COLUMNS)

    return ensure_schema_columns(table_schema)


def ensure_schema_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Return a schema DataFrame with expected columns and normalized types."""
    if df is None or df.empty:
        return pd.DataFrame([], columns=SCHEMA_OUTPUT_COLUMNS)

    out = df.copy()
    for column in SCHEMA_BASE_COLUMNS:
        if column not in out.columns:
            out[column] = ""
    if "parent" not in out.columns:
        out["parent"] = ""

    out = out[SCHEMA_OUTPUT_COLUMNS].copy()
    for column in SCHEMA_OUTPUT_COLUMNS:
        out[column] = out[column].fillna("").astype(str)
    return out


def is_id_column(column_name: str) -> bool:
    """Identify whether a column name looks like an ID key."""
    if not isinstance(column_name, str):
        return False
    normalized = column_name.strip().lower().replace("-", "_")
    if normalized == "id":
        return True
    return normalized.endswith("_id") or normalized.startswith("id_") or "_id_" in normalized


def _column_tokens_without_id(column_name: str) -> set[str]:
    normalized = str(column_name).strip().lower().replace("-", "_")
    return {token for token in normalized.split("_") if token and token != "id"}


def _format_parent_refs(matches: pd.DataFrame, *, include_table_name: bool) -> str:
    if matches.empty:
        return ""
    refs: List[str] = []
    for row in matches.itertuples(index=False):
        if include_table_name:
            refs.append(f"{row.table_name}.{row.col_name}")
        else:
            refs.append(str(row.col_name))
    return ", ".join(dict.fromkeys(refs))


def _resolve_parent_value(
    column_name: str,
    previous_schemas: pd.DataFrame,
    all_schemas: pd.DataFrame,
) -> str:
    if previous_schemas.empty:
        return ""

    normalized_col = str(column_name).strip().lower()
    previous = previous_schemas.copy()
    previous["_normalized_col"] = previous["col_name"].str.strip().str.lower()
    all_rows = all_schemas.copy()
    all_rows["_normalized_col"] = all_rows["col_name"].str.strip().str.lower()

    exact_matches = (
        previous.loc[previous["_normalized_col"] == normalized_col, ["table_name", "col_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    if exact_matches.empty:
        return ""

    if not is_id_column(column_name):
        return _format_parent_refs(exact_matches, include_table_name=True)

    occurrences = all_rows.loc[
        all_rows["_normalized_col"] == normalized_col,
        "table_name",
    ].nunique()
    if occurrences < COMMON_ID_COLUMN_THRESHOLD:
        return _format_parent_refs(exact_matches, include_table_name=True)

    id_candidates = (
        previous.loc[previous["col_name"].map(is_id_column), ["table_name", "col_name"]]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    token_filter = _column_tokens_without_id(column_name)
    if token_filter:
        filtered = id_candidates.loc[
            id_candidates["col_name"].map(
                lambda value: bool(token_filter.intersection(_column_tokens_without_id(value)))
            )
        ]
        if not filtered.empty:
            id_candidates = filtered

    if id_candidates.empty:
        return _format_parent_refs(exact_matches, include_table_name=False)
    return _format_parent_refs(id_candidates, include_table_name=False)


def add_parent_column_for_table(
    table_schema: pd.DataFrame,
    all_schemas: pd.DataFrame,
    previous_tables: List[str],
) -> pd.DataFrame:
    """Compute parent-column references for one table schema."""
    current = ensure_schema_columns(table_schema)
    if current.empty:
        return current

    all_rows = ensure_schema_columns(all_schemas)
    if previous_tables:
        previous_rows = all_rows.loc[all_rows["table_name"].isin(previous_tables)].copy()
    else:
        previous_rows = pd.DataFrame([], columns=SCHEMA_OUTPUT_COLUMNS)

    out = current.copy()
    out["parent"] = out["col_name"].map(
        lambda value: _resolve_parent_value(value, previous_rows, all_rows)
    )
    return ensure_schema_columns(out)


def add_parent_column_to_schemas(schemas: pd.DataFrame, table_order: List[str]) -> pd.DataFrame:
    """Apply parent-column lineage to every table in generation order."""
    all_rows = ensure_schema_columns(schemas)
    if all_rows.empty:
        return all_rows

    unique_tables = list(dict.fromkeys(all_rows["table_name"].tolist()))
    ordered_tables = [t for t in table_order if t in unique_tables]
    ordered_tables.extend([t for t in unique_tables if t not in ordered_tables])

    enriched_frames = []
    processed_tables: List[str] = []
    for table_name in ordered_tables:
        table_rows = all_rows.loc[all_rows["table_name"] == table_name].copy()
        enriched_frames.append(add_parent_column_for_table(table_rows, all_rows, processed_tables))
        processed_tables.append(table_name)

    if not enriched_frames:
        return pd.DataFrame([], columns=SCHEMA_OUTPUT_COLUMNS)

    combined = pd.concat(enriched_frames, ignore_index=True)
    combined = combined.drop_duplicates(
        subset=["table_name", "col_name", "data_type", "comment"],
        keep="first",
    ).reset_index(drop=True)
    return ensure_schema_columns(combined)


def infer_schema_from_outputs(table_names: List[str], config: Dict[str, Any]) -> pd.DataFrame:
    """Best-effort schema extraction from parquet datasets written by Spark/Pandas/Polars."""
    configs_section = config.get("configs", {}) if isinstance(config, dict) else {}
    output_root = configs_section.get("output_location") or configs_section.get("output_bucket")
    if not output_root:
        return pd.DataFrame([], columns=SCHEMA_OUTPUT_COLUMNS)

    try:
        import pyarrow.dataset as ds
    except Exception as exc:
        print(f"Could not infer schema.csv from outputs: {exc}")
        return pd.DataFrame([], columns=SCHEMA_OUTPUT_COLUMNS)

    inferred_rows: List[Dict[str, str]] = []
    for table_name in table_names:
        if table_name == "etlBase":
            continue

        candidates = [
            resolve_data_path(table_name, output_root),
            resolve_data_path(table_name, output_root, default_extension=".parquet"),
        ]
        schema_obj = None
        for candidate in dict.fromkeys(candidates):
            try:
                dataset = ds.dataset(candidate, format="parquet")
                schema_obj = dataset.schema
                if schema_obj is not None and len(schema_obj) > 0:
                    break
            except Exception:
                continue

        if schema_obj is None:
            continue

        for field in schema_obj:
            inferred_rows.append(
                {
                    "table_name": table_name,
                    "col_name": field.name,
                    "data_type": str(field.type),
                    "comment": "",
                }
            )

    inferred = pd.DataFrame(inferred_rows, columns=SCHEMA_BASE_COLUMNS)
    return ensure_schema_columns(inferred)


def generate_schema_csv(
    schemas: pd.DataFrame,
    table_order: List[str],
    config: Dict[str, Any],
) -> bool:
    """Write outputs/schema/schema.csv whenever schema data is available."""
    collected = ensure_schema_columns(schemas)
    inferred = infer_schema_from_outputs(table_order, config)

    if not inferred.empty:
        if collected.empty:
            collected = inferred
        else:
            known_tables = set(collected["table_name"].tolist())
            missing_rows = inferred.loc[~inferred["table_name"].isin(known_tables)].copy()
            if not missing_rows.empty:
                collected = pd.concat([collected, missing_rows], ignore_index=True)

    if collected.empty:
        return False

    enriched = add_parent_column_to_schemas(collected, table_order)
    Path(SCHEMA_DIR).mkdir(parents=True, exist_ok=True)
    enriched.to_csv(f"{SCHEMA_DIR}/schema.csv", index=False)
    return True

def dataframe_to_schema_rows(df: pd.DataFrame) -> str:
    """
    Convert a pandas DataFrame to HTML table rows.
    
    Args:
        df: DataFrame with columns ['col_name', 'data_type', 'comment']
        
    Returns:
        HTML string of <tr>...</tr> rows with values safely escaped
        
    Raises:
        ValueError: If required columns are missing
    """
    required = ["col_name", "data_type", "comment", "parent"]
    missing = [c for c in required if c not in df.columns]
    if missing:
        raise ValueError(f"DataFrame missing required columns: {missing}")

    # Reorder, fill NaNs, ensure string, and escape HTML
    safe = (
        df[required]
        .fillna("")
        .astype(str)
        .map(lambda x: escape(x))
    )
    
    rows = []
    for _, row in safe.iterrows():
        rows.append(
            f"<tr>"
            f"<td>{row['col_name']}</td>"
            f"<td>{row['data_type']}</td>"
            f"<td>{row['comment']}</td>"
            f"<td>{row['parent']}</td>"
            f"</tr>"
        )
    
    return "\n".join(rows)

class JobProcessor:
    """Handles processing of individual jobs for documentation generation."""
    
    def __init__(self, config: DocumentationConfig, spark=None):
        self.config = config
        self.spark = spark
        self.schemas = ensure_schema_columns(self._load_schemas())
        self.documentation_table_names = []
    
    def _load_schemas(self) -> pd.DataFrame:
        """Load table schemas from CSV or initialize empty DataFrame."""
        if self.spark is None:
            try:
                return ensure_schema_columns(pd.read_csv(f'{SCHEMA_DIR}/schema.csv'))
            except Exception as e:
                print(f'Could not find schema: {e}')
                return pd.DataFrame([], columns=SCHEMA_OUTPUT_COLUMNS)
        else:
            return pd.DataFrame([], columns=SCHEMA_OUTPUT_COLUMNS)
    
    def _process_job_dependencies(self, layer: str, job: Dict[str, Any], config: Dict[str, Any]) -> Tuple[List, List]:
        """Process job dependencies and return nodes and edges."""
        nodes, edges = get_nodes_n_edges(layer, job['output_table_name'], config)
        return list(set(nodes)), list(set(edges))
    
    def _get_job_schema(self, job_script_name: str, schema: str, catalog: str) -> pd.DataFrame:
        """Get schema for a job, either from Spark or CSV."""
        if self.spark is not None:
            table_schema = ensure_schema_columns(
                get_table_schema(self.spark, job_script_name, schema, catalog)
            )
            if len(table_schema) > 0:
                self.schemas = pd.concat([self.schemas, table_schema], axis=0, ignore_index=True)
                self.schemas = self.schemas.drop_duplicates(
                    subset=["table_name", "col_name", "data_type", "comment"],
                    keep="first",
                ).reset_index(drop=True)
            return table_schema
        else:
            return ensure_schema_columns(
                self.schemas.loc[self.schemas['table_name'] == job_script_name, :]
            )
    
    def _read_job_code(self, module_path: str) -> str:
        """Read job code from module file."""
        try:
            with open(module_path, 'r', encoding='utf-8') as f:
                return f.read()
        except FileNotFoundError:
            print(f"Warning: Could not find module {module_path}, skipping...")
            return ""
    
    def _format_input_tables(self, input_tables: Optional[Dict[str, str]]) -> str:
        """Format input tables for display."""
        if not input_tables:
            return ""
        return str(input_tables).replace("{", " ").replace("}", " ").replace("',", "',\n")
    
    def _create_mermaid_page(self, job_config: JobConfig, job_code: str, schema_rows: str, 
                           mermaid: str, module_path: str) -> str:
        """Create the complete mermaid page HTML."""
        with open(f'{TEMPLATE_DIR}/mermaid_page.txt', 'r', encoding='utf-8') as f:
            mermaid_page = f.read()
        
        job_script_name = f"{job_config.output_table_name}"
        
        # Prepare style configuration
        style_colors = resolve_style_colors(self.config.style_config)
        
        return mermaid_page.format(
            title=job_script_name,
            mermaid_code=mermaid,
            date=dt.datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            description=job_config.description,
            layer=job_config.module.split('_')[0].upper(),
            module=job_config.module,
            job_code=escape(job_code),
            module_path=module_path,
            input_tables=self._format_input_tables(job_config.input_tables),
            output_table_name=job_script_name,
            partition_by=job_config.partition_by,
            unload=job_config.unload,
            incremental=job_config.incremental,
            schema_rows=schema_rows,
            **style_colors  # Unpack all style colors
        )
    
    def process_job(self, layer: str, job: Dict[str, Any], config: Dict[str, Any], 
                   group_styles: Dict[str, Any], schema: str) -> bool:
        """
        Process a single job and generate its documentation.
        
        Returns:
            True if job was processed successfully, False otherwise
        """
        job_script_name = f"{layer}_{job['output_table_name']}"
        module_path = f"./{config['layers'][layer]}/{job['module']}.py"
        
        # Read job code
        job_code = self._read_job_code(module_path)
        if not job_code:
            return False
        catalog = config.get('configs',{}).get('catalog',None)
        # Get schema
        table_schema = self._get_job_schema(job_script_name, schema, catalog)
        table_schema = add_parent_column_for_table(
            table_schema,
            self.schemas,
            self.documentation_table_names,
        )
        schema_rows = dataframe_to_schema_rows(table_schema)
        
        # Process dependencies
        nodes, edges = self._process_job_dependencies(layer, job, config)
        mermaid = create_mermaid_graph(nodes, edges, group_styles)
        
        # Create job configuration object
        job_config = JobConfig(
            output_table_name=job['output_table_name'],
            module=job['module'],
            description=job.get('description', ""),
            input_tables=job.get('input_tables'),
            partition_by=job.get('partition_by'),
            unload=job.get('unload', True),
            incremental=job.get('incremental', False)
        )
        
        # Generate HTML page
        mermaid_page = self._create_mermaid_page(job_config, job_code, schema_rows, mermaid, module_path)
        
        # Write to file
        output_file = f"{TABLES_DIR}/{job_script_name}.html"
        with open(output_file, 'w', encoding="utf-8") as f:
            f.write(mermaid_page)
        
        self.documentation_table_names.append(job_script_name)
        return True

class LayerProcessor:
    """Handles processing of entire ETL layers."""
    
    def __init__(self, config: DocumentationConfig, spark=None):
        self.config = config
        self.job_processor = JobProcessor(config, spark)
    
    def _can_process_job(self, job: Dict[str, Any], layer: str, processed_jobs: set) -> bool:
        """Check if a job can be processed based on its dependencies."""
        input_tables = [str(v) for v in job.get('input_tables', {}).values()]
        return not any(
            (layer in table) and (table not in processed_jobs) 
            for table in input_tables
        )
    
    def process_layer(self, layer: str, module_prefix: str, config: Dict[str, Any], 
                     group_styles: Dict[str, Any], schema: str) -> int:
        """
        Process all jobs in a layer.
        
        Returns:
            Number of jobs processed
        """
        if layer not in config:
            print(f"Warning: Layer '{layer}' not found in config, skipping...")
            return 0
        
        job_count = 0
        queue = deque(list(config.get(layer, [])))
        processed_jobs = set()
        
        stalled_passes = 0
        max_stalls = len(queue) + 1
        
        while queue and stalled_passes < max_stalls:
            jobs_processed_this_pass = 0
            
            for _ in range(len(queue)):
                job = queue.popleft()
                
                if self._can_process_job(job, layer, processed_jobs):
                    job_script_name = f"{layer}_{job['output_table_name']}"
                    processed_jobs.add(job_script_name)
                    
                    if self.job_processor.process_job(layer, job, config, group_styles, schema):
                        job_count += 1
                        jobs_processed_this_pass += 1
                else:
                    queue.append(job)
            
            if jobs_processed_this_pass == 0:
                stalled_passes += 1
            else:
                stalled_passes = 0
        
        return job_count

def setup_directories() -> None:
    """Create necessary output directories."""
    os.makedirs(OUTPUTS_DIR, exist_ok=True)
    os.makedirs(DOCUMENTATION_DIR, exist_ok=True)
    os.makedirs(TABLES_DIR, exist_ok=True)
    os.makedirs(SCHEMA_DIR, exist_ok=True)

def orchestrator(
    target_layers: Optional[List[str]] = None,
    spark=None,
) -> Tuple[List[str], Dict[str, Any], pd.DataFrame]:
    """
    Generate all the orchestration for documentation.
    
    Returns:
        Tuple of (documentation_table_names, config, collected_schemas)
    """
    # Load configurations
    etl_config = load_yaml_config(DEFAULT_CONFIG_PATH)
    style_config = load_yaml_config(STYLE_CONFIG_PATH)
    
    config = DocumentationConfig(
        etl_config=etl_config,
        style_config=style_config,
        target_layers=target_layers
    )
    
    # Setup directories
    setup_directories()
    
    # Extract configuration values
    schema = etl_config['configs']['schema']
    layers = etl_config['layers']
    
    # Filter layers if specific ones are requested
    if target_layers:
        layers = {k: v for k, v in layers.items() if k in target_layers}
    
    # Get style configuration
    group_styles = style_config.get('style_colors', {}).get('layers_colors', {})
    
    # Process layers
    layer_processor = LayerProcessor(config, spark)
    
    for layer, module_prefix in layers.items():
        job_count = layer_processor.process_layer(layer, module_prefix, etl_config, group_styles, schema)
        print(f"Finished Layer: {layer} with generation of ({job_count} html documents)")
    
    return (
        layer_processor.job_processor.documentation_table_names,
        etl_config,
        ensure_schema_columns(layer_processor.job_processor.schemas),
    )

def build_html(items: List[str]) -> str:
    """Build the main HTML index page."""
    # Color palette
    style_config = resolve_style_colors(load_yaml_config(STYLE_CONFIG_PATH))
    colors = {
        'accent': style_config["index_accent"],
        'accent_hover': style_config["index_accent_hover"],
        'text': style_config["index_text"],
        'page_bg': style_config["index_page_bg"],
        'panel_bg': style_config["index_panel_bg"],
        'border': style_config["index_border"],
    }
    
    # Copy logo
    shutil.copy(f"{TEMPLATE_DIR}/logo.png", DOCUMENTATION_DIR)
    
    # Generate navigation items
    nav_items_html = "\n".join(
        f'''<li><a href="{item}" data-file="./tables/{item}.html">{item.upper().replace("_"," ")}</a></li>'''
        for item in items
    )
    
    logo_html = '<img src="logo.png" alt="Logo" class="logo"/>'
    
    # Load and format template
    with open(f'{TEMPLATE_DIR}/index.txt', 'r', encoding='utf-8') as f:
        html = f.read()
    
    return html.format(
        NAV_ITEMS=nav_items_html,
        LOGO_HTML=logo_html,
        **colors
    )

def generate_schema_script(tables_name: List[str], config: Dict[str, Any]) -> None:
    """Generate schema extraction script when Spark is not available."""
    with open(f"{TEMPLATE_DIR}/get_table_schemas.txt", 'r', encoding="utf-8") as f:
        function_table_schemas = f.read()
    
    serialized_tables = ",".join(f'"{name}"' for name in tables_name)

    function_table_schemas = function_table_schemas.format(
        schema=config['configs']['schema'],
        catalog=config['configs']['catalog'],
        tables_name=serialized_tables
    )
    
    with open(f"{SCHEMA_DIR}/schema.py", 'w', encoding="utf-8") as f:
        f.write(function_table_schemas)

def gen_doc(target_layers: Optional[List[str]] = None) -> None:
    """
    Generate Documentation with target layers.

    The schema-extraction script (`outputs/schema/schema.py`) is always
    emitted so notebook-only developers can produce `schema.csv` in their
    Spark environment without needing one locally.
    """
    # Try to create Spark session — optional. If unavailable, fall back to
    # the schema.csv that the user has produced via the generated schema.py.
    try:
        spark = create_session()
    except Exception as e:
        spark = None
        print(f"Spark Session could not be created: {e}")
        print("Falling back to schema.csv if present under ./outputs/schema/.")
        print("The schema-extraction script will be written to ./outputs/schema/schema.py — "
              "run it in your Spark environment, save the printed CSV as ./outputs/schema/schema.csv, "
              "and rerun `bolt generate documentation`.")

    # Generate documentation
    tables_name, config, schemas = orchestrator(target_layers, spark)

    # creating etlbase html
    style_colors = resolve_style_colors(load_yaml_config(STYLE_CONFIG_PATH))
    configs_section = config.get("configs", {})
    fixed_schema = configs_section.get("schema") or "cxdw_dm"
    save_catalog = configs_section.get("catalog") or "dev_catalog"
    generated_at = dt.datetime.now().astimezone()

    with open(f"{TEMPLATE_DIR}/etl_base_html.txt", 'r', encoding="utf-8") as f:
        etlbasehtml = f.read()
    with open(str(ETL_BASE_SOURCE), 'r', encoding="utf-8") as f:
        etlbasecode = f.read()
    etlbasehtml = etlbasehtml.format(
        etlbasecode=escape(etlbasecode),
        date=generated_at.strftime("%Y-%m-%d %H:%M:%S"),
        timezone=generated_at.tzname() or "local",
        module_path=str(ETL_BASE_SOURCE),
        fixed_schema=fixed_schema,
        read_catalog="shared_catalog",
        write_catalog=save_catalog,
        iceberg_identifier=f"{save_catalog}.{fixed_schema}.<layer>_<output_table_name>",
        incremental_column=configs_section.get("incremental_column", "year_month"),
        incremental_type=configs_section.get("incremental_type", "int"),
        incremental_unit=configs_section.get("incremental_unit", 3),
        incremental_date_grain=configs_section.get("incremental_date_grain", "monthly"),
        **style_colors,
    )
    with open(f"{DOCUMENTATION_DIR}/tables/etlBase.html", 'w', encoding="utf-8") as f:
        f.write(etlbasehtml)
    tables_name.insert(0,"etlBase")
    schema_table_names = [table for table in tables_name if table != "etlBase"]

    # Always emit the schema-extraction script. Useful in two flows:
    # (1) The user has no local Spark — they run schema.py in their cluster
    #     and paste the output into ./outputs/schema/schema.csv.
    # (2) The user wants to refresh schemas later without re-running docs.
    generate_schema_script(schema_table_names, config)
    schema_csv_generated = generate_schema_csv(schemas, schema_table_names, config)

    # Generate main HTML index
    with open(f"{DOCUMENTATION_DIR}/index.html", 'w', encoding="utf-8") as f:
        f.write(build_html(tables_name))

    print("\nDocumentation generation completed!"
          "\nYou may find the documentation under outputs/documentation/index.html"
          "\nSchema-extraction script written to outputs/schema/schema.py"
          + (
              "\nschema.csv generated at outputs/schema/schema.csv"
              if schema_csv_generated
              else "\nCould not auto-generate outputs/schema/schema.csv."
          ))

if __name__ == "__main__":
    target_layers = sys.argv[1:] if len(sys.argv) > 1 else None
    if target_layers:
        print(f"Generating documentation for layers: {', '.join(target_layers)}")
    else:
        print("Generating documentation for all layers...")
    gen_doc(target_layers)
    
