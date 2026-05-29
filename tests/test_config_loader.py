from bolt_pipeliner.config.loader import (
    DEFAULT_CLASS_NAME,
    DEFAULT_FLATFILE_LOCATION,
    DEFAULT_INCREMENTAL_COLUMN,
    DEFAULT_INCREMENTAL_DATE_GRAIN,
    DEFAULT_INCREMENTAL_TYPE,
    DEFAULT_INCREMENTAL_UNIT,
    DEFAULT_OUTPUT_LOCATION,
    DEFAULT_SCHEMA,
    load_config,
    normalize_job,
)


def test_loader_applies_defaults_when_missing(write_config):
    path = write_config(
        """
        layers:
          bronze: etl/0_bronze
        bronze:
          - module: foo
            input_tables: {a: bar}
            output_table_name: foo_out
        """
    )

    config = load_config(path)

    assert config["configs"]["schema"] == DEFAULT_SCHEMA
    assert config["configs"]["incremental_column"] == DEFAULT_INCREMENTAL_COLUMN
    assert config["configs"]["incremental_type"] == DEFAULT_INCREMENTAL_TYPE
    assert config["configs"]["incremental_unit"] == DEFAULT_INCREMENTAL_UNIT
    assert config["configs"]["incremental_date_grain"] == DEFAULT_INCREMENTAL_DATE_GRAIN
    assert config["configs"]["flatfile_location"] == DEFAULT_FLATFILE_LOCATION
    assert config["configs"]["output_location"] == DEFAULT_OUTPUT_LOCATION
    assert config["bronze"][0]["class_name"] == DEFAULT_CLASS_NAME


def test_loader_preserves_user_set_schema(write_config):
    path = write_config(
        """
        configs:
          schema: my_schema
          incremental_column: yearMonth
        layers:
          bronze: etl/0_bronze
        bronze: []
        """
    )

    config = load_config(path)

    assert config["configs"]["schema"] == "my_schema"
    assert config["configs"]["incremental_column"] == "yearMonth"


def test_loader_maps_legacy_bucket_keys_to_new_location_keys(write_config):
    path = write_config(
        """
        configs:
          flatfile_bucket: s3://raw-data/flatfiles/
          output_bucket: s3://warehouse/tables/
        layers:
          bronze: etl/0_bronze
        bronze: []
        """
    )

    config = load_config(path)

    assert config["configs"]["flatfile_location"] == "s3://raw-data/flatfiles/"
    assert config["configs"]["output_location"] == "s3://warehouse/tables/"


def test_loader_maps_new_location_keys_to_legacy_aliases(write_config):
    path = write_config(
        """
        configs:
          flatfile_location: data/flatfiles
          output_location: outputs/tables
        layers:
          bronze: etl/0_bronze
        bronze: []
        """
    )

    config = load_config(path)

    assert config["configs"]["flatfile_bucket"] == "data/flatfiles"
    assert config["configs"]["output_bucket"] == "outputs/tables"


def test_normalize_renames_peco_input_tables_key():
    job = {"module": "m", "_input_tables": {"a": "b"}, "output_table_name": "o"}
    normalized = normalize_job(job)
    assert "_input_tables" not in normalized
    assert normalized["input_tables"] == {"a": "b"}


def test_normalize_does_not_overwrite_existing_input_tables():
    job = {
        "module": "m",
        "input_tables": {"explicit": "x"},
        "_input_tables": {"peco": "y"},
        "output_table_name": "o",
    }
    normalized = normalize_job(job)
    assert normalized["input_tables"] == {"explicit": "x"}


def test_loader_handles_placeholder_string_layer(write_config):
    """Real-world configs sometimes have `bronze: ...` which YAML parses to the
    string '...'. The loader should treat that as an empty layer, not iterate
    the characters.
    """
    path = write_config(
        """
        layers:
          bronze: etl/0_bronze
          silver: etl/1_silver
        bronze: ...
        silver:
          - module: m
            input_tables: {a: b}
            output_table_name: o
        """
    )

    config = load_config(path)

    assert config["bronze"] == []
    assert len(config["silver"]) == 1


def test_loader_handles_missing_layer_section(write_config):
    """If `layers:` declares a layer but the config has no matching section,
    the loader should normalize it to an empty list rather than KeyError.
    """
    path = write_config(
        """
        layers:
          bronze: etl/0_bronze
          silver: etl/1_silver
        bronze: []
        """
    )

    config = load_config(path)
    assert config["silver"] == []


def test_loader_skips_non_dict_job_entries(write_config):
    """A malformed entry (e.g., a bare string) should be silently skipped
    rather than crashing the normalizer.
    """
    path = write_config(
        """
        layers:
          bronze: etl/0_bronze
        bronze:
          - module: good
            input_tables: {a: b}
            output_table_name: out
          - "not-a-dict"
        """
    )

    config = load_config(path)
    assert len(config["bronze"]) == 1
    assert config["bronze"][0]["module"] == "good"
