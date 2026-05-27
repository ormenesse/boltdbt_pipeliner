from __future__ import annotations

from pathlib import Path

import pytest
from typer.testing import CliRunner

from bolt_pipeliner.cli import generate as gen_cmd
from bolt_pipeliner.cli.app import app


runner = CliRunner()


def test_generate_help_omits_removed_snowflake_target():
    result = runner.invoke(app, ["generate", "--help"])

    assert result.exit_code == 0
    out = result.stdout
    for target in ("airflow", "documentation", "layers", "notebook", "all"):
        assert target in out
    assert "snowflakeddl" not in out


def test_generate_rejects_removed_snowflake_target():
    with pytest.raises(SystemExit) as exc:
        gen_cmd.execute(["snowflakeddl"], Path("configs/etl_config.yaml"))

    msg = str(exc.value)
    assert "Unknown generate target" in msg
    assert "snowflakeddl" in msg
    assert "snowflakeddl" not in msg.split("Valid:", 1)[1]
