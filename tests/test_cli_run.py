"""Smoke tests for `bolt run` CLI option wiring."""

from __future__ import annotations

from typer.testing import CliRunner

from bolt_pipeliner.cli.app import app


runner = CliRunner()


def test_run_rejects_select_combined_with_layer_flags():
    """--select and --bronze (or any layer flag) are mutually exclusive.
    Mixing them is a user error and must exit non-zero with a helpful message,
    not silently pick a winner.
    """
    result = runner.invoke(app, ["run", "--select", "bronze_x", "--bronze"])
    assert result.exit_code == 2
    assert "mutually exclusive" in result.output


def test_run_help_mentions_selector_syntax():
    """The help output for `bolt run` must surface the new --select / -s / -l
    options so users discover them without reading the README first.
    """
    result = runner.invoke(app, ["run", "--help"])
    assert result.exit_code == 0
    out = result.stdout
    assert "--select" in out
    assert "-s" in out
    assert "--layer" in out
    assert "-l" in out
    assert "--verbose" in out
