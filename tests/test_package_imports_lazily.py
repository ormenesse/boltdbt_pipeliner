"""Importing bolt_pipeliner must not pull in PySpark / Polars / Pandas.
Engine modules are only imported when a job actually instantiates a base.
"""

import sys


def test_package_import_does_not_import_pyspark():
    # Drop any prior cached imports so we exercise the lazy path.
    for mod in list(sys.modules):
        if mod.startswith("pyspark"):
            sys.modules.pop(mod)

    import bolt_pipeliner  # noqa: F401

    assert not any(m.startswith("pyspark") for m in sys.modules)


def test_runner_import_does_not_import_pyspark():
    for mod in list(sys.modules):
        if mod.startswith("pyspark") or mod.startswith("polars"):
            sys.modules.pop(mod)

    import bolt_pipeliner.runner  # noqa: F401

    assert not any(m.startswith("pyspark") for m in sys.modules)
    assert not any(m.startswith("polars") for m in sys.modules)
