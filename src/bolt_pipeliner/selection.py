"""dbt-style job selection for `bolt run` / `bolt test`.

Resolves selectors like ``+silver_orders``, ``bronze_x+``, ``+silver_y+``, or
plain ``silver_y`` into the concrete ordered set of ``(layer, job)`` pairs
that the runner should execute. Bare ``output_table_name`` is accepted when
unambiguous; pass ``layer=`` to constrain when the same name appears in
multiple layers.

Selector grammar
----------------
- ``name``    → just that job
- ``+name``   → upstream-of-name + name
- ``name+``   → name + downstream-of-name
- ``+name+``  → upstream + name + downstream

Where ``name`` is either the full job id (``{layer}_{output_table_name}``)
or a bare ``output_table_name``.
"""

from __future__ import annotations

from typing import Any


JobId = str  # f"{layer}_{output_table_name}"


def parse_selector(raw: str) -> tuple[bool, str, bool]:
    """Split ``raw`` into ``(upstream, name, downstream)``.

    >>> parse_selector("+silver_x+")
    (True, 'silver_x', True)
    >>> parse_selector("orders")
    (False, 'orders', False)
    """
    s = raw.strip()
    if not s or s in {"+", "++"}:
        raise ValueError(f"Empty selector after stripping '+': {raw!r}")
    upstream = s.startswith("+")
    downstream = s.endswith("+")
    name = s
    if upstream:
        name = name[1:]
    if downstream:
        name = name[:-1]
    if not name:
        raise ValueError(f"Selector {raw!r} has no table name between the '+' markers")
    return upstream, name, downstream


def build_graph(
    config: dict[str, Any],
) -> tuple[dict[JobId, tuple[str, dict[str, Any]]], dict[JobId, set[JobId]]]:
    """Walk ``etl_config`` and build (jobs_by_id, upstream_edges).

    ``upstream_edges[job_id]`` is the set of job ids that feed ``job_id``.

    External sources — flatfile paths, shared-catalog references like
    ``raw.crm_account`` — are ignored: they don't correspond to a job in
    this project and never get scheduled.
    """
    layer_names = set((config.get("layers") or {}).keys())

    jobs: dict[JobId, tuple[str, dict[str, Any]]] = {}
    for layer in layer_names:
        for job in config.get(layer) or []:
            if not isinstance(job, dict):
                continue
            output = job.get("output_table_name")
            if not output:
                continue
            jobs[f"{layer}_{output}"] = (layer, job)

    upstream: dict[JobId, set[JobId]] = {job_id: set() for job_id in jobs}

    for job_id, (_layer, job) in jobs.items():
        inputs = job.get("input_tables") or {}
        if not isinstance(inputs, dict):
            continue
        for value in inputs.values():
            if not isinstance(value, str):
                continue
            # A value refers to another job when it matches an existing
            # job_id exactly. We do not infer-by-layer-prefix because that
            # would false-match values like "bronze_orders" being read by
            # a silver job that also has output_table_name "orders" (giving
            # job_id "silver_orders" — different job).
            if value in jobs:
                upstream[job_id].add(value)

    return jobs, upstream


def _reverse(edges: dict[JobId, set[JobId]]) -> dict[JobId, set[JobId]]:
    """Build downstream adjacency from upstream adjacency."""
    rev: dict[JobId, set[JobId]] = {job_id: set() for job_id in edges}
    for child, parents in edges.items():
        for parent in parents:
            rev.setdefault(parent, set()).add(child)
    return rev


def _walk(adj: dict[JobId, set[JobId]], start: JobId) -> set[JobId]:
    """BFS over ``adj`` starting at ``start``. Inclusive of ``start``."""
    seen = {start}
    queue = [start]
    while queue:
        node = queue.pop()
        for neighbour in adj.get(node, ()):
            if neighbour not in seen:
                seen.add(neighbour)
                queue.append(neighbour)
    return seen


def resolve_name(
    jobs: dict[JobId, tuple[str, dict[str, Any]]],
    name: str,
    layer: str | None = None,
) -> JobId:
    """Resolve ``name`` to a job id.

    ``name`` is matched against:
      - full job_id (``{layer}_{output_table_name}``)
      - bare ``output_table_name``

    When the bare form is ambiguous (the same ``output_table_name`` exists
    in multiple layers), pass ``layer=`` to disambiguate. Raises ValueError
    on no-match or unresolved ambiguity.
    """
    candidates: list[JobId] = []
    for job_id, (job_layer, job) in jobs.items():
        if layer is not None and job_layer != layer:
            continue
        if job_id == name or job.get("output_table_name") == name:
            candidates.append(job_id)

    if not candidates:
        known = sorted(jobs.keys())
        hint = f" (layer={layer!r})" if layer else ""
        raise ValueError(
            f"No job matches selector {name!r}{hint}. Known jobs: {known}"
        )
    if len(candidates) > 1:
        raise ValueError(
            f"Selector {name!r} is ambiguous — matches {sorted(candidates)}. "
            f"Disambiguate with `--layer <layer>` or pass the full "
            f"`<layer>_<output_table_name>` form."
        )
    return candidates[0]


def select(
    config: dict[str, Any],
    selector: str,
    layer: str | None = None,
) -> list[tuple[str, dict[str, Any]]]:
    """Resolve ``selector`` against ``config`` and return ``(layer, job)``
    pairs in execution order.

    Execution order: layers in YAML ``layers:`` declaration order; within
    each layer, jobs in YAML appearance order. This matches the
    unconstrained ``bolt run`` behavior, just filtered to the selection.
    """
    jobs, upstream = build_graph(config)
    downstream = _reverse(upstream)

    up, name, down = parse_selector(selector)
    target = resolve_name(jobs, name, layer=layer)

    chosen: set[JobId] = {target}
    if up:
        chosen |= _walk(upstream, target)
    if down:
        chosen |= _walk(downstream, target)

    return _order(config, jobs, chosen)


def _order(
    config: dict[str, Any],
    jobs: dict[JobId, tuple[str, dict[str, Any]]],
    chosen: set[JobId],
) -> list[tuple[str, dict[str, Any]]]:
    """Sort selected jobs into YAML layer-order, YAML job-order within
    each layer. This is deterministic and matches the order an unfiltered
    `bolt run` would produce."""
    plan: list[tuple[str, dict[str, Any]]] = []
    layer_order = list((config.get("layers") or {}).keys())
    for layer in layer_order:
        for job in config.get(layer) or []:
            if not isinstance(job, dict):
                continue
            output = job.get("output_table_name")
            if not output:
                continue
            job_id = f"{layer}_{output}"
            if job_id in chosen:
                plan.append((layer, job))
    return plan


__all__ = [
    "parse_selector",
    "build_graph",
    "resolve_name",
    "select",
]
