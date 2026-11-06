"""Model discovery, ref parsing, DAG ordering, and selection.

A model is a SQL file. Dependencies come from ``{{ ref('name') }}`` calls in the
SQL. An optional TOML block at the top declares how to build it and its tests::

    /* config
    materialized = "incremental"
    unique_key = "event_id"
    [[tests]]
    type = "unique"
    column = "event_id"
    */
"""

from __future__ import annotations

import re
import tomllib
from dataclasses import dataclass, field
from pathlib import Path

from .bronze import SOURCES

LAYERS = ["bronze", "silver", "gold"]
REF_RE = re.compile(r"\{\{\s*ref\(\s*['\"]([A-Za-z0-9_]+)['\"]\s*\)\s*\}\}")
CONFIG_RE = re.compile(r"/\*\s*config\s*\n(.*?)\*/", re.S)
MODELS_DIR = Path(__file__).parent / "models"


class CycleError(ValueError):
    pass


class DagError(ValueError):
    pass


@dataclass
class Model:
    name: str
    layer: str
    sql: str = ""
    kind: str = "sql"  # "sql" or "ingest"
    config: dict = field(default_factory=dict)
    deps: list[str] = field(default_factory=list)
    path: str = ""

    @property
    def materialized(self) -> str:
        return self.config.get("materialized", "table")

    @property
    def tests(self) -> list[dict]:
        return self.config.get("tests", [])


def parse_refs(sql: str) -> list[str]:
    """Return referenced model names in order of first appearance, without duplicates."""
    seen: list[str] = []
    for name in REF_RE.findall(sql):
        if name not in seen:
            seen.append(name)
    return seen


def parse_config(sql: str) -> dict:
    m = CONFIG_RE.search(sql)
    return tomllib.loads(m.group(1)) if m else {}


def layer_of(name: str, config: dict) -> str:
    layer = config.get("layer") or name.split("_", 1)[0]
    if layer not in LAYERS:
        raise DagError(f"model {name}: unknown layer {layer!r}")
    return layer


def model_from_sql(name: str, sql: str, path: str = "") -> Model:
    cfg = parse_config(sql)
    return Model(name=name, layer=layer_of(name, cfg), sql=sql, config=cfg, deps=parse_refs(sql), path=path)


def load_models(models_dir: Path | None = None, include_bronze: bool = True) -> dict[str, Model]:
    models: dict[str, Model] = {}
    if include_bronze:
        for name in SOURCES:
            models[name] = Model(name=name, layer="bronze", kind="ingest", path=f"landing/{SOURCES[name].folder}")
    for path in sorted(Path(models_dir or MODELS_DIR).rglob("*.sql")):
        m = model_from_sql(path.stem, path.read_text(), str(path))
        if m.name in models:
            raise DagError(f"duplicate model name {m.name}")
        models[m.name] = m
    validate(models)
    return models


def validate(models: dict[str, Model]) -> None:
    for m in models.values():
        for d in m.deps:
            if d not in models:
                raise DagError(f"{m.name} refs unknown model {d!r}")
            if LAYERS.index(models[d].layer) > LAYERS.index(m.layer):
                raise DagError(f"{m.name} ({m.layer}) cannot depend on {d} ({models[d].layer})")
    topo_sort(models)


def _find_cycle(models: dict[str, Model]) -> list[str]:
    state: dict[str, int] = {}
    stack: list[str] = []

    def visit(n: str) -> list[str] | None:
        state[n] = 1
        stack.append(n)
        for d in models[n].deps:
            if d not in models:
                continue
            if state.get(d) == 1:
                return stack[stack.index(d):] + [d]
            if d not in state and (found := visit(d)):
                return found
        stack.pop()
        state[n] = 2
        return None

    for n in sorted(models):
        if n not in state and (found := visit(n)):
            return found
    return []


def topo_sort(models: dict[str, Model]) -> list[str]:
    """Kahn's algorithm. Ties break by layer, then name, so the order is stable."""
    indeg = {n: 0 for n in models}
    children: dict[str, list[str]] = {n: [] for n in models}
    for m in models.values():
        for d in m.deps:
            if d in models:
                indeg[m.name] += 1
                children[d].append(m.name)

    def key(n: str) -> tuple:
        return (LAYERS.index(models[n].layer), n)

    ready = sorted((n for n, k in indeg.items() if k == 0), key=key)
    order: list[str] = []
    while ready:
        n = ready.pop(0)
        order.append(n)
        for c in children[n]:
            indeg[c] -= 1
            if indeg[c] == 0:
                ready.append(c)
        ready.sort(key=key)
    if len(order) != len(models):
        raise CycleError("cycle detected: " + " -> ".join(_find_cycle(models)))
    return order


def descendants(models: dict[str, Model], name: str) -> set[str]:
    out: set[str] = set()
    frontier = [name]
    while frontier:
        cur = frontier.pop()
        for m in models.values():
            if cur in m.deps and m.name not in out:
                out.add(m.name)
                frontier.append(m.name)
    return out


def ancestors(models: dict[str, Model], name: str) -> set[str]:
    out: set[str] = set()
    frontier = [name]
    while frontier:
        for d in models[frontier.pop()].deps:
            if d not in out:
                out.add(d)
                frontier.append(d)
    return out


def select(models: dict[str, Model], specs: list[str] | None) -> list[str]:
    """dbt-style selection: ``name``, ``name+`` (and downstream), ``+name`` (and upstream),
    ``layer:silver``. No specs selects everything. Returns names in run order."""
    order = topo_sort(models)
    if not specs:
        return order
    chosen: set[str] = set()
    for raw in specs:
        for spec in raw.split(","):
            spec = spec.strip()
            if not spec:
                continue
            if spec.startswith("layer:"):
                chosen |= {n for n, m in models.items() if m.layer == spec[6:]}
                continue
            up, down = spec.startswith("+"), spec.endswith("+")
            name = spec.strip("+")
            if name not in models:
                raise DagError(f"unknown model in --select: {name}")
            chosen.add(name)
            if down:
                chosen |= descendants(models, name)
            if up:
                chosen |= ancestors(models, name)
    return [n for n in order if n in chosen]
