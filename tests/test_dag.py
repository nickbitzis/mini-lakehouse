import pytest

from minilakehouse.dag import (
    CycleError,
    DagError,
    Model,
    load_models,
    model_from_sql,
    parse_config,
    parse_refs,
    select,
    topo_sort,
    validate,
)


def m(name, *deps, layer=None):
    return Model(name=name, layer=layer or name.split("_")[0], deps=list(deps))


def test_parse_refs_in_order_without_duplicates():
    sql = "select * from {{ ref('silver_a') }} join {{ref(\"silver_b\")}} using (id) join {{ ref( 'silver_a' ) }}"
    assert parse_refs(sql) == ["silver_a", "silver_b"]


def test_parse_refs_ignores_plain_text():
    assert parse_refs("select ref('x') from silver_a -- {{ var('as_of') }}") == []


def test_parse_config_reads_toml_header():
    cfg = parse_config('/* config\nmaterialized = "incremental"\n[[tests]]\ntype = "unique"\ncolumn = "id"\n*/\nselect 1')
    assert cfg["materialized"] == "incremental"
    assert cfg["tests"] == [{"type": "unique", "column": "id"}]


def test_model_from_sql_infers_layer_and_deps():
    mod = model_from_sql("gold_x", "select * from {{ ref('silver_y') }}")
    assert mod.layer == "gold" and mod.deps == ["silver_y"] and mod.materialized == "table"


def test_topo_sort_puts_dependencies_first():
    models = {x.name: x for x in [m("gold_c", "silver_b", "silver_a"), m("silver_b", "silver_a"), m("silver_a")]}
    assert topo_sort(models) == ["silver_a", "silver_b", "gold_c"]


def test_topo_sort_is_stable_by_layer_then_name():
    models = {x.name: x for x in [m("silver_z"), m("bronze_q"), m("silver_a"), m("gold_k", "silver_z")]}
    assert topo_sort(models) == ["bronze_q", "silver_a", "silver_z", "gold_k"]


def test_cycle_is_detected_with_path():
    models = {x.name: x for x in [m("silver_a", "silver_c"), m("silver_b", "silver_a"), m("silver_c", "silver_b")]}
    with pytest.raises(CycleError) as exc:
        topo_sort(models)
    msg = str(exc.value)
    assert "cycle detected" in msg and "silver_a" in msg and "silver_c" in msg


def test_unknown_ref_is_rejected():
    with pytest.raises(DagError, match="unknown model"):
        validate({"silver_a": m("silver_a", "silver_missing")})


def test_layer_order_is_enforced():
    with pytest.raises(DagError, match="cannot depend"):
        validate({"silver_a": m("silver_a", "gold_b"), "gold_b": m("gold_b")})


def test_bundled_project_loads(models):
    assert len(models) == 12
    assert set(models["gold_customer_360"].deps) == {
        "silver_subscriptions", "silver_invoices", "silver_product_events", "silver_customers"}
    order = topo_sort(models)
    assert order.index("silver_billing_events") < order.index("silver_invoices") < order.index("gold_customer_360")


def test_select_downstream(models):
    assert select(models, ["silver_invoices+"]) == ["silver_invoices", "gold_customer_360"]


def test_select_upstream(models):
    assert select(models, ["+silver_invoices"]) == ["bronze_billing_events", "silver_billing_events",
                                                     "silver_invoices"]


def test_select_layer_and_comma_list(models):
    assert select(models, ["layer:gold"]) == [n for n in topo_sort(models) if n.startswith("gold_")]
    assert select(models, ["silver_customers,gold_mrr_by_month"]) == ["silver_customers", "gold_mrr_by_month"]


def test_select_unknown_model_fails(models):
    with pytest.raises(DagError):
        select(models, ["silver_nope+"])


def test_duplicate_model_names_rejected(tmp_path):
    (tmp_path / "a").mkdir()
    (tmp_path / "b").mkdir()
    (tmp_path / "a" / "silver_x.sql").write_text("select 1")
    (tmp_path / "b" / "silver_x.sql").write_text("select 2")
    with pytest.raises(DagError, match="duplicate"):
        load_models(tmp_path, include_bronze=False)
