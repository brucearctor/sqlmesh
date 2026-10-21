from __future__ import annotations

import typing as t

import pytest
from pytest import FixtureRequest
from sqlglot import exp

from sqlmesh.core.engine_adapter import DorisEngineAdapter
from tests.core.engine_adapter.integration import (
    ENGINES_BY_NAME,
    IntegrationTestEngine,
    TestContext,
    generate_pytest_params,
)


@pytest.fixture(params=list(generate_pytest_params(ENGINES_BY_NAME["doris"])))
def ctx(
    request: FixtureRequest,
    create_test_context: t.Callable[[IntegrationTestEngine, str, str], t.Iterable[TestContext]],
) -> t.Iterable[TestContext]:
    yield from create_test_context(*request.param)


@pytest.fixture
def engine_adapter(ctx: TestContext) -> DorisEngineAdapter:
    assert isinstance(ctx.engine_adapter, DorisEngineAdapter)
    return ctx.engine_adapter


def test_engine_adapter(ctx: TestContext):
    assert isinstance(ctx.engine_adapter, DorisEngineAdapter)
    assert ctx.engine_adapter.fetchone("select 1") == (1,)


def test_doris_iceberg_local_catalog(ctx: TestContext):
    adapter = ctx.engine_adapter
    assert isinstance(adapter, DorisEngineAdapter)

    catalog_name = f"local_iceberg_{ctx.test_id}"
    schema_name = f"iceberg_db_{ctx.test_id}"
    table_fqn = f"{catalog_name}.{schema_name}.events"

    try:
        adapter.execute(
            f"""
            CREATE CATALOG IF NOT EXISTS `{catalog_name}` PROPERTIES (
                'type' = 'iceberg',
                'iceberg.catalog.type' = 'hadoop',
                'warehouse' = 'file:///tmp/doris_iceberg_warehouse/{catalog_name}'
            )
            """,
            quote_identifiers=False,
        )
        adapter.create_schema(f"{catalog_name}.{schema_name}")
        adapter.create_table(
            table_fqn,
            {
                "id": exp.DataType.build("int"),
                "ds": exp.DataType.build("date"),
            },
            partitioned_by=[exp.to_column("ds")],
        )

        cols = adapter.columns(table_fqn)
        assert "id" in cols
        assert "ds" in cols

        data_objects = adapter.get_data_objects(f"{catalog_name}.{schema_name}")
        assert any(obj.name == "events" for obj in data_objects)

        schema_target_fqn = f"{catalog_name}.{schema_name}.events_target"
        adapter.create_table(
            schema_target_fqn,
            {
                "id": exp.DataType.build("int"),
                "ds": exp.DataType.build("date"),
                "region": exp.DataType.build("varchar(32)"),
            },
            partitioned_by=[exp.to_column("ds")],
        )
        alter_ops = adapter.get_alter_operations(table_fqn, schema_target_fqn)
        assert len(alter_ops) == 1
        adapter.alter_table(alter_ops)

        updated_cols = adapter.columns(table_fqn)
        assert "region" in updated_cols
    finally:
        try:
            adapter.drop_schema(
                f"{catalog_name}.{schema_name}", ignore_if_not_exists=True, cascade=True
            )
        except Exception:
            pass
        try:
            adapter.execute(f"DROP CATALOG IF EXISTS `{catalog_name}`", quote_identifiers=False)
        except Exception:
            pass
        adapter.set_current_catalog("internal")


def test_materialized_view_sync(ctx: TestContext):
    import pandas as pd  # noqa: TID253
    from tests.core.engine_adapter.integration import wait_until

    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    source_table = ctx.table("source_table")
    ctx.engine_adapter.ctas(source_table, ctx.input_data(input_data), ctx.columns_to_types)
    view = exp.to_table("test_sync_mv")
    first_column = next(iter(ctx.columns_to_types))
    view_query = exp.select(first_column).from_(source_table)
    ctx.engine_adapter.create_view(
        view,
        view_query,
        materialized=True,
        view_properties={
            "materialized_type": exp.Literal.string("SYNC"),
            "source_table": source_table,
        },
    )

    def drop_view_success() -> bool:
        try:
            ctx.engine_adapter.drop_view(
                view,
                ignore_if_not_exists=False,
                materialized=True,
                view_properties={
                    "materialized_type": exp.Literal.string("SYNC"),
                    "source_table": source_table,
                },
            )
            return True
        except Exception:
            return False

    wait_until(drop_view_success, attempts=30, wait=1)


def test_materialized_view_async_mtmv(ctx: TestContext):
    import pandas as pd  # noqa: TID253
    from tests.core.engine_adapter.integration import wait_until

    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    source_table = ctx.table("mtmv_source")
    ctx.engine_adapter.ctas(source_table, ctx.input_data(input_data), ctx.columns_to_types)
    view = ctx.table("test_async_mtmv")
    view_query = exp.select(*ctx.columns_to_types).from_(source_table)
    ctx.engine_adapter.create_view(
        view,
        view_query,
        materialized=True,
        view_properties={
            "build": exp.Literal.string("IMMEDIATE"),
            "refresh": exp.Literal.string("COMPLETE"),
            "refresh_trigger": exp.Literal.string("ON MANUAL"),
            "distributed_by": exp.Literal.string("RANDOM BUCKETS AUTO"),
        },
    )
    wait_until(lambda: len(ctx.get_current_data(view)) == len(input_data), attempts=30, wait=1)
    ctx.compare_with_current(view, input_data)
    results = ctx.get_metadata_results()
    assert exp.to_table(view).name in results.materialized_views
    assert exp.to_table(view).name not in results.tables
    ctx.engine_adapter.drop_view(view, materialized=True)
    assert len(ctx.get_metadata_results().materialized_views) == 0


def test_merge_unique_key(ctx: TestContext):
    import pandas as pd  # noqa: TID253

    table = ctx.table("test_merge_uk")
    ctx.engine_adapter.create_table(
        table,
        ctx.columns_to_types,
        table_properties={"unique_key": exp.Literal.string("id")},
    )
    input_data = pd.DataFrame(
        [
            {"id": 1, "ds": "2022-01-01"},
            {"id": 2, "ds": "2022-01-02"},
            {"id": 3, "ds": "2022-01-03"},
        ]
    )
    ctx.engine_adapter.merge(
        table,
        ctx.input_data(input_data),
        target_columns_to_types=None,
        unique_key=[exp.to_identifier("id")],
    )
    merge_data = pd.DataFrame(
        [
            {"id": 2, "ds": "2022-01-10"},
            {"id": 4, "ds": "2022-01-04"},
        ]
    )
    ctx.engine_adapter.merge(
        table,
        ctx.input_data(merge_data),
        target_columns_to_types=None,
        unique_key=[exp.to_identifier("id")],
    )
    ctx.compare_with_current(
        table,
        pd.DataFrame(
            [
                {"id": 1, "ds": "2022-01-01"},
                {"id": 2, "ds": "2022-01-10"},
                {"id": 3, "ds": "2022-01-03"},
                {"id": 4, "ds": "2022-01-04"},
            ]
        ),
    )
