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
