import typing as t

import pytest
from pytest_mock.plugin import MockerFixture
from sqlglot import expressions as exp
from sqlglot import parse_one

from sqlmesh.core.engine_adapter.doris import DorisEngineAdapter
from sqlmesh.core.engine_adapter.shared import CatalogSupport, DataObject, DataObjectType
from tests.core.engine_adapter import to_sql_calls

pytestmark = [pytest.mark.doris, pytest.mark.engine]


def test_create_view(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"))
    adapter.create_view("test_view", parse_one("SELECT a FROM tbl"), replace=False)
    # check view properties
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=False,
        view_properties={"a": exp.convert(1)},
    )

    assert to_sql_calls(adapter) == [
        "DROP VIEW IF EXISTS `test_view`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
        "CREATE VIEW `test_view` AS SELECT `a` FROM `tbl`",
    ]


def test_create_view_with_comment(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=False,
        target_columns_to_types={"a": exp.DataType.build("INT")},
        table_description="test_description",
        column_descriptions={"a": "test_column_description"},
    )

    assert to_sql_calls(adapter) == [
        "CREATE VIEW `test_view` (`a` COMMENT 'test_column_description') COMMENT 'test_description' AS SELECT `a` FROM `tbl`",
    ]


def test_create_materialized_view(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        materialized=True,
        target_columns_to_types={"a": exp.DataType.build("INT")},
    )
    adapter.create_view(
        "test_view",
        parse_one("SELECT a FROM tbl"),
        replace=False,
        materialized=True,
        target_columns_to_types={"a": exp.DataType.build("INT")},
    )

    assert to_sql_calls(adapter) == [
        "DROP MATERIALIZED VIEW IF EXISTS `test_view`",
        "CREATE MATERIALIZED VIEW `test_view` (`a`) AS SELECT `a` FROM `tbl`",
        "CREATE MATERIALIZED VIEW `test_view` (`a`) AS SELECT `a` FROM `tbl`",
    ]

    adapter.cursor.reset_mock()
    adapter.create_view(
        "test_view",
        parse_one("SELECT a, b FROM tbl"),
        replace=False,
        materialized=True,
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description", "b": "test_column_description"},
    )
    adapter.create_view(
        "test_view", parse_one("SELECT a, b FROM tbl"), replace=False, materialized=True
    )

    assert to_sql_calls(adapter) == [
        "CREATE MATERIALIZED VIEW `test_view` (`a` COMMENT 'test_column_description', `b` COMMENT 'test_column_description') AS SELECT `a`, `b` FROM `tbl`",
        "CREATE MATERIALIZED VIEW `test_view` AS SELECT `a`, `b` FROM `tbl`",
    ]


def test_create_table(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description"},
        table_properties={"unique_key": exp.Column(this=exp.Identifier(this="a", quoted=True))},
    )
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        column_descriptions={"a": "test_column_description"},
        table_properties={"duplicate_key": exp.Column(this=exp.Identifier(this="a", quoted=True))},
    )
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        table_description="test_description",
        column_descriptions={"a": "test_column_description"},
    )
    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT 'test_column_description') UNIQUE KEY (`a`) DISTRIBUTED BY HASH (`a`) BUCKETS 10",
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT 'test_column_description') DUPLICATE KEY (`a`)",
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT COMMENT 'test_column_description') COMMENT 'test_description'",
    ]

    adapter.cursor.reset_mock()


def test_create_table_like(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.create_table_like("target_table", "source_table")

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `target_table` LIKE `source_table`",
    ]


def test_create_schema(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_schema("test_schema")

    assert to_sql_calls(adapter) == [
        "CREATE DATABASE IF NOT EXISTS `test_schema`",
    ]


def test_drop_schema(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.drop_schema("test_schema")
    adapter.drop_schema("test_schema", ignore_if_not_exists=False)

    assert to_sql_calls(adapter) == [
        "DROP DATABASE IF EXISTS `test_schema`",
        "DROP DATABASE `test_schema`",
    ]


def test_rename_table(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.rename_table("old_table", "new_table")
    adapter.cursor.execute.assert_called_once_with("ALTER TABLE `old_table` RENAME `new_table`")


def test_replace_by_key(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    temp_table = parse_one("temp_table")
    mocker.patch.object(adapter, "_get_temp_table", return_value=temp_table)
    mocker.patch.object(adapter, "ctas")
    mocker.patch.object(adapter, "delete_from")
    mocker.patch.object(adapter, "drop_table")

    columns_to_types = {"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")}
    source_query = parse_one("SELECT a, b FROM src")
    target_table = "target_table"

    adapter._replace_by_key(target_table, source_query, columns_to_types, [exp.column("a")], True)
    adapter._replace_by_key(target_table, source_query, columns_to_types, [exp.column("a")], False)
    adapter._replace_by_key(
        target_table, source_query, columns_to_types, [exp.column("a"), exp.column("b")], True
    )
    adapter._replace_by_key(
        target_table, source_query, columns_to_types, [exp.column("a"), exp.column("b")], False
    )

    assert to_sql_calls(adapter, identify=True) == [
        "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM (SELECT `a` AS `a`, `b` AS `b`, ROW_NUMBER() OVER (PARTITION BY `a` ORDER BY `a`) AS _row_number FROM `temp_table`) AS _t WHERE _row_number = 1",
        "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM `temp_table`",
        "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM (SELECT `a` AS `a`, `b` AS `b`, ROW_NUMBER() OVER (PARTITION BY `a`, `b` ORDER BY `a`, `b`) AS _row_number FROM `temp_table`) AS _t WHERE _row_number = 1",
        "INSERT INTO `target_table` (`a`, `b`) SELECT `a`, `b` FROM `temp_table`",
    ]


def test_create_index(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.create_index("test_table", "test_index", ("cola",))
    adapter.cursor.execute.assert_called_once_with(
        "CREATE INDEX IF NOT EXISTS `test_index` ON `test_table`(`cola`)"
    )


def test_create_table_with_distributed_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    distributed_by = {
        "expressions": ["a", "b"],
        "kind": "HASH",
        "buckets": 8,
    }
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` INT) DISTRIBUTED BY HASH (`a`, `b`) BUCKETS 8",
    ]

    adapter.cursor.execute.reset_mock()

    distributed_by = {
        "expressions": None,
        "kind": "RANDOM",
        "buckets": None,
    }
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` INT) DISTRIBUTED BY RANDOM",
    ]

    adapter.cursor.execute.reset_mock()

    distributed_by = {
        "expressions": ["a"],
        "kind": "HASH",
        "buckets": "AUTO",
    }
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("INT")},
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` INT) DISTRIBUTED BY HASH (`a`) BUCKETS AUTO",
    ]


def test_create_table_with_properties(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT")},
        table_properties={
            "refresh_interval": "86400",
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT) PROPERTIES ('refresh_interval'='86400')",
    ]


def test_create_table_with_partitioned_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={"a": exp.DataType.build("INT"), "b": exp.DataType.build("DATE")},
        partitioned_by=[exp.Literal.string("RANGE(b)")],
        table_properties={
            "partitions": exp.Literal.string(
                "FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR"
            )
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`a` INT, `b` DATE) PARTITION BY RANGE (`b`) (FROM ('2000-11-14') TO ('2021-11-14') INTERVAL 2 YEAR)",
    ]


def test_create_table_with_range_partitioned_by_with_partitions(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "waiter_id": exp.DataType.build("INT"),
            "customer_id": exp.DataType.build("INT"),
            "ds": exp.DataType.build("DATETIME"),
        },
        partitioned_by=[exp.Literal.string("RANGE(ds)")],
        table_properties={
            "partitions": exp.Tuple(
                expressions=[
                    exp.Literal.string('PARTITION `p2023` VALUES [("2023-01-01"), ("2024-01-01"))'),
                    exp.Literal.string('PARTITION `p2024` VALUES [("2024-01-01"), ("2025-01-01"))'),
                    exp.Literal.string('PARTITION `p2025` VALUES [("2025-01-01"), ("2026-01-01"))'),
                    exp.Literal.string("PARTITION `other` VALUES LESS THAN MAXVALUE"),
                ]
            ),
            "distributed_by": exp.Tuple(
                expressions=[
                    exp.EQ(
                        this=exp.Column(this=exp.Identifier(this="kind", quoted=True)),
                        expression=exp.Literal.string("HASH"),
                    ),
                    exp.EQ(
                        this=exp.Column(this=exp.Identifier(this="expressions", quoted=True)),
                        expression=exp.Column(this=exp.Identifier(this="id", quoted=True)),
                    ),
                    exp.EQ(
                        this=exp.Column(this=exp.Identifier(this="buckets", quoted=True)),
                        expression=exp.Literal.number(10),
                    ),
                ]
            ),
            "replication_allocation": exp.Literal.string("tag.location.default: 3"),
            "in_memory": exp.Literal.string("false"),
            "storage_format": exp.Literal.string("V2"),
            "disable_auto_compaction": exp.Literal.string("false"),
        },
    )

    expected_sql = (
        "CREATE TABLE IF NOT EXISTS `test_table` "
        "(`id` INT, `waiter_id` INT, `customer_id` INT, `ds` DATETIME) "
        "PARTITION BY RANGE (`ds`) "
        '(PARTITION `p2023` VALUES [("2023-01-01"), ("2024-01-01")), '
        'PARTITION `p2024` VALUES [("2024-01-01"), ("2025-01-01")), '
        'PARTITION `p2025` VALUES [("2025-01-01"), ("2026-01-01")), '
        "PARTITION `other` VALUES LESS THAN MAXVALUE) "
        "DISTRIBUTED BY HASH (`id`) BUCKETS 10 "
        "PROPERTIES ("
        "'replication_allocation'='tag.location.default: 3', "
        "'in_memory'='false', "
        "'storage_format'='V2', "
        "'disable_auto_compaction'='false')"
    )

    assert to_sql_calls(adapter) == [expected_sql]


def test_create_table_with_list_partitioned_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "status": exp.DataType.build("VARCHAR(10)"),
        },
        partitioned_by=[exp.Literal.string("LIST(status)")],
        table_properties={
            "partitions": exp.Tuple(
                expressions=[
                    exp.Literal.string('PARTITION `active` VALUES IN ("active", "pending")'),
                    exp.Literal.string('PARTITION `inactive` VALUES IN ("inactive", "disabled")'),
                ]
            ),
        },
    )

    expected_sql = (
        "CREATE TABLE IF NOT EXISTS `test_table` "
        "(`id` INT, `status` VARCHAR(10)) "
        "PARTITION BY LIST (`status`) "
        '(PARTITION `active` VALUES IN ("active", "pending"), '
        'PARTITION `inactive` VALUES IN ("inactive", "disabled"))'
    )

    assert to_sql_calls(adapter) == [expected_sql]


def test_create_table_with_range_partitioned_by_anonymous_function(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    """Test that RANGE(ds) function call syntax generates correct SQL without duplicate RANGE."""
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "ds": exp.DataType.build("DATETIME"),
        },
        # This simulates how partitioned_by RANGE(ds) gets parsed from model definition
        partitioned_by=[exp.Anonymous(this="RANGE", expressions=[exp.to_column("ds")])],
        table_properties={
            "partitions": exp.Literal.string(
                'FROM ("2000-11-14") TO ("2099-11-14") INTERVAL 1 MONTH'
            )
        },
    )

    expected_sql = (
        "CREATE TABLE IF NOT EXISTS `test_table` "
        "(`id` INT, `ds` DATETIME) "
        "PARTITION BY RANGE (`ds`) "
        '(FROM ("2000-11-14") TO ("2099-11-14") INTERVAL 1 MONTH)'
    )

    assert to_sql_calls(adapter) == [expected_sql]


def test_create_materialized_view_with_duplicate_key(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_view(
        "test_mv",
        parse_one("SELECT id, status, COUNT(*) as cnt FROM orders GROUP BY id, status"),
        materialized=True,
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "status": exp.DataType.build("VARCHAR(10)"),
            "cnt": exp.DataType.build("BIGINT"),
        },
        view_properties={
            "duplicate_key": exp.Tuple(
                expressions=[
                    exp.to_column("id"),
                    exp.to_column("status"),
                ]
            ),
        },
    )

    expected_sqls = [
        "DROP MATERIALIZED VIEW IF EXISTS `test_mv`",
        (
            "CREATE MATERIALIZED VIEW `test_mv` "
            "(`id`, `status`, `cnt`) "
            "DUPLICATE KEY (`id`, `status`) "
            "AS SELECT `id`, `status`, COUNT(*) AS `cnt` FROM `orders` GROUP BY `id`, `status`"
        ),
    ]

    assert to_sql_calls(adapter) == expected_sqls


def test_create_full_materialized_view(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    view_properties: t.Dict[str, exp.Expr] = {
        "build": exp.Literal.string("IMMEDIATE"),
        "refresh": exp.Tuple(
            expressions=[
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="method", quoted=True)),
                    expression=exp.Literal.string("AUTO"),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="kind", quoted=True)),
                    expression=exp.Literal.string("SCHEDULE"),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="every", quoted=True)),
                    expression=exp.Literal.number(1),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="unit", quoted=True)),
                    expression=exp.Literal.string("DAY"),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="starts", quoted=True)),
                    expression=exp.Literal.string("2024-12-01 20:30:00"),
                ),
            ],
        ),
        "distributed_by": exp.Tuple(
            expressions=[
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="kind", quoted=True)),
                    expression=exp.Literal.string("HASH"),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="expressions", quoted=True)),
                    expression=exp.Column(this=exp.Identifier(this="orderkey", quoted=True)),
                ),
                exp.EQ(
                    this=exp.Column(this=exp.Identifier(this="buckets", quoted=True)),
                    expression=exp.Literal.number(2),
                ),
            ]
        ),
        "unique_key": exp.to_column("orderkey"),
        "replication_num": exp.Literal.string("1"),
    }
    materialized_properties = {
        "partitioned_by": [
            parse_one("DATE_TRUNC(o_orderdate, 'MONTH')", dialect="doris"),
        ],
        "clustered_by": [],
        "partition_interval_unit": None,
    }
    columns_to_types = {
        "orderdate": exp.DataType.build("DATE"),
        "orderkey": exp.DataType.build("INT"),
        "partkey": exp.DataType.build("INT"),
    }
    column_descriptions = {
        "orderdate": "order date",
        "orderkey": "order key",
        "partkey": "part key",
    }
    query = parse_one(
        """
        SELECT 
        o_orderdate, 
        l_orderkey, 
        l_partkey 
        FROM 
        orders 
        LEFT JOIN lineitem ON l_orderkey = o_orderkey 
        LEFT JOIN partsupp ON ps_partkey = l_partkey 
        and l_suppkey = ps_suppkey
        """
    )
    adapter.create_view(
        "complete_mv",
        query,
        replace=False,
        materialized=True,
        target_columns_to_types=columns_to_types,
        column_descriptions=column_descriptions,
        table_description="test_description",
        view_properties=view_properties,
        materialized_properties=materialized_properties,
    )
    expected_sqls = [
        "CREATE MATERIALIZED VIEW `complete_mv` (`orderdate` COMMENT 'order date', `orderkey` COMMENT 'order key', `partkey` COMMENT 'part key') "
        "BUILD IMMEDIATE REFRESH AUTO ON SCHEDULE EVERY 1 DAY STARTS '2024-12-01 20:30:00' KEY (`orderkey`) COMMENT 'test_description' "
        "PARTITION BY (DATE_TRUNC(`o_orderdate`, 'MONTH')) "
        "DISTRIBUTED BY HASH (`orderkey`) BUCKETS 2 PROPERTIES ('replication_num'='1') "
        "AS SELECT `o_orderdate`, `l_orderkey`, `l_partkey` FROM `orders` LEFT JOIN `lineitem` ON `l_orderkey` = `o_orderkey` LEFT JOIN `partsupp` ON `ps_partkey` = `l_partkey` AND `l_suppkey` = `ps_suppkey`",
    ]
    sql_calls = to_sql_calls(adapter)

    # Remove extra spaces for comparison
    def norm(s):
        return " ".join(s.split())

    for expected_sql in expected_sqls:
        assert any(norm(expected_sql) == norm(sql) for sql in sql_calls), (
            f"Expected SQL not found.\nExpected: {expected_sql}\nGot: {sql_calls}"
        )


def test_create_table_with_single_string_distributed_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    """Test creating table with distributed_by where expressions is a single string."""
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    distributed_by = {
        "expressions": "recordid",  # Single string instead of array
        "kind": "HASH",
        "buckets": 10,
    }
    adapter.create_table(
        "test_table",
        target_columns_to_types={
            "recordid": exp.DataType.build("INT"),
            "name": exp.DataType.build("VARCHAR"),
        },
        table_properties={"distributed_by": distributed_by},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_table` (`recordid` INT, `name` VARCHAR) DISTRIBUTED BY HASH (`recordid`) BUCKETS 10",
    ]


def test_delete_from_with_subquery(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Test DELETE FROM with IN subquery
    adapter.delete_from(
        "test_schema_xtll416o.test_table",
        "id IN (SELECT id FROM test_schema_xtll416o.temp_test_table_ik29031e)",
    )

    # Test DELETE FROM with NOT IN subquery (must NOT rewrite to USING ... WHERE <>)
    adapter.delete_from(
        "test_schema_xtll416o.test_table",
        "id NOT IN (SELECT id FROM test_schema_xtll416o.temp_test_table_ik29031e)",
    )

    # Test simple DELETE FROM (should use base implementation)
    adapter.delete_from("test_schema_xtll416o.test_table", "id = 1")

    assert to_sql_calls(adapter) == [
        "DELETE FROM `test_schema_xtll416o`.`test_table` AS `_t1` USING (SELECT `id` FROM `test_schema_xtll416o`.`temp_test_table_ik29031e`) AS `_t2` WHERE `_t1`.`id` = `_t2`.`id`",
        "DELETE FROM `test_schema_xtll416o`.`test_table` WHERE NOT `id` IN (SELECT `id` FROM `test_schema_xtll416o`.`temp_test_table_ik29031e`)",
        "DELETE FROM `test_schema_xtll416o`.`test_table` WHERE `id` = 1",
    ]


def test_delete_from_with_complex_subquery(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Test DELETE FROM with complex subquery (multiple columns)
    adapter.delete_from(
        "test_schema_xtll416o.test_table",
        "(id, name) IN (SELECT id, name FROM test_schema_xtll416o.temp_test_table_ik29031e WHERE active = 1)",
    )

    # Test DELETE FROM with subquery that has WHERE clause
    adapter.delete_from(
        "test_schema_xtll416o.test_table",
        "id IN (SELECT id FROM test_schema_xtll416o.temp_test_table_ik29031e WHERE created_date > '2024-01-01')",
    )

    assert to_sql_calls(adapter) == [
        "DELETE FROM `test_schema_xtll416o`.`test_table` AS `_t1` USING (SELECT `id`, `name` FROM `test_schema_xtll416o`.`temp_test_table_ik29031e` WHERE `active` = 1) AS `_t2` WHERE `_t1`.`id` = `_t2`.`id` AND `_t1`.`name` = `_t2`.`name`",
        "DELETE FROM `test_schema_xtll416o`.`test_table` AS `_t1` USING (SELECT `id` FROM `test_schema_xtll416o`.`temp_test_table_ik29031e` WHERE `created_date` > '2024-01-01') AS `_t2` WHERE `_t1`.`id` = `_t2`.`id`",
    ]


def test_delete_from_fallback_to_base(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Test DELETE FROM with simple conditions (should use base implementation)
    adapter.delete_from("test_table", "id = 1")
    adapter.delete_from("test_table", "name = 'test' AND status = 'active'")
    adapter.delete_from("test_table", "id IN (1, 2, 3)")  # Simple IN with values, not subquery

    assert to_sql_calls(adapter) == [
        "DELETE FROM `test_table` WHERE `id` = 1",
        "DELETE FROM `test_table` WHERE `name` = 'test' AND `status` = 'active'",
        "DELETE FROM `test_table` WHERE `id` IN (1, 2, 3)",
    ]


def test_delete_from_full_table(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Test DELETE FROM with WHERE TRUE (should use TRUNCATE)
    adapter.delete_from("test_table", exp.true())

    assert to_sql_calls(adapter) == [
        "TRUNCATE TABLE `test_table`",
    ]


def test_create_table_olap_key_prefix_reordering_and_order_by(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "events",
        target_columns_to_types={
            "payload": exp.DataType.build("VARCHAR(255)"),
            "id": exp.DataType.build("BIGINT"),
            "ds": exp.DataType.build("DATE"),
            "updated_at": exp.DataType.build("DATETIME"),
        },
        unique_key=[exp.to_column("id"), exp.to_column("ds")],
        table_properties={
            "order_by": exp.Tuple(expressions=[exp.to_column("updated_at"), exp.to_column("id")]),
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `events` (`id` BIGINT, `ds` DATE, `payload` VARCHAR(255), `updated_at` DATETIME) UNIQUE KEY (`id`, `ds`) ORDER BY (`updated_at`, `id`) DISTRIBUTED BY HASH (`id`) BUCKETS 10",
    ]


def test_create_table_aggregate_key_and_clustered_by_buckets(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "metrics_agg",
        target_columns_to_types={
            "total": exp.DataType.build("BIGINT"),
            "tenant_id": exp.DataType.build("INT"),
            "ds": exp.DataType.build("DATE"),
        },
        clustered_by=[exp.to_column("tenant_id")],
        table_properties={
            "aggregate_key": exp.Tuple(
                expressions=[exp.to_column("tenant_id"), exp.to_column("ds")]
            ),
            "buckets": exp.Literal.number(16),
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `metrics_agg` (`tenant_id` INT, `ds` DATE, `total` BIGINT) AGGREGATE KEY (`tenant_id`, `ds`) DISTRIBUTED BY HASH (`tenant_id`) BUCKETS 16",
    ]


def test_create_table_auto_partition_and_unannotated_partitioned_by_guard(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    # Without partitions or auto_partition, OLAP partitioned_by is safely omitted
    adapter.create_table(
        "olap_no_partitions",
        target_columns_to_types={"id": exp.DataType.build("INT"), "ds": exp.DataType.build("DATE")},
        partitioned_by=[exp.to_column("ds")],
    )
    # With auto_partition = true, emits AUTO PARTITION BY RANGE (...) ()
    adapter.create_table(
        "olap_auto_partition",
        target_columns_to_types={"id": exp.DataType.build("INT"), "ds": exp.DataType.build("DATE")},
        partitioned_by=[parse_one("RANGE(DATE_TRUNC(ds, 'DAY'))", dialect="doris")],
        table_properties={"auto_partition": exp.Literal.string("true")},
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `olap_no_partitions` (`id` INT, `ds` DATE)",
        "CREATE TABLE IF NOT EXISTS `olap_auto_partition` (`id` INT, `ds` DATE) AUTO PARTITION BY RANGE (DATE_TRUNC(`ds`, 'DAY')) ()",
    ]


def test_iceberg_multi_catalog_ddl_and_dml(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    assert adapter.catalog_support == CatalogSupport.FULL_SUPPORT
    assert adapter.default_catalog is None

    # 1. Explicit table_format='iceberg' on 2-part table name emits ENGINE=iceberg and PARTITION BY LIST (...) ()
    adapter.create_table(
        "silver.iceberg_events",
        target_columns_to_types={
            "id": exp.DataType.build("BIGINT"),
            "ds": exp.DataType.build("DATE"),
            "region": exp.DataType.build("STRING"),
        },
        table_format="iceberg",
        partitioned_by=[parse_one("DAY(ds)", dialect="doris"), exp.to_column("region")],
        unique_key=[exp.to_column("id")],
        table_properties={"write-format": exp.Literal.string("parquet")},
    )

    # 2. 3-part external Iceberg catalog CTAS omits ENGINE=iceberg (invalid in Doris CTAS grammar)
    adapter.ctas(
        "iceberg_cat.silver.events_ctas",
        parse_one("SELECT id, ds FROM bronze.events"),
        target_columns_to_types={
            "id": exp.DataType.build("BIGINT"),
            "ds": exp.DataType.build("DATE"),
        },
    )

    # 3. replace_query on existing Iceberg table uses atomic INSERT OVERWRITE TABLE
    mocker.patch.object(
        adapter,
        "get_data_object",
        return_value=DataObject(
            catalog="iceberg_cat", schema="silver", name="events_ctas", type=DataObjectType.TABLE
        ),
    )
    adapter.replace_query(
        "iceberg_cat.silver.events_ctas",
        parse_one("SELECT id, ds FROM bronze.events"),
        target_columns_to_types={
            "id": exp.DataType.build("BIGINT"),
            "ds": exp.DataType.build("DATE"),
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `silver`.`iceberg_events` (`id` BIGINT, `ds` DATE, `region` STRING) ENGINE=iceberg PARTITION BY LIST (DAY(`ds`), `region`) () PROPERTIES ('write-format'='parquet')",
        "CREATE TABLE IF NOT EXISTS `iceberg_cat`.`silver`.`events_ctas` AS SELECT CAST(`id` AS BIGINT) AS `id`, CAST(`ds` AS DATE) AS `ds` FROM (SELECT `id`, `ds` FROM `bronze`.`events`) AS `_subquery`",
        "INSERT OVERWRITE TABLE `iceberg_cat`.`silver`.`events_ctas` (`id`, `ds`) SELECT `id`, `ds` FROM `bronze`.`events`",
    ]


def test_columns_normalization_largeint_and_struct(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.cursor.fetchall.return_value = [
        ("id", "LARGEINT", "No", "true", None, ""),
        ("price", "DECIMALV3(9, 0)", "Yes", "false", None, ""),
        ("is_active", "TINYINT(1)", "Yes", "false", None, ""),
        ("count_val", "INT(11)", "Yes", "false", None, ""),
        ("meta", "STRUCT<f1:INT(11), f2:VARCHAR(20)>", "Yes", "false", None, ""),
    ]

    cols = adapter.columns("test_db.test_table")
    assert cols == {
        "id": exp.DataType.build("INT128"),
        "price": exp.DataType.build("DECIMAL(9, 0)", dialect="doris"),
        "is_active": exp.DataType.build("BOOLEAN", dialect="doris"),
        "count_val": exp.DataType.build("INT", dialect="doris"),
        "meta": exp.DataType.build("STRUCT<f1:INT, f2:VARCHAR(20)>", dialect="doris"),
    }

    adapter.cursor.reset_mock()
    adapter.create_table(
        "test_db.struct_tbl",
        target_columns_to_types={"id": cols["id"], "meta": cols["meta"]},
    )
    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `test_db`.`struct_tbl` (`id` LARGEINT, `meta` STRUCT<`f1`: INT, `f2`: VARCHAR(20)>)",
    ]


def test_get_data_objects_with_catalog_and_mv(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter, patch_get_data_objects=False)
    mocker.patch.object(
        adapter,
        "fetchall",
        return_value=[
            ("silver", "tbl1", "table"),
            ("silver", "mv1", "materialized_view"),
        ],
    )

    objs = adapter._get_data_objects("iceberg_cat.silver")
    assert len(objs) == 2
    assert objs[0].type == DataObjectType.TABLE
    assert objs[0].catalog == "iceberg_cat"
    assert objs[0].schema_name == "silver"
    assert objs[0].name == "tbl1"
    assert objs[1].type == DataObjectType.MATERIALIZED_VIEW
    assert objs[1].name == "mv1"


def test_comments_escaping(make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter]):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    table_comment_sql = adapter._build_create_comment_table_exp(
        exp.to_table("db.tbl"), " O'Reilly's \"table\" \\ test "
    )
    col_comment_sql = adapter._build_create_comment_column_exp(
        exp.to_table("db.tbl"), "col'1", ' Col\'s "comment" '
    )
    assert (
        table_comment_sql
        == "ALTER TABLE `db`.`tbl` MODIFY COMMENT 'O''Reilly''s \"table\" \\\\ test'"
    )
    assert (
        col_comment_sql
        == "ALTER TABLE `db`.`tbl` MODIFY COLUMN `col'1` COMMENT 'Col''s \"comment\"'"
    )


def test_delete_from_between_concat_ws_and_literal_fallback(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # 1. BETWEEN rewrite in delete_from
    adapter.delete_from("test_table", "ds BETWEEN '2024-01-01' AND '2024-01-31'")

    # 2. _replace_by_key / LogicalMergeMixin CONCAT_WS unwrap into multi-column USING
    key_exp = exp.func("CONCAT_WS", "'__SQLMESH_DELIM__'", exp.column("id"), exp.column("ds"))
    merge_where = key_exp.isin(query=exp.select(key_exp).from_("src_table"))
    adapter.delete_from("target_table", where=merge_where)

    assert to_sql_calls(adapter) == [
        "DELETE FROM `test_table` WHERE `ds` >= '2024-01-01' AND `ds` <= '2024-01-31'",
        "DELETE FROM `target_table` AS `_t1` USING (SELECT `id`, `ds` FROM `src_table`) AS `_t2` WHERE `_t1`.`id` = `_t2`.`id` AND `_t1`.`ds` = `_t2`.`ds`",
    ]

    # 3. Fallback to zero-OR batched _delete_by_literal_keys when USING fails on DUPLICATE KEY / Iceberg
    adapter.cursor.reset_mock()
    mocker.patch.object(
        adapter,
        "fetchall",
        return_value=[("tenant_a", 1), ("tenant_a", 2), ("tenant_a", None)],
    )
    adapter._delete_by_literal_keys(
        "dup_table",
        [exp.column("tenant"), exp.column("id")],
        parse_one("SELECT tenant, id FROM src_table", into=exp.Select),
    )
    assert to_sql_calls(adapter) == [
        "DELETE FROM `dup_table` WHERE `tenant` = 'tenant_a' AND `id` IN (1, 2)",
        "DELETE FROM `dup_table` WHERE `tenant` = 'tenant_a' AND `id` IS NULL",
    ]


def test_execute_operational_error_reconnect(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    class OperationalError(Exception):
        pass

    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    conn_mock = mocker.MagicMock()
    mocker.patch.object(adapter._connection_pool, "get", return_value=conn_mock)

    call_count = 0

    def side_effect(*args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            raise OperationalError(2013, "Lost connection to MySQL server during query")

    adapter.cursor.execute.side_effect = side_effect
    adapter._execute("SELECT 1")

    assert call_count == 2
    conn_mock.ping.assert_called_once_with(reconnect=True)


def test_external_catalog_view_skip_and_materialized_error(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    from sqlmesh.utils.errors import SQLMeshError

    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Regular view creation and deletion on external catalog is a safe no-op (warns instead of failing dev promotion)
    adapter.create_view("iceberg_cat.silver__dev.events", parse_one("SELECT 1 AS id"))
    adapter.drop_view("iceberg_cat.silver__dev.events")
    assert to_sql_calls(adapter) == []

    # Materialized view on external catalog still raises SQLMeshError
    with pytest.raises(
        SQLMeshError, match="Doris only supports creating materialized views in the 'internal'"
    ):
        adapter.create_view(
            "iceberg_cat.silver.events_mv",
            parse_one("SELECT 1 AS id"),
            materialized=True,
        )


def test_duplicate_key_with_buckets_and_random_buckets_fallback(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "dup_bucketed",
        target_columns_to_types={"id": exp.DataType.build("INT"), "val": exp.DataType.build("INT")},
        table_properties={
            "duplicate_key": exp.Tuple(expressions=[exp.to_column("id")]),
            "buckets": exp.Literal.number(16),
        },
    )
    adapter.create_table(
        "random_bucketed",
        target_columns_to_types={"id": exp.DataType.build("INT"), "val": exp.DataType.build("INT")},
        table_properties={
            "buckets": exp.Literal.number(8),
        },
    )

    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `dup_bucketed` (`id` INT, `val` INT) DUPLICATE KEY (`id`) DISTRIBUTED BY HASH (`id`) BUCKETS 16",
        "CREATE TABLE IF NOT EXISTS `random_bucketed` (`id` INT, `val` INT) DISTRIBUTED BY RANDOM BUCKETS 8",
    ]


def test_truncate_table_fallback_on_unsupported_truncate(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    mocker.patch.object(
        adapter,
        "columns",
        return_value={"id": exp.DataType.build("BIGINT"), "ds": exp.DataType.build("DATE")},
    )

    call_count = 0

    def side_effect(sql, *args, **kwargs):
        nonlocal call_count
        call_count += 1
        if call_count == 1 and "TRUNCATE" in str(sql):
            raise Exception("errCode = 2, detailMessage = Not support truncate table on iceberg")

    adapter.cursor.execute.side_effect = side_effect
    adapter._truncate_table("silver.iceberg_two_part")

    assert to_sql_calls(adapter) == [
        "TRUNCATE TABLE `silver`.`iceberg_two_part`",
        "DELETE FROM `silver`.`iceberg_two_part` WHERE NOT `id` IS NULL",
        "DELETE FROM `silver`.`iceberg_two_part` WHERE `id` IS NULL",
    ]


def test_row_diff_mixin_concat_columns(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    concat_expr = adapter.concat_columns(
        {
            "id": exp.DataType.build("INT"),
            "flag": exp.DataType.build("BOOLEAN"),
            "ts": exp.DataType.build("DATETIME"),
        }
    )
    assert (
        concat_expr.sql("doris")
        == "CONCAT(COALESCE(CAST(CAST(id AS BIGINT) AS VARCHAR), ''), ',', COALESCE(CAST(CAST(flag AS INT) AS VARCHAR), ''), ',', COALESCE(CAST(DATE_FORMAT(ts, '%Y-%m-%d %T.%f') AS VARCHAR), ''))"
    )


def test_temp_table_length_and_catalog_switch_and_wrapped_concat_ws(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # 1. _get_temp_table never exceeds Doris's 64-character identifier limit
    long_table = "db." + ("a" * 60)
    temp_tbl = adapter._get_temp_table(long_table)
    assert len(temp_tbl.name) <= 64

    # 2. set_current_catalog preserves backtick quoting on hyphenated catalog names
    adapter.set_current_catalog("iceberg-cat")

    # 3. delete_from unwraps parenthesized CONCAT_WS(...)
    key_exp = exp.func("CONCAT_WS", "'__SQLMESH_DELIM__'", exp.column("id"), exp.column("ds"))
    wrapped_where = exp.Paren(this=key_exp).isin(query=exp.select(key_exp).from_("src_table"))
    adapter.delete_from("target_table", where=wrapped_where)

    assert to_sql_calls(adapter) == [
        "SWITCH `iceberg-cat`",
        "DELETE FROM `target_table` AS `_t1` USING (SELECT `id`, `ds` FROM `src_table`) AS `_t2` WHERE `_t1`.`id` = `_t2`.`id` AND `_t1`.`ds` = `_t2`.`ds`",
    ]


def test_insert_overwrite_by_partition_unwraps_wrappers_and_transforms(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    mocker.patch.object(
        adapter,
        "_get_temp_table",
        return_value=exp.table_("__temp_tbl", db="db"),
    )

    adapter.insert_overwrite_by_partition(
        "db.events",
        parse_one("SELECT id, ds FROM db.src", read="doris"),
        partitioned_by=[
            parse_one("RANGE(DATE_TRUNC(ds, 'day'))", read="doris"),
        ],
        target_columns_to_types={
            "id": exp.DataType.build("INT"),
            "ds": exp.DataType.build("DATETIME"),
        },
    )

    sql_calls = to_sql_calls(adapter)
    assert (
        "DELETE FROM `db`.`events` WHERE DATE_TRUNC(`ds`, 'day') IN (SELECT DISTINCT DATE_TRUNC(`ds`, 'day') FROM `db`.`__temp_tbl`)"
        not in sql_calls
    )
    assert any(
        "SELECT DISTINCT `ds` FROM `db`.`__temp_tbl`" in call
        or "USING (SELECT DISTINCT `ds` FROM `db`.`__temp_tbl`)" in call
        for call in sql_calls
    )


def test_sync_materialized_view_drop_and_replace(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.create_view(
        "db.sync_mv",
        parse_one(
            "SELECT region, SUM(amount) AS total FROM db.orders GROUP BY region", read="doris"
        ),
        replace=True,
        materialized=True,
        view_properties={
            "materialized_type": exp.Literal.string("sync"),
            "source_table": exp.Literal.string("db.orders"),
        },
    )

    assert to_sql_calls(adapter) == [
        "DROP MATERIALIZED VIEW IF EXISTS `db`.`sync_mv` ON `db`.`orders`",
        "CREATE MATERIALIZED VIEW `db`.`sync_mv` AS SELECT `region` AS `mv_region`, SUM(`amount`) AS `total` FROM `db`.`orders` GROUP BY `region`",
    ]

    adapter.cursor.execute.reset_mock()

    adapter.create_view(
        "sync_mv",
        parse_one("SELECT id FROM db.orders", read="doris"),
        replace=True,
        materialized=True,
        view_properties={
            "materialized_type": exp.Literal.string("SYNC"),
            "source_table": exp.to_table("db.orders"),
        },
    )

    assert to_sql_calls(adapter) == [
        "USE `db`",
        "DROP MATERIALIZED VIEW IF EXISTS `sync_mv` ON `db`.`orders`",
        "USE `db`",
        "CREATE MATERIALIZED VIEW `sync_mv` AS SELECT `id` AS `mv_id` FROM `db`.`orders`",
    ]


def test_delete_with_subquery_fallback_and_cardinality_aware_composite_batching(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter], mocker: MockerFixture
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # When DELETE ... USING fails on DUPLICATE/AGGREGATE KEY OLAP tables, fallback to literal batching
    # and pick the highest-cardinality column (id: 3 values) to batch in IN (...) while grouping by low-cardinality ds (1 value).
    mocker.patch.object(
        adapter,
        "fetchall",
        return_value=[(1, "2024-01-01"), (2, "2024-01-01"), (3, "2024-01-01")],
    )

    first_delete = True

    def execute_side_effect(sql, *args, **kwargs):
        nonlocal first_delete
        if first_delete and "USING" in str(sql):
            first_delete = False
            raise Exception("errCode = 2, detailMessage = delete using only support unique key")

    adapter.cursor.execute.side_effect = execute_side_effect

    key_exp = exp.func("CONCAT_WS", "'__SQLMESH_DELIM__'", exp.column("id"), exp.column("ds"))
    where = exp.Paren(this=key_exp).isin(query=exp.select(key_exp).from_("src_table"))
    adapter.delete_from("target_table", where=where)

    assert to_sql_calls(adapter) == [
        "DELETE FROM `target_table` AS `_t1` USING (SELECT `id`, `ds` FROM `src_table`) AS `_t2` WHERE `_t1`.`id` = `_t2`.`id` AND `_t1`.`ds` = `_t2`.`ds`",
        "DELETE FROM `target_table` WHERE `ds` = '2024-01-01' AND `id` IN (1, 2, 3)",
    ]


def test_rewrite_between_in_where_preserves_not_between_and_wraps_or(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    adapter.delete_from(
        "target_table",
        where=parse_one(
            "(ds BETWEEN '2024-01-01' AND '2024-01-07' OR status = 'expired') AND id NOT BETWEEN 10 AND 20",
            read="doris",
        ),
    )

    assert to_sql_calls(adapter) == [
        "DELETE FROM `target_table` WHERE ((`ds` >= '2024-01-01' AND `ds` <= '2024-01-07') OR `status` = 'expired') AND NOT `id` BETWEEN 10 AND 20",
    ]


@pytest.mark.parametrize(
    "key_cols,rows",
    [
        (["id"], [(1,), (None,), (2,), (3,)]),
        (["id", "ds"], [(1, "2024-01-01"), (2, "2024-01-01"), (None, "2024-01-01"), (1, None)]),
        (
            ["tenant", "region", "id"],
            [
                ("t1", "us", 10),
                ("t1", "us", 20),
                ("t1", "us", None),
                ("t1", None, 30),
                (None, None, None),
            ],
        ),
    ],
)
def test_delete_by_literal_keys_doris_delete_handler_invariants(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
    mocker: MockerFixture,
    key_cols: t.List[str],
    rows: t.List[t.Tuple[t.Any, ...]],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    mocker.patch.object(adapter, "fetchall", return_value=rows)

    adapter._delete_by_literal_keys(
        exp.to_table("db.target_tbl"),
        [exp.column(c) for c in key_cols],
        parse_one(f"SELECT {', '.join(key_cols)} FROM db.src_tbl", read="doris", into=exp.Select),
    )

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) >= 1
    for sql in sql_calls:
        parsed = parse_one(sql, read="doris")
        assert isinstance(parsed, exp.Delete)
        # Doris DeleteHandler forbids OR and BETWEEN in DELETE WHERE predicates
        assert parsed.find(exp.Or) is None
        assert parsed.find(exp.Between) is None


@pytest.mark.parametrize(
    "non_key_type",
    ["FLOAT", "DOUBLE", "ARRAY<INT>", "MAP<STRING, INT>", "STRUCT<a: INT>", "JSON", "VARIANT"],
)
def test_olap_key_type_and_prefix_invariants(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
    non_key_type: str,
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)
    adapter.create_table(
        "db.complex_tbl",
        target_columns_to_types={
            "bad_first_col": exp.DataType.build(non_key_type, dialect="doris"),
            "valid_key": exp.DataType.build("BIGINT", dialect="doris"),
        },
    )

    sql_calls = to_sql_calls(adapter)
    assert len(sql_calls) == 1
    # valid_key must be moved to column index 0 ahead of non-keyable bad_first_col
    assert sql_calls[0].startswith(
        "CREATE TABLE IF NOT EXISTS `db`.`complex_tbl` (`valid_key` BIGINT, `bad_first_col`"
    )


def test_adjust_physical_properties_for_incremental(
    make_mocked_engine_adapter: t.Callable[..., DorisEngineAdapter],
):
    adapter = make_mocked_engine_adapter(DorisEngineAdapter)

    # Single unique_key promoted when no explicit key is present
    props = adapter.adjust_physical_properties_for_incremental(
        {},
        requires_delete_capable_table=True,
        unique_key=[exp.to_column("id")],
        model_name="db.incremental_model",
    )
    assert props == {"unique_key": exp.to_column("id")}

    # Composite unique_key promoted as exp.Tuple
    props_composite = adapter.adjust_physical_properties_for_incremental(
        {},
        requires_delete_capable_table=True,
        unique_key=[exp.to_column("id"), exp.to_column("ds")],
        model_name="db.incremental_model",
    )
    assert props_composite == {
        "unique_key": exp.Tuple(expressions=[exp.to_column("id"), exp.to_column("ds")])
    }

    # Explicit duplicate_key in physical_properties is preserved without overwriting
    props_explicit = adapter.adjust_physical_properties_for_incremental(
        {"duplicate_key": exp.to_column("id")},
        requires_delete_capable_table=True,
        unique_key=[exp.to_column("id")],
        model_name="db.incremental_model",
    )
    assert props_explicit == {"duplicate_key": exp.to_column("id")}

    # Verify end-to-end CREATE TABLE with promoted unique_key reorders key columns first and emits UNIQUE KEY
    adapter.create_table(
        "db.incremental_model",
        target_columns_to_types={
            "name": exp.DataType.build("VARCHAR(50)", dialect="doris"),
            "id": exp.DataType.build("INT", dialect="doris"),
        },
        table_properties=props,
    )
    assert to_sql_calls(adapter) == [
        "CREATE TABLE IF NOT EXISTS `db`.`incremental_model` (`id` INT, `name` VARCHAR(50)) UNIQUE KEY (`id`) DISTRIBUTED BY HASH (`id`) BUCKETS 10"
    ]
