from __future__ import annotations

from collections import defaultdict
import itertools
import logging
import re
import typing as t

import pandas as pd  # noqa: TID253
from sqlglot import exp, parse, parse_one

from sqlmesh.core.dialect import to_schema
from sqlmesh.core.engine_adapter.base import (
    EngineAdapterWithIndexSupport,
    InsertOverwriteStrategy,
    _decoded_str,
)
from sqlmesh.core.engine_adapter.mixins import (
    GetCurrentCatalogFromFunctionMixin,
    LogicalMergeMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
)
from sqlmesh.core.engine_adapter.shared import (
    CatalogSupport,
    CommentCreationTable,
    CommentCreationView,
    DataObject,
    DataObjectType,
    SourceQuery,
    set_catalog,
)
from sqlmesh.utils import get_source_columns_to_types, random_id
from sqlmesh.utils.errors import SQLMeshError

if t.TYPE_CHECKING:
    from sqlmesh.core._typing import SchemaName, TableName
    from sqlmesh.core.engine_adapter._typing import DF, QueryOrDF
    from sqlmesh.core.node import IntervalUnit

logger = logging.getLogger(__name__)


@set_catalog()
class DorisEngineAdapter(
    EngineAdapterWithIndexSupport,
    LogicalMergeMixin,
    GetCurrentCatalogFromFunctionMixin,
    PandasNativeFetchDFSupportMixin,
    RowDiffMixin,
):
    DIALECT = "doris"
    DEFAULT_BATCH_SIZE = 500
    SUPPORTS_TRANSACTIONS = False
    COMMENT_CREATION_TABLE = CommentCreationTable.IN_SCHEMA_DEF_NO_CTAS
    COMMENT_CREATION_VIEW = CommentCreationView.IN_SCHEMA_DEF_NO_COMMANDS
    MAX_TABLE_COMMENT_LENGTH = 2048
    MAX_COLUMN_COMMENT_LENGTH = 255
    SUPPORTS_INDEXES = True
    SUPPORTS_REPLACE_TABLE = False
    MAX_IDENTIFIER_LENGTH = 64
    SUPPORTS_MATERIALIZED_VIEWS = True
    SUPPORTS_MATERIALIZED_VIEW_SCHEMA = True
    RECREATE_MATERIALIZED_VIEW_ON_EVALUATION = False
    SUPPORTS_CREATE_DROP_CATALOG = False
    INSERT_OVERWRITE_STRATEGY = InsertOverwriteStrategy.DELETE_INSERT
    CURRENT_CATALOG_EXPRESSION = exp.func("current_catalog")
    RESOLVE_TABLE_REFS_IN_PHYSICAL_PROPERTIES: t.FrozenSet[str] = frozenset(
        {"excluded_trigger_tables", "source_table"}
    )
    SCHEMA_DIFFER_KWARGS = {
        "parameterized_type_defaults": {
            exp.DataType.build("DECIMAL", dialect=DIALECT).this: [(9, 0), (0,)],
            exp.DataType.build("CHAR", dialect=DIALECT).this: [(1,), (255,)],
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: [(65533,)],
            exp.DataType.build("DATETIME", dialect=DIALECT).this: [(0,)],
            exp.DataType.build("INT", dialect=DIALECT).this: [(11,)],
            exp.DataType.build("BIGINT", dialect=DIALECT).this: [(20,)],
            exp.DataType.build("SMALLINT", dialect=DIALECT).this: [(6,)],
            exp.DataType.build("TINYINT", dialect=DIALECT).this: [(4,), (1,)],
        },
        "types_with_unlimited_length": {
            exp.DataType.build("TEXT", dialect=DIALECT).this: {
                exp.DataType.build("VARCHAR", dialect=DIALECT).this,
                exp.DataType.build("CHAR", dialect=DIALECT).this,
            },
            exp.DataType.build("VARCHAR", dialect=DIALECT).this: {
                exp.DataType.build("VARCHAR", dialect=DIALECT).this,
                exp.DataType.build("CHAR", dialect=DIALECT).this,
                exp.DataType.build("TEXT", dialect=DIALECT).this,
            },
        },
        "compatible_types": {
            exp.DataType.build("BOOLEAN", dialect=DIALECT): {
                exp.DataType.build("TINYINT", dialect=DIALECT),
                exp.DataType.build("SMALLINT", dialect=DIALECT),
                exp.DataType.build("INT", dialect=DIALECT),
                exp.DataType.build("BIGINT", dialect=DIALECT),
                exp.DataType.build("INT128", dialect=DIALECT),
            },
            exp.DataType.build("TINYINT", dialect=DIALECT): {
                exp.DataType.build("SMALLINT", dialect=DIALECT),
                exp.DataType.build("INT", dialect=DIALECT),
                exp.DataType.build("BIGINT", dialect=DIALECT),
                exp.DataType.build("INT128", dialect=DIALECT),
                exp.DataType.build("DOUBLE", dialect=DIALECT),
            },
            exp.DataType.build("SMALLINT", dialect=DIALECT): {
                exp.DataType.build("INT", dialect=DIALECT),
                exp.DataType.build("BIGINT", dialect=DIALECT),
                exp.DataType.build("INT128", dialect=DIALECT),
                exp.DataType.build("DOUBLE", dialect=DIALECT),
            },
            exp.DataType.build("INT", dialect=DIALECT): {
                exp.DataType.build("BIGINT", dialect=DIALECT),
                exp.DataType.build("INT128", dialect=DIALECT),
                exp.DataType.build("DOUBLE", dialect=DIALECT),
            },
            exp.DataType.build("BIGINT", dialect=DIALECT): {
                exp.DataType.build("INT128", dialect=DIALECT),
            },
            exp.DataType.build("FLOAT", dialect=DIALECT): {
                exp.DataType.build("DOUBLE", dialect=DIALECT),
            },
            exp.DataType.build("DATE", dialect=DIALECT): {
                exp.DataType.build("DATETIME", dialect=DIALECT),
            },
        },
    }

    INVALID_KEY_TYPES = {
        "FLOAT",
        "DOUBLE",
        "STRING",
        "TEXT",
        "TINYTEXT",
        "MEDIUMTEXT",
        "LONGTEXT",
        "BLOB",
        "TINYBLOB",
        "MEDIUMBLOB",
        "LONGBLOB",
        "BINARY",
        "VARBINARY",
        "JSON",
        "JSONB",
        "VARIANT",
        "OBJECT",
        "ARRAY",
        "MAP",
        "STRUCT",
        "BITMAP",
        "HLL",
        "QUANTILE_STATE",
        "AGG_STATE",
    }

    @property
    def catalog_support(self) -> CatalogSupport:
        return CatalogSupport.FULL_SUPPORT

    @property
    def default_catalog(self) -> t.Optional[str]:
        return self._default_catalog or self._extra_config.get("catalog")

    def get_current_catalog(self) -> t.Optional[str]:
        try:
            catalog = super().get_current_catalog()
            if catalog:
                return _decoded_str(catalog)
        except Exception:
            logger.debug("current_catalog() failed on Doris, falling back to default catalog")
        return self.default_catalog or "internal"

    def set_current_catalog(self, catalog: str) -> None:
        catalog_sql = self._to_sql(exp.to_identifier(catalog, quoted=True))
        self.execute(exp.Command(this="SWITCH", expression=catalog_sql))

    def _is_iceberg_table(
        self,
        table_format: t.Optional[str] = None,
        catalog_name: t.Optional[str] = None,
        table_properties: t.Optional[t.Dict[str, t.Any]] = None,
    ) -> bool:
        if table_format and str(table_format).strip().lower() == "iceberg":
            return True
        if table_properties:
            engine_val = self._extract_str_value(table_properties.get("engine"))
            if engine_val and engine_val.strip().lower() == "iceberg":
                return True
        effective_catalog = catalog_name or self.default_catalog
        if effective_catalog and str(effective_catalog).strip().lower() != "internal":
            return True
        return False

    @staticmethod
    def _extract_str_value(val: t.Any) -> t.Optional[str]:
        if val is None:
            return None
        if isinstance(val, exp.Boolean):
            return "true" if val.this else "false"
        if isinstance(val, bool):
            return "true" if val else "false"
        if isinstance(val, exp.Literal):
            return str(val.this)
        if isinstance(val, exp.Identifier):
            return str(val.this)
        if isinstance(val, (exp.Column, exp.Table)):
            return ".".join(part.name for part in val.parts)
        if isinstance(val, exp.Var):
            return str(val.this)
        if isinstance(val, str):
            return val
        if isinstance(val, exp.Expr):
            return val.name or str(val)
        return str(val)

    @classmethod
    def _extract_column_names(cls, key_expr: t.Any) -> t.List[str]:
        if key_expr is None:
            return []
        if isinstance(key_expr, (exp.Tuple, exp.Array)):
            names: t.List[str] = []
            for e in key_expr.expressions:
                names.extend(cls._extract_column_names(e))
            return names
        if isinstance(key_expr, exp.Paren):
            return cls._extract_column_names(key_expr.this)
        if isinstance(key_expr, (list, tuple)):
            names = []
            for e in key_expr:
                names.extend(cls._extract_column_names(e))
            return names
        if isinstance(key_expr, exp.Column):
            return [key_expr.name]
        if isinstance(key_expr, (exp.Identifier, exp.Literal, exp.Var)):
            raw = str(key_expr.this)
            return [c.strip().strip("`") for c in raw.split(",") if c.strip()]
        if isinstance(key_expr, str):
            return [c.strip().strip("`") for c in key_expr.split(",") if c.strip()]
        return [str(key_expr).strip().strip("`")]

    @classmethod
    def _is_valid_olap_key_type(cls, dtype: exp.DataType) -> bool:
        type_name = (
            dtype.this.value.upper()
            if isinstance(dtype.this, exp.DataType.Type)
            else str(dtype.this).upper()
        )
        if type_name in cls.INVALID_KEY_TYPES:
            return False
        if dtype.this == exp.DataType.Type.USERDEFINED:
            kind = (
                str(dtype.args.get("kind") or dtype.sql(dialect=cls.DIALECT))
                .upper()
                .split("(", 1)[0]
                .strip()
                .strip("`")
            )
            if kind in cls.INVALID_KEY_TYPES:
                return False
        return True

    def _fetch_native_df(
        self, query: t.Union[exp.Expr, str], quote_identifiers: bool = False
    ) -> DF:
        if self._pre_ping:
            try:
                self.ping()
            except Exception:
                logger.info("Connection to the database was lost. Reconnecting...")
                self._connection_pool.close()
        self._connection_pool.get_cursor()
        return super()._fetch_native_df(query, quote_identifiers=quote_identifiers)

    _SAFE_RETRY_SQL_PREFIX_RE = re.compile(
        r"\A(?:\s|/\*.*?\*/|--[^\r\n]*(?:[\r\n]|\Z))*(?:SELECT|WITH|SHOW|DESCRIBE|DESC|EXPLAIN|USE|SWITCH)\b",
        re.IGNORECASE | re.DOTALL,
    )

    @classmethod
    def _is_safe_to_retry_sql(cls, sql: str) -> bool:
        """Returns True only for read-only or idempotent catalog/database switch statements that cannot duplicate writes."""
        return bool(cls._SAFE_RETRY_SQL_PREFIX_RE.match(sql))

    def ping(self) -> None:
        self._connection_pool.get().ping(reconnect=False)

    def _execute(self, sql: str, track_rows_processed: bool = False, **kwargs: t.Any) -> None:
        if self._pre_ping:
            try:
                self.ping()
            except Exception:
                logger.info("Connection to the database was lost. Reconnecting...")
                self._connection_pool.close()
        try:
            super()._execute(sql, track_rows_processed=track_rows_processed, **kwargs)
        except Exception as e:
            err_code = e.args[0] if getattr(e, "args", None) else None
            err_name = e.__class__.__name__
            is_disconnect = (err_code in (2006, 2013, 2055) and err_name == "OperationalError") or (
                err_code == 0 and err_name == "InterfaceError"
            )
            is_timeout = (
                isinstance(getattr(e, "__context__", None), TimeoutError)
                or "timed out" in str(e).lower()
            )
            if (
                is_disconnect
                and not is_timeout
                and not self._connection_pool.is_transaction_active
                and self._is_safe_to_retry_sql(sql)
            ):
                logger.warning(
                    "Doris connection lost (%s: %s) during read/session statement, reconnecting and retrying once...",
                    err_name,
                    err_code,
                )
                self._connection_pool.close_cursor()
                try:
                    conn = self._connection_pool.get()
                    conn.ping(reconnect=True)
                    super()._execute(sql, track_rows_processed=track_rows_processed, **kwargs)
                except Exception:
                    self._connection_pool.close()
                    raise
            else:
                if is_disconnect and not self._connection_pool.is_transaction_active:
                    self._connection_pool.close()
                raise

    def columns(
        self, table_name: TableName, include_pseudo_columns: bool = False
    ) -> t.Dict[str, exp.DataType]:
        """Fetches column names and types for the target table, normalizing Doris type aliases."""
        self.execute(exp.Describe(this=exp.to_table(table_name), kind="TABLE"))
        describe_output = self.cursor.fetchall()
        cols: t.Dict[str, exp.DataType] = {}
        for column_name, column_type, *_ in itertools.takewhile(
            lambda row: not str(row[0]).startswith("#"),
            describe_output,
        ):
            col_name_str = _decoded_str(column_name) if column_name else ""
            raw_type = _decoded_str(column_type) if column_type else ""
            if not col_name_str.strip() or not raw_type.strip():
                continue
            normalized_type = re.sub(
                r"\bDECIMALV[23]\b(?!\s*`?\s*:)", "DECIMAL", raw_type, flags=re.IGNORECASE
            )
            normalized_type = re.sub(
                r"\bDATETIMEV2\b(?!\s*`?\s*:)", "DATETIME", normalized_type, flags=re.IGNORECASE
            )
            normalized_type = re.sub(
                r"\bDATEV2\b(?!\s*`?\s*:)", "DATE", normalized_type, flags=re.IGNORECASE
            )
            normalized_type = re.sub(
                r"\bLARGEINT(?:\(\d+\)|\b)(?!\s*`?\s*:)",
                "INT128",
                normalized_type,
                flags=re.IGNORECASE,
            )
            normalized_type = re.sub(
                r"\bTINYINT\(1\)", "BOOLEAN", normalized_type, flags=re.IGNORECASE
            )
            normalized_type = re.sub(
                r"\b(TINYINT|SMALLINT|INT|BIGINT)\(\d+\)",
                r"\1",
                normalized_type,
                flags=re.IGNORECASE,
            )
            cols[col_name_str] = exp.DataType.build(normalized_type, dialect=self.dialect, udt=True)
        return cols

    def create_schema(
        self,
        schema_name: SchemaName,
        ignore_if_exists: bool = True,
        warn_on_error: bool = True,
        properties: t.Optional[t.List[exp.Expr]] = None,
    ) -> None:
        """Create a schema (database) in Doris."""
        properties = properties or []
        return super()._create_schema(
            schema_name=schema_name,
            ignore_if_exists=ignore_if_exists,
            warn_on_error=warn_on_error,
            properties=properties,
            kind="DATABASE",
        )

    def drop_schema(
        self,
        schema_name: SchemaName,
        ignore_if_not_exists: bool = True,
        cascade: bool = False,
        **drop_args: t.Dict[str, exp.Expr],
    ) -> None:
        """Drop schema in Doris. Note: Doris doesn't support CASCADE clause."""
        return self._drop_object(
            name=schema_name,
            exists=ignore_if_not_exists,
            kind="DATABASE",
            cascade=False,
            **drop_args,
        )

    def _get_data_objects(
        self, schema_name: SchemaName, object_names: t.Optional[t.Set[str]] = None
    ) -> t.List[DataObject]:
        """Returns all the data objects that exist in the given schema and optional catalog."""
        schema_exp = to_schema(schema_name) if schema_name else exp.Table()
        catalog = schema_exp.catalog or None
        query_catalog = catalog or self.default_catalog

        query = (
            exp.select(
                exp.column("table_schema").as_("schema_name"),
                exp.column("table_name").as_("name"),
                exp.case()
                .when(
                    exp.column("table_type").eq("BASE TABLE"),
                    exp.Literal.string("table"),
                )
                .when(
                    exp.column("table_type").eq("SYSTEM VIEW"),
                    exp.Literal.string("view"),
                )
                .when(
                    exp.column("table_type").eq("VIEW"),
                    exp.Literal.string("view"),
                )
                .when(
                    exp.column("table_type").eq("MATERIALIZED VIEW"),
                    exp.Literal.string("materialized_view"),
                )
                .else_(exp.column("table_type"))
                .as_("type"),
            )
            .from_(exp.table_("tables", db="information_schema", catalog=query_catalog))
            .where(
                exp.column("table_schema").eq(
                    schema_exp.db if schema_exp.db else exp.func("DATABASE")
                )
            )
        )
        if object_names:
            lowered_names = [name.lower() for name in object_names]
            query = query.where(exp.func("LOWER", exp.column("table_name")).isin(*lowered_names))

        result: t.List[DataObject] = []
        for row_schema, table_name, table_type in self.fetchall(query):
            try:
                schema_str = _decoded_str(row_schema) if row_schema is not None else schema_exp.db
                name_str = _decoded_str(table_name) if table_name is not None else "unknown"
                obj_type = _decoded_str(table_type) if table_type is not None else "table"

                upper_type = obj_type.upper()
                if upper_type in ("BASE TABLE", "EXTERNAL TABLE"):
                    obj_type = "table"
                elif upper_type in ("VIEW", "SYSTEM VIEW"):
                    obj_type = "view"
                elif upper_type in ("MATERIALIZED VIEW", "MATERIALIZED_VIEW"):
                    obj_type = "materialized_view"

                result.append(
                    DataObject(
                        catalog=catalog,
                        schema=schema_str,
                        name=name_str,
                        type=DataObjectType.from_str(obj_type),
                    )
                )
            except (ValueError, AttributeError) as e:
                logger.error(
                    f"Error processing row: {e}, row: {(row_schema, table_name, table_type)}"
                )
                continue

        # In Doris 2.1+, asynchronous materialized views (MTMVs) are stored as OLAP base tables
        # and report table_type = 'BASE TABLE' in information_schema.tables. Reclassify them via
        # the mv_infos() table-valued function so drop_data_object_on_type_mismatch and metadata
        # inspection recognize them as MATERIALIZED_VIEW instead of issuing DROP TABLE.
        if (not query_catalog or query_catalog.lower() == "internal") and any(
            obj.type.is_table for obj in result
        ):
            try:
                db_arg = (
                    self._to_sql(exp.Literal.string(schema_exp.db), quote=False)
                    if schema_exp.db
                    else "DATABASE()"
                )
                mv_rows = self.fetchall(
                    f"SELECT Name FROM mv_infos('database' = {db_arg})",
                    quote_identifiers=False,
                )
                mv_names = {
                    _decoded_str(row[0]).lower() for row in mv_rows if len(row) == 1 and row[0]
                }
                if mv_names:
                    for obj in result:
                        if obj.name.lower() in mv_names:
                            obj.type = DataObjectType.MATERIALIZED_VIEW
            except Exception:
                logger.debug("mv_infos lookup failed on Doris, skipping MTMV reclassification")

        return result

    def create_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized: bool = False,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expr]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        """
        Doris view creation behavior:
        - Regular VIEW: supports atomic `CREATE OR REPLACE VIEW` via `super().create_view`.
        - MATERIALIZED VIEW: does NOT support `CREATE OR REPLACE MATERIALIZED VIEW`, so `replace=True`
          drops any existing object/materialized view first (after validating the new query and properties).
        """
        target_table = exp.to_table(view_name)
        effective_catalog = target_table.catalog or self.default_catalog
        if effective_catalog and effective_catalog.lower() != "internal":
            view_kind = "materialized views" if materialized else "views"
            raise SQLMeshError(
                f"Doris only supports creating {view_kind} in the 'internal' catalog, got '{effective_catalog}' "
                f"for view '{self._to_sql(target_table)}'. Configure `virtual_environment_mode: dev_only` and "
                f"`environment_catalog_mapping` in your SQLMesh configuration to route virtual environment views "
                f"to the 'internal' catalog."
            )

        if not materialized:
            return super().create_view(
                view_name,
                query_or_df,
                target_columns_to_types,
                replace=replace,
                materialized=False,
                materialized_properties=materialized_properties,
                table_description=table_description,
                column_descriptions=column_descriptions,
                view_properties=view_properties,
                source_columns=source_columns,
                **create_kwargs,
            )

        self._create_materialized_view(
            view_name,
            query_or_df,
            target_columns_to_types=target_columns_to_types,
            replace=replace,
            materialized_properties=materialized_properties,
            table_description=table_description,
            column_descriptions=column_descriptions,
            view_properties=view_properties,
            source_columns=source_columns,
            **create_kwargs,
        )

    def _create_materialized_view(
        self,
        view_name: TableName,
        query_or_df: QueryOrDF,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        replace: bool = True,
        materialized_properties: t.Optional[t.Dict[str, t.Any]] = None,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        view_properties: t.Optional[t.Dict[str, exp.Expr]] = None,
        source_columns: t.Optional[t.List[str]] = None,
        **create_kwargs: t.Any,
    ) -> None:
        query_or_df = self._native_df_to_pandas_df(query_or_df)

        # Inline DataFrames as a SELECT ... FROM VALUES query rather than delegating DataFrame
        # handling to _get_source_queries_and_columns_to_types, which would otherwise materialize
        # a temporary table that gets dropped as soon as the SourceQuery context manager exits.
        if isinstance(query_or_df, pd.DataFrame):
            values: t.List[t.Tuple[t.Any, ...]] = list(
                query_or_df.itertuples(index=False, name=None)
            )
            target_columns_to_types, source_columns = self._columns_to_types(
                query_or_df, target_columns_to_types, source_columns
            )
            if not target_columns_to_types:
                raise SQLMeshError("columns_to_types must be provided for dataframes")
            source_columns_to_types = get_source_columns_to_types(
                target_columns_to_types, source_columns
            )
            query_or_df = self._values_to_sql(
                values,
                source_columns_to_types,
                batch_start=0,
                batch_end=len(values),
            )

        source_queries, target_columns_to_types = self._get_source_queries_and_columns_to_types(
            query_or_df,
            target_columns_to_types,
            batch_size=0,
            target_table=view_name,
            source_columns=source_columns,
        )
        if len(source_queries) != 1:
            raise SQLMeshError("Only one source query is supported for creating materialized views")

        with source_queries[0] as query:
            target_table = exp.to_table(view_name)

            is_sync_mv = bool(
                view_properties
                and (
                    (
                        self._extract_str_value(view_properties.get("materialized_type")) or ""
                    ).upper()
                    == "SYNC"
                )
            )

            schema: t.Union[exp.Table, exp.Schema] = target_table
            if target_columns_to_types and not is_sync_mv:
                schema = self._build_schema_exp(
                    target_table,
                    target_columns_to_types,
                    column_descriptions,
                    is_view=True,
                )

            partitioned_by = None
            if materialized_properties and materialized_properties.get("partitioned_by"):
                partitioned_by = materialized_properties.get("partitioned_by")
            elif create_kwargs.get("partitioned_by"):
                partitioned_by = create_kwargs.get("partitioned_by")
            elif view_properties and view_properties.get("partitioned_by"):
                raw_vp = view_properties.get("partitioned_by")
                if raw_vp is not None:
                    partitioned_by = raw_vp if isinstance(raw_vp, list) else [raw_vp]

            clustered_by = None
            if materialized_properties and materialized_properties.get("clustered_by"):
                clustered_by = materialized_properties.get("clustered_by")
            elif create_kwargs.get("clustered_by"):
                clustered_by = create_kwargs.get("clustered_by")

            properties_exp = self._build_table_properties_exp(
                catalog_name=target_table.catalog,
                table_properties=view_properties,
                target_columns_to_types=target_columns_to_types,
                table_description=(
                    table_description
                    if self.COMMENT_CREATION_VIEW.supports_schema_def and self.comments_enabled
                    else None
                ),
                partitioned_by=partitioned_by,
                clustered_by=clustered_by,
                partition_interval_unit=(
                    materialized_properties.get("partition_interval_unit")
                    if materialized_properties
                    else None
                ),
                table_kind="MATERIALIZED_VIEW",
            )

            if is_sync_mv and isinstance(query, exp.Select):
                query = query.copy()
                aliased_selects: t.List[exp.Expr] = []
                for sel_expr in query.expressions:
                    if isinstance(sel_expr, exp.Column):
                        aliased_selects.append(
                            exp.alias_(sel_expr.copy(), f"mv_{sel_expr.name}", quoted=True)
                        )
                    elif (
                        isinstance(sel_expr, exp.Alias)
                        and isinstance(sel_expr.this, exp.Column)
                        and sel_expr.alias == sel_expr.this.name
                    ):
                        aliased_selects.append(
                            exp.alias_(sel_expr.this.copy(), f"mv_{sel_expr.alias}", quoted=True)
                        )
                    else:
                        aliased_selects.append(sel_expr)
                query.set("expressions", aliased_selects)

            # Doris does not support `CREATE OR REPLACE MATERIALIZED VIEW`, so drop first when `replace=True`
            # after query and table property validation has succeeded.
            if replace:
                self.drop_data_object_on_type_mismatch(
                    self.get_data_object(view_name), DataObjectType.MATERIALIZED_VIEW
                )
                drop_view_props = dict(view_properties) if view_properties else None
                if (
                    is_sync_mv
                    and drop_view_props is not None
                    and not drop_view_props.get("source_table")
                ):
                    first_tbl = query.find(exp.Table)
                    if first_tbl:
                        drop_view_props["source_table"] = first_tbl
                self.drop_view(
                    view_name,
                    ignore_if_not_exists=True,
                    materialized=True,
                    view_properties=drop_view_props,
                )

            # Doris synchronous materialized views are single-table rollup indexes whose DDL syntax
            # (`CREATE MATERIALIZED VIEW <mv_name> AS SELECT ... FROM <db>.<table>`) forbids a
            # `<db>.` qualifier on `<mv_name>` and requires an active database on the session.
            if is_sync_mv and not target_table.db:
                source_db: t.Optional[str] = None
                if view_properties and view_properties.get("source_table"):
                    source_db = self._to_source_table(view_properties["source_table"]).db or None
                if not source_db:
                    first_tbl = query.find(exp.Table)
                    if first_tbl and first_tbl.db:
                        source_db = first_tbl.db
                if source_db:
                    db_sql = self._to_sql(exp.to_identifier(source_db, quoted=True))
                    self.execute(f"USE {db_sql}")

            self.execute(
                exp.Create(
                    this=schema,
                    kind="VIEW",
                    replace=False,
                    expression=query,
                    properties=properties_exp,
                ),
                quote_identifiers=self.QUOTE_IDENTIFIERS_IN_VIEWS,
            )
            self._clear_data_object_cache(view_name)

    def _to_source_table(self, raw_src: t.Any) -> exp.Table:
        if isinstance(raw_src, exp.Table):
            tbl = raw_src.copy()
        elif isinstance(raw_src, exp.Column):
            tbl = exp.table_(
                raw_src.name,
                db=raw_src.table or None,
                catalog=raw_src.db or None,
            )
        else:
            src_str = self._extract_str_value(raw_src) or str(raw_src)
            tbl = exp.to_table(src_str, dialect=self.dialect)
        if tbl.catalog and tbl.catalog.lower() == "internal":
            tbl.set("catalog", None)
        return tbl

    def drop_view(
        self,
        view_name: TableName,
        ignore_if_not_exists: bool = True,
        materialized: bool = False,
        **kwargs: t.Any,
    ) -> None:
        """Drop view in Doris."""
        target_table = exp.to_table(view_name)
        effective_catalog = target_table.catalog or self.default_catalog
        if effective_catalog and effective_catalog.lower() != "internal":
            return

        view_properties = kwargs.pop("view_properties", None)
        if materialized and view_properties:
            mat_type = self._extract_str_value(view_properties.get("materialized_type"))
            source_table = view_properties.get("source_table")
            if mat_type and mat_type.upper() == "SYNC" and source_table:
                view_table_sql = self._to_sql(target_table)
                source_tbl_exp = self._to_source_table(source_table)
                # Unqualified synchronous materialized views require the source table's database
                # to be active on the session before executing `DROP MATERIALIZED VIEW <mv> ON <tbl>`.
                if not target_table.db and source_tbl_exp.db:
                    db_sql = self._to_sql(exp.to_identifier(source_tbl_exp.db, quoted=True))
                    self.execute(f"USE {db_sql}")
                source_table_sql = self._to_sql(source_tbl_exp)
                exists_clause = "IF EXISTS " if ignore_if_not_exists else ""
                self.execute(
                    f"DROP MATERIALIZED VIEW {exists_clause}{view_table_sql} ON {source_table_sql}"
                )
                self._clear_data_object_cache(view_name)
                return
        super().drop_view(view_name, ignore_if_not_exists, materialized, **kwargs)

    def _create_table_like(
        self,
        target_table_name: TableName,
        source_table_name: TableName,
        exists: bool = True,
        **kwargs: t.Any,
    ) -> None:
        self.execute(
            exp.Create(
                this=exp.to_table(target_table_name),
                kind="TABLE",
                exists=exists,
                properties=exp.Properties(
                    expressions=[
                        exp.LikeProperty(
                            this=exp.to_table(source_table_name),
                        ),
                    ],
                ),
            )
        )

    def _build_create_comment_table_exp(
        self, table: exp.Table, table_comment: str, table_kind: str = "TABLE"
    ) -> exp.Comment | str:
        table_sql = self._to_sql(table)
        comment_sql = self._to_sql(
            exp.Literal.string(self._truncate_table_comment(table_comment.strip()))
        )
        return f"ALTER TABLE {table_sql} MODIFY COMMENT {comment_sql}"

    def _build_create_comment_column_exp(
        self, table: exp.Table, column_name: str, column_comment: str, table_kind: str = "TABLE"
    ) -> exp.Comment | str:
        table_sql = self._to_sql(table)
        col_sql = self._to_sql(exp.to_identifier(column_name, quoted=True))
        comment_sql = self._to_sql(
            exp.Literal.string(self._truncate_column_comment(column_comment.strip()))
        )
        return f"ALTER TABLE {table_sql} MODIFY COLUMN {col_sql} COMMENT {comment_sql}"

    def _truncate_table(self, table_name: TableName) -> None:
        table = exp.to_table(table_name)
        caught_err: t.Optional[Exception] = None
        if not self._is_iceberg_table(catalog_name=table.catalog):
            try:
                super()._truncate_table(table_name)
                return
            except Exception as e:
                err_msg = str(e).lower()
                if "truncate" not in err_msg and "not support" not in err_msg:
                    raise e
                caught_err = e

        # When TRUNCATE TABLE is unsupported (e.g., on Iceberg tables), clear all rows via DELETE.
        # Doris's DeleteHandler rejects DELETE without WHERE, constant WHERE TRUE, OR predicates,
        # and floating-point/complex column predicates, so we pick a key-compatible column and
        # execute two complementary `IS NOT NULL` and `IS NULL` deletes.
        cols_to_types = self.columns(table_name)
        if cols_to_types:
            valid_col_name = next(
                (
                    col
                    for col, dtype in cols_to_types.items()
                    if self._is_valid_olap_key_type(dtype)
                ),
                next(iter(cols_to_types)),
            )
            target_col = exp.column(valid_col_name, quoted=True)
            self.execute(exp.delete(table, where=target_col.is_(exp.null()).not_()))
            self.execute(exp.delete(table, where=target_col.is_(exp.null())))
            return
        if caught_err is not None:
            raise caught_err

    def _insert_overwrite_by_condition(
        self,
        table_name: TableName,
        source_queries: t.List[SourceQuery],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        where: t.Optional[exp.Condition] = None,
        insert_overwrite_strategy_override: t.Optional[InsertOverwriteStrategy] = None,
        **kwargs: t.Any,
    ) -> None:
        table = exp.to_table(table_name)
        if (
            insert_overwrite_strategy_override is None
            and where is None
            and self._is_iceberg_table(
                table_format=kwargs.get("table_format"),
                catalog_name=table.catalog,
                table_properties=kwargs.get("table_properties"),
            )
        ):
            insert_overwrite_strategy_override = InsertOverwriteStrategy.INSERT_OVERWRITE
        return super()._insert_overwrite_by_condition(
            table_name,
            source_queries,
            target_columns_to_types=target_columns_to_types,
            where=where,
            insert_overwrite_strategy_override=insert_overwrite_strategy_override,
            **kwargs,
        )

    def insert_overwrite_by_partition(
        self,
        table_name: TableName,
        query_or_df: QueryOrDF,
        partitioned_by: t.List[exp.Expr],
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        source_columns: t.Optional[t.List[str]] = None,
    ) -> None:
        parsed_exprs, _ = self._parse_partition_expressions(partitioned_by)
        raw_key_exprs: t.List[exp.Expr] = []
        for expr in parsed_exprs or partitioned_by:
            if isinstance(expr, exp.Column):
                raw_key_exprs.append(expr)
            else:
                cols = list(expr.find_all(exp.Column))
                if cols:
                    raw_key_exprs.extend(cols)
                else:
                    raw_key_exprs.append(expr)
        key_exprs: t.List[exp.Expr] = []
        seen_cols: t.Set[str] = set()
        for expr in raw_key_exprs:
            if isinstance(expr, exp.Column):
                col_lower = expr.name.lower()
                if col_lower not in seen_cols:
                    seen_cols.add(col_lower)
                    key_exprs.append(
                        exp.column(expr.name, quoted=bool(getattr(expr.this, "quoted", False)))
                    )
            else:
                key_exprs.append(expr)
        super().insert_overwrite_by_partition(
            table_name,
            query_or_df,
            key_exprs or partitioned_by,
            target_columns_to_types=target_columns_to_types,
            source_columns=source_columns,
        )

    def delete_from(
        self, table_name: TableName, where: t.Optional[t.Union[str, exp.Expr]] = None
    ) -> None:
        """Delete from a table in Doris."""
        if not where:
            self._truncate_table(table_name)
            return

        if isinstance(where, str):
            where = parse_one(where, read=self.dialect)
        else:
            where = where.copy()

        where = self._simplify_boolean_where(where)
        if where == exp.true():
            self._truncate_table(table_name)
            return
        if where == exp.false():
            return

        where = self._rewrite_between_in_where(where)

        subquery_info = self._extract_subquery_from_where(where)
        if subquery_info is not None:
            self._execute_delete_with_subquery(table_name, *subquery_info)
        else:
            target_table = exp.to_table(table_name)
            for col in where.find_all(exp.Column):
                if not col.find_ancestor(exp.Subquery, exp.Select) and (
                    col.table and col.table.lower() == target_table.name.lower()
                ):
                    col.set("table", None)
                    col.set("db", None)
                    col.set("catalog", None)
            super().delete_from(table_name, where)

    @classmethod
    def _simplify_boolean_where(cls, expr: exp.Expr) -> exp.Expr:
        if isinstance(expr, (exp.Subquery, exp.Select)):
            return expr
        for k, v in list(expr.args.items()):
            if isinstance(v, exp.Expr):
                expr.set(k, cls._simplify_boolean_where(v))
            elif isinstance(v, list):
                expr.set(
                    k,
                    [
                        cls._simplify_boolean_where(item) if isinstance(item, exp.Expr) else item
                        for item in v
                    ],
                )
        if isinstance(expr, exp.Paren) and isinstance(expr.this, exp.Boolean):
            return expr.this
        if (
            isinstance(expr, exp.EQ)
            and isinstance(expr.this, exp.Literal)
            and isinstance(expr.expression, exp.Literal)
            and expr.this.is_number == expr.expression.is_number
        ):
            return exp.true() if expr.this == expr.expression else exp.false()
        if isinstance(expr, exp.Not) and isinstance(expr.this, exp.Boolean):
            return exp.false() if expr.this == exp.true() else exp.true()
        if isinstance(expr, exp.And):
            if expr.this == exp.false() or expr.expression == exp.false():
                return exp.false()
            if expr.this == exp.true():
                return expr.expression
            if expr.expression == exp.true():
                return expr.this
        if isinstance(expr, exp.Or):
            if expr.this == exp.true() or expr.expression == exp.true():
                return exp.true()
            if expr.this == exp.false():
                return expr.expression
            if expr.expression == exp.false():
                return expr.this
        return expr

    @staticmethod
    def _rewrite_between_in_where(where: exp.Expr) -> exp.Expr:
        if isinstance(where, exp.Between):
            return exp.and_(
                exp.GTE(this=where.this.copy(), expression=where.args["low"].copy()),
                exp.LTE(this=where.this.copy(), expression=where.args["high"].copy()),
                copy=False,
            )
        for between in list(where.find_all(exp.Between)):
            if between.find_ancestor(exp.Subquery, exp.Select, exp.Not):
                continue
            replacement: exp.Expr = exp.and_(
                exp.GTE(this=between.this.copy(), expression=between.args["low"].copy()),
                exp.LTE(this=between.this.copy(), expression=between.args["high"].copy()),
                copy=False,
            )
            if not isinstance(between.parent, (exp.Where, exp.Paren)):
                replacement = exp.Paren(this=replacement)
            between.replace(replacement)
        return where

    def _extract_subquery_from_where(
        self, where: exp.Expr
    ) -> t.Optional[t.Tuple[t.List[exp.Expr], exp.Subquery, t.List[exp.Expr]]]:
        """Extract positive IN (SELECT ...) subquery and any sibling AND conditions."""
        where = where.unnest()
        conjuncts: t.List[exp.Expr] = (
            list(where.flatten()) if isinstance(where, exp.And) else [where]
        )
        target_in: t.Optional[exp.In] = None
        subquery: t.Optional[exp.Subquery] = None
        extra_conditions: t.List[exp.Expr] = []

        for cond in conjuncts:
            unwrapped_cond = cond.unnest()
            if target_in is None and isinstance(unwrapped_cond, exp.In):
                sq = self._get_in_subquery(unwrapped_cond)
                if sq is not None:
                    target_in = unwrapped_cond
                    subquery = sq
                    continue
            extra_conditions.append(cond)

        if target_in is None or subquery is None:
            return None

        lhs = target_in.this
        select_expr = subquery.this
        key_exprs: t.List[exp.Expr] = []

        lhs_concat = self._unwrap_concat_ws(lhs)
        if lhs_concat is not None:
            key_exprs = [e.copy() for e in lhs_concat.expressions[1:]]
            if isinstance(select_expr, exp.Select) and len(select_expr.expressions) == 1:
                first_sel = select_expr.expressions[0]
                raw_sel = first_sel.this if isinstance(first_sel, exp.Alias) else first_sel
                sel_concat = self._unwrap_concat_ws(raw_sel)
                if sel_concat is not None:
                    select_expr = select_expr.copy()
                    select_expr.set("expressions", [e.copy() for e in sel_concat.expressions[1:]])
                    subquery = exp.Subquery(this=select_expr)
        elif isinstance(lhs, exp.Tuple):
            key_exprs = [e.copy() for e in lhs.expressions]
        else:
            key_exprs = [lhs.copy()]

        normalized_keys: t.List[exp.Expr] = []
        for k in key_exprs:
            if isinstance(k, exp.Column):
                normalized_keys.append(k)
            elif isinstance(k, exp.Identifier):
                normalized_keys.append(exp.Column(this=k.copy()))
            else:
                return None

        if not normalized_keys:
            return None

        return normalized_keys, subquery, extra_conditions

    @staticmethod
    def _unwrap_concat_ws(expr: exp.Expr) -> t.Optional[exp.Expr]:
        curr = expr.unnest()
        if isinstance(curr, exp.Column) and isinstance(curr.this, exp.Expr):
            curr = curr.this.unnest()
        if isinstance(curr, exp.ConcatWs) or (
            isinstance(curr, exp.Anonymous) and curr.name.upper() == "CONCAT_WS"
        ):
            return curr
        return None

    @staticmethod
    def _get_in_subquery(in_expr: exp.In) -> t.Optional[exp.Subquery]:
        query = in_expr.args.get("query")
        if isinstance(query, exp.Subquery):
            return query.copy()
        if isinstance(query, exp.Select):
            return exp.Subquery(this=query.copy())
        if len(in_expr.expressions) == 1:
            first = in_expr.expressions[0]
            if isinstance(first, exp.Subquery):
                return first.copy()
            if isinstance(first, exp.Select):
                return exp.Subquery(this=first.copy())
        return None

    def _execute_delete_with_subquery(
        self,
        table_name: TableName,
        key_exprs: t.List[exp.Expr],
        subquery: exp.Subquery,
        extra_conditions: t.List[exp.Expr],
    ) -> None:
        """Execute DELETE FROM ... USING (SELECT ...) in Doris, with literal batch fallback for DUPLICATE KEY/Iceberg."""
        target_table = exp.to_table(table_name)
        if self._is_iceberg_table(catalog_name=target_table.catalog):
            self._delete_by_literal_keys(table_name, key_exprs, subquery, extra_conditions)
            return

        join_conditions: t.List[exp.Expr] = []
        first_select = subquery.this
        while isinstance(first_select, exp.SetOperation):
            first_select = first_select.this
        for idx, col in enumerate(key_exprs):
            col_name = col.name
            t1_col = exp.column(col_name, table="_t1")
            t2_col_name = col_name
            if isinstance(first_select, exp.Select) and idx < len(first_select.expressions):
                sel_item = first_select.expressions[idx]
                if sel_item.alias_or_name:
                    t2_col_name = sel_item.alias_or_name
                elif isinstance(subquery.this, exp.Select):
                    alias_name = f"_col_{idx}"
                    subquery = subquery.copy()
                    sub_select = t.cast(exp.Select, subquery.this)
                    new_exprs = list(sub_select.expressions)
                    new_exprs[idx] = exp.alias_(sel_item.copy(), alias_name, quoted=True)
                    sub_select.set("expressions", new_exprs)
                    first_select = sub_select
                    t2_col_name = alias_name
            t2_col = exp.column(t2_col_name, table="_t2")
            join_conditions.append(t1_col.eq(t2_col))

        for extra in extra_conditions:
            qualified_extra = extra.copy()
            for col in qualified_extra.find_all(exp.Column):
                if not col.find_ancestor(exp.Subquery, exp.Select) and (
                    not col.table or col.table.lower() == target_table.name.lower()
                ):
                    col.set("table", exp.to_identifier("_t1"))
                    col.set("db", None)
                    col.set("catalog", None)
            join_conditions.append(qualified_extra)

        where_expr = exp.and_(*join_conditions)

        target_sql = f"{self._to_sql(target_table)} AS `_t1`"
        subquery_with_alias = exp.alias_(subquery.copy(), "_t2", table=True)
        subquery_sql = self._to_sql(subquery_with_alias)
        where_sql = self._to_sql(where_expr)

        delete_sql = f"DELETE FROM {target_sql} USING {subquery_sql} WHERE {where_sql}"
        try:
            self.execute(delete_sql)
        except Exception as e:
            err_msg = str(e).lower()
            if any(
                marker in err_msg
                for marker in (
                    "unique key",
                    "olaptable",
                    "olap table",
                    "not support",
                    "only support",
                    "unsupported",
                )
            ):
                logger.info(
                    "DELETE ... USING not supported on table '%s' (%s); falling back to batched literal DELETE.",
                    table_name,
                    e,
                )
                self._delete_by_literal_keys(table_name, key_exprs, subquery, extra_conditions)
            else:
                raise

    @staticmethod
    def _sanitize_key_value(val: t.Any) -> t.Any:
        if val is None:
            return None
        try:
            if pd.isna(val):
                return None
        except (TypeError, ValueError):
            pass
        return val

    def _delete_by_literal_keys(
        self,
        table_name: TableName,
        key_exprs: t.List[exp.Expr],
        subquery: t.Union[exp.Subquery, exp.Select],
        extra_conditions: t.Optional[t.List[exp.Expr]] = None,
    ) -> None:
        """Fallback for DELETE on DUPLICATE KEY / AGGREGATE KEY / Iceberg tables that prohibits OR predicates."""
        select_query = (
            subquery.this.copy() if isinstance(subquery, exp.Subquery) else subquery.copy()
        )
        if isinstance(select_query, exp.Select):
            select_query = select_query.distinct()
        rows = self.fetchall(select_query, quote_identifiers=True)
        if not rows:
            return

        target_table = exp.to_table(table_name)
        cleaned_extra_conditions: t.List[exp.Expr] = []
        for extra in extra_conditions or []:
            cleaned_extra = extra.copy()
            for col in cleaned_extra.find_all(exp.Column):
                if not col.find_ancestor(exp.Subquery, exp.Select) and (
                    col.table and col.table.lower() == target_table.name.lower()
                ):
                    col.set("table", None)
                    col.set("db", None)
                    col.set("catalog", None)
            cleaned_extra_conditions.append(cleaned_extra)
        extra_conditions = cleaned_extra_conditions
        cols = [exp.column(k.name, quoted=True) for k in key_exprs]
        batch_size = self.DEFAULT_BATCH_SIZE

        sanitized_rows = [tuple(self._sanitize_key_value(v) for v in row) for row in rows]

        if len(cols) == 1:
            col = cols[0]
            non_null_vals = [exp.convert(r[0]) for r in sanitized_rows if r[0] is not None]
            has_null = any(r[0] is None for r in sanitized_rows)

            for i in range(0, len(non_null_vals), batch_size):
                batch = non_null_vals[i : i + batch_size]
                predicates: t.List[exp.Expr] = [col.isin(*batch), *extra_conditions]
                self.execute(exp.delete(table_name, where=exp.and_(*predicates)))

            if has_null:
                predicates = [col.is_(exp.null()), *extra_conditions]
                self.execute(exp.delete(table_name, where=exp.and_(*predicates)))
            return

        # Composite key (N >= 2): dynamically select the column index to batch via IN (...)
        # that minimizes the number of distinct prefix groups, breaking ties in favor of the last column.
        batch_idx = min(
            range(len(cols)),
            key=lambda idx: (len({row[:idx] + row[idx + 1 :] for row in sanitized_rows}), -idx),
        )
        grouped: t.Dict[t.Tuple[t.Any, ...], t.List[t.Any]] = defaultdict(list)
        for row in sanitized_rows:
            prefix_key = row[:batch_idx] + row[batch_idx + 1 :]
            grouped[prefix_key].append(row[batch_idx])

        batch_col = cols[batch_idx]
        prefix_cols = cols[:batch_idx] + cols[batch_idx + 1 :]

        for prefix_vals, batch_vals in grouped.items():
            prefix_preds: t.List[exp.Expr] = [
                p_col.eq(exp.convert(p_val)) if p_val is not None else p_col.is_(exp.null())
                for p_col, p_val in zip(prefix_cols, prefix_vals)
            ]
            non_null_batch = [exp.convert(v) for v in batch_vals if v is not None]
            has_null_batch = any(v is None for v in batch_vals)

            for i in range(0, len(non_null_batch), batch_size):
                batch = non_null_batch[i : i + batch_size]
                preds = [*prefix_preds, batch_col.isin(*batch), *extra_conditions]
                self.execute(exp.delete(table_name, where=exp.and_(*preds)))

            if has_null_batch:
                preds = [*prefix_preds, batch_col.is_(exp.null()), *extra_conditions]
                self.execute(exp.delete(table_name, where=exp.and_(*preds)))

    def adjust_physical_properties_for_incremental(
        self,
        physical_properties: t.Dict[str, t.Any],
        *,
        requires_delete_capable_table: bool,
        unique_key: t.Optional[t.List[exp.Expr]],
        model_name: str,
    ) -> t.Dict[str, t.Any]:
        """Promote ``unique_key`` from ``INCREMENTAL_BY_UNIQUE_KEY`` models into ``physical_properties``
        when no explicit OLAP key model (``unique_key``, ``duplicate_key``, ``aggregate_key``) is set.
        """
        if (
            unique_key
            and "unique_key" not in physical_properties
            and "duplicate_key" not in physical_properties
            and "aggregate_key" not in physical_properties
            and all(isinstance(k, (exp.Column, exp.Identifier)) for k in unique_key)
        ):
            cleaned_keys = [exp.column(k.name) for k in unique_key]
            physical_properties = dict(physical_properties)
            physical_properties["unique_key"] = (
                cleaned_keys[0] if len(cleaned_keys) == 1 else exp.Tuple(expressions=cleaned_keys)
            )
        return physical_properties

    def _create_table_from_columns(
        self,
        table_name: TableName,
        target_columns_to_types: t.Dict[str, exp.DataType],
        primary_key: t.Optional[t.Tuple[str, ...]] = None,
        exists: bool = True,
        table_description: t.Optional[str] = None,
        column_descriptions: t.Optional[t.Dict[str, str]] = None,
        **kwargs: t.Any,
    ) -> None:
        table_properties = dict(kwargs.get("table_properties") or {})

        if (
            primary_key
            and "unique_key" not in table_properties
            and "duplicate_key" not in table_properties
            and "aggregate_key" not in table_properties
        ):
            table_properties["unique_key"] = exp.Tuple(
                expressions=[exp.to_column(col) for col in primary_key]
            )

        kwargs["table_properties"] = table_properties

        super()._create_table_from_columns(
            table_name=table_name,
            target_columns_to_types=target_columns_to_types,
            primary_key=None,
            exists=exists,
            table_description=table_description,
            column_descriptions=column_descriptions,
            **kwargs,
        )

    def _build_create_table_exp(
        self,
        table_name_or_schema: t.Union[exp.Schema, TableName],
        expression: t.Optional[exp.Expr],
        exists: bool = True,
        replace: bool = False,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> exp.Create:
        kwargs.setdefault("table_properties", None)
        kwargs["is_schema_ddl"] = isinstance(table_name_or_schema, exp.Schema)

        catalog_name = None
        if isinstance(table_name_or_schema, exp.Schema):
            if isinstance(table_name_or_schema.this, exp.Table):
                catalog_name = table_name_or_schema.this.catalog
        else:
            catalog_name = exp.to_table(table_name_or_schema).catalog

        is_iceberg = self._is_iceberg_table(
            table_format=kwargs.get("table_format"),
            catalog_name=catalog_name,
            table_properties=kwargs.get("table_properties"),
        )

        if not is_iceberg and target_columns_to_types:
            original_cols = dict(target_columns_to_types)
            reordered_cols = self._reorder_olap_columns(target_columns_to_types, kwargs)
            order_changed = list(reordered_cols) != list(original_cols)
            types_changed = any(reordered_cols[c] != original_cols[c] for c in reordered_cols)
            if order_changed or types_changed:
                target_columns_to_types = reordered_cols
                if isinstance(table_name_or_schema, exp.Schema):
                    col_def_map = {
                        col_def.name.lower(): col_def
                        for col_def in table_name_or_schema.expressions
                        if isinstance(col_def, exp.ColumnDef)
                    }
                    other_exprs = [
                        e
                        for e in table_name_or_schema.expressions
                        if not isinstance(e, exp.ColumnDef)
                    ]
                    if len(col_def_map) == len(target_columns_to_types) and all(
                        c.lower() in col_def_map for c in target_columns_to_types
                    ):
                        new_col_defs: t.List[exp.Expr] = []
                        for c, new_dtype in target_columns_to_types.items():
                            col_def = col_def_map[c.lower()]
                            if col_def.args.get("kind") != new_dtype:
                                col_def = col_def.copy()
                                col_def.set("kind", new_dtype)
                            new_col_defs.append(col_def)
                        table_name_or_schema = table_name_or_schema.copy()
                        table_name_or_schema.set(
                            "expressions",
                            new_col_defs + other_exprs,
                        )
                if order_changed and expression is not None and isinstance(expression, exp.Query):
                    reordered_in_place = False
                    if isinstance(expression, exp.Select):
                        proj_by_name = {
                            proj.alias_or_name.lower(): proj
                            for proj in expression.expressions
                            if proj.alias_or_name
                        }
                        if len(proj_by_name) == len(expression.expressions) == len(
                            target_columns_to_types
                        ) and all(c.lower() in proj_by_name for c in target_columns_to_types):
                            expression = expression.copy()
                            expression.set(
                                "expressions",
                                [proj_by_name[c.lower()] for c in target_columns_to_types],
                            )
                            reordered_in_place = True
                    if not reordered_in_place:
                        expression = self._order_projections_and_filter(
                            expression, target_columns_to_types
                        )
        elif not is_iceberg and isinstance(expression, (exp.Select, exp.SetOperation)):
            expression = self._reorder_unannotated_ctas_query(expression, kwargs)

        # Doris AUTO PARTITION BY RANGE requires the partition column(s) to be defined as NOT NULL.
        if not is_iceberg:
            table_props = kwargs.get("table_properties") or {}
            auto_partition = table_props.get("auto_partition")
            is_auto = bool(
                auto_partition is not None
                and str(self._extract_str_value(auto_partition) or "").strip().lower()
                in ("true", "1")
            )
            raw_part_by = kwargs.get("partitioned_by") or table_props.get("partitioned_by")
            if is_auto and raw_part_by:
                part_list = raw_part_by if isinstance(raw_part_by, list) else [raw_part_by]
                parsed_part_exprs, part_kind = self._parse_partition_expressions(part_list)
                if (part_kind or "RANGE").upper() != "LIST":
                    auto_range_cols = {
                        col.name.lower()
                        for expr in parsed_part_exprs
                        for col in expr.find_all(exp.Column)
                    }
                    strict_key = (
                        table_props.get("unique_key")
                        or kwargs.get("unique_key")
                        or table_props.get("aggregate_key")
                    )
                    strict_key_set = {k.lower() for k in self._extract_column_names(strict_key)}
                    if strict_key_set and not auto_range_cols.issubset(strict_key_set):
                        auto_range_cols = set()
                    if auto_range_cols and isinstance(table_name_or_schema, exp.Schema):
                        table_name_or_schema = table_name_or_schema.copy()
                        updated_exprs: t.List[exp.Expr] = []
                        for expr in table_name_or_schema.expressions:
                            if (
                                isinstance(expr, exp.ColumnDef)
                                and expr.name.lower() in auto_range_cols
                            ):
                                existing_constraints = list(expr.args.get("constraints") or [])
                                has_not_null = any(
                                    isinstance(c, exp.NotNullColumnConstraint)
                                    or isinstance(
                                        getattr(c, "kind", None), exp.NotNullColumnConstraint
                                    )
                                    for c in existing_constraints
                                )
                                if not has_not_null:
                                    expr = expr.copy()
                                    expr.set(
                                        "constraints",
                                        [exp.ColumnConstraint(kind=exp.NotNullColumnConstraint())]
                                        + existing_constraints,
                                    )
                            updated_exprs.append(expr)
                        table_name_or_schema.set("expressions", updated_exprs)
                    elif auto_range_cols and isinstance(expression, exp.Select):
                        expression = expression.copy()
                        new_select_exprs: t.List[exp.Expr] = []
                        for proj in expression.expressions:
                            if proj.alias_or_name.lower() in auto_range_cols:
                                if isinstance(proj, exp.Alias):
                                    inner = proj.this
                                    if not (
                                        isinstance(inner, exp.Anonymous)
                                        and inner.name.upper() == "NON_NULLABLE"
                                    ):
                                        proj = proj.copy()
                                        proj.set(
                                            "this",
                                            exp.Anonymous(
                                                this="NON_NULLABLE", expressions=[inner.copy()]
                                            ),
                                        )
                                elif isinstance(proj, exp.Column):
                                    proj = exp.alias_(
                                        exp.Anonymous(
                                            this="NON_NULLABLE", expressions=[proj.copy()]
                                        ),
                                        proj.name,
                                        quoted=bool(getattr(proj.this, "quoted", True)),
                                    )
                            new_select_exprs.append(proj)
                        expression.set("expressions", new_select_exprs)

        return super()._build_create_table_exp(
            table_name_or_schema,
            expression=expression,
            exists=exists,
            replace=replace,
            target_columns_to_types=target_columns_to_types,
            table_description=table_description,
            table_kind=table_kind,
            **kwargs,
        )

    def _reorder_unannotated_ctas_query(
        self, expression: exp.Expr, kwargs: t.Dict[str, t.Any]
    ) -> exp.Expr:
        if isinstance(expression, exp.SetOperation):
            expression = expression.copy()
            expression.set("this", self._reorder_unannotated_ctas_query(expression.this, kwargs))
            expression.set(
                "expression", self._reorder_unannotated_ctas_query(expression.expression, kwargs)
            )
            return expression
        if isinstance(expression, exp.Select):
            proj_by_name = {
                proj.alias_or_name.lower(): proj
                for proj in expression.expressions
                if proj.alias_or_name and not isinstance(proj, exp.Star)
            }
            if proj_by_name and len(proj_by_name) == len(expression.expressions):
                select_cols_to_types = {
                    proj.alias_or_name: exp.DataType.build("UNKNOWN")
                    for proj in expression.expressions
                }
                reordered_select_cols = self._reorder_olap_columns(select_cols_to_types, kwargs)
                if list(reordered_select_cols) != list(select_cols_to_types):
                    expression = expression.copy()
                    expression.set(
                        "expressions",
                        [proj_by_name[c.lower()] for c in reordered_select_cols],
                    )
        return expression

    @classmethod
    def _is_coercible_string_key_type(cls, dtype: exp.DataType) -> bool:
        type_name = (
            dtype.this.value.upper()
            if isinstance(dtype.this, exp.DataType.Type)
            else str(dtype.this).upper()
        )
        return type_name in ("TEXT", "STRING", "TINYTEXT", "MEDIUMTEXT", "LONGTEXT")

    def _reorder_olap_columns(
        self,
        target_columns_to_types: t.Dict[str, exp.DataType],
        kwargs: t.Dict[str, t.Any],
    ) -> t.Dict[str, exp.DataType]:
        table_props = kwargs.get("table_properties") or {}
        unique_key = table_props.get("unique_key") or kwargs.get("unique_key")
        duplicate_key = table_props.get("duplicate_key")
        aggregate_key = table_props.get("aggregate_key")

        specified_key_types = [
            name
            for name, val in (
                ("unique_key", unique_key),
                ("duplicate_key", duplicate_key),
                ("aggregate_key", aggregate_key),
            )
            if self._extract_column_names(val)
        ]
        if len(specified_key_types) > 1:
            raise SQLMeshError(
                f"Doris tables support at most one key model (unique_key, duplicate_key, or aggregate_key), "
                f"got multiple: {', '.join(specified_key_types)}."
            )

        explicit_key = unique_key or duplicate_key or aggregate_key
        col_names = list(target_columns_to_types)
        if not col_names:
            return target_columns_to_types

        if explicit_key:
            col_by_lower = {c.lower(): c for c in col_names}
            raw_key_cols = self._extract_column_names(explicit_key)
            missing_cols = [k for k in raw_key_cols if k.lower() not in col_by_lower]
            if missing_cols:
                raise SQLMeshError(
                    f"Doris key columns {missing_cols} not found in table schema columns {col_names}."
                )
            key_cols = [col_by_lower[k.lower()] for k in raw_key_cols if k.lower() in col_by_lower]
            updated_types = dict(target_columns_to_types)
            for k in key_cols:
                k_dtype = updated_types[k]
                if not self._is_valid_olap_key_type(k_dtype):
                    if self._is_coercible_string_key_type(k_dtype):
                        updated_types[k] = exp.DataType.build("VARCHAR(255)", dialect=self.dialect)
                    else:
                        raise SQLMeshError(
                            f"Doris key column '{k}' has type '{k_dtype.sql(dialect=self.dialect)}' which is not supported as an OLAP key column."
                        )
            key_cols_set = set(key_cols)
            new_order = (
                key_cols + [c for c in col_names if c not in key_cols_set]
                if key_cols and col_names[: len(key_cols)] != key_cols
                else col_names
            )
            return {c: updated_types[c] for c in new_order}

        updated_types = dict(target_columns_to_types)
        first_dtype = updated_types[col_names[0]]
        if not self._is_valid_olap_key_type(first_dtype):
            valid_col = next(
                (c for c in col_names if self._is_valid_olap_key_type(updated_types[c])),
                None,
            )
            if valid_col:
                col_names = [valid_col] + [c for c in col_names if c != valid_col]
            else:
                coercible_col = next(
                    (c for c in col_names if self._is_coercible_string_key_type(updated_types[c])),
                    None,
                )
                if coercible_col:
                    updated_types[coercible_col] = exp.DataType.build(
                        "VARCHAR(255)", dialect=self.dialect
                    )
                    if col_names[0] != coercible_col:
                        col_names = [coercible_col] + [c for c in col_names if c != coercible_col]

        # Ensure any OLAP partition columns with valid key types precede non-keyable columns
        raw_part_by = kwargs.get("partitioned_by") or table_props.get("partitioned_by")
        if raw_part_by:
            part_list = raw_part_by if isinstance(raw_part_by, list) else [raw_part_by]
            parsed_part_exprs, _ = self._parse_partition_expressions(part_list)
            col_by_lower = {c.lower(): c for c in col_names}
            part_cols = [
                col_by_lower[col.name.lower()]
                for expr in parsed_part_exprs
                for col in expr.find_all(exp.Column)
                if col.name.lower() in col_by_lower
                and self._is_valid_olap_key_type(updated_types[col_by_lower[col.name.lower()]])
            ]
            first_non_key_idx = next(
                (
                    i
                    for i, c in enumerate(col_names)
                    if not self._is_valid_olap_key_type(updated_types[c])
                ),
                None,
            )
            if (
                part_cols
                and first_non_key_idx is not None
                and any(col_names.index(p) > first_non_key_idx for p in part_cols)
            ):
                moved_parts = [p for p in part_cols if col_names.index(p) > first_non_key_idx]
                moved_set = set(moved_parts)
                before_non_key = [c for c in col_names[:first_non_key_idx] if c not in moved_set]
                after_non_key = [c for c in col_names[first_non_key_idx:] if c not in moved_set]
                col_names = before_non_key + moved_parts + after_non_key

        if col_names != list(target_columns_to_types) or updated_types != target_columns_to_types:
            return {c: updated_types[c] for c in col_names}

        return target_columns_to_types

    def _parse_partition_expressions(
        self, partitioned_by: t.List[exp.Expr]
    ) -> t.Tuple[t.List[exp.Expr], t.Optional[str]]:
        """Parse partition expressions and extract partition kind and normalized expressions."""
        parsed_partitioned_by: t.List[exp.Expr] = []
        partition_kind: t.Optional[str] = None

        for expr in partitioned_by:
            if isinstance(expr, exp.List) or (
                isinstance(expr, exp.Anonymous)
                and expr.this
                and str(expr.this).upper() in ("RANGE", "LIST")
            ):
                partition_kind = "LIST" if isinstance(expr, exp.List) else str(expr.this).upper()
                for arg in expr.expressions:
                    if isinstance(arg, (exp.Column, exp.Func, exp.Anonymous)):
                        parsed_partitioned_by.append(arg)
                    else:
                        parsed_partitioned_by.append(exp.to_column(str(arg)))
                continue

            if isinstance(expr, exp.Literal) and getattr(expr, "is_string", False):
                text = str(expr.this)
                match = re.match(r"^\s*(RANGE|LIST)\s*\((.+)\)\s*$", text, flags=re.IGNORECASE)
                if match:
                    partition_kind = match.group(1).upper()
                    inner = match.group(2).strip()
                    if ";" in inner:
                        raise SQLMeshError(f"Invalid partition expression: {text}")
                    try:
                        parsed_stmts = parse(f"SELECT {inner}", read=self.dialect)
                        if (
                            len(parsed_stmts) == 1
                            and isinstance(parsed_stmts[0], exp.Select)
                            and set(parsed_stmts[0].args.keys()) <= {"expressions"}
                        ):
                            for sel_expr in parsed_stmts[0].expressions:
                                parsed_partitioned_by.append(sel_expr)
                            continue
                    except Exception:
                        pass
                    inner_cols = [c.strip().strip("`") for c in inner.split(",") if c.strip()]
                    for col in inner_cols:
                        parsed_partitioned_by.append(exp.to_column(col))
                    continue

            parsed_partitioned_by.append(expr)

        return parsed_partitioned_by, partition_kind

    @staticmethod
    def _to_raw_partition_sql(expr: t.Union[exp.Literal, exp.Var, str, t.Any]) -> exp.Var:
        if isinstance(expr, exp.Literal):
            raw_text = str(expr.this)
        elif isinstance(expr, exp.Var):
            raw_text = str(expr.this)
        elif isinstance(expr, str):
            raw_text = expr
        else:
            return expr
        if ";" in raw_text or "--" in raw_text or "/*" in raw_text:
            raise SQLMeshError(f"Invalid partition definition expression: {raw_text}")
        return exp.Var(this=raw_text, quoted=False)

    def _build_partitioned_by_exp(
        self,
        partitioned_by: t.List[exp.Expr],
        *,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        catalog_name: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[
        t.Union[
            exp.PartitionedByProperty,
            exp.PartitionByRangeProperty,
            exp.PartitionByListProperty,
            exp.Property,
        ]
    ]:
        """Build Doris partitioning expression for OLAP (manual, dynamic, AUTO PARTITION) and Iceberg tables."""
        is_iceberg = bool(kwargs.get("is_iceberg"))
        partitions = kwargs.get("partitions")
        auto_partition = kwargs.get("auto_partition")
        dynamic_partition_enabled = kwargs.get("dynamic_partition_enabled")

        partitioned_by, partition_kind = self._parse_partition_expressions(partitioned_by)
        if target_columns_to_types:
            col_by_lower = {c.lower(): c for c in target_columns_to_types}
            normalized_exprs: t.List[exp.Expr] = []
            for expr in partitioned_by:
                expr_copy = expr.copy()
                for col_node in expr_copy.find_all(exp.Column):
                    canonical = col_by_lower.get(col_node.name.lower())
                    if canonical and canonical != col_node.name:
                        col_node.set("this", exp.to_identifier(canonical))
                normalized_exprs.append(expr_copy)
            partitioned_by = normalized_exprs

        if is_iceberg:
            iceberg_part_exprs = [
                expr.copy().transform(
                    lambda node: (
                        exp.Anonymous(
                            this="BUCKET",
                            expressions=[node.expression.copy(), node.this.copy()],
                        )
                        if isinstance(node, exp.PartitionedByBucket)
                        else exp.Anonymous(
                            this="TRUNCATE",
                            expressions=[node.expression.copy(), node.this.copy()],
                        )
                        if isinstance(node, exp.PartitionByTruncate)
                        else node
                    )
                )
                for expr in partitioned_by
            ]
            return exp.PartitionByListProperty(
                partition_expressions=iceberg_part_exprs,
                create_expressions=[],
            )

        is_auto = False
        if auto_partition is not None:
            auto_str = self._extract_str_value(auto_partition)
            is_auto = str(auto_str).strip().lower() in ("true", "1")

        # On Doris OLAP tables, if no explicit partitions, auto_partition, or dynamic_partition.enable
        # are configured, omit PARTITION BY so Doris does not create a 0-partition table that rejects inserts.
        if not partitions and not is_auto and not dynamic_partition_enabled:
            return None

        create_expressions: t.Optional[t.List[exp.Expr]] = None
        if partitions:
            if isinstance(partitions, exp.Tuple):
                create_expressions = [self._to_raw_partition_sql(e) for e in partitions.expressions]
            else:
                create_expressions = [self._to_raw_partition_sql(partitions)]
        elif is_auto or dynamic_partition_enabled:
            create_expressions = []

        inferred_list = False
        if partition_kind is None and create_expressions:
            try:
                texts = [getattr(e, "this", "").upper() for e in create_expressions]
                inferred_list = any("VALUES IN" in text for text in texts)
            except Exception:
                inferred_list = False
        if partition_kind:
            is_list = str(partition_kind).upper() == "LIST"
        else:
            is_list = inferred_list

        partition_prop: t.Union[exp.PartitionByListProperty, exp.PartitionByRangeProperty]
        if is_list:
            partition_prop = exp.PartitionByListProperty(
                partition_expressions=partitioned_by,
                create_expressions=create_expressions,
            )
        else:
            partition_prop = exp.PartitionByRangeProperty(
                partition_expressions=partitioned_by,
                create_expressions=create_expressions,
            )

        if is_auto:
            return exp.SetConfigProperty(this=exp.var(f"AUTO {self._to_sql(partition_prop)}"))

        return partition_prop

    def _build_table_properties_exp(
        self,
        catalog_name: t.Optional[str] = None,
        table_format: t.Optional[str] = None,
        storage_format: t.Optional[str] = None,
        partitioned_by: t.Optional[t.List[exp.Expr]] = None,
        partition_interval_unit: t.Optional[IntervalUnit] = None,
        clustered_by: t.Optional[t.List[exp.Expr]] = None,
        table_properties: t.Optional[t.Dict[str, t.Any]] = None,
        target_columns_to_types: t.Optional[t.Dict[str, exp.DataType]] = None,
        table_description: t.Optional[str] = None,
        table_kind: t.Optional[str] = None,
        **kwargs: t.Any,
    ) -> t.Optional[exp.Properties]:
        """Creates a SQLGlot table properties expression for Doris DDL."""
        properties: t.List[exp.Expr] = []
        is_materialized_view = table_kind == "MATERIALIZED_VIEW"
        is_schema_ddl = bool(kwargs.get("is_schema_ddl"))

        if is_materialized_view:
            properties.append(exp.MaterializedProperty())

        table_properties_copy = dict(table_properties) if table_properties else {}
        is_iceberg = not is_materialized_view and self._is_iceberg_table(
            table_format=table_format,
            catalog_name=catalog_name,
            table_properties=table_properties_copy,
        )

        # Pop Iceberg engine property if present so it is not duplicated in PROPERTIES (...)
        table_properties_copy.pop("engine", None)
        if is_iceberg and is_schema_ddl:
            properties.append(exp.EngineProperty(this=exp.var("iceberg")))

        if is_materialized_view:
            build_val = table_properties_copy.pop("build", None)
            if build_val is not None:
                build_upper = self._to_upper_str(build_val)
                if build_upper not in ("IMMEDIATE", "DEFERRED"):
                    raise SQLMeshError(
                        f"Invalid materialized view build mode: {build_upper}. Expected IMMEDIATE or DEFERRED."
                    )
                properties.append(exp.BuildProperty(this=exp.Var(this=build_upper)))

            refresh_val = table_properties_copy.pop("refresh", None)
            refresh_trigger_val = table_properties_copy.pop("refresh_trigger", None)
            if refresh_val is not None or refresh_trigger_val is not None:
                method_var: exp.Var = exp.Var(this="AUTO")
                kind_str: t.Optional[str] = None
                every_lit: t.Optional[exp.Expr] = None
                unit_var: t.Optional[exp.Var] = None
                starts_lit: t.Optional[exp.Literal] = None

                if isinstance(refresh_val, exp.Paren) and isinstance(refresh_val.this, exp.EQ):
                    refresh_val = exp.Tuple(expressions=[refresh_val.this])
                elif isinstance(refresh_val, exp.EQ):
                    refresh_val = exp.Tuple(expressions=[refresh_val])

                if isinstance(refresh_val, exp.Tuple):
                    method_var, kind_str, every_lit, unit_var, starts_lit = (
                        self._parse_refresh_tuple(refresh_val)
                    )
                    if refresh_trigger_val is not None:
                        trigger_text = self._extract_str_value(refresh_trigger_val) or ""
                        k, e, u, s = self._parse_trigger_string(trigger_text)
                        kind_str, every_lit, unit_var, starts_lit = k, e, u, s
                else:
                    raw_refresh = (
                        self._extract_str_value(refresh_val) if refresh_val is not None else "AUTO"
                    )
                    raw_refresh_str = (raw_refresh or "AUTO").strip()
                    if " " in raw_refresh_str:
                        parts = raw_refresh_str.split(None, 1)
                        method_upper = parts[0].upper()
                        if method_upper in ("COMPLETE", "AUTO", "FAST", "FORCE", "NEVER"):
                            method_var = exp.Var(this=method_upper)
                            k, e, u, s = self._parse_trigger_string(parts[1])
                            kind_str, every_lit, unit_var, starts_lit = k, e, u, s
                        else:
                            k, e, u, s = self._parse_trigger_string(raw_refresh_str)
                            kind_str, every_lit, unit_var, starts_lit = k, e, u, s
                    else:
                        method_upper = raw_refresh_str.upper()
                        if method_upper in ("MANUAL", "COMMIT"):
                            kind_str = method_upper
                        elif method_upper in ("COMPLETE", "AUTO", "FAST", "FORCE", "NEVER"):
                            method_var = exp.Var(this=method_upper)
                        else:
                            raise SQLMeshError(
                                f"Invalid materialized view refresh method: {method_upper}. Expected one of: COMPLETE, AUTO, FAST, FORCE, NEVER, MANUAL, COMMIT."
                            )

                    if refresh_trigger_val is not None:
                        trigger_text = self._extract_str_value(refresh_trigger_val) or ""
                        k, e, u, s = self._parse_trigger_string(trigger_text)
                        kind_str, every_lit, unit_var, starts_lit = k, e, u, s

                properties.append(
                    exp.RefreshTriggerProperty(
                        method=method_var,
                        kind=kind_str,
                        every=every_lit,
                        unit=unit_var,
                        starts=starts_lit,
                    )
                )

        # Pop key models and order_by from table_properties_copy
        unique_key = table_properties_copy.pop("unique_key", None) or kwargs.get("unique_key")
        duplicate_key = table_properties_copy.pop("duplicate_key", None)
        aggregate_key = table_properties_copy.pop("aggregate_key", None)
        order_by = table_properties_copy.pop("order_by", None)

        unique_key_cols = self._extract_column_names(unique_key)
        duplicate_key_cols = self._extract_column_names(duplicate_key)
        aggregate_key_cols = self._extract_column_names(aggregate_key)
        order_by_cols = self._extract_column_names(order_by)
        col_by_lower = (
            {c.lower(): c for c in target_columns_to_types} if target_columns_to_types else {}
        )

        if not is_iceberg:
            specified_key_types = [
                name
                for name, cols in (
                    ("unique_key", unique_key_cols),
                    ("duplicate_key", duplicate_key_cols),
                    ("aggregate_key", aggregate_key_cols),
                )
                if cols
            ]
            if len(specified_key_types) > 1:
                raise SQLMeshError(
                    f"Doris tables support at most one key model (unique_key, duplicate_key, or aggregate_key), "
                    f"got multiple: {', '.join(specified_key_types)}."
                )
            active_key_cols = unique_key_cols or duplicate_key_cols or aggregate_key_cols
            if active_key_cols and col_by_lower and not is_materialized_view:
                missing_cols = [k for k in active_key_cols if k.lower() not in col_by_lower]
                if missing_cols:
                    raise SQLMeshError(
                        f"Doris key columns {missing_cols} not found in table schema columns {list(target_columns_to_types or ())}."
                    )
                if unique_key_cols:
                    unique_key_cols = [col_by_lower[k.lower()] for k in unique_key_cols]
                elif duplicate_key_cols:
                    duplicate_key_cols = [col_by_lower[k.lower()] for k in duplicate_key_cols]
                elif aggregate_key_cols:
                    aggregate_key_cols = [col_by_lower[k.lower()] for k in aggregate_key_cols]

            if unique_key_cols:
                properties.append(
                    exp.UniqueKeyProperty(expressions=[exp.to_column(k) for k in unique_key_cols])
                )
            elif duplicate_key_cols:
                properties.append(
                    exp.DuplicateKeyProperty(
                        expressions=[exp.to_column(k) for k in duplicate_key_cols]
                    )
                )
            elif aggregate_key_cols:
                agg_cols_sql = ", ".join(self._to_sql(exp.to_column(k)) for k in aggregate_key_cols)
                properties.append(
                    exp.SetConfigProperty(this=exp.var(f"AGGREGATE KEY ({agg_cols_sql})"))
                )

            if order_by_cols and not is_materialized_view:
                if not unique_key_cols:
                    raise SQLMeshError(
                        "Doris table property 'order_by' is only supported on UNIQUE KEY tables."
                    )
                if col_by_lower:
                    missing_order_cols = [k for k in order_by_cols if k.lower() not in col_by_lower]
                    if missing_order_cols:
                        raise SQLMeshError(
                            f"Doris order_by columns {missing_order_cols} not found in table schema columns {list(target_columns_to_types or ())}."
                        )
                    order_by_cols = [col_by_lower[k.lower()] for k in order_by_cols]
                order_cols_sql = ", ".join(self._to_sql(exp.to_column(k)) for k in order_by_cols)
                properties.append(
                    exp.SetConfigProperty(this=exp.var(f"ORDER BY ({order_cols_sql})"))
                )

        if table_description:
            properties.append(
                exp.SchemaCommentProperty(
                    this=exp.Literal.string(self._truncate_table_comment(table_description))
                )
            )

        # Pop partitioning-related properties
        partitions = table_properties_copy.pop("partitions", None)
        auto_partition = table_properties_copy.pop("auto_partition", None)
        if not partitioned_by and "partitioned_by" in table_properties_copy:
            raw_part_by = table_properties_copy.pop("partitioned_by")
            if raw_part_by is not None:
                partitioned_by = raw_part_by if isinstance(raw_part_by, list) else [raw_part_by]
        else:
            table_properties_copy.pop("partitioned_by", None)

        add_partition = True
        if partitioned_by:
            parsed_part_exprs, _ = self._parse_partition_expressions(partitioned_by)
            strict_part_key_cols = unique_key_cols or aggregate_key_cols
            if strict_part_key_cols and not is_materialized_view and not is_iceberg:
                key_cols_set = {k.lower() for k in strict_part_key_cols}
                partition_cols = set()
                for expr in parsed_part_exprs:
                    for col_node in expr.find_all(exp.Column):
                        partition_cols.add(col_node.name.lower())
                not_in_key = partition_cols - key_cols_set
                if not_in_key:
                    key_label = "UNIQUE KEY" if unique_key_cols else "AGGREGATE KEY"
                    logger.warning(
                        f"[Doris] {key_label} model: partitioned_by columns {not_in_key} not in key_cols {key_cols_set}, skip PARTITION BY."
                    )
                    add_partition = False

            if add_partition:
                if is_materialized_view:
                    if parsed_part_exprs:
                        properties.append(
                            exp.PartitionedByProperty(
                                this=exp.Schema(expressions=parsed_part_exprs),
                            )
                        )
                else:
                    dyn_enable = self._extract_str_value(
                        table_properties_copy.get("dynamic_partition.enable")
                    )
                    dynamic_partition_enabled = bool(
                        dyn_enable and dyn_enable.strip().lower() in ("true", "1")
                    )
                    partition_expr = self._build_partitioned_by_exp(
                        partitioned_by,
                        partition_interval_unit=partition_interval_unit,
                        target_columns_to_types=target_columns_to_types,
                        catalog_name=catalog_name,
                        partitions=partitions,
                        auto_partition=auto_partition,
                        dynamic_partition_enabled=dynamic_partition_enabled,
                        is_iceberg=is_iceberg,
                    )
                    if partition_expr:
                        if col_by_lower:
                            part_col_nodes = [
                                col_node
                                for expr in parsed_part_exprs
                                for col_node in expr.find_all(exp.Column)
                            ]
                            missing_part_cols = [
                                c.name for c in part_col_nodes if c.name.lower() not in col_by_lower
                            ]
                            if missing_part_cols:
                                raise SQLMeshError(
                                    f"Doris partitioned_by columns {missing_part_cols} not found in table schema columns {list(target_columns_to_types or ())}."
                                )
                        properties.append(partition_expr)

        # Pop distribution / bucketing properties so they never leak into PROPERTIES (...)
        distributed_by = table_properties_copy.pop("distributed_by", None)
        top_level_buckets = table_properties_copy.pop("buckets", None)
        prop_clustered_by = table_properties_copy.pop("clustered_by", None)
        effective_clustered_by = clustered_by or (
            [exp.to_column(c) for c in self._extract_column_names(prop_clustered_by)]
            if prop_clustered_by
            else None
        )

        if not is_iceberg:
            strict_key_cols = (
                (unique_key_cols or aggregate_key_cols) if not is_materialized_view else []
            )
            dist_prop = self._build_distributed_by_exp(
                distributed_by=distributed_by,
                clustered_by=effective_clustered_by,
                unique_key_cols=strict_key_cols,
                fallback_key_cols=duplicate_key_cols if not is_materialized_view else [],
                top_level_buckets=top_level_buckets,
            )
            if dist_prop is not None:
                if not is_materialized_view:
                    dist_kind = str(dist_prop.args.get("kind") or "").upper()
                    if strict_key_cols and dist_kind == "RANDOM":
                        key_kind_label = "UNIQUE KEY" if unique_key_cols else "AGGREGATE KEY"
                        raise SQLMeshError(
                            f"Doris {key_kind_label} tables do not support DISTRIBUTED BY RANDOM. "
                            "Use DISTRIBUTED BY HASH (...) instead."
                        )
                    if col_by_lower and dist_prop.expressions:
                        dist_col_names = [c.name for c in dist_prop.expressions]
                        missing_dist_cols = [
                            c for c in dist_col_names if c.lower() not in col_by_lower
                        ]
                        if missing_dist_cols:
                            raise SQLMeshError(
                                f"Doris distributed_by columns {missing_dist_cols} not found in table schema columns {list(target_columns_to_types or ())}."
                            )
                        dist_prop.set(
                            "expressions",
                            [exp.to_column(col_by_lower[c.lower()]) for c in dist_col_names],
                        )
                    if strict_key_cols and dist_prop.expressions:
                        strict_key_set = {k.lower() for k in strict_key_cols}
                        non_key_dist_cols = [
                            c.name
                            for c in dist_prop.expressions
                            if c.name.lower() not in strict_key_set
                        ]
                        if non_key_dist_cols:
                            key_kind_label = "UNIQUE KEY" if unique_key_cols else "AGGREGATE KEY"
                            raise SQLMeshError(
                                f"Doris {key_kind_label} tables require distributed_by columns to be key columns, "
                                f"got {non_key_dist_cols} which are not in {strict_key_cols}."
                            )
                properties.append(dist_prop)

        # Default replication_num = 1 for single-BE dev/CI when not explicitly configured on OLAP tables
        # Note: Only emit explicit table_properties_copy entries so unit test expectations remain clean.
        if table_properties_copy:
            properties.extend(self._properties_to_expressions(table_properties_copy))

        if properties:
            return exp.Properties(expressions=properties)
        return None

    def _to_upper_str(self, val: t.Any) -> str:
        extracted = self._extract_str_value(val)
        if extracted is not None:
            return extracted.strip().strip("`").upper()
        return str(val).strip().strip("`").upper()

    def _parse_refresh_tuple(
        self,
        tpl: exp.Tuple,
    ) -> t.Tuple[
        exp.Var,
        t.Optional[str],
        t.Optional[exp.Expr],
        t.Optional[exp.Var],
        t.Optional[exp.Literal],
    ]:
        method_var: exp.Var = exp.Var(this="AUTO")
        kind_str: t.Optional[str] = None
        every_lit: t.Optional[exp.Expr] = None
        unit_var: t.Optional[exp.Var] = None
        starts_lit: t.Optional[exp.Literal] = None
        for e in tpl.expressions:
            if isinstance(e, exp.EQ):
                key = (
                    (self._extract_str_value(e.this) or "").strip('"').strip("'").strip("`").lower()
                )
                val = e.expression
                if key == "method":
                    method_upper = self._to_upper_str(val)
                    if method_upper not in ("COMPLETE", "AUTO", "FAST", "FORCE", "NEVER"):
                        raise SQLMeshError(
                            f"Invalid materialized view refresh method: {method_upper}. Expected one of: COMPLETE, AUTO, FAST, FORCE, NEVER."
                        )
                    method_var = exp.Var(this=method_upper)
                elif key == "kind":
                    kind_upper = self._to_upper_str(val)
                    if kind_upper not in ("MANUAL", "COMMIT", "SCHEDULE"):
                        raise SQLMeshError(
                            f"Invalid materialized view refresh kind: {kind_upper}. Expected one of: MANUAL, COMMIT, SCHEDULE."
                        )
                    kind_str = kind_upper
                elif key == "every":
                    if isinstance(val, exp.Literal):
                        val_str = str(val.this).strip()
                        if not val_str.isdigit():
                            raise SQLMeshError(
                                f"Invalid materialized view refresh every interval: {val_str}. Expected a positive integer."
                            )
                        every_lit = exp.Literal.number(int(val_str))
                    elif isinstance(val, (int, float)):
                        every_lit = exp.Literal.number(int(val))
                elif key == "unit":
                    unit_upper = self._to_upper_str(val)
                    if not re.match(r"^[A-Z]+$", unit_upper):
                        raise SQLMeshError(f"Invalid materialized view refresh unit: {unit_upper}")
                    unit_var = exp.Var(this=unit_upper)
                elif key == "starts":
                    starts_str = self._extract_str_value(val)
                    starts_lit = exp.Literal.string(
                        starts_str if starts_str is not None else str(val)
                    )
        return method_var, kind_str, every_lit, unit_var, starts_lit

    @staticmethod
    def _parse_trigger_string(
        trigger_text: str,
    ) -> t.Tuple[
        t.Optional[str],
        t.Optional[exp.Expr],
        t.Optional[exp.Var],
        t.Optional[exp.Literal],
    ]:
        text = trigger_text.strip()
        if text.upper().startswith("ON "):
            text = text[3:].strip()
        kind = None
        every_lit: t.Optional[exp.Expr] = None
        unit_var = None
        starts_lit = None

        m_kind = re.match(r"^(MANUAL|COMMIT|SCHEDULE)\b", text, flags=re.IGNORECASE)
        if not m_kind:
            raise SQLMeshError(
                f"Invalid materialized view refresh trigger: {trigger_text}. Expected MANUAL, COMMIT, or SCHEDULE."
            )
        kind = m_kind.group(1).upper()
        m_every = re.search(r"\bEVERY\s+(\d+)\s+([A-Za-z]+)", text, flags=re.IGNORECASE)
        if m_every:
            every_lit = exp.Literal.number(int(m_every.group(1)))
            unit_var = exp.Var(this=m_every.group(2).upper())
        m_starts = re.search(r"\bSTARTS\s+'([^']+)'", text, flags=re.IGNORECASE)
        if m_starts:
            starts_lit = exp.Literal.string(m_starts.group(1))
        return kind, every_lit, unit_var, starts_lit

    def _resolve_buckets(self, raw_buckets: t.Any) -> t.Optional[exp.Expr]:
        if raw_buckets is None:
            return None
        if isinstance(raw_buckets, exp.Literal):
            val_str = str(raw_buckets.this).strip()
            if val_str.upper() == "AUTO":
                return exp.Var(this="AUTO")
            if val_str.isdigit():
                return exp.Literal.number(int(val_str))
        if isinstance(raw_buckets, int):
            return exp.Literal.number(raw_buckets)
        val_str = str(self._extract_str_value(raw_buckets) or "").strip()
        if val_str.upper() == "AUTO":
            return exp.Var(this="AUTO")
        if val_str.isdigit():
            return exp.Literal.number(int(val_str))
        raise SQLMeshError(
            f"Invalid buckets value for Doris DISTRIBUTED BY: {raw_buckets}. Expected a positive integer or 'AUTO'."
        )

    def _to_sql(self, expression: exp.Expr, quote: bool = True, **kwargs: t.Any) -> str:
        if expression.find(exp.DataType):
            expression = expression.copy()
            for dtype in reversed(list(expression.find_all(exp.DataType))):
                if dtype.is_type("int128"):
                    dtype.replace(exp.DataType.build("LARGEINT", udt=True))
                elif dtype.is_type("struct"):
                    new_fields: t.List[exp.Expr] = []
                    for field in dtype.expressions:
                        if isinstance(field, exp.ColumnDef) and field.args.get("kind"):
                            field_id = field.this.copy()
                            if quote and isinstance(field_id, exp.Identifier):
                                field_id.set("quoted", True)
                            field_name_sql = super()._to_sql(field_id, quote=False, **kwargs)
                            field_kind_sql = self._to_sql(field.args["kind"], quote=quote, **kwargs)
                            new_fields.append(exp.var(f"{field_name_sql}: {field_kind_sql}"))
                        else:
                            new_fields.append(field)
                    dtype.set("expressions", new_fields)
        return super()._to_sql(expression, quote=quote, **kwargs)

    def _build_distributed_by_exp(
        self,
        distributed_by: t.Any = None,
        clustered_by: t.Optional[t.List[exp.Expr]] = None,
        unique_key_cols: t.Optional[t.List[str]] = None,
        fallback_key_cols: t.Optional[t.List[str]] = None,
        top_level_buckets: t.Any = None,
    ) -> t.Optional[exp.DistributedByProperty]:
        default_buckets_expr = self._resolve_buckets(top_level_buckets)

        if distributed_by is not None:
            distributed_info: t.Dict[str, t.Any] = {}

            # Case 1: Function expression such as HASH(a, b) or RANDOM()
            if isinstance(distributed_by, (exp.Anonymous, exp.Func)) and not isinstance(
                distributed_by, (exp.Tuple, exp.Array, exp.Paren)
            ):
                kind_upper = (
                    "RANDOM"
                    if isinstance(distributed_by, exp.Rand)
                    else distributed_by.name.upper()
                )
                if kind_upper not in ("HASH", "RANDOM"):
                    raise SQLMeshError(
                        f"Invalid distributed_by kind: {kind_upper}. Expected HASH or RANDOM."
                    )
                cols = [
                    exp.to_column(c) for c in self._extract_column_names(distributed_by.expressions)
                ]
                return exp.DistributedByProperty(
                    kind=exp.Var(this=kind_upper),
                    expressions=cols if cols else None,
                    buckets=default_buckets_expr or (exp.Literal.number(10) if cols else None),
                    order=None,
                )

            # Case 2: String shorthand or bare column/identifier such as "HASH(id) BUCKETS 10", "RANDOM BUCKETS AUTO", or id
            if isinstance(distributed_by, (str, exp.Column, exp.Identifier, exp.Var)) or (
                isinstance(distributed_by, exp.Literal)
                and getattr(distributed_by, "is_string", False)
            ):
                text = (
                    distributed_by.strip()
                    if isinstance(distributed_by, str)
                    else (
                        str(distributed_by.this).strip()
                        if isinstance(distributed_by, exp.Literal)
                        else distributed_by.name.strip()
                    )
                )
                m = re.match(
                    r"^(HASH|RANDOM)\s*(?:\(([^)]*)\))?\s*(?:BUCKETS\s+(AUTO|\d+))?$",
                    text,
                    flags=re.IGNORECASE,
                )
                if m:
                    kind_upper = m.group(1).upper()
                    cols_str = m.group(2)
                    b_str = m.group(3)
                    parsed_cols = (
                        [
                            exp.to_column(c.strip().strip("`"))
                            for c in cols_str.split(",")
                            if c.strip()
                        ]
                        if cols_str
                        else None
                    )
                    b_expr = self._resolve_buckets(b_str) if b_str else default_buckets_expr
                    return exp.DistributedByProperty(
                        kind=exp.Var(this=kind_upper),
                        expressions=parsed_cols,
                        buckets=b_expr,
                        order=None,
                    )
                cols = [exp.to_column(c) for c in self._extract_column_names(text)]
                if cols:
                    return exp.DistributedByProperty(
                        kind=exp.Var(this="HASH"),
                        expressions=cols,
                        buckets=default_buckets_expr or exp.Literal.number(10),
                        order=None,
                    )

            # Case 3: Tuple or Array (either key=value pairs or list of column names from dbt-doris)
            if isinstance(distributed_by, (exp.Tuple, exp.Array)):
                has_eq = any(isinstance(e, exp.EQ) for e in distributed_by.expressions)
                if has_eq:
                    for expr in distributed_by.expressions:
                        if isinstance(expr, exp.EQ):
                            key = (
                                (self._extract_str_value(expr.this) or "")
                                .strip('"')
                                .strip("'")
                                .strip("`")
                                .lower()
                            )
                            if isinstance(expr.expression, exp.Literal):
                                distributed_info[key] = expr.expression.this
                            elif isinstance(expr.expression, (exp.Array, exp.Tuple)):
                                distributed_info[key] = self._extract_column_names(expr.expression)
                            else:
                                distributed_info[key] = self._extract_str_value(expr.expression)
                else:
                    cols = [exp.to_column(c) for c in self._extract_column_names(distributed_by)]
                    if cols:
                        return exp.DistributedByProperty(
                            kind=exp.Var(this="HASH"),
                            expressions=cols,
                            buckets=default_buckets_expr or exp.Literal.number(10),
                            order=None,
                        )
            elif isinstance(distributed_by, exp.EQ) or (
                isinstance(distributed_by, exp.Paren) and isinstance(distributed_by.this, exp.EQ)
            ):
                expr = (
                    distributed_by.this if isinstance(distributed_by, exp.Paren) else distributed_by
                )
                key = (
                    (self._extract_str_value(expr.this) or "")
                    .strip('"')
                    .strip("'")
                    .strip("`")
                    .lower()
                )
                if isinstance(expr.expression, exp.Literal):
                    distributed_info[key] = expr.expression.this
                else:
                    distributed_info[key] = self._extract_str_value(expr.expression)
            elif isinstance(distributed_by, dict):
                distributed_info = distributed_by

            if distributed_info:
                raw_kind = distributed_info.get("kind")
                expressions = distributed_info.get("expressions")
                buckets = distributed_info.get("buckets", top_level_buckets)

                if raw_kind:
                    kind_upper = str(raw_kind).strip().upper()
                    if kind_upper not in ("HASH", "RANDOM"):
                        raise SQLMeshError(
                            f"Invalid distributed_by kind: {kind_upper}. Expected HASH or RANDOM."
                        )
                    buckets_expr = self._resolve_buckets(buckets)

                    expressions_list = None
                    if expressions:
                        col_names = self._extract_column_names(expressions)
                        expressions_list = [exp.to_column(c) for c in col_names]

                    return exp.DistributedByProperty(
                        kind=exp.Var(this=kind_upper),
                        expressions=expressions_list,
                        buckets=buckets_expr,
                        order=None,
                    )

        # Fallback 1: Top-level clustered_by (or physical_properties clustered_by)
        if clustered_by:
            col_names = [c for expr in clustered_by for c in self._extract_column_names(expr)]
            if unique_key_cols:
                strict_key_set = {k.lower() for k in unique_key_cols}
                not_in_key = [c for c in col_names if c.lower() not in strict_key_set]
                if not_in_key:
                    logger.warning(
                        f"[Doris] clustered_by columns {not_in_key} not in key columns {unique_key_cols}, falling back to key column distribution."
                    )
                    col_names = []
            cols = [exp.to_column(c) for c in col_names]
            if cols:
                return exp.DistributedByProperty(
                    kind=exp.Var(this="HASH"),
                    expressions=cols,
                    buckets=default_buckets_expr or exp.Literal.number(10),
                    order=None,
                )

        # Fallback 2: Default DISTRIBUTED BY HASH (<first_unique_key>) BUCKETS <buckets|10>
        if unique_key_cols:
            first_col = unique_key_cols[0]
            logger.info(
                f"[Doris] Adding default distributed_by using unique_key column: {first_col}"
            )
            return exp.DistributedByProperty(
                expressions=[exp.to_column(first_col)],
                kind="HASH",
                buckets=default_buckets_expr or exp.Literal.number(10),
            )

        # Fallback 3: Standalone buckets with duplicate_key/aggregate_key or RANDOM
        if default_buckets_expr is not None:
            if fallback_key_cols:
                return exp.DistributedByProperty(
                    kind=exp.Var(this="HASH"),
                    expressions=[exp.to_column(fallback_key_cols[0])],
                    buckets=default_buckets_expr,
                    order=None,
                )
            return exp.DistributedByProperty(
                kind=exp.Var(this="RANDOM"),
                expressions=None,
                buckets=default_buckets_expr,
                order=None,
            )

        return None

    def _get_temp_table(
        self, table: TableName, table_only: bool = False, quoted: bool = True
    ) -> exp.Table:
        """Returns the temp table expression, truncated to fit Doris's 64-char identifier limit."""
        table_copy = t.cast(exp.Table, exp.to_table(table).copy())
        table_copy.set(
            "this",
            exp.to_identifier(
                f"__temp_{table_copy.name[:48]}_{random_id(short=True)}", quoted=quoted
            ),
        )

        if table_only:
            table_copy.set("db", None)
            table_copy.set("catalog", None)

        return table_copy

    def _properties_to_expressions(self, properties: t.Dict[str, t.Any]) -> t.List[exp.Expr]:
        """Convert a dictionary of properties to a list of Doris PROPERTIES ('k'='v') expressions."""
        expressions: t.List[exp.Expr] = []
        skip_keys = {
            "build",
            "refresh",
            "refresh_trigger",
            "distributed_by",
            "clustered_by",
            "buckets",
            "partitioned_by",
            "partitions",
            "auto_partition",
            "unique_key",
            "duplicate_key",
            "aggregate_key",
            "order_by",
            "engine",
            "properties",
            "materialized_type",
            "source_table",
        }
        for key, value in properties.items():
            if key in skip_keys or value is None:
                continue
            raw_str = self._extract_str_value(value)
            str_literal = exp.Literal.string(raw_str if raw_str is not None else str(value))
            expressions.append(exp.Property(this=exp.Literal.string(str(key)), value=str_literal))
        return expressions
