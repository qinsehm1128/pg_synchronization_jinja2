# engine/schema_manager.py
"""
负责数据库模式 (DDL) 同步的管理器。
"""
import logging
import re
from sqlalchemy import (Engine, Table, MetaData, Column, BigInteger,
                        PrimaryKeyConstraint, text)

logger = logging.getLogger(__name__)


class SchemaManager:
    """负责数据库模式同步的管理器"""

    def __init__(self, source_engine: Engine, destination_engine: Engine):
        self.source_engine = source_engine
        self.destination_engine = destination_engine

    def sync_table_structure(self, schema_name: str, table_name: str):
        """同步单个表的结构，如果目标表不存在则创建。"""
        logger.info(f"Starting table structure sync for {schema_name}.{table_name}")

        dest_metadata = MetaData()
        try:
            Table(table_name, dest_metadata, autoload_with=self.destination_engine, schema=schema_name)
            logger.info(f"Table {schema_name}.{table_name} already exists in destination.")
            return
        except Exception:
            logger.info(f"Destination table {schema_name}.{table_name} does not exist. Creating it.")

        source_metadata = MetaData()
        try:
            source_table = Table(table_name, source_metadata, autoload_with=self.source_engine, schema=schema_name)
        except Exception as e:
            raise Exception(f"Failed to load source table {schema_name}.{table_name}: {e}")

        self._create_table_safely(source_table, schema_name, table_name)

    def _create_table_safely(self, source_table: Table, schema_name: str, table_name: str):
        """安全地在目标库创建表、序列和索引，使用单个事务。"""
        with self.destination_engine.connect() as conn:
            with conn.begin():
                dest_metadata = MetaData()
                dest_table = Table(table_name, dest_metadata, schema=schema_name)

                failed_sequences = self._create_sequences_for_table(conn, source_table, schema_name)
                self._copy_columns(source_table, dest_table, failed_sequences)
                self._copy_primary_key(source_table, dest_table)

                dest_metadata.create_all(conn, checkfirst=False)
                logger.info(f"Table {schema_name}.{table_name} created successfully.")

                self._create_indexes_for_table(conn, source_table)

    def _create_sequences_for_table(self, conn, source_table: Table, schema_name: str) -> set:
        """为表创建所有需要的序列，返回创建失败的列名集合。"""
        failed_columns = set()
        for column in source_table.columns:
            if column.default and 'nextval' in str(column.default).lower():
                try:
                    full_seq_name, _ = self._extract_sequence_name(str(column.default), schema_name, source_table.name,
                                                                   column.name)
                    if '.' in full_seq_name:
                        seq_schema, seq_name = full_seq_name.split('.', 1)
                    else:
                        seq_schema, seq_name = schema_name, full_seq_name

                    check_sql = text(
                        "SELECT EXISTS (SELECT 1 FROM information_schema.sequences WHERE sequence_schema=:s AND sequence_name=:n)")
                    if not conn.execute(check_sql, {'s': seq_schema, 'n': seq_name}).scalar():
                        create_sql = text(f"CREATE SEQUENCE {seq_schema}.{seq_name}")
                        conn.execute(create_sql)
                        logger.info(f"Sequence {seq_schema}.{seq_name} created.")
                except Exception as e:
                    logger.error(f"Failed to create sequence for column {column.name}: {e}")
                    failed_columns.add(column.name)
        return failed_columns

    def _copy_columns(self, source_table: Table, dest_table: Table, failed_sequences: set):
        """复制列定义，特殊处理序列失败的列。"""
        for column in source_table.columns:
            if column.name in failed_sequences:
                logger.warning(f"Column {column.name} sequence creation failed, converting to BIGSERIAL.")
                new_column = Column(column.name, BigInteger, primary_key=column.primary_key, nullable=column.nullable,
                                    autoincrement=True)
            else:
                new_column = column.copy()
                if new_column.default and 'nextval' in str(new_column.default).lower():
                    full_seq_name, _ = self._extract_sequence_name(str(new_column.default), dest_table.schema,
                                                                   dest_table.name, column.name)
                    new_column.default = text(f"nextval('{full_seq_name}'::regclass)")

            new_column.index = False
            dest_table.append_column(new_column)

    def _copy_primary_key(self, source_table: Table, dest_table: Table):
        """复制主键约束。"""
        if source_table.primary_key:
            pk_cols = [dest_table.c[col.name] for col in source_table.primary_key.columns]
            pk_constraint = PrimaryKeyConstraint(*pk_cols, name=f"{dest_table.name}_pkey")
            dest_table.append_constraint(pk_constraint)

    def _create_indexes_for_table(self, conn, source_table: Table):
        """为表创建所有索引。"""
        for index in source_table.indexes:
            try:
                index_name = f"{source_table.name}_{index.name}" if not index.name.startswith(
                    source_table.name) else index.name
                columns_str = ', '.join([col.name for col in index.columns])
                is_gin = any(t in str(col.type).lower() for col in index.columns for t in ['json', 'array', 'tsvector'])
                index_type = "GIN" if is_gin else "BTREE"
                sql = text(
                    f"CREATE INDEX IF NOT EXISTS {index_name} ON {source_table.schema}.{source_table.name} USING {index_type} ({columns_str})")
                conn.execute(sql)
                logger.info(f"Index {index_name} ({index_type}) created.")
            except Exception as e:
                logger.warning(f"Could not create index {index.name}: {e}")

    def _extract_sequence_name(self, default_str: str, schema: str, table: str, column: str) -> tuple:
        """从默认值字符串中提取序列名。"""
        match = re.search(r"nextval\('([^']*)'", default_str)
        if match:
            seq_name = match.group(1).replace('"', '')
            if '.' in seq_name:
                return seq_name, seq_name.split('.')[-1]
            return f"{schema}.{seq_name}", seq_name
        standard_name = f"{table}_{column}_seq"
        return f"{schema}.{standard_name}", standard_name