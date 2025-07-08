# engine/data_manager.py
"""
负责数据 (DML) 同步的管理器。
"""
import json
import logging
from typing import List, Dict, Any, Optional

from sqlalchemy import Engine, text, MetaData, Table
from sqlalchemy.orm import Session
# 导入 PostgreSQL 特定的 insert 方法
from sqlalchemy.dialects.postgresql import insert as pg_insert

from models.backup_jobs import BackupJob, SyncMode, ConflictStrategy, IncrementalStrategy

logger = logging.getLogger(__name__)

class DataManager:
    """负责数据同步的管理器"""
    def __init__(self, job: BackupJob, source_engine: Engine, destination_engine: Engine, db_session: Session):
        self.job = job
        self.source_engine = source_engine
        self.destination_engine = destination_engine
        self.db_session = db_session

    # sync_table_data 方法保持不变，这里省略以保持简洁...
    def sync_table_data(self, target_table, progress_tracker: dict, cancellation_check: callable) -> int:
        """同步单个表的数据。"""
        schema = target_table.schema_name
        table = target_table.table_name
        full_table_name = f"{schema}.{table}"
        batch_size = 1000

        if self.job.sync_mode == SyncMode.FULL or target_table.incremental_strategy == IncrementalStrategy.NONE:
            self._truncate_target_table(schema, table)

        query_sql = self._build_sync_query(target_table)

        count_query = f"SELECT COUNT(*) FROM ({query_sql}) AS sub"
        with self.source_engine.connect() as conn:
            total_records = conn.execute(text(count_query)).scalar_one_or_none() or 0

        progress_tracker['current_table_total_records'] = total_records
        progress_tracker['current_table_processed_records'] = 0

        records_synced = 0
        with self.source_engine.connect() as s_conn:
            result_stream = s_conn.execution_options(stream_results=True).execute(text(query_sql))
            columns = list(result_stream.keys())
            batch_data = []
            max_sync_value = None

            for row in result_stream:
                record = dict(zip(columns, row))
                batch_data.append(record)

                max_sync_value = self._update_max_value(record, target_table.incremental_field, max_sync_value)

                if len(batch_data) >= batch_size:
                    if cancellation_check():
                        raise InterruptedError("Task cancelled by user during batch processing.")

                    self._batch_insert(schema, table, columns, batch_data)
                    records_synced += len(batch_data)
                    progress_tracker['current_table_processed_records'] = records_synced
                    batch_data = []

            if batch_data:
                if cancellation_check():
                    raise InterruptedError("Task cancelled by user during final batch processing.")

                self._batch_insert(schema, table, columns, batch_data)
                records_synced += len(batch_data)
                progress_tracker['current_table_processed_records'] = records_synced

        if max_sync_value is not None:
            target_table.last_sync_value = str(max_sync_value)
            self.db_session.commit()
            logger.info(f"Updated last_sync_value for {full_table_name} to {max_sync_value}")

        return records_synced

    def _truncate_target_table(self, schema: str, table: str):
        """清空目标表。"""
        full_table_name = f"{schema}.{table}"
        logger.info(f"Truncating target table: {full_table_name}")
        try:
            with self.destination_engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE"))
        except Exception as e:
            logger.warning(f"TRUNCATE failed for {full_table_name}, trying DELETE. Error: {e}")
            try:
                with self.destination_engine.connect() as conn:
                    with conn.begin():
                        conn.execute(text(f"DELETE FROM {full_table_name}"))
            except Exception as del_e:
                logger.error(f"Could not clear target table {full_table_name}: {del_e}")
                raise

    def _batch_insert(self, schema: str, table: str, columns: List[str], data: List[Dict]):
        """
        将数据批量插入目标表，并根据作业配置处理冲突。
        """
        if not data:
            return

        processed_data = self._preprocess_data(data)

        with self.destination_engine.connect() as conn:
            with conn.begin():
                dest_table = Table(table, MetaData(), autoload_with=conn, schema=schema)

                # **核心改动：根据冲突策略构建不同的插入语句**

                # 对于 SKIP 策略，需要逐条插入，这是低效的，但符合策略要求
                if self.job.conflict_strategy == ConflictStrategy.SKIP:
                    self._insert_with_skip(conn, dest_table, processed_data)
                    return

                # 构建基础的插入语句
                insert_stmt = pg_insert(dest_table).values(processed_data)

                # 策略：IGNORE (忽略冲突)
                if self.job.conflict_strategy == ConflictStrategy.IGNORE:
                    # 使用 on_conflict_do_nothing
                    final_stmt = insert_stmt.on_conflict_do_nothing()
                    conn.execute(final_stmt)

                # 策略：REPLACE (替换冲突)
                elif self.job.conflict_strategy == ConflictStrategy.REPLACE:
                    primary_keys = self._get_primary_keys(conn, schema, table)
                    if not primary_keys:
                        logger.warning(f"REPLACE strategy requires a primary key on table {schema}.{table}. Falling back to IGNORE.")
                        final_stmt = insert_stmt.on_conflict_do_nothing()
                        conn.execute(final_stmt)
                        return

                    # 构建 on_conflict_do_update 语句
                    # 从待插入的值中，更新所有非主键的列
                    update_dict = {
                        c.name: c
                        for c in insert_stmt.excluded
                        if c.name not in primary_keys
                    }
                    final_stmt = insert_stmt.on_conflict_do_update(
                        index_elements=primary_keys,
                        set_=update_dict,
                    )
                    conn.execute(final_stmt)

                # 策略：ERROR (默认，遇到冲突就报错)
                else: # self.job.conflict_strategy == ConflictStrategy.ERROR
                    conn.execute(dest_table.insert(), processed_data)

    def _insert_with_skip(self, connection, table_obj, data):
        """为 SKIP 策略逐条插入数据，捕获并忽略异常。"""
        logger.warning(f"Using SKIP strategy for table {table_obj.name}, which may be slow due to row-by-row insertion.")
        successful_inserts = 0
        skipped_records = 0

        for record in data:
            try:
                connection.execute(table_obj.insert().values(record))
                successful_inserts += 1
            except Exception as e:
                # 捕获所有可能的数据库错误（如唯一性冲突），并跳过
                skipped_records += 1
                logger.debug(f"Skipped record due to conflict: {e}")
        logger.info(f"Insert with SKIP complete. Inserted: {successful_inserts}, Skipped: {skipped_records}.")

    def _get_primary_keys(self, connection, schema: str, table: str) -> List[str]:
        """获取指定表的主键列名列表。"""
        query = text("""
            SELECT a.attname
            FROM pg_index i
            JOIN   pg_attribute a ON a.attrelid = i.indrelid
                                AND a.attnum = ANY(i.indkey)
            WHERE  i.indrelid = :table_ref::regclass
            AND    i.indisprimary;
        """)
        result = connection.execute(query, {'table_ref': f'{schema}.{table}'})
        return [row[0] for row in result]

    def _preprocess_data(self, data: List[Dict]) -> List[Dict]:
        """预处理数据，将复杂类型序列化为JSON字符串。"""
        return [
            {k: json.dumps(v, default=str) if isinstance(v, (dict, list)) else v for k, v in row.items()}
            for row in data
        ]

    def _build_sync_query(self, target_table) -> str:
        """构建用于同步的SELECT查询。"""
        base_query = f"SELECT * FROM {target_table.schema_name}.{target_table.table_name}"
        conditions = []
        order_by = ""

        if self.job.sync_mode == SyncMode.INCREMENTAL and target_table.incremental_strategy != IncrementalStrategy.NONE:
            field = target_table.incremental_field
            last_value = target_table.last_sync_value
            if field and last_value:
                try:
                    float(last_value)
                    conditions.append(f"{field} > {last_value}")
                except ValueError:
                    conditions.append(f"{field} > '{last_value}'")
                order_by = f" ORDER BY {field}"

        if self.job.where_condition:
            conditions.append(f"({self.job.where_condition})")

        if conditions:
            return f"{base_query} WHERE {' AND '.join(conditions)}{order_by}"
        return f"{base_query}{order_by}"

    def _update_max_value(self, record: dict, field: Optional[str], current_max: Any) -> Any:
        """在处理记录时，追踪增量字段的最大值。"""
        if not field or field not in record or record[field] is None:
            return current_max

        value = record[field]
        if current_max is None or value > current_max:
            return value
        return current_max