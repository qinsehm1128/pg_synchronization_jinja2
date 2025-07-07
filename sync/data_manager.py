# engine/data_manager.py
"""
负责数据 (DML) 同步的管理器。
"""
import json
import logging
from typing import List, Dict, Any, Optional

from sqlalchemy import Engine, text, MetaData, Table
from sqlalchemy.orm import Session

# 注意这里的导入路径，假设您的模型在 'models' 包下
from models.backup_jobs import BackupJob, SyncMode, IncrementalStrategy

logger = logging.getLogger(__name__)


class DataManager:
    """负责数据同步的管理器"""

    def __init__(self, job: BackupJob, source_engine: Engine, destination_engine: Engine, db_session: Session):
        self.job = job
        self.source_engine = source_engine
        self.destination_engine = destination_engine
        self.db_session = db_session

    def sync_table_data(self, target_table, progress_tracker: dict, cancellation_check: callable) -> int:
        """同步单个表的数据。"""
        schema = target_table.schema_name
        table = target_table.table_name
        full_table_name = f"{schema}.{table}"

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
                if cancellation_check():
                    raise InterruptedError("Task cancelled by user during data transfer.")

                record = dict(zip(columns, row))
                batch_data.append(record)

                max_sync_value = self._update_max_value(record, target_table.incremental_field, max_sync_value)

                if len(batch_data) >= 1000:
                    self._batch_insert(schema, table, batch_data)
                    records_synced += len(batch_data)
                    progress_tracker['current_table_processed_records'] = records_synced
                    batch_data = []

            if batch_data:
                self._batch_insert(schema, table, batch_data)
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

    def _batch_insert(self, schema: str, table: str, data: List[Dict]):
        """将数据批量插入目标表。"""
        if not data:
            return

        processed_data = self._preprocess_data(data)

        with self.destination_engine.connect() as conn:
            with conn.begin():
                dest_table = Table(table, MetaData(), autoload_with=conn, schema=schema)
                conn.execute(dest_table.insert(), processed_data)

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