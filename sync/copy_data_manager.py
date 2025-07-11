# sync/copy_data_manager.py
"""
高性能数据传输管理器 - 使用PostgreSQL COPY命令
提供比传统INSERT方式快10-40倍的数据传输性能
"""
import io
import csv
import json
import logging
from typing import List, Dict, Any, Optional
from datetime import datetime

from sqlalchemy import Engine, text
from sqlalchemy.orm import Session
import psycopg2

from models.backup_jobs import BackupJob, SyncMode, IncrementalStrategy

logger = logging.getLogger(__name__)

class CopyDataManager:
    """使用PostgreSQL COPY命令的高性能数据传输管理器"""
    
    def __init__(self, job: BackupJob, source_engine: Engine, destination_engine: Engine, db_session: Session):
        self.job = job
        self.source_engine = source_engine
        self.destination_engine = destination_engine
        self.db_session = db_session
        
        # 性能配置
        self.batch_size = 50000  # COPY模式使用更大的批量大小
        self.progress_update_interval = 10  # 每10个批次更新一次进度
        self.use_binary_copy = False  # 是否使用二进制COPY（更快但兼容性稍差）
        
    def sync_table_data(self, target_table, progress_tracker: dict, cancellation_check: callable) -> int:
        """使用COPY命令同步单个表的数据"""
        schema = target_table.schema_name
        table = target_table.table_name
        full_table_name = f"{schema}.{table}"
        
        logger.info(f"开始使用COPY模式同步表 {full_table_name}")
        
        # 全量同步时清空目标表
        if self.job.sync_mode == SyncMode.FULL or target_table.incremental_strategy == IncrementalStrategy.NONE:
            self._truncate_target_table(schema, table)
        
        # 构建查询SQL
        query_sql = self._build_sync_query(target_table)
        
        # 获取总记录数
        total_records = self._get_record_count(query_sql)
        progress_tracker['current_table_total_records'] = total_records
        progress_tracker['current_table_processed_records'] = 0
        
        if total_records == 0:
            logger.info(f"表 {full_table_name} 没有需要同步的数据")
            return 0
        
        # 获取表结构信息
        columns = self._get_table_columns(schema, table)
        
        # 执行COPY传输
        records_synced = self._execute_copy_transfer(
            query_sql, schema, table, columns, 
            progress_tracker, cancellation_check
        )
        
        logger.info(f"表 {full_table_name} COPY同步完成，共传输 {records_synced} 条记录")
        return records_synced
    
    def _execute_copy_transfer(self, query_sql: str, schema: str, table: str, 
                             columns: List[str], progress_tracker: dict, 
                             cancellation_check: callable) -> int:
        """执行COPY数据传输"""
        full_table_name = f"{schema}.{table}"
        total_records = 0
        batch_count = 0
        
        # 获取原始psycopg2连接用于COPY操作
        dest_raw_conn = self.destination_engine.raw_connection()
        
        try:
            with self.source_engine.connect() as source_conn:
                # 使用流式查询读取源数据
                result = source_conn.execution_options(stream_results=True).execute(text(query_sql))
                
                batch_data = []
                max_sync_value = None
                
                for row in result:
                    # 检查是否被取消
                    if cancellation_check():
                        raise InterruptedError("任务被用户取消")
                    
                    # 转换行数据为字典
                    record = dict(zip(columns, row))
                    batch_data.append(record)
                    
                    # 更新最大同步值（用于增量同步）
                    max_sync_value = self._update_max_sync_value(record, max_sync_value)
                    
                    # 达到批量大小时执行COPY
                    if len(batch_data) >= self.batch_size:
                        self._copy_batch_to_table(dest_raw_conn, full_table_name, columns, batch_data)
                        
                        total_records += len(batch_data)
                        batch_count += 1
                        
                        # 更新进度（降低频率）
                        if batch_count % self.progress_update_interval == 0:
                            progress_tracker['current_table_processed_records'] = total_records
                            
                        batch_data = []
                
                # 处理剩余数据
                if batch_data:
                    self._copy_batch_to_table(dest_raw_conn, full_table_name, columns, batch_data)
                    total_records += len(batch_data)
                
                # 最终进度更新
                progress_tracker['current_table_processed_records'] = total_records
                
        finally:
            dest_raw_conn.close()
        
        return total_records
    
    def _copy_batch_to_table(self, connection, full_table_name: str, 
                           columns: List[str], data: List[Dict[str, Any]]):
        """使用COPY命令批量插入数据"""
        if not data:
            return
        
        # 预处理数据
        processed_data = self._preprocess_data_for_copy(data)
        
        # 创建CSV缓冲区
        buffer = io.StringIO()
        
        if self.use_binary_copy:
            # 二进制模式（更快但实现复杂）
            self._write_binary_copy_data(buffer, processed_data, columns)
        else:
            # 文本模式（兼容性更好）
            self._write_text_copy_data(buffer, processed_data, columns)
        
        buffer.seek(0)
        
        # 执行COPY命令
        with connection.cursor() as cursor:
            try:
                if self.use_binary_copy:
                    cursor.copy_expert(
                        f"COPY {full_table_name} ({','.join(columns)}) FROM STDIN WITH BINARY",
                        buffer
                    )
                else:
                    cursor.copy_expert(
                        f"COPY {full_table_name} ({','.join(columns)}) FROM STDIN WITH CSV DELIMITER E'\\t' NULL '\\N'",
                        buffer
                    )
                connection.commit()
                
            except Exception as e:
                connection.rollback()
                logger.error(f"COPY命令执行失败: {e}")
                # 如果COPY失败，可以回退到传统INSERT方式
                self._fallback_to_insert(full_table_name, columns, data)
                raise
    
    def _write_text_copy_data(self, buffer: io.StringIO, data: List[Dict[str, Any]], columns: List[str]):
        """将数据写入文本格式的COPY缓冲区"""
        writer = csv.writer(buffer, delimiter='\t', quoting=csv.QUOTE_MINIMAL)
        
        for record in data:
            row = []
            for col in columns:
                value = record.get(col)
                if value is None:
                    row.append('\\N')  # PostgreSQL NULL标记
                elif isinstance(value, (dict, list)):
                    # JSON数据需要转义
                    json_str = json.dumps(value, ensure_ascii=False, separators=(',', ':'))
                    row.append(json_str)
                elif isinstance(value, str):
                    # 转义特殊字符
                    escaped_value = value.replace('\\', '\\\\')\
                                        .replace('\t', '\\t')\
                                        .replace('\n', '\\n')\
                                        .replace('\r', '\\r')
                    row.append(escaped_value)
                else:
                    row.append(str(value))
            writer.writerow(row)
    
    def _write_binary_copy_data(self, buffer: io.StringIO, data: List[Dict[str, Any]], columns: List[str]):
        """将数据写入二进制格式的COPY缓冲区（高级功能）"""
        # 二进制COPY实现较复杂，这里提供基础框架
        # 实际使用时可能需要根据具体数据类型进行优化
        logger.warning("二进制COPY模式尚未完全实现，回退到文本模式")
        self._write_text_copy_data(buffer, data, columns)
    
    def _preprocess_data_for_copy(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """为COPY命令预处理数据（简化版本）"""
        processed_data = []
        
        for record in data:
            processed_record = {}
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    # 保持JSON格式，COPY时会进行转义
                    processed_record[key] = value
                elif isinstance(value, datetime):
                    # 时间戳格式化
                    processed_record[key] = value.isoformat()
                else:
                    processed_record[key] = value
            processed_data.append(processed_record)
        
        return processed_data
    
    def _fallback_to_insert(self, full_table_name: str, columns: List[str], data: List[Dict[str, Any]]):
        """COPY失败时的INSERT回退方案"""
        logger.warning(f"COPY失败，回退到INSERT模式处理 {len(data)} 条记录")
        
        columns_str = ", ".join(columns)
        placeholders = ", ".join([f":{col}" for col in columns])
        insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
        
        with self.destination_engine.connect() as conn:
            with conn.begin():
                conn.execute(text(insert_sql), data)
    
    def _get_table_columns(self, schema: str, table: str) -> List[str]:
        """获取表的列名列表"""
        query = """
            SELECT column_name 
            FROM information_schema.columns 
            WHERE table_schema = :schema_name 
              AND table_name = :table_name 
            ORDER BY ordinal_position
        """
        
        with self.source_engine.connect() as conn:
            result = conn.execute(text(query), {'schema_name': schema, 'table_name': table})
            return [row[0] for row in result]
    
    def _get_record_count(self, query_sql: str) -> int:
        """获取查询结果的记录总数"""
        count_query = f"SELECT COUNT(*) FROM ({query_sql}) AS count_subquery"
        
        with self.source_engine.connect() as conn:
            result = conn.execute(text(count_query))
            return result.scalar() or 0
    
    def _build_sync_query(self, target_table) -> str:
        """构建同步查询SQL（简化版本）"""
        base_query = f"SELECT * FROM {target_table.schema_name}.{target_table.table_name}"
        conditions = []
        
        # 增量同步条件
        if (self.job.sync_mode == SyncMode.INCREMENTAL and 
            target_table.incremental_strategy != IncrementalStrategy.NONE):
            
            field = target_table.incremental_field
            last_value = target_table.last_sync_value
            
            if field and last_value:
                try:
                    # 尝试作为数值处理
                    float(last_value)
                    conditions.append(f"{field} > {last_value}")
                except ValueError:
                    # 作为字符串处理
                    conditions.append(f"{field} > '{last_value}'")
        
        # 全局WHERE条件
        if self.job.where_condition:
            conditions.append(f"({self.job.where_condition})")
        
        if conditions:
            return f"{base_query} WHERE {' AND '.join(conditions)}"
        return base_query
    
    def _truncate_target_table(self, schema: str, table: str):
        """清空目标表"""
        full_table_name = f"{schema}.{table}"
        logger.info(f"清空目标表: {full_table_name}")
        
        try:
            with self.destination_engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE"))
        except Exception as e:
            logger.warning(f"TRUNCATE失败，尝试DELETE: {e}")
            with self.destination_engine.connect() as conn:
                with conn.begin():
                    conn.execute(text(f"DELETE FROM {full_table_name}"))
    
    def _update_max_sync_value(self, record: Dict[str, Any], current_max: Any) -> Any:
        """更新最大同步值（用于增量同步）"""
        # 这里可以根据具体的增量字段来实现
        # 简化实现，实际使用时需要根据target_table.incremental_field来处理
        return current_max


# 策略选择器
class DataTransferStrategy:
    """数据传输策略选择器"""
    
    COPY_MODE = "copy"      # 高性能COPY模式
    STREAM_MODE = "stream"  # 传统流式模式
    
    @staticmethod
    def create_manager(strategy: str, job: BackupJob, source_engine: Engine, 
                      destination_engine: Engine, db_session: Session):
        """根据策略创建对应的数据管理器"""
        
        if strategy == DataTransferStrategy.COPY_MODE:
            logger.info("使用高性能COPY传输模式")
            return CopyDataManager(job, source_engine, destination_engine, db_session)
        
        elif strategy == DataTransferStrategy.STREAM_MODE:
            logger.info("使用传统流式传输模式")
            # 这里可以导入并返回原有的DataManager
            from .data_manager import DataManager
            return DataManager(job, source_engine, destination_engine, db_session)
        
        else:
            raise ValueError(f"不支持的传输策略: {strategy}")


# 使用示例：
# 
# # 在你的同步代码中，可以这样切换策略：
# 
# # 选择传输策略
# TRANSFER_STRATEGY = DataTransferStrategy.COPY_MODE  # 或 STREAM_MODE
# 
# # 创建对应的数据管理器
# data_manager = DataTransferStrategy.create_manager(
#     TRANSFER_STRATEGY, job, source_engine, dest_engine, db_session
# )
# 
# # 使用统一的接口进行数据同步
# records_synced = data_manager.sync_table_data(target_table, progress_tracker, cancellation_check)