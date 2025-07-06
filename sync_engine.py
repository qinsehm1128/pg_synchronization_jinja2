"""
数据库同步引擎
核心同步逻辑实现
"""
import json
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Any
from sqlalchemy import create_engine, text, MetaData, Table
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine
from sqlalchemy.sql import func

from database import SessionLocal
from models.backup_jobs import BackupJob, SyncMode, ConflictStrategy, IncrementalStrategy
from models.job_execution_logs import JobExecutionLog, ExecutionStatus
from models.database_connections import DatabaseConnection
from utils.encryption import decrypt_connection_string

logger = logging.getLogger(__name__)

class SyncEngine:
    """数据库同步引擎"""
    
    def __init__(self, job_id: int):
        """
        初始化同步引擎
        
        Args:
            job_id: 备份任务ID
        """
        self.job_id = job_id
        self.job: BackupJob = None
        self.source_engine: Engine = None
        self.destination_engine: Engine = None
        self.log_entry: JobExecutionLog = None
        self.db_session: Session = None
        
    def execute(self) -> bool:
        """
        执行同步任务
        
        Returns:
            bool: 执行是否成功
        """
        self.db_session = SessionLocal()
        success = False
        
        try:
            # 查询任务对象
            self.job = self.db_session.query(BackupJob).filter(BackupJob.id == self.job_id).first()
            if not self.job:
                raise ValueError(f"Job {self.job_id} not found")
            
            # 创建执行日志记录
            self._create_log_entry()
            
            # 建立数据库连接
            self._establish_connections()
            
            # 执行同步
            self._perform_sync()
            
            # 标记成功
            self._mark_success()
            success = True
            
        except Exception as e:
            logger.error(f"Sync job {self.job.id} failed: {e}")
            self._mark_failure(str(e), traceback.format_exc())
            
        finally:
            self._cleanup()
            
        return success
    
    def _create_log_entry(self):
        """创建执行日志记录"""
        self.log_entry = JobExecutionLog(
            job_id=self.job.id,
            status=ExecutionStatus.RUNNING,
            log_details="任务开始执行..."
        )
        self.db_session.add(self.log_entry)
        self.db_session.flush()  # 使用flush而不commit，让数据库设置start_time
        self.db_session.refresh(self.log_entry)  # 刷新获取数据库设置的时间
        logger.info(f"Created log entry for job {self.job.id}")
    
    def _establish_connections(self):
        """建立源和目标数据库连接"""
        try:
            # 获取源数据库连接信息
            source_db = self.db_session.query(DatabaseConnection).filter(
                DatabaseConnection.id == self.job.source_db_id
            ).first()
            
            if not source_db:
                raise ValueError(f"Source database {self.job.source_db_id} not found")
            
            # 获取目标数据库连接信息
            dest_db = self.db_session.query(DatabaseConnection).filter(
                DatabaseConnection.id == self.job.destination_db_id
            ).first()
            
            if not dest_db:
                raise ValueError(f"Destination database {self.job.destination_db_id} not found")
            
            # 解密连接字符串并建立连接
            source_conn_str = decrypt_connection_string(source_db.connection_string_encrypted)
            dest_conn_str = decrypt_connection_string(dest_db.connection_string_encrypted)
            
            self.source_engine = create_engine(source_conn_str, pool_pre_ping=True)
            self.destination_engine = create_engine(dest_conn_str, pool_pre_ping=True)
            
            # 测试连接
            with self.source_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            with self.destination_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._update_log("数据库连接建立成功")
            logger.info(f"Database connections established for job {self.job.id}")
            
        except Exception as e:
            raise Exception(f"Failed to establish database connections: {e}")
    
    def _perform_sync(self):
        """执行数据同步"""
        tables_processed = 0
        total_records = 0
        
        try:
            # 获取要同步的表列表
            target_tables = self.job.target_tables
            
            if not target_tables:
                raise ValueError("No target tables specified for sync")
            
            self._update_log(f"开始同步 {len(target_tables)} 个表...")
            
            for target_table in target_tables:
                if not target_table.is_active:
                    continue
                
                schema_name = target_table.schema_name
                table_name = target_table.table_name
                
                self._update_log(f"正在同步表: {schema_name}.{table_name} （增量策略: {target_table.incremental_strategy.value if target_table.incremental_strategy else 'none'}）")
                
                # 同步表结构
                self._sync_table_structure(schema_name, table_name)
                
                # 同步数据（传递target_table对象）
                records_count = self._sync_table_data(target_table)
                
                tables_processed += 1
                total_records += records_count
                
                self._update_log(f"表 {schema_name}.{table_name} 同步完成，传输 {records_count} 条记录")
            
            # 更新统计信息
            self.log_entry.tables_processed = tables_processed
            self.log_entry.records_transferred = total_records
            self.db_session.commit()
            
            logger.info(f"Sync completed for job {self.job.id}: {tables_processed} tables, {total_records} records")
            
        except Exception as e:
            raise Exception(f"Sync operation failed: {e}")
    
    def _sync_table_structure(self, schema_name: str, table_name: str):
        """同步表结构"""
        try:
            # 获取源表结构
            source_metadata = MetaData()
            source_table = Table(
                table_name, 
                source_metadata, 
                autoload_with=self.source_engine,
                schema=schema_name
            )
            
            # 检查目标表是否存在
            dest_metadata = MetaData()
            
            try:
                # 尝试加载目标表
                Table(
                    table_name, 
                    dest_metadata, 
                    autoload_with=self.destination_engine,
                    schema=schema_name
                )
                logger.info(f"Table {schema_name}.{table_name} already exists in destination")
                
            except Exception:
                # 目标表不存在，创建它
                logger.info(f"Creating table {schema_name}.{table_name} in destination")
                
                # 在目标数据库中创建表
                dest_metadata = MetaData()
                dest_table = source_table.tometadata(dest_metadata, schema=schema_name)
                dest_metadata.create_all(self.destination_engine)
                
                logger.info(f"Table {schema_name}.{table_name} created successfully")
                
        except Exception as e:
            raise Exception(f"Failed to sync table structure for {schema_name}.{table_name}: {e}")
    
    def _sync_table_data(self, target_table) -> int:
        """
        同步表数据（支持每表独立增量同步策略）
        
        Args:
            target_table: JobTargetTable对象，包含表信息和增量同步配置
        
        Returns:
            int: 传输的记录数量
        """
        try:
            schema_name = target_table.schema_name
            table_name = target_table.table_name
            full_table_name = f"{schema_name}.{table_name}"
            
            # 根据表的增量同步策略构建查询SQL
            query = self._build_sync_query(target_table)
            
            self._update_log(f"开始同步表 {full_table_name}，查询: {query}")
            
            # 如果是全量同步或表的增量策略为NONE，先清空目标表
            if (self.job.sync_mode == SyncMode.FULL or 
                target_table.incremental_strategy == IncrementalStrategy.NONE):
                self._truncate_target_table(schema_name, table_name)
            
            # 使用流式查询读取源数据
            with self.source_engine.connect() as source_conn:
                result = source_conn.execution_options(stream_results=True).execute(text(query))
                
                # 获取列名
                columns = list(result.keys())
                
                # 确定增量字段（用于更新last_sync_value）
                incremental_field = None
                if target_table.incremental_strategy == IncrementalStrategy.AUTO_ID:
                    incremental_field = target_table.incremental_field or self._detect_id_field(schema_name, table_name)
                elif target_table.incremental_strategy == IncrementalStrategy.AUTO_TIMESTAMP:
                    incremental_field = target_table.incremental_field or self._detect_timestamp_field(schema_name, table_name)
                
                # 批量处理数据
                batch_size = 1000
                total_records = 0
                batch_data = []
                all_data = []  # 保存所有数据用于更新last_sync_value
                
                for row in result:
                    record = dict(zip(columns, row))
                    batch_data.append(record)
                    all_data.append(record)
                    
                    if len(batch_data) >= batch_size:
                        # 批量插入数据
                        self._batch_insert(schema_name, table_name, columns, batch_data)
                        total_records += len(batch_data)
                        batch_data = []
                
                # 处理剩余数据
                if batch_data:
                    self._batch_insert(schema_name, table_name, columns, batch_data)
                    total_records += len(batch_data)
                
                # 更新增量同步值（如果是增量模式且有增量字段）
                if (target_table.incremental_strategy in [IncrementalStrategy.AUTO_ID, IncrementalStrategy.AUTO_TIMESTAMP] 
                    and incremental_field and all_data):
                    self._update_last_sync_value(target_table, incremental_field, all_data)
                
                self._update_log(f"表 {full_table_name} 同步完成，共处理 {total_records} 条记录")
                return total_records
                
        except Exception as e:
            raise Exception(f"Failed to sync data for {schema_name}.{table_name}: {e}")
    
    def _preprocess_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """预处理数据，将字典和列表字段序列化为JSON字符串"""
        processed_data = []
        
        for record in data:
            processed_record = {}
            for key, value in record.items():
                if isinstance(value, (dict, list)):
                    # 将字典和列表序列化为JSON字符串
                    try:
                        processed_record[key] = json.dumps(value, ensure_ascii=False, default=str)
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Failed to serialize field {key}: {e}, using string representation")
                        processed_record[key] = str(value)
                else:
                    processed_record[key] = value
            processed_data.append(processed_record)
        
        return processed_data
    
    def _build_sync_query(self, target_table) -> str:
        """
        根据表的增量同步策略构建查询SQL
        
        Args:
            target_table: JobTargetTable对象
            
        Returns:
            str: 查询SQL语句
        """
        schema_name = target_table.schema_name
        table_name = target_table.table_name
        full_table_name = f"{schema_name}.{table_name}"
        
        # 如果是全量同步模式或表的增量策略为NONE，直接返回全表查询
        if (self.job.sync_mode == SyncMode.FULL or 
            target_table.incremental_strategy == IncrementalStrategy.NONE):
            return f"SELECT * FROM {full_table_name}"
        
        # 增量同步逻辑
        if target_table.incremental_strategy == IncrementalStrategy.CUSTOM_CONDITION:
            # 使用表级自定义条件
            if target_table.custom_condition:
                return f"SELECT * FROM {full_table_name} WHERE {target_table.custom_condition}"
            else:
                self._update_log(f"警告：表 {full_table_name} 设置了自定义条件策略但未提供条件，将执行全量同步")
                return f"SELECT * FROM {full_table_name}"
        
        elif target_table.incremental_strategy == IncrementalStrategy.AUTO_ID:
            # 自动基于ID字段增量
            id_field = target_table.incremental_field or self._detect_id_field(schema_name, table_name)
            if id_field:
                last_value = self._get_last_sync_value(target_table, id_field)
                condition = f"{id_field} > {last_value}" if last_value else f"{id_field} IS NOT NULL"
                return f"SELECT * FROM {full_table_name} WHERE {condition} ORDER BY {id_field}"
            else:
                self._update_log(f"警告：表 {full_table_name} 未找到ID字段，将执行全量同步")
                return f"SELECT * FROM {full_table_name}"
        
        elif target_table.incremental_strategy == IncrementalStrategy.AUTO_TIMESTAMP:
            # 自动基于时间戳字段增量
            timestamp_field = target_table.incremental_field or self._detect_timestamp_field(schema_name, table_name)
            if timestamp_field:
                last_value = self._get_last_sync_value(target_table, timestamp_field)
                if last_value:
                    condition = f"{timestamp_field} > '{last_value}'"
                else:
                    # 如果没有上次同步的时间，使用最近24小时的数据
                    condition = f"{timestamp_field} >= NOW() - INTERVAL '24 hours'"
                return f"SELECT * FROM {full_table_name} WHERE {condition} ORDER BY {timestamp_field}"
            else:
                self._update_log(f"警告：表 {full_table_name} 未找到时间戳字段，将执行全量同步")
                return f"SELECT * FROM {full_table_name}"
        
        # 默认情况
        return f"SELECT * FROM {full_table_name}"
    
    def _detect_id_field(self, schema_name: str, table_name: str) -> str:
        """检测ID字段"""
        try:
            # 常见的ID字段名
            id_candidates = ['id', 'ID', 'Id', 'pk_id', 'primary_id', 'uid']
            
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema_name 
                  AND table_name = :table_name 
                  AND (column_name IN :candidates 
                       OR column_name LIKE '%_id' 
                       OR column_name LIKE 'id_%')
                  AND data_type IN ('integer', 'bigint', 'smallint', 'serial', 'bigserial')
                ORDER BY 
                    CASE 
                        WHEN column_name = 'id' THEN 1
                        WHEN column_name = 'ID' THEN 2
                        WHEN column_name = 'Id' THEN 3
                        ELSE 4
                    END
                LIMIT 1
            """
            
            with self.source_engine.connect() as conn:
                result = conn.execute(
                    text(query), 
                    {
                        'schema_name': schema_name, 
                        'table_name': table_name,
                        'candidates': tuple(id_candidates)
                    }
                )
                row = result.fetchone()
                return row[0] if row else None
                
        except Exception as e:
            logger.warning(f"Failed to detect ID field for {schema_name}.{table_name}: {e}")
            return None
    
    def _detect_timestamp_field(self, schema_name: str, table_name: str) -> str:
        """检测时间戳字段"""
        try:
            # 常见的时间戳字段名
            timestamp_candidates = [
                'updated_at', 'created_at', 'modified_at', 'timestamp', 
                'last_modified', 'date_modified', 'update_time', 'create_time',
                'last_update', 'date_created', 'date_updated'
            ]
            
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema_name 
                  AND table_name = :table_name 
                  AND (column_name IN :candidates
                       OR column_name LIKE '%_at'
                       OR column_name LIKE '%_time'
                       OR column_name LIKE 'date_%')
                  AND data_type IN ('timestamp', 'timestamptz', 'datetime', 'timestamp with time zone', 'timestamp without time zone')
                ORDER BY 
                    CASE 
                        WHEN column_name = 'updated_at' THEN 1
                        WHEN column_name = 'created_at' THEN 2
                        WHEN column_name = 'modified_at' THEN 3
                        WHEN column_name = 'timestamp' THEN 4
                        ELSE 5
                    END
                LIMIT 1
            """
            
            with self.source_engine.connect() as conn:
                result = conn.execute(
                    text(query), 
                    {
                        'schema_name': schema_name, 
                        'table_name': table_name,
                        'candidates': tuple(timestamp_candidates)
                    }
                )
                row = result.fetchone()
                return row[0] if row else None
                
        except Exception as e:
            logger.warning(f"Failed to detect timestamp field for {schema_name}.{table_name}: {e}")
            return None
    
    def _get_last_sync_value(self, target_table, field_name: str) -> str:
        """
        获取目标表中指定字段的最大值作为增量同步起点
        
        Args:
            target_table: JobTargetTable对象
            field_name: 字段名
            
        Returns:
            str: 最大值（字符串形式）
        """
        try:
            # 先检查是否有上次同步的值
            if target_table.last_sync_value:
                self._update_log(f"使用上次同步的最大值: {target_table.last_sync_value}")
                return target_table.last_sync_value
            
            # 查询目标表中的最大值
            schema_name = target_table.schema_name
            table_name = target_table.table_name
            full_table_name = f"{schema_name}.{table_name}"
            
            query = f"SELECT MAX({field_name}) FROM {full_table_name}"
            
            with self.destination_engine.connect() as conn:
                result = conn.execute(text(query))
                row = result.fetchone()
                max_value = row[0] if row and row[0] is not None else None
                
                if max_value is not None:
                    max_value_str = str(max_value)
                    self._update_log(f"从目标表 {full_table_name} 获取最大值: {max_value_str}")
                    return max_value_str
                else:
                    self._update_log(f"目标表 {full_table_name} 中字段 {field_name} 没有数据，将进行全量同步")
                    return None
                    
        except Exception as e:
            logger.warning(f"Failed to get last sync value for {target_table.schema_name}.{target_table.table_name}.{field_name}: {e}")
            return None
    
    def _update_last_sync_value(self, target_table, field_name: str, data: List[Dict[str, Any]]):
        """
        更新表的最后同步值
        
        Args:
            target_table: JobTargetTable对象
            field_name: 字段名
            data: 同步的数据列表
        """
        try:
            if not data or not field_name:
                return
            
            # 找到数据中的最大值
            max_value = None
            for record in data:
                if field_name in record and record[field_name] is not None:
                    current_value = record[field_name]
                    if max_value is None or current_value > max_value:
                        max_value = current_value
            
            if max_value is not None:
                # 更新数据库中的last_sync_value
                target_table.last_sync_value = str(max_value)
                self.db_session.commit()
                self._update_log(f"更新表 {target_table.schema_name}.{target_table.table_name} 的最后同步值: {max_value}")
                
        except Exception as e:
            logger.warning(f"Failed to update last sync value: {e}")
    
    def _batch_insert(self, schema_name: str, table_name: str, columns: List[str], data: List[Dict[str, Any]]):
        """批量插入数据"""
        try:
            if not data:
                return
            
            # 预处理数据，序列化字典和列表字段
            processed_data = self._preprocess_data(data)
            
            full_table_name = f"{schema_name}.{table_name}"
            columns_str = ", ".join(columns)
            placeholders = ", ".join([f":{col}" for col in columns])
            
            # 根据冲突处理策略构建不同的SQL
            if self.job.conflict_strategy == ConflictStrategy.ERROR:
                # 默认行为：遇到冲突就报错
                insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
            elif self.job.conflict_strategy == ConflictStrategy.IGNORE:
                # 忽略冲突：ON CONFLICT DO NOTHING
                insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            elif self.job.conflict_strategy == ConflictStrategy.REPLACE:
                # 替换现有记录：ON CONFLICT DO UPDATE
                # 需要获取主键字段
                primary_keys = self._get_primary_keys(schema_name, table_name)
                if primary_keys:
                    update_clauses = [f"{col} = EXCLUDED.{col}" for col in columns if col not in primary_keys]
                    conflict_target = f"({', '.join(primary_keys)})"
                    insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders}) ON CONFLICT {conflict_target} DO UPDATE SET {', '.join(update_clauses)}"
                else:
                    # 没有主键，退回到忽略策略
                    insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders}) ON CONFLICT DO NOTHING"
            elif self.job.conflict_strategy == ConflictStrategy.SKIP:
                # 跳过冲突记录：逐条插入，捕获异常但继续执行
                self._insert_with_skip(full_table_name, columns_str, placeholders, processed_data)
                return
            else:
                # 默认处理
                insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
            
            # 执行批量插入
            with self.destination_engine.connect() as dest_conn:
                with dest_conn.begin():
                    try:
                        dest_conn.execute(text(insert_sql), processed_data)
                    except Exception as e:
                        if self.job.conflict_strategy == ConflictStrategy.ERROR:
                            raise e
                        else:
                            # 对于其他策略，记录错误但继续执行
                            logger.warning(f"Batch insert failed with strategy {self.job.conflict_strategy.value}: {e}")
                            self._update_log(f"批量插入时遇到冲突，策略：{self.job.conflict_strategy.value}")
                    
        except Exception as e:
            if self.job.conflict_strategy == ConflictStrategy.ERROR:
                raise Exception(f"Failed to batch insert data: {e}")
            else:
                logger.warning(f"Batch insert error handled by strategy {self.job.conflict_strategy.value}: {e}")
                self._update_log(f"插入数据时出现错误，但根据冲突处理策略继续执行: {e}")
    
    def _get_timestamp_columns(self, schema_name: str, table_name: str) -> List[str]:
        """获取表中的时间戳字段"""
        try:
            # 常见的时间戳字段名
            timestamp_candidates = [
                'updated_at', 'created_at', 'modified_at', 'timestamp', 
                'last_modified', 'date_modified', 'update_time', 'create_time'
            ]
            
            # 查询表结构获取时间相关字段
            query = """
                SELECT column_name 
                FROM information_schema.columns 
                WHERE table_schema = :schema_name 
                  AND table_name = :table_name 
                  AND (data_type IN ('timestamp', 'timestamptz', 'datetime') 
                       OR column_name IN :timestamp_candidates)
                ORDER BY ordinal_position
            """
            
            with self.source_engine.connect() as conn:
                result = conn.execute(
                    text(query), 
                    {
                        'schema_name': schema_name, 
                        'table_name': table_name,
                        'timestamp_candidates': tuple(timestamp_candidates)
                    }
                )
                return [row[0] for row in result]
                
        except Exception as e:
            logger.warning(f"Failed to get timestamp columns for {schema_name}.{table_name}: {e}")
            return []
    
    def _truncate_target_table(self, schema_name: str, table_name: str):
        """清空目标表（全量同步时使用）"""
        try:
            full_table_name = f"{schema_name}.{table_name}"
            truncate_sql = f"TRUNCATE TABLE {full_table_name} RESTART IDENTITY CASCADE"
            
            with self.destination_engine.connect() as dest_conn:
                with dest_conn.begin():
                    dest_conn.execute(text(truncate_sql))
                    
            self._update_log(f"已清空目标表 {full_table_name}")
            
        except Exception as e:
            # 如果TRUNCATE失败，尝试使用DELETE
            try:
                full_table_name = f"{schema_name}.{table_name}"
                delete_sql = f"DELETE FROM {full_table_name}"
                
                with self.destination_engine.connect() as dest_conn:
                    with dest_conn.begin():
                        dest_conn.execute(text(delete_sql))
                        
                self._update_log(f"已删除目标表 {full_table_name} 中的所有数据")
                
            except Exception as delete_error:
                logger.warning(f"Failed to clear target table {schema_name}.{table_name}: {delete_error}")
                self._update_log(f"警告：无法清空目标表 {full_table_name}，可能会导致数据重复")
    
    def _get_primary_keys(self, schema_name: str, table_name: str) -> List[str]:
        """获取表的主键字段"""
        try:
            query = """
                SELECT column_name
                FROM information_schema.key_column_usage kcu
                JOIN information_schema.table_constraints tc
                  ON kcu.constraint_name = tc.constraint_name
                  AND kcu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY'
                  AND kcu.table_schema = :schema_name
                  AND kcu.table_name = :table_name
                ORDER BY kcu.ordinal_position
            """
            
            with self.destination_engine.connect() as conn:
                result = conn.execute(
                    text(query), 
                    {'schema_name': schema_name, 'table_name': table_name}
                )
                return [row[0] for row in result]
                
        except Exception as e:
            logger.warning(f"Failed to get primary keys for {schema_name}.{table_name}: {e}")
            return []
    
    def _insert_with_skip(self, full_table_name: str, columns_str: str, placeholders: str, data: List[Dict[str, Any]]):
        """逐条插入数据，跳过冲突记录"""
        insert_sql = f"INSERT INTO {full_table_name} ({columns_str}) VALUES ({placeholders})"
        successful_inserts = 0
        skipped_records = 0
        
        with self.destination_engine.connect() as dest_conn:
            for record in data:
                try:
                    with dest_conn.begin():
                        dest_conn.execute(text(insert_sql), [record])
                        successful_inserts += 1
                except Exception as e:
                    skipped_records += 1
                    logger.debug(f"Skipped conflicted record: {e}")
        
        self._update_log(f"成功插入 {successful_inserts} 条记录，跳过 {skipped_records} 条冲突记录")
    
    def _update_log(self, message: str):
        """更新日志详情"""
        try:
            current_log = self.log_entry.log_details or ""
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_log = f"{current_log}\n[{timestamp}] {message}"
            
            self.log_entry.log_details = new_log
            self.db_session.commit()
            
        except Exception as e:
            logger.error(f"Failed to update log: {e}")
    
    def _mark_success(self):
        """标记任务执行成功"""
        try:
            # 设置结束时间和状态
            self.log_entry.status = ExecutionStatus.SUCCESS
            self.log_entry.end_time = func.now()  # 使用数据库时间
            
            self._update_log("任务执行成功完成")
            
            # 更新任务的最后运行时间
            self.job.last_run_at = func.now()  # 使用数据库时间
            
            self.db_session.commit()
            
            # 刷新日志对象获取数据库计算的时间
            self.db_session.refresh(self.log_entry)
            
            # 计算执行时间（秒）
            if self.log_entry.start_time and self.log_entry.end_time:
                duration = (self.log_entry.end_time - self.log_entry.start_time).total_seconds()
                self.log_entry.duration_seconds = int(duration)
                self.db_session.commit()
            
            logger.info(f"Job {self.job.id} marked as successful")
            
        except Exception as e:
            logger.error(f"Failed to mark job as successful: {e}")
    
    def _mark_failure(self, error_message: str, error_traceback: str):
        """标记任务执行失败"""
        try:
            # 设置状态和错误信息
            self.log_entry.status = ExecutionStatus.FAILED
            self.log_entry.end_time = func.now()  # 使用数据库时间
            self.log_entry.error_message = error_message
            self.log_entry.error_traceback = error_traceback
            
            self._update_log(f"任务执行失败: {error_message}")
            
            self.db_session.commit()
            
            # 刷新日志对象获取数据库计算的时间
            self.db_session.refresh(self.log_entry)
            
            # 计算执行时间（秒）
            if self.log_entry.start_time and self.log_entry.end_time:
                duration = (self.log_entry.end_time - self.log_entry.start_time).total_seconds()
                self.log_entry.duration_seconds = int(duration)
                self.db_session.commit()
            
            logger.error(f"Job {self.job.id} marked as failed")
            
        except Exception as e:
            logger.error(f"Failed to mark job as failed: {e}")
    
    def _cleanup(self):
        """清理资源"""
        try:
            if self.source_engine:
                self.source_engine.dispose()
            if self.destination_engine:
                self.destination_engine.dispose()
            if self.db_session:
                self.db_session.close()
                
        except Exception as e:
            logger.error(f"Error during cleanup: {e}")

def execute_sync_job(job_id: int) -> bool:
    """
    执行同步任务的入口函数
    
    Args:
        job_id: 任务ID
        
    Returns:
        bool: 执行是否成功
    """
    try:
        # 获取任务信息
        db = SessionLocal()
        try:
            job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
            if not job:
                logger.error(f"Job {job_id} not found")
                return False
            
            # 检查任务是否已在运行
            if job.is_running:
                logger.warning(f"Job {job_id} is already running, skipping")
                return False
            
            # 标记任务为运行中
            job.is_running = True
            db.commit()
            
        finally:
            db.close()
        
        # 执行同步
        sync_engine = SyncEngine(job_id)
        success = sync_engine.execute()
        
        # 更新任务状态
        db = SessionLocal()
        try:
            job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
            if job:
                job.is_running = False
                db.commit()
        finally:
            db.close()
        
        return success
        
    except Exception as e:
        logger.error(f"Failed to execute sync job {job_id}: {e}")
        
        # 确保清除运行状态
        try:
            db = SessionLocal()
            try:
                job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
                if job:
                    job.is_running = False
                    db.commit()
            finally:
                db.close()
        except:
            pass
        
        return False
