"""
数据库同步引擎
核心同步逻辑实现
"""
import json
import logging
import traceback
import asyncio
from datetime import datetime
from typing import List, Dict, Any, Optional
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
    
    def __init__(self, job_id: int, progress_callback: Optional[callable] = None):
        """
        初始化同步引擎
        
        Args:
            job_id: 备份任务ID
            progress_callback: 进度回调函数
        """
        self.job_id = job_id
        self.job: BackupJob = None
        self.source_engine: Engine = None
        self.destination_engine: Engine = None
        self.log_entry: JobExecutionLog = None
        self.db_session: Session = None
        self.progress_callback = progress_callback
        self.current_progress = {
            'stage': 'initializing',
            'current_table': '',
            'tables_completed': 0,
            'total_tables': 0,
            'records_processed': 0,
            'total_records': 0,
            'percentage': 0
        }
        
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
            target_tables = [t for t in self.job.target_tables if t.is_active]
            
            if not target_tables:
                raise ValueError("No target tables specified for sync")
            
            # 更新进度信息
            self.current_progress.update({
                'stage': 'syncing',
                'total_tables': len(target_tables),
                'tables_completed': 0
            })
            self._notify_progress()
            
            self._update_log(f"开始同步 {len(target_tables)} 个表...")
            
            for i, target_table in enumerate(target_tables):
                schema_name = target_table.schema_name
                table_name = target_table.table_name
                
                # 更新当前表进度
                self.current_progress.update({
                    'current_table': f"{schema_name}.{table_name}",
                    'tables_completed': i,
                    'percentage': int((i / len(target_tables)) * 100)
                })
                self._notify_progress()
                
                self._update_log(f"正在同步表: {schema_name}.{table_name} （增量策略: {target_table.incremental_strategy.value if target_table.incremental_strategy else 'none'}）")
                
                # 同步表结构
                self._sync_table_structure(schema_name, table_name)
                
                # 同步数据（传递target_table对象）
                records_count = self._sync_table_data(target_table)
                
                tables_processed += 1
                total_records += records_count
                
                # 更新完成的表数量
                self.current_progress.update({
                    'tables_completed': tables_processed,
                    'records_processed': total_records,
                    'percentage': int((tables_processed / len(target_tables)) * 100)
                })
                self._notify_progress()
                
                self._update_log(f"表 {schema_name}.{table_name} 同步完成，传输 {records_count} 条记录")
            
            # 更新统计信息
            self.log_entry.tables_processed = tables_processed
            self.log_entry.records_transferred = total_records
            self.db_session.commit()
            
            # 最终进度更新
            self.current_progress.update({
                'stage': 'completed',
                'percentage': 100
            })
            self._notify_progress()
            
            logger.info(f"Sync completed for job {self.job.id}: {tables_processed} tables, {total_records} records")
            
        except Exception as e:
            self.current_progress.update({
                'stage': 'error',
                'error': str(e)
            })
            self._notify_progress()
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
                
                # 安全地创建表结构，避免索引问题
                self._create_table_safely(source_table, schema_name, table_name)
                
                logger.info(f"Table {schema_name}.{table_name} created successfully")
                
        except Exception as e:
            raise Exception(f"Failed to sync table structure for {schema_name}.{table_name}: {e}")
    
    def _create_table_safely(self, source_table, schema_name: str, table_name: str):
        """安全地创建表结构，避免GIN索引问题"""
        try:
            # 创建目标表的元数据，但不包含索引
            dest_metadata = MetaData()
            
            # 复制表结构但排除索引
            dest_table = Table(
                table_name,
                dest_metadata,
                schema=schema_name
            )
            
            # 复制列定义
            for column in source_table.columns:
                # 创建新列，但不复制索引相关属性
                new_column = column.copy()
                new_column.index = False  # 禁用自动索引创建
                dest_table.append_column(new_column)
            
            # 复制主键约束
            if source_table.primary_key.columns:
                dest_table.append_constraint(
                    source_table.primary_key.copy()
                )
            
            # 复制外键约束（如果需要）
            for fk in source_table.foreign_keys:
                try:
                    dest_table.append_constraint(fk.constraint.copy())
                except Exception as fk_error:
                    logger.warning(f"Failed to copy foreign key constraint: {fk_error}")
            
            # 创建表（不包含索引）
            dest_metadata.create_all(self.destination_engine)
            
            # 单独创建安全的索引
            self._create_safe_indexes(source_table, schema_name, table_name)
            
        except Exception as e:
            logger.error(f"Failed to create table safely: {e}")
            # 如果安全创建失败，尝试原始方法
            try:
                dest_metadata = MetaData()
                dest_table = source_table.tometadata(dest_metadata, schema=schema_name)
                dest_metadata.create_all(self.destination_engine)
                logger.warning(f"Fallback to original table creation method for {schema_name}.{table_name}")
            except Exception as fallback_error:
                raise Exception(f"Both safe and fallback table creation failed: {fallback_error}")
    
    def _create_safe_indexes(self, source_table, schema_name: str, table_name: str):
        """为表创建安全的索引，避免GIN索引问题"""
        try:
            with self.destination_engine.connect() as conn:
                with conn.begin():
                    for index in source_table.indexes:
                        try:
                            # 检查索引类型和列类型
                            index_name = f"{table_name}_{index.name}" if not index.name.startswith(table_name) else index.name
                            
                            # 构建安全的索引创建SQL
                            columns = [col.name for col in index.columns]
                            columns_str = ', '.join(columns)
                            
                            # 检查是否为字符串字段，避免使用GIN索引
                            column_types = self._get_column_types(source_table, columns)
                            
                            # 根据列类型选择合适的索引类型
                            if self._should_use_gin_index(column_types):
                                # 对于适合GIN的类型（如数组、JSON），使用GIN
                                index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_name}.{table_name} USING gin ({columns_str})"
                            else:
                                # 对于普通字符串等类型，使用B-tree索引
                                index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_name}.{table_name} ({columns_str})"
                            
                            conn.execute(text(index_sql))
                            logger.info(f"Created index {index_name} for table {schema_name}.{table_name}")
                            
                        except Exception as idx_error:
                            logger.warning(f"Failed to create index {index.name} for {schema_name}.{table_name}: {idx_error}")
                            # 继续创建其他索引
                            continue
                            
        except Exception as e:
            logger.warning(f"Failed to create indexes for {schema_name}.{table_name}: {e}")
    
    def _get_column_types(self, table, column_names: list) -> dict:
        """获取列的数据类型"""
        column_types = {}
        for col_name in column_names:
            for column in table.columns:
                if column.name == col_name:
                    column_types[col_name] = str(column.type).lower()
                    break
        return column_types
    
    def _should_use_gin_index(self, column_types: dict) -> bool:
        """判断是否应该使用GIN索引"""
        gin_suitable_types = {
            'array', 'json', 'jsonb', 'tsvector', 'tsquery'
        }
        
        for col_type in column_types.values():
            # 检查是否为数组类型
            if '[]' in col_type or 'array' in col_type:
                return True
            # 检查是否为JSON类型
            if any(gin_type in col_type for gin_type in gin_suitable_types):
                return True
        
        return False
    
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
                        
                        # 更新记录处理进度
                        self.current_progress['records_processed'] = self.current_progress.get('records_processed', 0) + len(batch_data)
                        self._notify_progress()
                        
                        batch_data = []
                
                # 处理剩余数据
                if batch_data:
                    self._batch_insert(schema_name, table_name, columns, batch_data)
                    total_records += len(batch_data)
                    
                    # 更新最终记录处理进度
                    self.current_progress['records_processed'] = self.current_progress.get('records_processed', 0) + len(batch_data)
                    self._notify_progress()
                
                # 更新增量同步值（如果是增量模式且有增量字段）
                if (target_table.incremental_strategy in [IncrementalStrategy.AUTO_ID, IncrementalStrategy.AUTO_TIMESTAMP] 
                    and incremental_field and all_data):
                    self._update_last_sync_value(target_table, incremental_field, all_data)
                
                self._update_log(f"表 {full_table_name} 同步完成，共处理 {total_records} 条记录")
                return total_records
                
        except Exception as e:
            raise Exception(f"Failed to sync data for {schema_name}.{table_name}: {e}")
    
    def _preprocess_data(self, data: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """预处理数据，将字典和列表字段序列化为JSON字符串或PostgreSQL数组格式"""
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
    
    def _preprocess_data_with_arrays(self, data: List[Dict[str, Any]], array_columns: Dict[str, str]) -> List[Dict[str, Any]]:
        """预处理数据，包括PostgreSQL数组字段的特殊处理"""
        processed_data = []
        
        for record in data:
            processed_record = {}
            for key, value in record.items():
                if key in array_columns:
                    # 处理PostgreSQL数组字段
                    processed_record[key] = self._format_postgresql_array(value, array_columns[key])
                elif isinstance(value, (dict, list)):
                    # 将字典和列表序列化为JSON字符串
                    try:
                        processed_record[key] = json.dumps(value, ensure_ascii=False, default=str)
                    except (TypeError, ValueError) as e:
                        logger.warning(f"Failed to serialize field {key}: {e}, using string representation")
                        processed_record[key] = str(value)
                elif isinstance(value, str) and self._is_json_field(key, value):
                    # 处理可能是JSON字符串的字段
                    processed_record[key] = self._sanitize_json_string(value)
                else:
                    processed_record[key] = value
            processed_data.append(processed_record)
        
        return processed_data
    
    def _is_json_field(self, field_name: str, value: str) -> bool:
        """判断字段是否可能是JSON字段"""
        if not isinstance(value, str) or not value.strip():
            return False
        
        # 检查字段名是否暗示这是JSON字段
        json_field_indicators = [
            'json', 'data', 'metadata', 'config', 'settings', 'params', 
            'properties', 'attributes', 'extra', 'custom', 'payload'
        ]
        
        field_lower = field_name.lower()
        if any(indicator in field_lower for indicator in json_field_indicators):
            return True
        
        # 检查值是否看起来像JSON
        value_stripped = value.strip()
        if (value_stripped.startswith(('{', '[')) and value_stripped.endswith(('}', ']'))) or \
           value_stripped.startswith('"') and value_stripped.endswith('"'):
            return True
        
        return False
    
    def _sanitize_json_string(self, value: str) -> str:
        """清理和验证JSON字符串"""
        if not value or not isinstance(value, str):
            return value
        
        value = value.strip()
        
        # 如果是空字符串，返回null
        if not value:
            return 'null'
        
        # 尝试解析和重新序列化以确保有效的JSON
        try:
            # 首先尝试作为JSON解析
            parsed = json.loads(value)
            # 重新序列化以确保格式正确
            return json.dumps(parsed, ensure_ascii=False, separators=(',', ':'))
        except (json.JSONDecodeError, TypeError):
            # 如果不是有效的JSON，尝试作为普通字符串处理
            try:
                # 检查是否包含特殊字符需要转义
                if any(char in value for char in ['/', '\\', '"', '\n', '\r', '\t']):
                    # 转义特殊字符
                    escaped_value = value.replace('\\', '\\\\')\
                                        .replace('"', '\\"')\
                                        .replace('/', '\\/')\
                                        .replace('\n', '\\n')\
                                        .replace('\r', '\\r')\
                                        .replace('\t', '\\t')
                    # 包装为JSON字符串
                    return json.dumps(escaped_value, ensure_ascii=False)
                else:
                    # 简单字符串，直接包装
                    return json.dumps(value, ensure_ascii=False)
            except Exception as e:
                logger.warning(f"Failed to sanitize JSON string: {e}, using null")
                return 'null'
    
    def _get_array_columns(self, schema_name: str, table_name: str) -> Dict[str, str]:
        """获取表中的PostgreSQL数组字段及其数据类型"""
        try:
            query = """
                SELECT column_name, data_type, udt_name
                FROM information_schema.columns 
                WHERE table_schema = :schema_name 
                  AND table_name = :table_name 
                  AND (data_type = 'ARRAY' OR udt_name LIKE '%[]')
                ORDER BY ordinal_position
            """
            
            with self.destination_engine.connect() as conn:
                result = conn.execute(
                    text(query), 
                    {'schema_name': schema_name, 'table_name': table_name}
                )
                return {row[0]: row[2] for row in result}  # column_name: udt_name
                
        except Exception as e:
            logger.warning(f"Failed to get array columns for {schema_name}.{table_name}: {e}")
            return {}
    
    def _format_postgresql_array(self, value: Any, array_type: str) -> str:
        """将Python值格式化为PostgreSQL数组格式"""
        if value is None:
            return None
        
        # 如果已经是字符串且看起来像PostgreSQL数组格式，直接返回
        if isinstance(value, str):
            if value.startswith('{') and value.endswith('}'):
                return value
            # 如果是JSON数组格式，需要转换
            if value.startswith('[') and value.endswith(']'):
                try:
                    # 解析JSON数组
                    json_array = json.loads(value)
                    return self._convert_to_pg_array(json_array, array_type)
                except (json.JSONDecodeError, TypeError):
                    # 如果解析失败，当作普通字符串处理
                    return value
            return value
        
        # 如果是Python列表，转换为PostgreSQL数组格式
        if isinstance(value, list):
            return self._convert_to_pg_array(value, array_type)
        
        # 其他类型，转换为字符串
        return str(value)
    
    def _convert_to_pg_array(self, python_list: list, array_type: str) -> str:
        """将Python列表转换为PostgreSQL数组格式"""
        if not python_list:
            return '{}'
        
        # 根据数组类型决定是否需要引号
        needs_quotes = self._array_element_needs_quotes(array_type)
        
        formatted_elements = []
        for item in python_list:
            if item is None:
                formatted_elements.append('NULL')
            elif needs_quotes:
                # 转义引号和反斜杠
                escaped_item = str(item).replace('\\', '\\\\').replace('"', '\\"')
                formatted_elements.append(f'"{escaped_item}"')
            else:
                formatted_elements.append(str(item))
        
        return '{' + ','.join(formatted_elements) + '}'
    
    def _array_element_needs_quotes(self, array_type: str) -> bool:
        """判断数组元素是否需要引号"""
        # 数值类型不需要引号
        numeric_types = {
            '_int2', '_int4', '_int8', '_float4', '_float8', '_numeric',
            '_smallint', '_integer', '_bigint', '_real', '_double', '_decimal'
        }
        
        # 布尔类型不需要引号
        boolean_types = {'_bool', '_boolean'}
        
        return array_type not in numeric_types and array_type not in boolean_types
    
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
        
        # 构建基础查询
        base_query = f"SELECT * FROM {full_table_name}"
        conditions = []
        order_by = ""
        
        # 处理增量同步逻辑
        if (self.job.sync_mode == SyncMode.INCREMENTAL and 
            target_table.incremental_strategy != IncrementalStrategy.NONE):
            
            if target_table.incremental_strategy == IncrementalStrategy.CUSTOM_CONDITION:
                # 使用表级自定义条件
                if target_table.custom_condition:
                    conditions.append(target_table.custom_condition)
                else:
                    self._update_log(f"警告：表 {full_table_name} 设置了自定义条件策略但未提供条件")
            
            elif target_table.incremental_strategy == IncrementalStrategy.AUTO_ID:
                # 自动基于ID字段增量
                id_field = target_table.incremental_field or self._detect_id_field(schema_name, table_name)
                if id_field:
                    last_value = self._get_last_sync_value(target_table, id_field)
                    condition = f"{id_field} > {last_value}" if last_value else f"{id_field} IS NOT NULL"
                    conditions.append(condition)
                    order_by = f" ORDER BY {id_field}"
                else:
                    self._update_log(f"警告：表 {full_table_name} 未找到ID字段")
            
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
                    conditions.append(condition)
                    order_by = f" ORDER BY {timestamp_field}"
                else:
                    self._update_log(f"警告：表 {full_table_name} 未找到时间戳字段")
        
        # 处理全局WHERE条件
        if self.job.where_condition and self.job.where_condition.strip():
            global_condition = self.job.where_condition.strip()
            # 如果全局条件不为空，添加到条件列表
            conditions.append(f"({global_condition})")
            self._update_log(f"应用全局WHERE条件到表 {full_table_name}: {global_condition}")
        
        # 构建最终查询
        if conditions:
            where_clause = " WHERE " + " AND ".join(conditions)
            return base_query + where_clause + order_by
        else:
            return base_query + order_by
    
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
            
            # 获取PostgreSQL数组字段信息
            array_columns = self._get_array_columns(schema_name, table_name)
            
            # 预处理数据，包括PostgreSQL数组格式化
            processed_data = self._preprocess_data_with_arrays(data, array_columns)
            
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
    
    def _notify_progress(self):
        """通知进度更新"""
        if self.progress_callback:
            try:
                self.progress_callback(self.job_id, self.current_progress.copy())
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")
    
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

def execute_sync_job(job_id: int, progress_callback: Optional[callable] = None) -> bool:
    """
    执行同步任务的入口函数
    
    Args:
        job_id: 任务ID
        progress_callback: 进度回调函数
        
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
        sync_engine = SyncEngine(job_id, progress_callback)
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
