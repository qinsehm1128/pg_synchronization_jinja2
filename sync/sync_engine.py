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
from sqlalchemy import create_engine, text, MetaData, Table, Column
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
        self.is_cancelled = False  # 添加取消标志
        self.current_progress = {
            'stage': 'initializing',
            'current_table': '',
            'tables_completed': 0,
            'total_tables': 0,
            'records_processed': 0,
            'total_records': 0,
            'percentage': 0
        }
    
    def _check_if_cancelled(self) -> bool:
        """
        检查任务是否被取消
        
        Returns:
            bool: 如果任务被取消返回True
        """
        if self.is_cancelled:
            return True
            
        # 检查数据库中的日志状态
        if self.log_entry:
            try:
                # 刷新日志条目状态
                self.db_session.refresh(self.log_entry)
                if self.log_entry.status == ExecutionStatus.CANCELLED:
                    self.is_cancelled = True
                    return True
            except Exception as e:
                logger.warning(f"检查取消状态时出错: {e}")
                return True
        return False
        
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
            # 检查是否是用户取消的任务
            if "Task cancelled by user" in str(e) or self.is_cancelled:
                # 标记为取消状态
                self._mark_cancelled("任务已被用户取消")
                logger.info(f"Sync cancelled by user for job {self.job_id}")
                success = False
            else:
                logger.error(f"Sync job {self.job.id} failed: {e}")
                self._mark_failure(str(e), traceback.format_exc())
                success = False
            
        finally:
            self._cleanup()
            
        return success
    
    def _create_log_entry(self):
        """创建执行日志记录"""
        try:
            self.log_entry = JobExecutionLog(
                job_id=self.job.id,
                status=ExecutionStatus.RUNNING,
                log_details="任务开始执行..."
            )
            self.db_session.add(self.log_entry)
            self.db_session.flush()  # 使用flush而不commit，让数据库设置start_time
            self.db_session.refresh(self.log_entry)  # 刷新获取数据库设置的时间
            logger.info(f"Created log entry for job {self.job.id}")
            
        except Exception as e:
            logger.error(f"Failed to create log entry: {e}")
            # 如果flush失败，回滚并重试
            try:
                self.db_session.rollback()
                self.log_entry = JobExecutionLog(
                    job_id=self.job.id,
                    status=ExecutionStatus.RUNNING,
                    log_details="任务开始执行..."
                )
                self.db_session.add(self.log_entry)
                self.db_session.commit()  # 直接提交而不是flush
                logger.info(f"Created log entry for job {self.job.id} after retry")
            except Exception as retry_error:
                logger.error(f"Failed to create log entry after retry: {retry_error}")
                raise Exception(f"Unable to create execution log: {retry_error}")
    
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
                # 检查任务是否被取消
                if self._check_if_cancelled():
                    self._update_log("任务已被用户取消")
                    raise ValueError("Task cancelled by user")
                    
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
        logger.info(f"Starting table structure sync for {schema_name}.{table_name}")
        
        # 获取源表结构
        logger.info(f"Loading source table metadata for {schema_name}.{table_name}")
        source_metadata = MetaData()
        
        try:
            source_table = Table(
                table_name, 
                source_metadata, 
                autoload_with=self.source_engine,
                schema=schema_name
            )
            logger.info(f"Source table loaded successfully. Columns: {[col.name for col in source_table.columns]}")
            
        except Exception as source_error:
            # 源表不存在或加载失败
            logger.error(f"Failed to load source table {schema_name}.{table_name}: {source_error}")
            if "NoSuchTableError" in str(type(source_error)) or "does not exist" in str(source_error).lower():
                raise Exception(f"Source table {schema_name}.{table_name} does not exist in source database. Please check your table configuration.")
            else:
                raise Exception(f"Failed to load source table {schema_name}.{table_name}: {source_error}")
        
        # 检查目标表是否存在
        dest_metadata = MetaData()
        
        try:
            logger.info(f"Checking if destination table {schema_name}.{table_name} exists")
            # 尝试加载目标表
            Table(
                table_name, 
                dest_metadata, 
                autoload_with=self.destination_engine,
                schema=schema_name
            )
            logger.info(f"Table {schema_name}.{table_name} already exists in destination")
            
        except Exception as check_error:
            # 目标表不存在，创建它
            logger.info(f"Destination table does not exist. Error: {check_error}")
            logger.info(f"Creating table {schema_name}.{table_name} in destination")
            
            try:
                # 安全地创建表结构，避免索引问题
                self._create_table_safely(source_table, schema_name, table_name)
                logger.info(f"Table {schema_name}.{table_name} created successfully")
                
            except Exception as create_error:
                logger.error(f"Failed to create table {schema_name}.{table_name}: {create_error}")
                logger.error(f"Exception details: {traceback.format_exc()}")
                raise Exception(f"Failed to create table {schema_name}.{table_name}: {create_error}")
    
    def _create_table_safely(self, source_table, schema_name: str, table_name: str):
        """安全地创建表结构，避免GIN索引问题，确保序列创建和表创建在同一事务中"""
        try:
            logger.info(f"Starting safe table creation for {schema_name}.{table_name}")
            
            # 在同一个事务中执行序列创建、表创建和索引创建
            with self.destination_engine.connect() as conn:
                with conn.begin():  # 开始一个事务
                    logger.info(f"Started transaction for table creation: {schema_name}.{table_name}")
                    
                    # 1. 在这个事务里创建序列
                    logger.info(f"开始为表 {schema_name}.{table_name} 创建序列...")
                    failed_columns = self._create_sequences(source_table, schema_name, table_name, conn)
                    logger.info(f"序列创建完成。失败的列: {list(failed_columns.keys())}")
                    if failed_columns:
                        logger.warning(f"警告: {len(failed_columns)} 个列的序列创建失败，将转换为BIGSERIAL类型")
                    else:
                        logger.info(f"所有序列创建成功！")
                    
                    # 创建目标表的元数据，但不包含索引
                    dest_metadata = MetaData()
                    
                    # 复制表结构但排除索引
                    dest_table = Table(
                        table_name,
                        dest_metadata,
                        schema=schema_name
                    )
                    
                    # 复制列定义，处理序列创建失败的列
                    logger.info(f"开始处理 {len(source_table.columns)} 个列，目标表: {schema_name}.{table_name}")
                    for column in source_table.columns:
                        logger.info(f"正在处理列: {column.name}, 类型: {column.type}, 默认值: {column.default}")
                        
                        # 如果该列的序列创建失败，转换为BIGSERIAL
                        if column.name in failed_columns:
                            logger.info(f"由于序列创建失败，将列 {column.name} 转换为BIGSERIAL类型")
                            logger.info(f"原始列默认值: {column.default}")
                            
                            # 将列类型转换为BIGSERIAL（自动递增的BIGINT）
                            from sqlalchemy import BigInteger
                            new_column = Column(
                                column.name,
                                BigInteger,
                                primary_key=column.primary_key,
                                nullable=column.nullable,
                                autoincrement=True,
                                default=None  # 移除原始的nextval默认值
                            )
                            new_column.index = False
                            logger.info(f"已创建BIGSERIAL列: {column.name}, 自动递增: {new_column.autoincrement}, 默认值: {new_column.default}")
                        else:
                            # 创建新列，但不复制索引相关属性
                            new_column = column.copy()
                            new_column.index = False  # 禁用自动索引创建
                            
                            # 检查是否为序列列，如果是，确保序列引用格式正确
                            if new_column.default is not None:
                                default_str = str(new_column.default.arg) if hasattr(new_column.default, 'arg') else str(new_column.default)
                                if 'nextval' in default_str.lower():
                                    logger.info(f"列 {column.name} 使用序列，原始默认值: {default_str}")
                                    # 提取并验证序列名
                                    full_sequence_name, sequence_name = self._extract_sequence_name(default_str, schema_name, table_name, column.name)
                                    if full_sequence_name:
                                        # 确保序列引用格式正确（不带引号）
                                        corrected_default = f"nextval('{full_sequence_name}'::regclass)"
                                        logger.info(f"修正序列引用格式: {corrected_default}")
                                        # 更新列的默认值
                                        from sqlalchemy import text as sql_text
                                        new_column.default = sql_text(corrected_default)
                            
                            logger.info(f"已复制列: {column.name}, 类型: {new_column.type}, 默认值: {new_column.default}")
                        
                        dest_table.append_column(new_column)
                        logger.info(f"列 {column.name} 已添加到目标表结构中")
                    
                    # 复制主键约束 - 修复SQLAlchemy警告
                    if source_table.primary_key.columns:
                        pk_columns = [col.name for col in source_table.primary_key.columns]
                        logger.info(f"正在创建主键约束，包含列: {pk_columns}")
                        logger.info(f"源表主键约束名称: {source_table.primary_key.name}")
                        
                        # 基于目标表中的实际列重新构建主键约束，避免列引用不一致
                        from sqlalchemy import PrimaryKeyConstraint
                        try:
                            # 确保引用的是目标表中的实际列
                            pk_table_columns = [dest_table.c[col_name] for col_name in pk_columns if col_name in dest_table.c]
                            if pk_table_columns:
                                # 使用标准的主键约束命名规范
                                pk_constraint_name = f"{table_name}_pkey"
                                primary_key_constraint = PrimaryKeyConstraint(*pk_table_columns, name=pk_constraint_name)
                                dest_table.append_constraint(primary_key_constraint)
                                
                                logger.info(f"主键约束已成功添加到目标表，约束名称: {pk_constraint_name}")
                                logger.info(f"主键约束包含的列数量: {len(pk_table_columns)}")
                                logger.info(f"主键约束列详情: {[col.name for col in pk_table_columns]}")
                            else:
                                logger.error(f"错误: 无法在目标表中找到主键列 {pk_columns}")
                        except Exception as pk_error:
                            logger.error(f"创建主键约束时发生错误: {pk_error}")
                            logger.error(f"主键约束创建错误详情: {traceback.format_exc()}")
                    else:
                        logger.warning(f"警告: 源表 {schema_name}.{table_name} 没有主键约束！")
                    
                    # 复制外键约束（如果需要）
                    for fk in source_table.foreign_keys:
                        try:
                            dest_table.append_constraint(fk.constraint.copy())
                        except Exception as fk_error:
                            logger.warning(f"Failed to copy foreign key constraint: {fk_error}")
                    
                    # 2. 在同一个事务里创建表
                    logger.info(f"正在创建表 {schema_name}.{table_name}，包含 {len(dest_table.columns)} 个列")
                    logger.info(f"表结构详情: 主键列数={len(dest_table.primary_key.columns) if dest_table.primary_key else 0}, 约束数={len(dest_table.constraints)}")
                    
                    # 打印即将创建的表的详细信息
                    for col in dest_table.columns:
                        logger.info(f"列详情: {col.name} - 类型: {col.type}, 主键: {col.primary_key}, 可空: {col.nullable}, 默认值: {col.default}")
                    
                    dest_metadata.create_all(conn, checkfirst=False)  # 使用同一个连接
                    logger.info(f"表 {schema_name}.{table_name} 创建成功！")
                    
                    # 验证表是否真正创建成功
                    from sqlalchemy import text as sql_text
                    verify_sql = f"SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'"
                    table_exists = conn.execute(sql_text(verify_sql)).scalar()
                    logger.info(f"表创建验证: {schema_name}.{table_name} 存在性检查结果 = {table_exists}")
                    
                    # 验证主键约束是否创建成功
                    pk_verify_sql = f"""SELECT constraint_name, column_name 
                                        FROM information_schema.key_column_usage 
                                        WHERE table_schema = '{schema_name}' 
                                        AND table_name = '{table_name}' 
                                        AND constraint_name LIKE '%pkey%'"""
                    pk_result = conn.execute(sql_text(pk_verify_sql)).fetchall()
                    logger.info(f"主键约束验证结果: {[(row[0], row[1]) for row in pk_result]}")
                    
                    # 3. 在同一个事务里创建索引
                    self._create_safe_indexes(source_table, schema_name, table_name, conn)
                    
                    logger.info(f"Transaction completed successfully for table {schema_name}.{table_name}")
            
        except Exception as e:
            logger.error(f"Failed to create table safely: {e}")
            logger.error(f"Exception details: {traceback.format_exc()}")
            # 如果安全创建失败，尝试原始方法
            try:
                logger.warning(f"Attempting fallback table creation for {schema_name}.{table_name}")
                dest_metadata = MetaData()
                dest_table = source_table.tometadata(dest_metadata, schema=schema_name)
                dest_metadata.create_all(self.destination_engine)
                logger.warning(f"Fallback to original table creation method for {schema_name}.{table_name}")
            except Exception as fallback_error:
                logger.error(f"Fallback table creation also failed: {fallback_error}")
                raise Exception(f"Both safe and fallback table creation failed: {fallback_error}")
    
    def _create_safe_indexes(self, source_table, schema_name: str, table_name: str, conn):
        """为表创建安全的索引，避免GIN索引问题，使用传入的连接对象"""
        try:
            logger.info(f"开始为表 {schema_name}.{table_name} 创建索引，共 {len(source_table.indexes)} 个索引")
            
            for index in source_table.indexes:
                try:
                    # 检查索引类型和列类型
                    index_name = f"{table_name}_{index.name}" if not index.name.startswith(table_name) else index.name
                    
                    # 构建安全的索引创建SQL
                    columns = [col.name for col in index.columns]
                    columns_str = ', '.join(columns)
                    
                    # 检查列类型
                    column_types = self._get_column_types(source_table, columns)
                    logger.info(f"索引 {index_name} 的列类型: {column_types}")
                    
                    # 检查是否有不支持索引的类型
                    unsupported_for_any_index = {'unknown', 'void'}
                    has_unsupported_type = any(
                        any(unsupported in col_type.lower() for unsupported in unsupported_for_any_index)
                        for col_type in column_types.values()
                    )
                    
                    if has_unsupported_type:
                        logger.warning(f"跳过索引 {index_name}，包含不支持索引的数据类型: {column_types}")
                        continue
                    
                    # 根据列类型选择合适的索引类型
                    if self._should_use_gin_index(column_types):
                        # 对于适合GIN的类型（如数组、JSON），使用GIN
                        index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_name}.{table_name} USING gin ({columns_str})"
                        logger.info(f"创建GIN索引: {index_name}")
                    else:
                        # 对于普通类型，使用B-tree索引
                        index_sql = f"CREATE INDEX IF NOT EXISTS {index_name} ON {schema_name}.{table_name} ({columns_str})"
                        logger.info(f"创建B-tree索引: {index_name}")
                    
                    from sqlalchemy import text as sql_text
                    conn.execute(sql_text(index_sql))
                    logger.info(f"索引 {index_name} 创建成功")
                    
                except Exception as idx_error:
                    logger.warning(f"索引 {index.name} 创建失败: {idx_error}")
                    logger.warning(f"索引创建错误详情: {traceback.format_exc()}")
                    # 继续创建其他索引
                    continue
            
            logger.info(f"表 {schema_name}.{table_name} 的索引创建过程完成")
                    
        except Exception as e:
            logger.warning(f"为表 {schema_name}.{table_name} 创建索引时发生错误: {e}")
            logger.warning(f"索引创建异常详情: {traceback.format_exc()}")
    
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
        
        # 不支持GIN索引的类型
        gin_unsupported_types = {
            'character varying', 'varchar', 'text', 'char', 'character',
            'integer', 'bigint', 'smallint', 'numeric', 'decimal',
            'real', 'double precision', 'boolean', 'date', 'timestamp',
            'timestamptz', 'time', 'timetz', 'interval', 'uuid'
        }
        
        for col_type in column_types.values():
            col_type_lower = col_type.lower()
            
            # 首先检查是否为明确不支持GIN的类型
            if any(unsupported_type in col_type_lower for unsupported_type in gin_unsupported_types):
                continue  # 跳过不支持的类型
            
            # 检查是否为数组类型
            if '[]' in col_type_lower or 'array' in col_type_lower:
                return True
            # 检查是否为JSON类型
            if any(gin_type in col_type_lower for gin_type in gin_suitable_types):
                return True
        
        return False
     
    def _create_sequences(self, source_table, schema_name: str, table_name: str, conn) -> dict:
         """创建表所需的序列，返回失败的列信息，使用传入的连接对象"""
         failed_columns = {}
         logger.info(f"Starting sequence creation for table {schema_name}.{table_name}")
         
         try:
             sequence_columns_found = 0
             for column in source_table.columns:
                 # 检查列是否有默认值且使用序列
                 if column.default is not None:
                     default_str = str(column.default.arg) if hasattr(column.default, 'arg') else str(column.default)
                     logger.info(f"Checking column {column.name} with default: {default_str}")
                     
                     # 检查是否使用nextval函数（序列）
                     if 'nextval' in default_str.lower():
                         sequence_columns_found += 1
                         logger.info(f"Found sequence column: {column.name} with default: {default_str}")
                         
                         # 提取序列名
                         full_sequence_name, sequence_name = self._extract_sequence_name(default_str, schema_name, table_name, column.name)
                         logger.info(f"Extracted sequence name: {full_sequence_name} (base: {sequence_name})")
                         
                         if sequence_name:
                             try:
                                 # 解析完整序列名的schema和名称
                                 if '.' in full_sequence_name:
                                     seq_schema, seq_name = full_sequence_name.split('.', 1)
                                     # 清理schema名中的引号
                                     seq_schema = seq_schema.strip('"').strip("'")
                                     seq_name = seq_name.strip('"').strip("'")
                                 else:
                                     seq_schema, seq_name = schema_name, sequence_name
                                     seq_name = seq_name.strip('"').strip("'")
                                 
                                 logger.info(f"正在检查序列是否存在: schema={seq_schema}, name={seq_name}")
                                 
                                 # 检查序列是否已存在
                                 check_seq_sql = f"""
                                 SELECT EXISTS (
                                     SELECT 1 FROM information_schema.sequences 
                                     WHERE sequence_schema = '{seq_schema}' 
                                     AND sequence_name = '{seq_name}'
                                 )
                                 """
                                 
                                 from sqlalchemy import text as sql_text
                                 result = conn.execute(sql_text(check_seq_sql)).scalar()
                                 logger.info(f"序列存在性检查结果: {result}")
                                 
                                 if not result:
                                     # 创建序列时不使用引号包围schema名
                                     clean_full_name = f"{seq_schema}.{seq_name}"
                                     create_seq_sql = f"CREATE SEQUENCE {clean_full_name}"
                                     logger.info(f"正在创建序列，SQL: {create_seq_sql}")
                                     conn.execute(sql_text(create_seq_sql))
                                     logger.info(f"序列创建成功: {clean_full_name}")
                                     
                                     # 验证序列是否真正创建成功
                                     verify_result = conn.execute(sql_text(check_seq_sql)).scalar()
                                     logger.info(f"序列创建验证结果: {verify_result}")
                                 else:
                                     logger.info(f"序列 {seq_schema}.{seq_name} 已存在，跳过创建")
                                     
                             except Exception as seq_error:
                                 logger.error(f"Failed to create sequence {full_sequence_name}: {seq_error}")
                                 logger.error(f"Sequence creation error details: {traceback.format_exc()}")
                                 # 记录失败的列，稍后转换为BIGSERIAL
                                 failed_columns[column.name] = {
                                     'column': column,
                                     'sequence_name': full_sequence_name,
                                     'error': str(seq_error),
                                     'default_str': default_str
                                 }
                                 logger.info(f"Added column {column.name} to failed_columns list")
                         else:
                             logger.warning(f"Could not extract sequence name from default: {default_str}")
                     else:
                         logger.debug(f"Column {column.name} does not use sequence (default: {default_str})")
                 else:
                     logger.debug(f"Column {column.name} has no default value")
             
             logger.info(f"Sequence creation completed. Found {sequence_columns_found} sequence columns, {len(failed_columns)} failed")
                                         
         except Exception as e:
             logger.error(f"Failed to create sequences for {schema_name}.{table_name}: {e}")
             logger.error(f"Sequence creation exception details: {traceback.format_exc()}")
             
         return failed_columns
     
    def _extract_sequence_name(self, default_str: str, schema_name: str, table_name: str, column_name: str) -> tuple:
         """从默认值字符串中提取序列名，返回(完整序列名, 序列名)"""
         logger.info(f"Extracting sequence name from default_str: {default_str}")
         
         try:
             # 常见的序列命名模式
             if 'nextval' in default_str.lower():
                 # 尝试从字符串中提取序列名
                 import re
                 
                 # 匹配多种模式：
                 # nextval('schema.sequence_name'::regclass)
                 # nextval('"schema".sequence_name'::regclass)
                 # nextval('sequence_name'::regclass)
                 patterns = [
                     r"nextval\(['\"]([^'\"]+)['\"].*\)",  # 基本模式
                     r"nextval\(['\"]\\?\"?([^'\"]+)\\?\"?['\"].*\)",  # 处理转义引号
                     r"nextval\('([^']+)'.*\)",  # 单引号模式
                     r"nextval\(\"([^\"]+)\".*\)"  # 双引号模式
                 ]
                 
                 logger.info(f"Trying to match patterns against: {default_str}")
                 
                 for i, pattern in enumerate(patterns):
                     logger.debug(f"Trying pattern {i+1}: {pattern}")
                     match = re.search(pattern, default_str)
                     if match:
                         full_sequence_name = match.group(1)
                         logger.info(f"Pattern {i+1} matched, extracted: {full_sequence_name}")
                         
                         # 清理可能的转义字符和引号
                         full_sequence_name = full_sequence_name.replace('\\"', '').replace('"', '')
                         logger.info(f"After cleaning: {full_sequence_name}")
                         
                         # 如果包含schema，分别提取schema和序列名
                         if '.' in full_sequence_name:
                             parts = full_sequence_name.split('.')
                             extracted_schema = parts[0].strip('"').strip("'")
                             sequence_name = parts[-1].strip('"').strip("'")
                             # 确保返回的格式不包含引号
                             result = f"{extracted_schema}.{sequence_name}", sequence_name
                             logger.info(f"Schema found, returning: {result}")
                             return result
                         else:
                             # 没有schema，使用当前schema
                             clean_sequence_name = full_sequence_name.strip('"').strip("'")
                             result = f"{schema_name}.{clean_sequence_name}", clean_sequence_name
                             logger.info(f"No schema found, using current schema: {result}")
                             return result
                 
                 # 如果无法解析，使用标准命名约定
                 logger.warning(f"No pattern matched, using standard naming convention")
                 standard_name = f"{table_name}_{column_name}_seq"
                 result = f"{schema_name}.{standard_name}", standard_name
                 logger.info(f"Standard naming result: {result}")
                 return result
             
             logger.info(f"No nextval found in default_str, returning None")
             return None, None
             
         except Exception as e:
             logger.error(f"Failed to extract sequence name from {default_str}: {e}")
             logger.error(f"Sequence name extraction error details: {traceback.format_exc()}")
             # 返回标准命名约定
             standard_name = f"{table_name}_{column_name}_seq"
             result = f"{schema_name}.{standard_name}", standard_name
             logger.info(f"Exception fallback result: {result}")
             return result
     
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
            
            # 获取总记录数用于准确的进度计算
            total_count = self._get_table_record_count(target_table, query)
            
            self._update_log(f"开始同步表 {full_table_name}，预计记录数: {total_count}，查询: {query}")
            
            # 更新进度信息，包含总记录数
            self.current_progress.update({
                'current_table_total_records': total_count,
                'current_table_processed_records': 0
            })
            self._notify_progress()
            
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
                        # 检查任务是否被取消（在批量插入前再次检查）
                        if self._check_if_cancelled():
                            self._update_log(f"表 {full_table_name} 批量插入被用户取消")
                            raise ValueError("Task cancelled by user")
                            
                        # 批量插入数据
                        self._batch_insert(schema_name, table_name, columns, batch_data)
                        total_records += len(batch_data)
                        
                        # 更新记录处理进度
                        self.current_progress['records_processed'] = self.current_progress.get('records_processed', 0) + len(batch_data)
                        self.current_progress['current_table_processed_records'] = self.current_progress.get('current_table_processed_records', 0) + len(batch_data)
                        
                        # 计算当前表的进度百分比
                        if self.current_progress.get('current_table_total_records', 0) > 0:
                            table_percentage = int((self.current_progress['current_table_processed_records'] / self.current_progress['current_table_total_records']) * 100)
                            self.current_progress['current_table_percentage'] = min(table_percentage, 100)
                        
                        self._notify_progress()
                        
                        batch_data = []
                
                # 处理剩余数据
                if batch_data:
                    self._batch_insert(schema_name, table_name, columns, batch_data)
                    total_records += len(batch_data)
                    
                    # 更新最终记录处理进度
                    self.current_progress['records_processed'] = self.current_progress.get('records_processed', 0) + len(batch_data)
                    self.current_progress['current_table_processed_records'] = self.current_progress.get('current_table_processed_records', 0) + len(batch_data)
                    
                    # 计算当前表的进度百分比
                    if self.current_progress.get('current_table_total_records', 0) > 0:
                        table_percentage = int((self.current_progress['current_table_processed_records'] / self.current_progress['current_table_total_records']) * 100)
                        self.current_progress['current_table_percentage'] = min(table_percentage, 100)
                    
                    self._notify_progress()
                
                # 更新增量同步值（如果是增量模式且有增量字段）
                if (target_table.incremental_strategy in [IncrementalStrategy.AUTO_ID, IncrementalStrategy.AUTO_TIMESTAMP] 
                    and incremental_field and all_data):
                    self._update_last_sync_value(target_table, incremental_field, all_data)
                
                self._update_log(f"表 {full_table_name} 同步完成，共处理 {total_records} 条记录")
                return total_records
                
        except Exception as e:
            raise Exception(f"Failed to sync data for {schema_name}.{table_name}: {e}")
    
    def _get_table_record_count(self, target_table, query: str) -> int:
        """
        获取表的总记录数用于进度计算
        
        Args:
            target_table: JobTargetTable对象
            query: 同步查询SQL
            
        Returns:
            int: 记录总数
        """
        try:
            # 将原查询包装为COUNT查询
            count_query = f"SELECT COUNT(*) FROM ({query}) AS count_subquery"
            
            with self.source_engine.connect() as source_conn:
                result = source_conn.execute(text(count_query))
                row = result.fetchone()
                total_count = row[0] if row else 0
                
                logger.info(f"Table {target_table.schema_name}.{target_table.table_name} total records: {total_count}")
                return total_count
                
        except Exception as e:
            logger.warning(f"Failed to get record count for {target_table.schema_name}.{target_table.table_name}: {e}")
            # 如果无法获取总数，返回0，这样就不会显示百分比
            return 0
    
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
            # 检查会话状态，如果已回滚则重新开始事务
            if self.db_session.is_active and self.db_session.in_transaction():
                # 会话处于事务中但可能已回滚，尝试刷新对象
                try:
                    self.db_session.refresh(self.log_entry)
                except Exception:
                    # 如果刷新失败，说明会话已回滚，需要重新开始
                    self.db_session.rollback()
                    # 重新获取日志对象
                    self.log_entry = self.db_session.query(JobExecutionLog).filter(
                        JobExecutionLog.id == self.log_entry.id
                    ).first()
                    if not self.log_entry:
                        logger.error("Log entry not found after rollback")
                        return
            
            current_log = self.log_entry.log_details or ""
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            new_log = f"{current_log}\n[{timestamp}] {message}"
            
            self.log_entry.log_details = new_log
            self.db_session.commit()
            
        except Exception as e:
            logger.error(f"Failed to update log: {e}")
            # 确保会话处于可用状态
            try:
                self.db_session.rollback()
            except:
                pass
    
    def _mark_success(self):
        """标记任务执行成功"""
        try:
            # 检查会话状态，如果已回滚则重新开始事务
            if self.db_session.is_active and self.db_session.in_transaction():
                try:
                    self.db_session.refresh(self.log_entry)
                except Exception:
                    # 如果刷新失败，说明会话已回滚，需要重新开始
                    self.db_session.rollback()
                    # 重新获取日志对象
                    self.log_entry = self.db_session.query(JobExecutionLog).filter(
                        JobExecutionLog.id == self.log_entry.id
                    ).first()
                    if not self.log_entry:
                        logger.error("Log entry not found after rollback")
                        return
            
            # 设置结束时间和状态
            self.log_entry.status = ExecutionStatus.SUCCESS
            self.log_entry.end_time = func.now()  # 使用数据库时间
            
            # 更新任务的最后运行时间
            self.job.last_run_at = func.now()  # 使用数据库时间
            
            # 先提交状态更新
            self.db_session.commit()
            
            # 然后更新日志
            self._update_log("任务执行成功完成")
            
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
            # 确保会话处于可用状态
            try:
                self.db_session.rollback()
            except:
                pass
    
    def _mark_failure(self, error_message: str, error_traceback: str):
        """标记任务执行失败"""
        try:
            # 检查会话状态，如果已回滚则重新开始事务
            if self.db_session.is_active and self.db_session.in_transaction():
                try:
                    self.db_session.refresh(self.log_entry)
                except Exception:
                    # 如果刷新失败，说明会话已回滚，需要重新开始
                    self.db_session.rollback()
                    # 重新获取日志对象
                    self.log_entry = self.db_session.query(JobExecutionLog).filter(
                        JobExecutionLog.id == self.log_entry.id
                    ).first()
                    if not self.log_entry:
                        logger.error("Log entry not found after rollback")
                        return
            
            # 设置状态和错误信息
            self.log_entry.status = ExecutionStatus.FAILED
            self.log_entry.end_time = func.now()  # 使用数据库时间
            self.log_entry.error_message = error_message
            self.log_entry.error_traceback = error_traceback
            
            # 先提交状态更新
            self.db_session.commit()
            
            # 然后更新日志
            self._update_log(f"任务执行失败: {error_message}")
            
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
            # 确保会话处于可用状态
            try:
                self.db_session.rollback()
            except:
                pass
    
    def _mark_cancelled(self, message: str = "任务已被用户取消"):
        """
        标记任务为取消状态
        
        Args:
            message: 取消消息
        """
        try:
            # 检查会话状态，如果已回滚则重新开始事务
            if self.db_session.is_active and self.db_session.in_transaction():
                try:
                    self.db_session.refresh(self.log_entry)
                except Exception:
                    # 如果刷新失败，说明会话已回滚，需要重新开始
                    self.db_session.rollback()
                    # 重新获取日志对象
                    self.log_entry = self.db_session.query(JobExecutionLog).filter(
                        JobExecutionLog.id == self.log_entry.id
                    ).first()
                    if not self.log_entry:
                        logger.error("Log entry not found after rollback")
                        return
            
            # 设置状态和取消信息
            self.log_entry.status = ExecutionStatus.CANCELLED
            self.log_entry.end_time = func.now()  # 使用数据库时间
            self.log_entry.error_message = message
            
            # 先提交状态更新
            self.db_session.commit()
            
            # 然后更新日志
            self._update_log(f"任务已取消: {message}")
            
            # 刷新日志对象获取数据库计算的时间
            self.db_session.refresh(self.log_entry)
            
            # 计算执行时间（秒）
            if self.log_entry.start_time and self.log_entry.end_time:
                duration = (self.log_entry.end_time - self.log_entry.start_time).total_seconds()
                self.log_entry.duration_seconds = int(duration)
                self.db_session.commit()
            
            # 更新进度信息
            self.current_progress.update({
                'stage': 'cancelled',
                'message': message
            })
            self._notify_progress()
            
            logger.info(f"Job {self.job.id} marked as cancelled")
            
        except Exception as e:
            logger.error(f"Failed to mark job as cancelled: {e}")
            # 确保会话处于可用状态
            try:
                self.db_session.rollback()
            except:
                pass
    
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

# def execute_sync_job(job_id: int, progress_callback: Optional[callable] = None) -> bool:
#     """
#     执行同步任务的入口函数
    
#     Args:
#         job_id: 任务ID
#         progress_callback: 进度回调函数
        
#     Returns:
#         bool: 执行是否成功
#     """
#     try:
#         # 获取任务信息
#         db = SessionLocal()
#         try:
#             job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
#             if not job:
#                 logger.error(f"Job {job_id} not found")
#                 return False
            
#             # 检查任务是否已在运行
#             if job.is_running:
#                 logger.warning(f"Job {job_id} is already running, skipping")
#                 return False
            
#             # 标记任务为运行中
#             job.is_running = True
#             db.commit()
            
#         finally:
#             db.close()
        
#         # 执行同步
#         sync_engine = SyncEngine(job_id, progress_callback)
#         success = sync_engine.execute()
        
#         # 更新任务状态
#         db = SessionLocal()
#         try:
#             job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
#             if job:
#                 job.is_running = False
#                 db.commit()
#         finally:
#             db.close()
        
#         return success
        
#     except Exception as e:
#         logger.error(f"Failed to execute sync job {job_id}: {e}")
        
#         # 确保清除运行状态
#         try:
#             db = SessionLocal()
#             try:
#                 job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
#                 if job:
#                     job.is_running = False
#                     db.commit()
#             finally:
#                 db.close()
#         except:
#             pass
        
#         return False
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
        
        # ========== 选择同步引擎 ==========
        # 方案1: 使用原始流式同步引擎 (注释掉下面的COPY模式代码来使用)
        # sync_engine = SyncEngine(job_id, progress_callback)
        # success = sync_engine.execute()
        
        # 方案2: 使用COPY高性能同步引擎 (取消注释下面的代码来使用)
        try:
            from sync.sync_engine_with_copy import EnhancedSyncEngine
            from sync.transfer_config import TransferMode
            
            # 使用COPY模式的增强同步引擎
            sync_engine = EnhancedSyncEngine(
                job_id=job_id, 
                progress_callback=progress_callback,
                transfer_mode=TransferMode.COPY  # 强制使用COPY模式
            )
            logger.info(f"Job {job_id}: Using COPY mode for high performance sync")
            success = sync_engine.execute()
            
        except ImportError as e:
            logger.warning(f"COPY mode not available, falling back to stream mode: {e}")
            # 回退到原始同步引擎
            sync_engine = SyncEngine(job_id, progress_callback)
            success = sync_engine.execute()
        except Exception as e:
            logger.error(f"COPY mode failed, falling back to stream mode: {e}")
            # 回退到原始同步引擎
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