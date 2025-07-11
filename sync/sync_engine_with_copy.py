"""\n数据库同步引擎 - 集成COPY传输策略\n支持传统流式传输和高性能COPY传输的混合模式\n"""
import json
import logging
import traceback
from datetime import datetime
from typing import List, Dict, Any, Optional
from sqlalchemy import create_engine, text, MetaData, Table, Column, func
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

# 导入原有模块
from database import SessionLocal
from models.backup_jobs import BackupJob, SyncMode, ConflictStrategy, IncrementalStrategy
from models.job_execution_logs import JobExecutionLog, ExecutionStatus
from models.database_connections import DatabaseConnection
from utils.encryption import decrypt_connection_string

# 导入新的COPY传输模块
from sync.copy_data_manager import CopyDataManager, DataTransferStrategy
from sync.transfer_config import TransferConfig, TransferMode
from sync.transfer_integration import IntegratedTransferManager

logger = logging.getLogger(__name__)

class EnhancedSyncEngine:
    """增强的数据库同步引擎，支持COPY和流式传输"""
    
    def __init__(self, job_id: int, progress_callback: Optional[callable] = None, 
                 transfer_mode: Optional[TransferMode] = None):
        """
        初始化同步引擎
        
        Args:
            job_id: 备份任务ID
            progress_callback: 进度回调函数
            transfer_mode: 传输模式（COPY或STREAM），None表示自动选择
        """
        self.job_id = job_id
        self.job: BackupJob = None
        self.source_engine: Engine = None
        self.destination_engine: Engine = None
        self.log_entry: JobExecutionLog = None
        self.db_session: Session = None
        self.progress_callback = progress_callback
        self.is_cancelled = False
        
        # 传输策略相关
        self.transfer_mode = transfer_mode
        self.transfer_manager: IntegratedTransferManager = None
        self.copy_manager: CopyDataManager = None
        
        # 进度跟踪
        self.current_progress = {
            'stage': 'initializing',
            'current_table': '',
            'tables_completed': 0,
            'total_tables': 0,
            'records_processed': 0,
            'total_records': 0,
            'percentage': 0,
            'transfer_mode': 'auto',
            'performance_stats': {}
        }
    
    def _initialize_transfer_managers(self):
        """初始化传输管理器"""
        try:
            # 创建COPY数据管理器
            self.copy_manager = CopyDataManager(
                job=self.job,
                source_engine=self.source_engine,
                destination_engine=self.destination_engine,
                db_session=self.db_session
            )
            
            # 创建集成传输管理器
            self.transfer_manager = IntegratedTransferManager(
                job=self.job,
                source_engine=self.source_engine,
                destination_engine=self.destination_engine,
                db_session=self.db_session
            )
            
            # 设置传输模式
            if self.transfer_mode:
                self.transfer_manager.current_mode = self.transfer_mode
            
            self._update_log("传输管理器初始化完成")
            logger.info(f"Transfer managers initialized for job {self.job_id}")
            
        except Exception as e:
            logger.error(f"Failed to initialize transfer managers: {e}")
            raise Exception(f"传输管理器初始化失败: {e}")
    
    def execute(self) -> bool:
        """执行同步任务"""
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
            
            # 初始化传输管理器
            self._initialize_transfer_managers()
            
            # 执行同步
            self._perform_sync()
            
            # 标记成功
            self._mark_success()
            success = True
            
        except Exception as e:
            if "Task cancelled by user" in str(e) or self.is_cancelled:
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
    
    def _perform_sync(self):
        """执行数据同步 - 增强版本"""
        tables_processed = 0
        total_records = 0
        performance_stats = {}
        
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
                full_table_name = f"{schema_name}.{table_name}"
                
                # 更新当前表进度
                self.current_progress.update({
                    'current_table': full_table_name,
                    'tables_completed': i,
                    'percentage': int((i / len(target_tables)) * 100)
                })
                self._notify_progress()
                
                self._update_log(f"正在同步表: {full_table_name}")
                
                # 同步表结构
                self._sync_table_structure(schema_name, table_name)
                
                # 使用增强的数据同步方法
                records_count, table_stats = self._sync_table_data_enhanced(target_table)
                
                tables_processed += 1
                total_records += records_count
                performance_stats[full_table_name] = table_stats
                
                # 更新完成的表数量
                self.current_progress.update({
                    'tables_completed': tables_processed,
                    'records_processed': total_records,
                    'percentage': int((tables_processed / len(target_tables)) * 100),
                    'performance_stats': performance_stats
                })
                self._notify_progress()
                
                self._update_log(f"表 {full_table_name} 同步完成，传输 {records_count} 条记录 "
                               f"(模式: {table_stats.get('transfer_mode', 'unknown')}, "
                               f"耗时: {table_stats.get('duration', 0):.2f}秒)")
            
            # 更新统计信息
            self.log_entry.tables_processed = tables_processed
            self.log_entry.records_transferred = total_records
            self.db_session.commit()
            
            # 最终进度更新
            self.current_progress.update({
                'stage': 'completed',
                'percentage': 100,
                'performance_stats': performance_stats
            })
            self._notify_progress()
            
            # 输出性能统计
            self._log_performance_summary(performance_stats)
            
            logger.info(f"Enhanced sync completed for job {self.job.id}: "
                       f"{tables_processed} tables, {total_records} records")
            
        except Exception as e:
            self.current_progress.update({
                'stage': 'error',
                'error': str(e)
            })
            self._notify_progress()
            raise Exception(f"Enhanced sync operation failed: {e}")
    
    def _sync_table_data_enhanced(self, target_table) -> tuple[int, dict]:
        """增强的表数据同步方法"""
        schema_name = target_table.schema_name
        table_name = target_table.table_name
        full_table_name = f"{schema_name}.{table_name}"
        
        start_time = datetime.now()
        
        try:
            # 分析表特征并选择传输模式
            selected_mode = self.transfer_manager._select_transfer_mode(
                target_table=target_table,
                force_mode=self.transfer_mode
            )
            
            self.current_progress['transfer_mode'] = selected_mode.value
            self._notify_progress()
            
            self._update_log(f"表 {full_table_name} 选择传输模式: {selected_mode.value}")
            
            # 构建同步查询
            sync_query = self._build_sync_query(target_table)
            
            # 根据选择的模式执行同步
            if selected_mode == TransferMode.COPY:
                records_count = self._sync_with_copy_mode(target_table, sync_query)
            else:
                records_count = self._sync_with_stream_mode(target_table, sync_query)
            
            # 计算性能统计
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            stats = {
                'transfer_mode': selected_mode.value,
                'records_count': records_count,
                'duration': duration,
                'records_per_second': records_count / duration if duration > 0 else 0,
                'start_time': start_time.isoformat(),
                'end_time': end_time.isoformat()
            }
            
            return records_count, stats
            
        except Exception as e:
            end_time = datetime.now()
            duration = (end_time - start_time).total_seconds()
            
            error_stats = {
                'transfer_mode': 'error',
                'records_count': 0,
                'duration': duration,
                'error': str(e)
            }
            
            raise Exception(f"Enhanced table sync failed for {full_table_name}: {e}")

    def _sync_with_copy_mode(self, target_table, sync_query: str) -> int:
        """使用COPY模式同步数据"""
        schema_name = target_table.schema_name
        table_name = target_table.table_name
        full_table_name = f"{schema_name}.{table_name}"
        
        try:
            # 如果是全量同步，清空目标表
            if target_table.incremental_strategy == IncrementalStrategy.NONE:
                self.copy_manager._truncate_target_table(schema_name, table_name)
                self._update_log(f"已清空目标表 {full_table_name}")
            
            # 使用COPY方式传输数据
            records_count = self.copy_manager.sync_table_data(
                target_table=target_table,
                progress_tracker=self.current_progress,
                cancellation_check=self._check_if_cancelled
            )
            
            return records_count
            
        except Exception as e:
            logger.error(f"COPY mode sync failed for {full_table_name}: {e}")
            # 如果COPY模式失败，可以选择回退到流式模式
            self._update_log(f"COPY模式失败，回退到流式模式: {e}")
            return self._sync_with_stream_mode(target_table, sync_query)
    
    def _sync_with_stream_mode(self, target_table, sync_query: str) -> int:
        """使用流式模式同步数据（原有逻辑）"""
        schema_name = target_table.schema_name
        table_name = target_table.table_name
        full_table_name = f"{schema_name}.{table_name}"
        
        try:
            # 这里可以调用原有的流式同步逻辑
            # 为了简化，这里提供一个基本实现
            
            batch_size = 1000
            total_records = 0
            
            # 如果是全量同步，清空目标表
            if target_table.incremental_strategy == IncrementalStrategy.NONE:
                self._truncate_target_table(schema_name, table_name)
                self._update_log(f"已清空目标表 {full_table_name}")
            
            # 获取表列信息
            columns = self._get_table_columns(schema_name, table_name)
            
            # 分批读取和插入数据
            with self.source_engine.connect() as source_conn:
                result = source_conn.execute(text(sync_query))
                
                batch_data = []
                for row in result:
                    if self._check_if_cancelled():
                        raise ValueError("Task cancelled by user")
                    
                    # 将行转换为字典
                    row_dict = dict(zip(columns, row))
                    batch_data.append(row_dict)
                    
                    if len(batch_data) >= batch_size:
                        self._batch_insert(schema_name, table_name, columns, batch_data)
                        total_records += len(batch_data)
                        
                        # 更新进度
                        self.current_progress['records_processed'] = \
                            self.current_progress.get('records_processed', 0) + len(batch_data)
                        self._notify_progress()
                        
                        batch_data = []
                
                # 处理剩余数据
                if batch_data:
                    self._batch_insert(schema_name, table_name, columns, batch_data)
                    total_records += len(batch_data)
                    
                    self.current_progress['records_processed'] = \
                        self.current_progress.get('records_processed', 0) + len(batch_data)
                    self._notify_progress()
            
            return total_records
            
        except Exception as e:
            raise Exception(f"Stream mode sync failed for {full_table_name}: {e}")
    
    def _copy_progress_callback(self, processed_records: int, total_records: int = None):
        """COPY操作的进度回调"""
        self.current_progress['records_processed'] = \
            self.current_progress.get('records_processed', 0) + processed_records
        
        if total_records:
            self.current_progress['current_table_total_records'] = total_records
            self.current_progress['current_table_processed_records'] = processed_records
            
            if total_records > 0:
                table_percentage = int((processed_records / total_records) * 100)
                self.current_progress['current_table_percentage'] = min(table_percentage, 100)
        
        self._notify_progress()
    
    def _build_sync_query(self, target_table) -> str:
        """构建同步查询SQL"""
        schema_name = target_table.schema_name
        table_name = target_table.table_name
        full_table_name = f"{schema_name}.{table_name}"
        
        # 基础查询
        base_query = f"SELECT * FROM {full_table_name}"
        
        # 处理增量同步
        if target_table.incremental_strategy in [IncrementalStrategy.AUTO_ID, IncrementalStrategy.AUTO_TIMESTAMP]:
            incremental_field = self._detect_incremental_field(target_table)
            if incremental_field:
                last_sync_value = self._get_last_sync_value(target_table, incremental_field)
                if last_sync_value:
                    base_query += f" WHERE {incremental_field} > '{last_sync_value}'"
        
        # 添加排序
        base_query += f" ORDER BY 1"
        
        return base_query
    
    def _log_performance_summary(self, performance_stats: dict):
        """记录性能统计摘要"""
        try:
            total_records = sum(stats.get('records_count', 0) for stats in performance_stats.values())
            total_duration = sum(stats.get('duration', 0) for stats in performance_stats.values())
            
            copy_tables = [name for name, stats in performance_stats.items() 
                          if stats.get('transfer_mode') == 'COPY']
            stream_tables = [name for name, stats in performance_stats.items() 
                            if stats.get('transfer_mode') == 'STREAM']
            
            summary = f"\n=== 性能统计摘要 ===\n"
            summary += f"总记录数: {total_records:,}\n"
            summary += f"总耗时: {total_duration:.2f}秒\n"
            summary += f"平均速度: {total_records/total_duration if total_duration > 0 else 0:.0f} 记录/秒\n"
            summary += f"COPY模式表数: {len(copy_tables)}\n"
            summary += f"流式模式表数: {len(stream_tables)}\n"
            
            if copy_tables:
                copy_records = sum(performance_stats[name].get('records_count', 0) for name in copy_tables)
                copy_duration = sum(performance_stats[name].get('duration', 0) for name in copy_tables)
                summary += f"COPY模式性能: {copy_records/copy_duration if copy_duration > 0 else 0:.0f} 记录/秒\n"
            
            if stream_tables:
                stream_records = sum(performance_stats[name].get('records_count', 0) for name in stream_tables)
                stream_duration = sum(performance_stats[name].get('duration', 0) for name in stream_tables)
                summary += f"流式模式性能: {stream_records/stream_duration if stream_duration > 0 else 0:.0f} 记录/秒\n"
            
            summary += "==================\n"
            
            self._update_log(summary)
            logger.info(summary)
            
        except Exception as e:
            logger.warning(f"Failed to log performance summary: {e}")
    
    # 以下方法需要从原始SyncEngine类中复制或引用
    # 为了简化，这里只提供方法签名
    
    def _check_if_cancelled(self) -> bool:
        """检查任务是否被取消"""
        # 从原始实现复制
        pass
    
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
             raise Exception(f"创建日志记录失败: {e}")
    
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
            
            # 创建数据库引擎
            self.source_engine = create_engine(
                source_conn_str,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            self.destination_engine = create_engine(
                dest_conn_str,
                pool_pre_ping=True,
                pool_recycle=3600,
                echo=False
            )
            
            # 测试连接
            with self.source_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            with self.destination_engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            
            self._update_log("数据库连接建立成功")
            logger.info(f"Database connections established for job {self.job_id}")
            
        except Exception as e:
            logger.error(f"Failed to establish database connections: {e}")
            raise Exception(f"数据库连接失败: {e}")
    
    def _sync_table_structure(self, schema_name: str, table_name: str):
        """同步表结构"""
        # 从原始实现复制
        pass
    
    def _get_table_columns(self, schema_name: str, table_name: str) -> List[str]:
        """获取表列信息"""
        # 从原始实现复制
        pass
    
    def _batch_insert(self, schema_name: str, table_name: str, columns: List[str], data: List[Dict[str, Any]]):
        """批量插入数据"""
        # 从原始实现复制
        pass
    
    def _truncate_target_table(self, schema_name: str, table_name: str):
        """清空目标表"""
        # 从原始实现复制
        pass
    
    def _detect_incremental_field(self, target_table) -> str:
        """检测增量字段"""
        # 从原始实现复制
        pass
    
    def _get_last_sync_value(self, target_table, field_name: str) -> str:
        """获取最后同步值"""
        # 从原始实现复制
        pass
    
    def _update_log(self, message: str):
        """更新日志"""
        try:
            if self.log_entry:
                current_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                new_log_line = f"[{current_time}] {message}\n"
                
                if self.log_entry.log_details:
                    self.log_entry.log_details += new_log_line
                else:
                    self.log_entry.log_details = new_log_line
                
                self.db_session.commit()
            
            logger.info(f"Job {self.job_id}: {message}")
        except Exception as e:
            logger.warning(f"Failed to update log: {e}")
    
    def _notify_progress(self):
        """通知进度更新"""
        if self.progress_callback:
            try:
                self.progress_callback(self.job_id, self.current_progress.copy())
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")
    
    def _check_if_cancelled(self) -> bool:
        """检查任务是否被取消"""
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
        """标记任务为取消状态"""
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
    



# 便捷函数
def create_enhanced_sync_engine(job_id: int, transfer_mode: TransferMode = None, 
                               progress_callback: callable = None) -> EnhancedSyncEngine:
    """创建增强同步引擎的便捷函数"""
    return EnhancedSyncEngine(
        job_id=job_id,
        transfer_mode=transfer_mode,
        progress_callback=progress_callback
    )
