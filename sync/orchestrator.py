# engine/orchestrator.py
"""
核心同步引擎协调器 (SyncEngine)。
管理同步流程的整体生命周期。
"""
import logging
import traceback
from datetime import datetime
from typing import Optional, Dict, Any, List

from sqlalchemy import create_engine, text
from sqlalchemy.orm import Session
from sqlalchemy.engine import Engine

from database import SessionLocal
from models.backup_jobs import BackupJob
from models.job_execution_logs import JobExecutionLog, ExecutionStatus
from models.job_execution_status import JobExecutionStatus, TaskControlStatus
from models.database_connections import DatabaseConnection
from utils.encryption import decrypt_connection_string

# 使用相对路径导入同个包内的模块
from .schema_manager import SchemaManager
from .data_manager import DataManager
from .status_manager import StatusManager
import psycopg2

logger = logging.getLogger(__name__)


class SyncEngine:
    """数据库同步引擎（协调器）"""

    def __init__(self, job_id: int, progress_callback: Optional[callable] = None):
        self.job_id = job_id
        self.progress_callback = progress_callback
        self.is_cancelled = False

        self.db_session: Session = SessionLocal()
        self.job: BackupJob = self.db_session.query(BackupJob).filter(BackupJob.id == self.job_id).one()
        self.log_entry: JobExecutionLog = self._create_log_entry()

        self.source_engine: Engine = self._create_db_engine(self.job.source_db_id)
        self.destination_engine: Engine = self._create_db_engine(self.job.destination_db_id)

        self.schema_manager = SchemaManager(self.source_engine, self.destination_engine)
        self.data_manager = DataManager(self.job, self.source_engine, self.destination_engine, self.db_session)
        self.status_manager = StatusManager(self.db_session)

        self.current_progress = {
            'stage': 'initializing', 'total_tables': 0, 'tables_completed': 0,
            'records_processed': 0, 'percentage': 0,
        }

    def execute(self):
        """执行同步任务"""
        status_record = None
        try:
            # 创建状态记录
            status_record = self.status_manager.create_execution_status(
                job_id=self.job_id, 
                execution_log_id=self.log_entry.id
            )
            
            self._update_log("Sync process started.")
            self._perform_sync(status_record)
            self._finalize_execution(ExecutionStatus.SUCCESS, "Sync completed successfully.")
            self.status_manager.mark_completed(status_record.id)
        except InterruptedError as e:
            logger.info(f"Sync for job {self.job_id} was cancelled.")
            self._finalize_execution(ExecutionStatus.CANCELLED, str(e))
            if status_record:
                self.status_manager.mark_cancelled(status_record.id)
        except Exception as e:
            logger.error(f"Sync job {self.job_id} failed: {e}", exc_info=True)
            self._finalize_execution(ExecutionStatus.FAILED, str(e), traceback.format_exc())
            if status_record:
                self.status_manager.mark_failed(status_record.id)
        finally:
            self._cleanup()

    def _create_log_entry(self) -> JobExecutionLog:
        """创建执行日志记录。"""
        log_entry = JobExecutionLog(job_id=self.job.id, status=ExecutionStatus.RUNNING)
        self.db_session.add(log_entry)
        self.db_session.commit()
        logger.info(f"Created log entry {log_entry.id} for job {self.job.id}")
        return log_entry

    def _create_db_engine(self, db_id: int) -> Engine:
        """根据ID创建并测试数据库引擎。"""
        db_conn_info = self.db_session.query(DatabaseConnection).filter(DatabaseConnection.id == db_id).one()
        conn_str = decrypt_connection_string(db_conn_info.connection_string_encrypted)
        engine = create_engine(conn_str, pool_pre_ping=True)
        try:
            with engine.connect() as conn:
                conn.execute(text("SELECT 1"))
            logger.info(f"Database connection successful for DB ID {db_id}")
            return engine
        except Exception as e:
            raise ConnectionError(f"Failed to connect to database ID {db_id}: {e}")

    def _check_if_cancelled(self) -> bool:
        """检查任务是否被取消。"""
        if self.is_cancelled:
            return True
        self.db_session.refresh(self.log_entry)
        if self.log_entry.status == ExecutionStatus.CANCELLED:
            self.is_cancelled = True
            return True
        return False

    def _perform_sync(self, status_record):
        """执行实际的表同步循环。"""
        target_tables = [t for t in self.job.target_tables if t.is_active]
        if not target_tables:
            raise ValueError("No active tables specified for synchronization.")

        self.current_progress.update({'stage': 'syncing', 'total_tables': len(target_tables)})
        self._notify_progress()
        total_records_synced = 0

        for i, table in enumerate(target_tables):
            # 使用状态管理器检查取消状态
            if self.status_manager.is_cancelled(status_record.id) or self._check_if_cancelled():
                raise InterruptedError("Task cancelled by user.")

            full_table_name = f"{table.schema_name}.{table.table_name}"
            self.current_progress.update({
                'current_table': full_table_name,
                'tables_completed': i,
                'percentage': int((i / len(target_tables)) * 100)
            })
            self._notify_progress()
            self._update_log(f"Processing table {i + 1}/{len(target_tables)}: {full_table_name}")

            self.schema_manager.sync_table_structure(table.schema_name, table.table_name)
            records_count = self.data_manager.sync_table_data(table, self.current_progress, lambda: self.status_manager.is_cancelled(status_record.id) or self._check_if_cancelled())
            total_records_synced += records_count

            self._update_log(f"Table {full_table_name} sync complete. Synced {records_count} records.")
            self.current_progress['records_processed'] = total_records_synced

        self.log_entry.tables_processed = len(target_tables)
        self.log_entry.records_transferred = total_records_synced
        self.db_session.commit()

    def _finalize_execution(self, status: ExecutionStatus, message: str, error_traceback: Optional[str] = None):
        """统一处理任务结束时的状态更新和日志记录。"""
        self.db_session.refresh(self.log_entry)
        self.log_entry.status = status
        self.log_entry.end_time = datetime.utcnow()
        if self.log_entry.start_time:
            self.log_entry.duration_seconds = (self.log_entry.end_time - self.log_entry.start_time).total_seconds()

        if status != ExecutionStatus.SUCCESS:
            self.log_entry.error_message = message
            self.log_entry.error_traceback = error_traceback

        self.job.last_run_at = datetime.utcnow()
        self._update_log(f"Final status: {status.value}. {message}")
        self.db_session.commit()

        self.current_progress.update({'stage': status.value.lower(), 'percentage': 100, 'message': message})
        self._notify_progress()

    def _update_log(self, message: str):
        """向数据库日志中追加条目。"""
        timestamp = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S UTC")
        self.log_entry.log_details = (self.log_entry.log_details or "") + f"\n[{timestamp}] {message}"
        self.db_session.commit()

    def _notify_progress(self):
        """通知进度更新。"""
        if self.progress_callback:
            try:
                self.progress_callback(self.job_id, self.current_progress.copy())
            except Exception as e:
                logger.warning(f"Progress callback failed: {e}")

    def _cleanup(self):
        """清理资源。"""
        if self.source_engine: self.source_engine.dispose()
        if self.destination_engine: self.destination_engine.dispose()
        if self.db_session: self.db_session.close()
        logger.info(f"Cleanup completed for job {self.job_id}")