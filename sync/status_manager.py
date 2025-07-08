"""任务状态管理器
负责任务状态的创建、更新和查询，与日志分离以提高性能
"""
import logging
from typing import Optional
from sqlalchemy.orm import Session
from models.job_execution_status import JobExecutionStatus, TaskControlStatus
from models.job_execution_logs import JobExecutionLog

logger = logging.getLogger(__name__)

class StatusManager:
    """任务状态管理器"""
    
    def __init__(self, db_session: Session):
        self.db_session = db_session
    
    def create_execution_status(self, job_id: int, execution_log_id: Optional[int] = None) -> JobExecutionStatus:
        """创建任务执行状态记录"""
        try:
            status = JobExecutionStatus(
                job_id=job_id,
                execution_log_id=execution_log_id,
                status=TaskControlStatus.RUNNING,
                is_cancellation_requested=False,
                current_stage="initializing",
                progress_percentage=0
            )
            
            self.db_session.add(status)
            self.db_session.flush()  # 获取ID但不提交
            self.db_session.refresh(status)
            
            logger.info(f"Created execution status for job {job_id}, status_id: {status.id}")
            return status
            
        except Exception as e:
            logger.error(f"Failed to create execution status for job {job_id}: {e}")
            self.db_session.rollback()
            raise
    
    def update_progress(self, status_id: int, stage: str, percentage: int) -> bool:
        """更新任务进度"""
        try:
            status = self.db_session.query(JobExecutionStatus).filter(
                JobExecutionStatus.id == status_id
            ).first()
            
            if not status:
                logger.warning(f"Execution status {status_id} not found")
                return False
            
            status.current_stage = stage
            status.progress_percentage = min(100, max(0, percentage))
            
            self.db_session.commit()
            return True
            
        except Exception as e:
            logger.error(f"Failed to update progress for status {status_id}: {e}")
            self.db_session.rollback()
            return False
    
    def check_cancellation_requested(self, status_id: int) -> bool:
        """检查是否请求取消任务（高性能查询）"""
        try:
            # 只查询必要的字段，避免加载大量日志数据
            result = self.db_session.query(JobExecutionStatus.is_cancellation_requested).filter(
                JobExecutionStatus.id == status_id
            ).scalar()
            
            return result is True
            
        except Exception as e:
            logger.warning(f"Failed to check cancellation status for {status_id}: {e}")
            return False
    
    def request_cancellation(self, status_id: int) -> bool:
        """请求取消任务"""
        try:
            status = self.db_session.query(JobExecutionStatus).filter(
                JobExecutionStatus.id == status_id
            ).first()
            
            if not status:
                logger.warning(f"Execution status {status_id} not found")
                return False
            
            status.request_cancellation()
            self.db_session.commit()
            
            logger.info(f"Cancellation requested for status {status_id}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to request cancellation for status {status_id}: {e}")
            self.db_session.rollback()
            return False
    
    def mark_completed(self, status_id: int) -> bool:
        """标记任务完成"""
        try:
            status = self.db_session.query(JobExecutionStatus).filter(
                JobExecutionStatus.id == status_id
            ).first()
            
            if not status:
                logger.warning(f"Execution status {status_id} not found")
                return False
            
            status.mark_completed()
            self.db_session.commit()
            
            logger.info(f"Marked status {status_id} as completed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark status {status_id} as completed: {e}")
            self.db_session.rollback()
            return False
    
    def mark_failed(self, status_id: int) -> bool:
        """标记任务失败"""
        try:
            status = self.db_session.query(JobExecutionStatus).filter(
                JobExecutionStatus.id == status_id
            ).first()
            
            if not status:
                logger.warning(f"Execution status {status_id} not found")
                return False
            
            status.mark_failed()
            self.db_session.commit()
            
            logger.info(f"Marked status {status_id} as failed")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark status {status_id} as failed: {e}")
            self.db_session.rollback()
            return False
    
    def mark_stopped(self, status_id: int) -> bool:
        """标记任务已停止"""
        try:
            status = self.db_session.query(JobExecutionStatus).filter(
                JobExecutionStatus.id == status_id
            ).first()
            
            if not status:
                logger.warning(f"Execution status {status_id} not found")
                return False
            
            status.mark_stopped()
            self.db_session.commit()
            
            logger.info(f"Marked status {status_id} as stopped")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark status {status_id} as stopped: {e}")
            self.db_session.rollback()
            return False
    
    def mark_cancelled(self, status_id: int) -> bool:
        """标记任务已取消"""
        try:
            status = self.db_session.query(JobExecutionStatus).filter(
                JobExecutionStatus.id == status_id
            ).first()
            
            if not status:
                logger.warning(f"Execution status {status_id} not found")
                return False
            
            status.mark_stopped()  # 取消也使用stopped状态
            self.db_session.commit()
            
            logger.info(f"Marked status {status_id} as cancelled")
            return True
            
        except Exception as e:
            logger.error(f"Failed to mark status {status_id} as cancelled: {e}")
            self.db_session.rollback()
            return False
    
    def is_cancelled(self, status_id: int) -> bool:
        """检查任务是否已被取消"""
        return self.check_cancellation_requested(status_id)
    
    def get_status_by_log_id(self, log_id: int) -> Optional[JobExecutionStatus]:
        """根据日志ID获取状态记录"""
        try:
            return self.db_session.query(JobExecutionStatus).filter(
                JobExecutionStatus.execution_log_id == log_id
            ).first()
        except Exception as e:
            logger.error(f"Failed to get status by log_id {log_id}: {e}")
            return None
    
    def cleanup_old_statuses(self, days: int = 30) -> int:
        """清理旧的状态记录"""
        try:
            from datetime import datetime, timedelta
            cutoff_date = datetime.utcnow() - timedelta(days=days)
            
            deleted_count = self.db_session.query(JobExecutionStatus).filter(
                JobExecutionStatus.created_at < cutoff_date,
                JobExecutionStatus.status.in_([
                    TaskControlStatus.COMPLETED,
                    TaskControlStatus.FAILED,
                    TaskControlStatus.STOPPED
                ])
            ).delete()
            
            self.db_session.commit()
            logger.info(f"Cleaned up {deleted_count} old status records")
            return deleted_count
            
        except Exception as e:
            logger.error(f"Failed to cleanup old statuses: {e}")
            self.db_session.rollback()
            return 0