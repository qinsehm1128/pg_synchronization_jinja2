"""任务执行状态模型
专门用于任务状态控制，与日志分离以提高查询性能
"""
from sqlalchemy import Column, Integer, String, DateTime, ForeignKey, Enum, Boolean
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
import enum

class TaskControlStatus(enum.Enum):
    """任务控制状态枚举"""
    RUNNING = "running"      # 运行中
    STOP_REQUESTED = "stop_requested"  # 请求停止
    STOPPED = "stopped"      # 已停止
    COMPLETED = "completed"  # 已完成
    FAILED = "failed"        # 失败

class JobExecutionStatus(Base):
    """任务执行状态模型 - 轻量级状态控制表"""
    
    __tablename__ = "job_execution_status"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("backup_jobs.id"), nullable=False, comment="任务ID")
    execution_log_id = Column(Integer, ForeignKey("job_execution_logs.id"), nullable=True, comment="关联的执行日志ID")
    
    # 状态控制
    status = Column(Enum(TaskControlStatus), nullable=False, default=TaskControlStatus.RUNNING, comment="任务控制状态")
    is_cancellation_requested = Column(Boolean, default=False, comment="是否请求取消")
    
    # 时间戳
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")
    
    # 进度信息（轻量级）
    current_stage = Column(String(50), comment="当前阶段")
    progress_percentage = Column(Integer, default=0, comment="进度百分比")
    
    # 关系
    job = relationship("BackupJob", back_populates="execution_statuses")
    execution_log = relationship("JobExecutionLog", back_populates="execution_status")
    
    def __repr__(self):
        return f"<JobExecutionStatus(id={self.id}, job_id={self.job_id}, status='{self.status.value}')>"
    
    def request_cancellation(self):
        """请求取消任务"""
        self.is_cancellation_requested = True
        self.status = TaskControlStatus.STOP_REQUESTED
    
    def mark_completed(self):
        """标记任务完成"""
        self.status = TaskControlStatus.COMPLETED
        self.progress_percentage = 100
    
    def mark_failed(self):
        """标记任务失败"""
        self.status = TaskControlStatus.FAILED
    
    def mark_stopped(self):
        """标记任务已停止"""
        self.status = TaskControlStatus.STOPPED