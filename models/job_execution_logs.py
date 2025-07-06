"""
任务执行日志模型
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
import enum

class ExecutionStatus(enum.Enum):
    """执行状态枚举"""
    RUNNING = "running"    # 运行中
    SUCCESS = "success"    # 成功
    FAILED = "failed"      # 失败
    CANCELLED = "cancelled"  # 已取消

class JobExecutionLog(Base):
    """任务执行日志模型"""
    
    __tablename__ = "job_execution_logs"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("backup_jobs.id"), nullable=False, comment="任务ID")
    
    # 执行信息
    status = Column(Enum(ExecutionStatus), nullable=False, comment="执行状态")
    start_time = Column(DateTime(timezone=True), server_default=func.now(), comment="开始时间")
    end_time = Column(DateTime(timezone=True), comment="结束时间")
    duration_seconds = Column(Integer, comment="执行时长（秒）")
    
    # 同步统计
    tables_processed = Column(Integer, default=0, comment="处理的表数量")
    records_transferred = Column(Integer, default=0, comment="传输的记录数量")
    
    # 日志详情
    log_details = Column(Text, comment="详细日志")
    error_message = Column(Text, comment="错误信息")
    error_traceback = Column(Text, comment="错误堆栈")
    
    # 关系
    job = relationship("BackupJob", back_populates="execution_logs")
    
    def __repr__(self):
        return f"<JobExecutionLog(id={self.id}, job_id={self.job_id}, status='{self.status.value}')>"
