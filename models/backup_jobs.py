"""
备份任务模型
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean, ForeignKey, Enum
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from database import Base
import enum

class JobStatus(enum.Enum):
    """任务状态枚举"""
    ACTIVE = "active"      # 激活
    INACTIVE = "inactive"  # 未激活
    PAUSED = "paused"      # 暂停

class SyncMode(enum.Enum):
    """同步模式枚举"""
    FULL = "full"          # 全量同步
    INCREMENTAL = "incremental"  # 增量同步

class ConflictStrategy(enum.Enum):
    """数据冲突处理策略枚举"""
    SKIP = "skip"          # 跳过冲突记录
    REPLACE = "replace"    # 替换已存在记录 (INSERT ... ON CONFLICT DO UPDATE)
    IGNORE = "ignore"      # 忽略冲突继续插入 (INSERT ... ON CONFLICT DO NOTHING)
    ERROR = "error"        # 遇到冲突时报错（默认行为）

class ExecutionMode(enum.Enum):
    """执行模式枚举"""
    IMMEDIATE = "immediate"    # 立即执行（不设置调度）
    SCHEDULED = "scheduled"    # 定时调度执行

class IncrementalStrategy(enum.Enum):
    """增量同步策略枚举"""
    NONE = "none"                    # 不使用增量同步（全量）
    AUTO_ID = "auto_id"              # 自动基于ID字段增量
    AUTO_TIMESTAMP = "auto_timestamp" # 自动基于时间戳字段增量
    CUSTOM_CONDITION = "custom_condition" # 自定义WHERE条件

class BackupJob(Base):
    """备份任务模型"""
    
    __tablename__ = "backup_jobs"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, comment="任务名称")
    description = Column(Text, comment="任务描述")
    
    # 数据库连接
    source_db_id = Column(Integer, ForeignKey("database_connections.id"), nullable=False, comment="源数据库ID")
    destination_db_id = Column(Integer, ForeignKey("database_connections.id"), nullable=False, comment="目标数据库ID")
    
    # 同步配置
    sync_mode = Column(Enum(SyncMode), default=SyncMode.FULL, comment="同步模式")
    conflict_strategy = Column(Enum(ConflictStrategy), default=ConflictStrategy.ERROR, comment="数据冲突处理策略")
    where_condition = Column(Text, comment="WHERE条件（增量同步时使用）")
    
    # 调度配置
    execution_mode = Column(Enum(ExecutionMode), default=ExecutionMode.SCHEDULED, comment="执行模式")
    cron_expression = Column(String(100), nullable=True, comment="Cron表达式（定时执行时必填）")
    timezone = Column(String(50), default="Asia/Shanghai", comment="时区")
    
    # 状态
    status = Column(Enum(JobStatus), default=JobStatus.ACTIVE, comment="任务状态")
    is_running = Column(Boolean, default=False, comment="是否正在运行")
    
    # 时间戳
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")
    last_run_at = Column(DateTime(timezone=True), comment="最后运行时间")
    next_run_at = Column(DateTime(timezone=True), comment="下次运行时间")
    
    # 关系
    source_db = relationship("DatabaseConnection", foreign_keys=[source_db_id])
    destination_db = relationship("DatabaseConnection", foreign_keys=[destination_db_id])
    target_tables = relationship("JobTargetTable", back_populates="job", cascade="all, delete-orphan")
    execution_logs = relationship("JobExecutionLog", back_populates="job", cascade="all, delete-orphan")
    
    def __repr__(self):
        return f"<BackupJob(id={self.id}, name='{self.name}', status='{self.status.value}')>"

class JobTargetTable(Base):
    """任务目标表模型"""
    
    __tablename__ = "job_target_tables"
    
    id = Column(Integer, primary_key=True, index=True)
    job_id = Column(Integer, ForeignKey("backup_jobs.id"), nullable=False, comment="任务ID")
    schema_name = Column(String(100), nullable=False, comment="Schema名称")
    table_name = Column(String(100), nullable=False, comment="表名称")
    is_active = Column(Boolean, default=True, comment="是否启用")
    
    # 增量同步配置（每个表独立配置）
    incremental_strategy = Column(Enum(IncrementalStrategy), default=IncrementalStrategy.NONE, comment="增量同步策略")
    incremental_field = Column(String(100), comment="增量字段名（id、created_at等）")
    custom_condition = Column(Text, comment="自定义WHERE条件")
    last_sync_value = Column(String(255), comment="上次同步的最大值（用于下次增量同步）")
    
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    
    # 关系
    job = relationship("BackupJob", back_populates="target_tables")
    
    def __repr__(self):
        return f"<JobTargetTable(id={self.id}, schema='{self.schema_name}', table='{self.table_name}')>"
