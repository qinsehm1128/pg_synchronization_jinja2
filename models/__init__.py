# Models package

# 按照依赖关系顺序导入模型
# 1. 首先导入基础模型（没有外键依赖的）
from .database_connections import DatabaseConnection

# 2. 然后导入有外键依赖的模型
from .job_execution_logs import JobExecutionLog
from .job_execution_status import JobExecutionStatus
from .backup_jobs import BackupJob, JobTargetTable

# 导出所有模型
__all__ = [
    'DatabaseConnection',
    'BackupJob', 
    'JobTargetTable',
    'JobExecutionLog',
    'JobExecutionStatus'
]