"""
任务调度器管理
"""
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.executors.pool import ThreadPoolExecutor
from apscheduler.events import EVENT_JOB_EXECUTED, EVENT_JOB_ERROR
from datetime import datetime
from sqlalchemy.orm import Session

from config import settings
from database import get_db, SessionLocal, engine
from models.backup_jobs import BackupJob, JobStatus, ExecutionMode
from models.job_execution_logs import JobExecutionLog, ExecutionStatus
from sync_engine import execute_sync_job
from sqlalchemy import text
from progress_manager import progress_manager

logger = logging.getLogger(__name__)

class SchedulerManager:
    """调度器管理器"""
    
    def __init__(self):
        """初始化调度器"""
        # 配置作业存储
        print("settings.database_url",settings.database_url)
        jobstores = {
            'default': SQLAlchemyJobStore(
                url=settings.database_url,
                tablename='apscheduler_jobs',
                metadata=None
            )
        }
        
        # 配置执行器
        executors = {
            'default': ThreadPoolExecutor(20),
        }
        
        # 作业默认配置
        job_defaults = {
            'coalesce': False,
            'max_instances': 1
        }
        
        # 创建调度器
        self.scheduler = AsyncIOScheduler(
            jobstores=jobstores,
            executors=executors,
            job_defaults=job_defaults,
            timezone='Asia/Shanghai'
        )
        
        # 添加事件监听器
        self.scheduler.add_listener(self._job_executed, EVENT_JOB_EXECUTED)
        self.scheduler.add_listener(self._job_error, EVENT_JOB_ERROR)
        
        # 创建 APScheduler 表
        self._create_apscheduler_table()
        
        self._running = False
    
    def start(self):
        """启动调度器"""
        try:
            self.scheduler.start()
            self._running = True
            logger.info("Scheduler started successfully")
            
            # 加载现有的活跃任务
            self._load_active_jobs()
            
        except Exception as e:
            logger.error(f"Failed to start scheduler: {e}")
            raise
    
    def shutdown(self):
        """关闭调度器"""
        try:
            if self._running:
                self.scheduler.shutdown(wait=True)
                self._running = False
                logger.info("Scheduler shutdown successfully")
        except Exception as e:
            logger.error(f"Error during scheduler shutdown: {e}")
    
    def is_running(self) -> bool:
        """检查调度器是否运行中"""
        return self._running and self.scheduler.running
    
    def add_job(self, job: BackupJob):
        """添加任务到调度器"""
        try:
            # 只处理调度执行模式的任务
            if job.execution_mode != ExecutionMode.SCHEDULED:
                logger.info(f"Job '{job.name}' (ID: {job.id}) is set to {job.execution_mode.value} mode, skipping scheduler")
                return
            
            # 检查cron表达式是否存在
            if not job.cron_expression or not job.cron_expression.strip():
                logger.warning(f"Job '{job.name}' (ID: {job.id}) has no cron expression, skipping")
                return
            
            job_id = f"backup_job_{job.id}"
            
            # 移除已存在的任务
            if self.scheduler.get_job(job_id):
                self.scheduler.remove_job(job_id)
            
            # 添加新任务
            self.scheduler.add_job(
                func=execute_sync_job,
                trigger='cron',
                args=[job.id, progress_manager.update_progress],
                id=job_id,
                name=job.name,
                **self._parse_cron_expression(job.cron_expression),
                timezone=job.timezone,
                replace_existing=True
            )
            
            logger.info(f"Job '{job.name}' (ID: {job.id}) added to scheduler with cron: {job.cron_expression}")
            
        except Exception as e:
            logger.error(f"Failed to add job {job.id} to scheduler: {e}")
            raise
    
    def remove_job(self, job_id: int):
        """从调度器中移除任务"""
        try:
            scheduler_job_id = f"backup_job_{job_id}"
            if self.scheduler.get_job(scheduler_job_id):
                self.scheduler.remove_job(scheduler_job_id)
                logger.info(f"Job {job_id} removed from scheduler")
        except Exception as e:
            logger.error(f"Failed to remove job {job_id} from scheduler: {e}")
    
    def _load_active_jobs(self):
        """加载所有活跃的任务"""
        try:
            db = SessionLocal()
            try:
                active_jobs = db.query(BackupJob).filter(
                    BackupJob.status == JobStatus.ACTIVE
                ).all()
                
                for job in active_jobs:
                    self.add_job(job)
                
                logger.info(f"Loaded {len(active_jobs)} active jobs")
                
            finally:
                db.close()
                
        except Exception as e:
            logger.error(f"Failed to load active jobs: {e}")
    
    def _parse_cron_expression(self, cron_expr: str) -> dict:
        """解析cron表达式"""
        try:
            parts = cron_expr.strip().split()
            if len(parts) != 5:
                raise ValueError("Cron expression must have 5 parts")
            
            return {
                'minute': parts[0],
                'hour': parts[1],
                'day': parts[2],
                'month': parts[3],
                'day_of_week': parts[4]
            }
        except Exception as e:
            logger.error(f"Invalid cron expression '{cron_expr}': {e}")
            raise
    
    def _job_executed(self, event):
        """任务执行成功事件处理"""
        logger.info(f"Job {event.job_id} executed successfully")
    
    def _job_error(self, event):
        """任务执行错误事件处理"""
        logger.error(f"Job {event.job_id} failed: {event.exception}")
    
    def _create_apscheduler_table(self):
        """创建 APScheduler 所需的表"""
        try:
            with engine.connect() as conn:
                # 检查表是否存在
                result = conn.execute(text("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables 
                        WHERE table_name = 'apscheduler_jobs'
                    )
                """))
                
                table_exists = result.scalar()
                
                if not table_exists:
                    # 创建 APScheduler 表
                    conn.execute(text("""
                        CREATE TABLE apscheduler_jobs (
                            id VARCHAR(191) NOT NULL,
                            next_run_time DOUBLE PRECISION,
                            job_state BYTEA NOT NULL,
                            PRIMARY KEY (id)
                        )
                    """))
                    conn.commit()
                    logger.info("APScheduler table created successfully")
                else:
                    logger.info("APScheduler table already exists")
                    
        except Exception as e:
            logger.error(f"Failed to create APScheduler table: {e}")
            # 不抛出异常，让程序继续运行

# 全局调度器管理器实例
scheduler_manager = SchedulerManager()
