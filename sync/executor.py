# engine/executor.py
"""
同步任务的入口函数，包含处理并发的原子锁。
"""
import logging
from typing import Optional

from database import SessionLocal
from models.backup_jobs import BackupJob
from models.job_execution_logs import ExecutionStatus

# 使用相对路径导入
from .orchestrator import SyncEngine

logger = logging.getLogger(__name__)


def execute_sync_job(job_id: int, progress_callback: Optional[callable] = None) -> bool:
    """执行同步任务的入口函数，包含处理并发的原子锁。"""
    job_to_run = None
    with SessionLocal() as db:
        try:
            job_to_run = db.query(BackupJob).filter(BackupJob.id == job_id).with_for_update(nowait=True).first()

            if not job_to_run:
                logger.error(f"Job {job_id} not found.")
                return False

            if job_to_run.is_running:
                logger.warning(f"Job {job_id} is already running (detected by lock). Skipping execution.")
                return False

            job_to_run.is_running = True
            db.commit()

        except Exception as e:
            logger.warning(f"Could not acquire lock for job {job_id}, it is likely already running. Error: {e}")
            return False

    sync_engine = None
    try:
        logger.info(f"Starting sync engine for job {job_id}.")
        sync_engine = SyncEngine(job_id, progress_callback)
        sync_engine.execute()
        return sync_engine.log_entry.status == ExecutionStatus.SUCCESS
    except Exception as e:
        logger.critical(f"Unhandled exception during SyncEngine execution for job {job_id}: {e}", exc_info=True)
        return False
    finally:
        with SessionLocal() as db:
            job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
            if job:
                job.is_running = False
                db.commit()
                logger.info(f"Released running flag for job {job_id}.")