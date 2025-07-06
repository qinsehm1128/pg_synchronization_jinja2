"""
执行日志管理API路由
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc
from pydantic import BaseModel
from datetime import datetime

from database import get_db
from models.job_execution_logs import JobExecutionLog, ExecutionStatus
from models.backup_jobs import BackupJob

router = APIRouter()

# Pydantic模型
class JobExecutionLogResponse(BaseModel):
    id: int
    job_id: int
    job_name: str
    status: ExecutionStatus
    start_time: str
    end_time: Optional[str]
    duration_seconds: Optional[int]
    tables_processed: int
    records_transferred: int
    log_details: Optional[str]
    error_message: Optional[str]
    error_traceback: Optional[str]
    
    class Config:
        from_attributes = True

class LogSummaryResponse(BaseModel):
    total_executions: int
    successful_executions: int
    failed_executions: int
    running_executions: int
    success_rate: float

@router.get("/", response_model=List[JobExecutionLogResponse])
async def list_logs(
    skip: int = 0,
    limit: int = 50,
    job_id: Optional[int] = None,
    status: Optional[ExecutionStatus] = None,
    db: Session = Depends(get_db)
):
    """获取执行日志列表"""
    query = db.query(JobExecutionLog).join(BackupJob)
    
    if job_id:
        query = query.filter(JobExecutionLog.job_id == job_id)
    
    if status:
        query = query.filter(JobExecutionLog.status == status)
    
    logs = query.order_by(desc(JobExecutionLog.start_time)).offset(skip).limit(limit).all()
    
    # 转换为响应格式
    result = []
    for log in logs:
        job = db.query(BackupJob).filter(BackupJob.id == log.job_id).first()
        job_name = job.name if job else "Unknown"
        
        result.append(JobExecutionLogResponse(
            id=log.id,
            job_id=log.job_id,
            job_name=job_name,
            status=log.status,
            start_time=log.start_time.isoformat(),
            end_time=log.end_time.isoformat() if log.end_time else None,
            duration_seconds=log.duration_seconds,
            tables_processed=log.tables_processed,
            records_transferred=log.records_transferred,
            log_details=log.log_details,
            error_message=log.error_message,
            error_traceback=log.error_traceback
        ))
    
    return result

@router.get("/{log_id}", response_model=JobExecutionLogResponse)
async def get_log(log_id: int, db: Session = Depends(get_db)):
    """获取单个执行日志"""
    log = db.query(JobExecutionLog).filter(JobExecutionLog.id == log_id).first()
    
    if not log:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution log not found"
        )
    
    job = db.query(BackupJob).filter(BackupJob.id == log.job_id).first()
    job_name = job.name if job else "Unknown"
    
    return JobExecutionLogResponse(
        id=log.id,
        job_id=log.job_id,
        job_name=job_name,
        status=log.status,
        start_time=log.start_time.isoformat(),
        end_time=log.end_time.isoformat() if log.end_time else None,
        duration_seconds=log.duration_seconds,
        tables_processed=log.tables_processed,
        records_transferred=log.records_transferred,
        log_details=log.log_details,
        error_message=log.error_message,
        error_traceback=log.error_traceback
    )

@router.get("/job/{job_id}", response_model=List[JobExecutionLogResponse])
async def get_job_logs(
    job_id: int,
    skip: int = 0,
    limit: int = 20,
    db: Session = Depends(get_db)
):
    """获取指定任务的执行日志"""
    # 验证任务是否存在
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    logs = db.query(JobExecutionLog).filter(
        JobExecutionLog.job_id == job_id
    ).order_by(desc(JobExecutionLog.start_time)).offset(skip).limit(limit).all()
    
    result = []
    for log in logs:
        result.append(JobExecutionLogResponse(
            id=log.id,
            job_id=log.job_id,
            job_name=job.name,
            status=log.status,
            start_time=log.start_time.isoformat(),
            end_time=log.end_time.isoformat() if log.end_time else None,
            duration_seconds=log.duration_seconds,
            tables_processed=log.tables_processed,
            records_transferred=log.records_transferred,
            log_details=log.log_details,
            error_message=log.error_message,
            error_traceback=log.error_traceback
        ))
    
    return result

@router.get("/summary/overall", response_model=LogSummaryResponse)
async def get_log_summary(db: Session = Depends(get_db)):
    """获取执行日志统计摘要"""
    total_executions = db.query(JobExecutionLog).count()
    successful_executions = db.query(JobExecutionLog).filter(
        JobExecutionLog.status == ExecutionStatus.SUCCESS
    ).count()
    failed_executions = db.query(JobExecutionLog).filter(
        JobExecutionLog.status == ExecutionStatus.FAILED
    ).count()
    running_executions = db.query(JobExecutionLog).filter(
        JobExecutionLog.status == ExecutionStatus.RUNNING
    ).count()
    
    success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 0
    
    return LogSummaryResponse(
        total_executions=total_executions,
        successful_executions=successful_executions,
        failed_executions=failed_executions,
        running_executions=running_executions,
        success_rate=round(success_rate, 2)
    )

@router.get("/summary/job/{job_id}", response_model=LogSummaryResponse)
async def get_job_log_summary(job_id: int, db: Session = Depends(get_db)):
    """获取指定任务的执行日志统计摘要"""
    # 验证任务是否存在
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    total_executions = db.query(JobExecutionLog).filter(
        JobExecutionLog.job_id == job_id
    ).count()
    
    successful_executions = db.query(JobExecutionLog).filter(
        JobExecutionLog.job_id == job_id,
        JobExecutionLog.status == ExecutionStatus.SUCCESS
    ).count()
    
    failed_executions = db.query(JobExecutionLog).filter(
        JobExecutionLog.job_id == job_id,
        JobExecutionLog.status == ExecutionStatus.FAILED
    ).count()
    
    running_executions = db.query(JobExecutionLog).filter(
        JobExecutionLog.job_id == job_id,
        JobExecutionLog.status == ExecutionStatus.RUNNING
    ).count()
    
    success_rate = (successful_executions / total_executions * 100) if total_executions > 0 else 0
    
    return LogSummaryResponse(
        total_executions=total_executions,
        successful_executions=successful_executions,
        failed_executions=failed_executions,
        running_executions=running_executions,
        success_rate=round(success_rate, 2)
    )

@router.delete("/{log_id}")
async def delete_log(log_id: int, db: Session = Depends(get_db)):
    """删除执行日志"""
    log = db.query(JobExecutionLog).filter(JobExecutionLog.id == log_id).first()
    
    if not log:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Execution log not found"
        )
    
    if log.status == ExecutionStatus.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete a running execution log"
        )
    
    try:
        db.delete(log)
        db.commit()
        return {"message": "Execution log deleted successfully"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete execution log: {str(e)}"
        )

@router.delete("/job/{job_id}/clear")
async def clear_job_logs(
    job_id: int,
    keep_latest: int = 10,
    db: Session = Depends(get_db)
):
    """清理指定任务的历史日志"""
    # 验证任务是否存在
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    try:
        # 获取要保留的最新日志ID
        latest_logs = db.query(JobExecutionLog.id).filter(
            JobExecutionLog.job_id == job_id
        ).order_by(desc(JobExecutionLog.start_time)).limit(keep_latest).all()
        
        latest_log_ids = [log.id for log in latest_logs]
        
        # 删除旧日志（不包括正在运行的）
        query = db.query(JobExecutionLog).filter(
            JobExecutionLog.job_id == job_id,
            JobExecutionLog.status != ExecutionStatus.RUNNING
        )
        
        if latest_log_ids:
            query = query.filter(~JobExecutionLog.id.in_(latest_log_ids))
        
        deleted_count = query.count()
        query.delete(synchronize_session=False)
        db.commit()
        
        return {
            "message": f"Cleared {deleted_count} old execution logs",
            "kept_latest": keep_latest
        }
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to clear job logs: {str(e)}"
        )

@router.get("/recent/{limit}")
async def get_recent_logs(
    limit: int = 10,
    db: Session = Depends(get_db)
):
    """获取最近的执行日志"""
    if limit > 100:
        limit = 100  # 限制最大数量
    
    logs = db.query(JobExecutionLog).join(BackupJob).order_by(
        desc(JobExecutionLog.start_time)
    ).limit(limit).all()
    
    result = []
    for log in logs:
        job = db.query(BackupJob).filter(BackupJob.id == log.job_id).first()
        job_name = job.name if job else "Unknown"
        
        result.append({
            "id": log.id,
            "job_id": log.job_id,
            "job_name": job_name,
            "status": log.status.value,
            "start_time": log.start_time.isoformat(),
            "duration_seconds": log.duration_seconds,
            "records_transferred": log.records_transferred,
            "error_message": log.error_message[:100] + "..." if log.error_message and len(log.error_message) > 100 else log.error_message
        })
    
    return result
