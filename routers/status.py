"""任务状态管理API路由
提供基于状态表的高性能任务控制接口
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from sqlalchemy import desc
from pydantic import BaseModel
from datetime import datetime

from database import get_db
from models.job_execution_status import JobExecutionStatus, TaskControlStatus
from models.job_execution_logs import JobExecutionLog
from models.backup_jobs import BackupJob
from sync.status_manager import StatusManager
from progress_manager import progress_manager

router = APIRouter()

# Pydantic模型
class JobExecutionStatusResponse(BaseModel):
    id: int
    job_id: int
    job_name: str
    execution_log_id: Optional[int]
    status: TaskControlStatus
    is_cancellation_requested: bool
    current_stage: str
    progress_percentage: int
    created_at: str
    updated_at: str
    
    class Config:
        from_attributes = True

class StatusUpdateRequest(BaseModel):
    stage: str
    percentage: int

class CancellationRequest(BaseModel):
    reason: Optional[str] = "User requested cancellation"

@router.get("/", response_model=List[JobExecutionStatusResponse])
async def list_statuses(
    skip: int = 0,
    limit: int = 50,
    job_id: Optional[int] = None,
    status_filter: Optional[TaskControlStatus] = None,
    db: Session = Depends(get_db)
):
    """获取任务状态列表"""
    query = db.query(JobExecutionStatus)
    
    if job_id:
        query = query.filter(JobExecutionStatus.job_id == job_id)
    
    if status_filter:
        query = query.filter(JobExecutionStatus.status == status_filter)
    
    statuses = query.order_by(desc(JobExecutionStatus.created_at)).offset(skip).limit(limit).all()
    
    # 转换为响应格式
    result = []
    for status_record in statuses:
        job = db.query(BackupJob).filter(BackupJob.id == status_record.job_id).first()
        job_name = job.name if job else "Unknown"
        
        result.append(JobExecutionStatusResponse(
            id=status_record.id,
            job_id=status_record.job_id,
            job_name=job_name,
            execution_log_id=status_record.execution_log_id,
            status=status_record.status,
            is_cancellation_requested=status_record.is_cancellation_requested,
            current_stage=status_record.current_stage,
            progress_percentage=status_record.progress_percentage,
            created_at=status_record.created_at.isoformat(),
            updated_at=status_record.updated_at.isoformat()
        ))
    
    return result

@router.get("/running", response_model=List[JobExecutionStatusResponse])
async def get_running_tasks(db: Session = Depends(get_db)):
    """获取所有运行中的任务状态"""
    statuses = db.query(JobExecutionStatus).filter(
        JobExecutionStatus.status == TaskControlStatus.RUNNING
    ).order_by(desc(JobExecutionStatus.created_at)).all()
    
    result = []
    for status_record in statuses:
        job = db.query(BackupJob).filter(BackupJob.id == status_record.job_id).first()
        job_name = job.name if job else "Unknown"
        
        result.append(JobExecutionStatusResponse(
            id=status_record.id,
            job_id=status_record.job_id,
            job_name=job_name,
            execution_log_id=status_record.execution_log_id,
            status=status_record.status,
            is_cancellation_requested=status_record.is_cancellation_requested,
            current_stage=status_record.current_stage,
            progress_percentage=status_record.progress_percentage,
            created_at=status_record.created_at.isoformat(),
            updated_at=status_record.updated_at.isoformat()
        ))
    
    return result

@router.post("/{status_id}/cancel")
async def cancel_task(
    status_id: int, 
    request: CancellationRequest,
    db: Session = Depends(get_db)
):
    """取消任务（高性能接口）"""
    status_manager = StatusManager(db)
    
    # 检查状态记录是否存在
    status_record = db.query(JobExecutionStatus).filter(
        JobExecutionStatus.id == status_id
    ).first()
    
    if not status_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task status not found"
        )
    
    if status_record.status != TaskControlStatus.RUNNING:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Task is not running"
        )
    
    try:
        # 请求取消任务
        success = status_manager.request_cancellation(status_id)
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to request task cancellation"
            )
        
        # 通知进度管理器
        progress_manager.update_progress(status_record.job_id, {
            "status": "cancellation_requested",
            "message": f"Cancellation requested: {request.reason}"
        })
        
        return {
            "message": "Cancellation requested successfully",
            "status_id": status_id,
            "reason": request.reason
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cancel task: {str(e)}"
        )

@router.put("/{status_id}/progress")
async def update_progress(
    status_id: int,
    request: StatusUpdateRequest,
    db: Session = Depends(get_db)
):
    """更新任务进度"""
    status_manager = StatusManager(db)
    
    # 验证状态记录存在
    status_record = db.query(JobExecutionStatus).filter(
        JobExecutionStatus.id == status_id
    ).first()
    
    if not status_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task status not found"
        )
    
    try:
        success = status_manager.update_progress(
            status_id, 
            request.stage, 
            request.percentage
        )
        
        if not success:
            raise HTTPException(
                status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                detail="Failed to update progress"
            )
        
        # 通知进度管理器
        progress_manager.update_progress(status_record.job_id, {
            "stage": request.stage,
            "percentage": request.percentage
        })
        
        return {
            "message": "Progress updated successfully",
            "status_id": status_id,
            "stage": request.stage,
            "percentage": request.percentage
        }
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update progress: {str(e)}"
        )

@router.get("/{status_id}", response_model=JobExecutionStatusResponse)
async def get_status(status_id: int, db: Session = Depends(get_db)):
    """获取单个任务状态"""
    status_record = db.query(JobExecutionStatus).filter(
        JobExecutionStatus.id == status_id
    ).first()
    
    if not status_record:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Task status not found"
        )
    
    job = db.query(BackupJob).filter(BackupJob.id == status_record.job_id).first()
    job_name = job.name if job else "Unknown"
    
    return JobExecutionStatusResponse(
        id=status_record.id,
        job_id=status_record.job_id,
        job_name=job_name,
        execution_log_id=status_record.execution_log_id,
        status=status_record.status,
        is_cancellation_requested=status_record.is_cancellation_requested,
        current_stage=status_record.current_stage,
        progress_percentage=status_record.progress_percentage,
        created_at=status_record.created_at.isoformat(),
        updated_at=status_record.updated_at.isoformat()
    )

@router.get("/job/{job_id}", response_model=List[JobExecutionStatusResponse])
async def get_job_statuses(
    job_id: int,
    skip: int = 0,
    limit: int = 20,
    db: Session = Depends(get_db)
):
    """获取指定任务的状态记录"""
    # 验证任务是否存在
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    statuses = db.query(JobExecutionStatus).filter(
        JobExecutionStatus.job_id == job_id
    ).order_by(desc(JobExecutionStatus.created_at)).offset(skip).limit(limit).all()
    
    result = []
    for status_record in statuses:
        result.append(JobExecutionStatusResponse(
            id=status_record.id,
            job_id=status_record.job_id,
            job_name=job.name,
            execution_log_id=status_record.execution_log_id,
            status=status_record.status,
            is_cancellation_requested=status_record.is_cancellation_requested,
            current_stage=status_record.current_stage,
            progress_percentage=status_record.progress_percentage,
            created_at=status_record.created_at.isoformat(),
            updated_at=status_record.updated_at.isoformat()
        ))
    
    return result

@router.get("/check-cancellation/{status_id}")
async def check_cancellation(status_id: int, db: Session = Depends(get_db)):
    """快速检查任务是否被请求取消（高性能接口）"""
    status_manager = StatusManager(db)
    
    try:
        is_cancelled = status_manager.check_cancellation_requested(status_id)
        return {
            "status_id": status_id,
            "is_cancellation_requested": is_cancelled
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to check cancellation status: {str(e)}"
        )

@router.delete("/cleanup")
async def cleanup_old_statuses(
    days: int = 30,
    db: Session = Depends(get_db)
):
    """清理旧的状态记录"""
    status_manager = StatusManager(db)
    
    try:
        deleted_count = status_manager.cleanup_old_statuses(days)
        return {
            "message": f"Cleaned up {deleted_count} old status records",
            "deleted_count": deleted_count,
            "days": days
        }
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to cleanup old statuses: {str(e)}"
        )