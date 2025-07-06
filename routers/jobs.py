"""
备份任务管理API路由
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel

from database import get_db
from models.backup_jobs import BackupJob, JobTargetTable, JobStatus, SyncMode, ConflictStrategy, ExecutionMode, IncrementalStrategy
from models.database_connections import DatabaseConnection
from scheduler import scheduler_manager

router = APIRouter()

# Pydantic模型
class JobTargetTableCreate(BaseModel):
    schema_name: str
    table_name: str
    is_active: bool = True
    incremental_strategy: IncrementalStrategy = IncrementalStrategy.NONE
    incremental_field: Optional[str] = None
    custom_condition: Optional[str] = None

class JobTargetTableResponse(BaseModel):
    id: int
    schema_name: str
    table_name: str
    is_active: bool
    incremental_strategy: IncrementalStrategy
    incremental_field: Optional[str]
    custom_condition: Optional[str]
    last_sync_value: Optional[str]
    
    class Config:
        from_attributes = True

class BackupJobCreate(BaseModel):
    name: str
    description: Optional[str] = None
    source_db_id: int
    destination_db_id: int
    sync_mode: SyncMode = SyncMode.FULL
    conflict_strategy: ConflictStrategy = ConflictStrategy.ERROR
    where_condition: Optional[str] = None
    execution_mode: ExecutionMode = ExecutionMode.SCHEDULED
    cron_expression: Optional[str] = None
    timezone: str = "Asia/Shanghai"
    target_tables: List[JobTargetTableCreate]

class BackupJobUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    source_db_id: Optional[int] = None
    destination_db_id: Optional[int] = None
    sync_mode: Optional[SyncMode] = None
    conflict_strategy: Optional[ConflictStrategy] = None
    where_condition: Optional[str] = None
    execution_mode: Optional[ExecutionMode] = None
    cron_expression: Optional[str] = None
    timezone: Optional[str] = None
    status: Optional[JobStatus] = None
    target_tables: Optional[List[JobTargetTableCreate]] = None

class BackupJobResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    source_db_id: int
    destination_db_id: int
    source_db_name: str
    destination_db_name: str
    sync_mode: SyncMode
    conflict_strategy: ConflictStrategy
    where_condition: Optional[str]
    execution_mode: ExecutionMode
    cron_expression: Optional[str]
    timezone: str
    status: JobStatus
    is_running: bool
    created_at: str
    updated_at: str
    last_run_at: Optional[str]
    next_run_at: Optional[str]
    target_tables: List[JobTargetTableResponse]
    
    class Config:
        from_attributes = True

@router.get("/", response_model=List[BackupJobResponse])
async def list_jobs(
    skip: int = 0,
    limit: int = 100,
    status: Optional[JobStatus] = None,
    db: Session = Depends(get_db)
):
    """获取备份任务列表"""
    query = db.query(BackupJob)
    
    if status:
        query = query.filter(BackupJob.status == status)
    
    jobs = query.offset(skip).limit(limit).all()
    
    # 转换为响应格式
    result = []
    for job in jobs:
        # 获取数据库连接名称
        source_db = db.query(DatabaseConnection).filter(
            DatabaseConnection.id == job.source_db_id
        ).first()
        dest_db = db.query(DatabaseConnection).filter(
            DatabaseConnection.id == job.destination_db_id
        ).first()
        
        result.append(BackupJobResponse(
            id=job.id,
            name=job.name,
            description=job.description,
            source_db_id=job.source_db_id,
            destination_db_id=job.destination_db_id,
            source_db_name=source_db.name if source_db else "Unknown",
            destination_db_name=dest_db.name if dest_db else "Unknown",
            sync_mode=job.sync_mode,
            conflict_strategy=job.conflict_strategy,
            where_condition=job.where_condition,
            execution_mode=job.execution_mode,
            cron_expression=job.cron_expression,
            timezone=job.timezone,
            status=job.status,
            is_running=job.is_running,
            created_at=job.created_at.isoformat(),
            updated_at=job.updated_at.isoformat(),
            last_run_at=job.last_run_at.isoformat() if job.last_run_at else None,
            next_run_at=job.next_run_at.isoformat() if job.next_run_at else None,
            target_tables=[
                JobTargetTableResponse(
                    id=table.id,
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                    is_active=table.is_active,
                    incremental_strategy=table.incremental_strategy,
                    incremental_field=table.incremental_field,
                    custom_condition=table.custom_condition,
                    last_sync_value=table.last_sync_value
                ) for table in job.target_tables
            ]
        ))
    
    return result

@router.get("/{job_id}", response_model=BackupJobResponse)
async def get_job(job_id: int, db: Session = Depends(get_db)):
    """获取单个备份任务"""
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    # 获取数据库连接名称
    source_db = db.query(DatabaseConnection).filter(
        DatabaseConnection.id == job.source_db_id
    ).first()
    dest_db = db.query(DatabaseConnection).filter(
        DatabaseConnection.id == job.destination_db_id
    ).first()
    
    return BackupJobResponse(
        id=job.id,
        name=job.name,
        description=job.description,
        source_db_id=job.source_db_id,
        destination_db_id=job.destination_db_id,
        source_db_name=source_db.name if source_db else "Unknown",
        destination_db_name=dest_db.name if dest_db else "Unknown",
        sync_mode=job.sync_mode,
        conflict_strategy=job.conflict_strategy,
        where_condition=job.where_condition,
        execution_mode=job.execution_mode,
        cron_expression=job.cron_expression,
        timezone=job.timezone,
        status=job.status,
        is_running=job.is_running,
        created_at=job.created_at.isoformat(),
        updated_at=job.updated_at.isoformat(),
        last_run_at=job.last_run_at.isoformat() if job.last_run_at else None,
        next_run_at=job.next_run_at.isoformat() if job.next_run_at else None,
        target_tables=[
            JobTargetTableResponse(
                id=table.id,
                schema_name=table.schema_name,
                table_name=table.table_name,
                is_active=table.is_active,
                incremental_strategy=table.incremental_strategy,
                incremental_field=table.incremental_field,
                custom_condition=table.custom_condition,
                last_sync_value=table.last_sync_value
            ) for table in job.target_tables
        ]
    )

@router.post("/", response_model=BackupJobResponse)
async def create_job(
    job_data: BackupJobCreate,
    db: Session = Depends(get_db)
):
    """创建新的备份任务"""
    try:
        # 验证源和目标数据库存在
        source_db = db.query(DatabaseConnection).filter(
            DatabaseConnection.id == job_data.source_db_id
        ).first()
        if not source_db:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Source database not found"
            )
        
        dest_db = db.query(DatabaseConnection).filter(
            DatabaseConnection.id == job_data.destination_db_id
        ).first()
        if not dest_db:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Destination database not found"
            )
        
        # 验证执行模式和cron表达式
        if job_data.execution_mode == ExecutionMode.SCHEDULED and not job_data.cron_expression:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cron expression is required for scheduled execution mode"
            )
        
        if job_data.execution_mode == ExecutionMode.IMMEDIATE and job_data.cron_expression:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Cron expression should not be provided for immediate execution mode"
            )
        
        # 创建备份任务
        backup_job = BackupJob(
            name=job_data.name,
            description=job_data.description,
            source_db_id=job_data.source_db_id,
            destination_db_id=job_data.destination_db_id,
            sync_mode=job_data.sync_mode,
            conflict_strategy=job_data.conflict_strategy,
            where_condition=job_data.where_condition,
            execution_mode=job_data.execution_mode,
            cron_expression=job_data.cron_expression,
            timezone=job_data.timezone
        )
        
        db.add(backup_job)
        db.flush()  # 获取ID但不提交
        
        # 添加目标表
        for table_data in job_data.target_tables:
            target_table = JobTargetTable(
                job_id=backup_job.id,
                schema_name=table_data.schema_name,
                table_name=table_data.table_name,
                is_active=table_data.is_active,
                incremental_strategy=table_data.incremental_strategy,
                incremental_field=table_data.incremental_field,
                custom_condition=table_data.custom_condition
            )
            db.add(target_table)
        
        db.commit()
        db.refresh(backup_job)
        
        # 添加到调度器（仅定时执行模式）
        if backup_job.status == JobStatus.ACTIVE and backup_job.execution_mode == ExecutionMode.SCHEDULED:
            scheduler_manager.add_job(backup_job)
        
        # 如果是立即执行模式，立即启动任务
        if backup_job.execution_mode == ExecutionMode.IMMEDIATE:
            from sync_engine import execute_sync_job
            import threading
            thread = threading.Thread(target=execute_sync_job, args=(backup_job.id,))
            thread.start()
        
        return BackupJobResponse(
            id=backup_job.id,
            name=backup_job.name,
            description=backup_job.description,
            source_db_id=backup_job.source_db_id,
            destination_db_id=backup_job.destination_db_id,
            source_db_name=source_db.name,
            destination_db_name=dest_db.name,
            sync_mode=backup_job.sync_mode,
            conflict_strategy=backup_job.conflict_strategy,
            where_condition=backup_job.where_condition,
            execution_mode=backup_job.execution_mode,
            cron_expression=backup_job.cron_expression,
            timezone=backup_job.timezone,
            status=backup_job.status,
            is_running=backup_job.is_running,
            created_at=backup_job.created_at.isoformat(),
            updated_at=backup_job.updated_at.isoformat(),
            last_run_at=backup_job.last_run_at.isoformat() if backup_job.last_run_at else None,
            next_run_at=backup_job.next_run_at.isoformat() if backup_job.next_run_at else None,
            target_tables=[
                JobTargetTableResponse(
                    id=table.id,
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                    is_active=table.is_active,
                    incremental_strategy=table.incremental_strategy,
                    incremental_field=table.incremental_field,
                    custom_condition=table.custom_condition,
                    last_sync_value=table.last_sync_value
                ) for table in backup_job.target_tables
            ]
        )
        
    except HTTPException:
        raise
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create backup job: {str(e)}"
        )

@router.put("/{job_id}", response_model=BackupJobResponse)
async def update_job(
    job_id: int,
    job_data: BackupJobUpdate,
    db: Session = Depends(get_db)
):
    """更新备份任务"""
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    try:
        update_data = job_data.dict(exclude_unset=True)
        
        # 验证执行模式和cron表达式
        if 'execution_mode' in update_data:
            execution_mode = update_data['execution_mode']
            cron_expression = update_data.get('cron_expression') or job.cron_expression
            
            if execution_mode == ExecutionMode.SCHEDULED and not cron_expression:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail="Cron expression is required for scheduled execution mode"
                )
            
            if execution_mode == ExecutionMode.IMMEDIATE and cron_expression:
                # 如果切换为立即执行模式，清除cron表达式
                update_data['cron_expression'] = None
        
        # 处理目标表更新
        if 'target_tables' in update_data:
            # 删除现有目标表
            db.query(JobTargetTable).filter(JobTargetTable.job_id == job_id).delete()
            
            # 添加新的目标表
            for table_data in update_data['target_tables']:
                target_table = JobTargetTable(
                    job_id=job_id,
                    schema_name=table_data['schema_name'],
                    table_name=table_data['table_name'],
                    is_active=table_data.get('is_active', True),
                    incremental_strategy=table_data.get('incremental_strategy', IncrementalStrategy.NONE),
                    incremental_field=table_data.get('incremental_field'),
                    custom_condition=table_data.get('custom_condition')
                )
                db.add(target_table)
            
            # 从更新数据中移除，避免直接赋值
            update_data.pop('target_tables')
        
        # 记录原状态
        old_status = job.status
        
        # 更新其他字段
        for field, value in update_data.items():
            setattr(job, field, value)
        
        db.commit()
        db.refresh(job)
        
        # 更新调度器
        if old_status != job.status or 'cron_expression' in update_data or 'execution_mode' in update_data:
            scheduler_manager.remove_job(job_id)
            if job.status == JobStatus.ACTIVE and job.execution_mode == ExecutionMode.SCHEDULED:
                scheduler_manager.add_job(job)
        
        # 如果切换为立即执行模式，立即启动任务
        if 'execution_mode' in update_data and update_data['execution_mode'] == ExecutionMode.IMMEDIATE:
            from sync_engine import execute_sync_job
            import threading
            thread = threading.Thread(target=execute_sync_job, args=(job_id,))
            thread.start()
        
        # 获取数据库连接名称
        source_db = db.query(DatabaseConnection).filter(
            DatabaseConnection.id == job.source_db_id
        ).first()
        dest_db = db.query(DatabaseConnection).filter(
            DatabaseConnection.id == job.destination_db_id
        ).first()
        
        return BackupJobResponse(
            id=job.id,
            name=job.name,
            description=job.description,
            source_db_id=job.source_db_id,
            destination_db_id=job.destination_db_id,
            source_db_name=source_db.name if source_db else "Unknown",
            destination_db_name=dest_db.name if dest_db else "Unknown",
            sync_mode=job.sync_mode,
            conflict_strategy=job.conflict_strategy,
            where_condition=job.where_condition,
            execution_mode=job.execution_mode,
            cron_expression=job.cron_expression,
            timezone=job.timezone,
            status=job.status,
            is_running=job.is_running,
            created_at=job.created_at.isoformat(),
            updated_at=job.updated_at.isoformat(),
            last_run_at=job.last_run_at.isoformat() if job.last_run_at else None,
            next_run_at=job.next_run_at.isoformat() if job.next_run_at else None,
            target_tables=[
                JobTargetTableResponse(
                    id=table.id,
                    schema_name=table.schema_name,
                    table_name=table.table_name,
                    is_active=table.is_active,
                    incremental_strategy=table.incremental_strategy,
                    incremental_field=table.incremental_field,
                    custom_condition=table.custom_condition,
                    last_sync_value=table.last_sync_value
                ) for table in job.target_tables
            ]
        )
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update backup job: {str(e)}"
        )

@router.delete("/{job_id}")
async def delete_job(job_id: int, db: Session = Depends(get_db)):
    """删除备份任务"""
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    if job.is_running:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Cannot delete a running job"
        )
    
    try:
        # 从调度器中移除
        scheduler_manager.remove_job(job_id)
        
        # 删除数据库记录（级联删除相关表和日志）
        db.delete(job)
        db.commit()
        
        return {"message": "Backup job deleted successfully"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete backup job: {str(e)}"
        )

@router.post("/{job_id}/run")
async def run_job_now(job_id: int, db: Session = Depends(get_db)):
    """立即执行备份任务"""
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    if job.is_running:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="Job is already running"
        )
    
    try:
        # 立即执行任务
        from sync_engine import execute_sync_job
        import threading
        
        # 在后台线程中执行，避免阻塞API响应
        thread = threading.Thread(target=execute_sync_job, args=(job_id,))
        thread.start()
        
        return {"message": "Job execution started"}
        
    except Exception as e:
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to start job execution: {str(e)}"
        )

@router.post("/{job_id}/pause")
async def pause_job(job_id: int, db: Session = Depends(get_db)):
    """暂停备份任务"""
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    try:
        job.status = JobStatus.PAUSED
        db.commit()
        
        # 从调度器中移除
        scheduler_manager.remove_job(job_id)
        
        return {"message": "Job paused successfully"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to pause job: {str(e)}"
        )

@router.post("/{job_id}/resume")
async def resume_job(job_id: int, db: Session = Depends(get_db)):
    """恢复备份任务"""
    job = db.query(BackupJob).filter(BackupJob.id == job_id).first()
    
    if not job:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Backup job not found"
        )
    
    try:
        job.status = JobStatus.ACTIVE
        db.commit()
        
        # 添加到调度器
        scheduler_manager.add_job(job)
        
        return {"message": "Job resumed successfully"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to resume job: {str(e)}"
        )
