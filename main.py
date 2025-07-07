"""
PostgreSQL数据库同步与备份自动化平台
主应用入口
"""
import logging
import uvicorn
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from contextlib import asynccontextmanager
from datetime import datetime
import asyncio
import json
from typing import AsyncGenerator

from sqlalchemy import text

from config import settings
from database import init_db
from scheduler import scheduler_manager
from routers import connections, jobs, logs
from database import engine
from progress_manager import progress_manager
# 配置日志
logging.basicConfig(
    level=getattr(logging, settings.log_level.upper()),
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

@asynccontextmanager
async def lifespan(app: FastAPI):
    """应用生命周期管理"""
    # 启动时执行
    logger.info("Starting PostgreSQL Sync Platform...")
    
    try:
        # 初始化数据库
        init_db()
        logger.info("Database initialized successfully")
        
        # 启动调度器
        scheduler_manager.start()
        logger.info("Scheduler started successfully")
        
        yield
        
    except Exception as e:
        logger.error(f"Failed to start application: {e}")
        raise
    finally:
        # 关闭时执行
        logger.info("Shutting down PostgreSQL Sync Platform...")
        scheduler_manager.shutdown()
        logger.info("Application shutdown complete")

# 创建FastAPI应用
app = FastAPI(
    title="PostgreSQL数据库同步与备份平台",
    description="基于Web的PostgreSQL数据库同步与备份自动化平台",
    version="1.0.0",
    lifespan=lifespan
)

# 挂载静态文件
app.mount("/static", StaticFiles(directory="static"), name="static")

# 配置模板
templates = Jinja2Templates(directory="templates")

# 注册路由
app.include_router(connections.router, prefix="/api/connections", tags=["connections"])
app.include_router(jobs.router, prefix="/api/jobs", tags=["jobs"])
app.include_router(logs.router, prefix="/api/logs", tags=["logs"])

# 前端路由
@app.get("/", response_class=HTMLResponse)
async def dashboard(request: Request):
    """仪表板页面"""
    return templates.TemplateResponse("dashboard.html", {"request": request})

@app.get("/connections", response_class=HTMLResponse)
async def connections_page(request: Request):
    """数据库连接管理页面"""
    return templates.TemplateResponse("connections.html", {"request": request})

@app.get("/jobs", response_class=HTMLResponse)
async def jobs_page(request: Request):
    """备份任务管理页面"""
    return templates.TemplateResponse("jobs.html", {"request": request})

@app.get("/logs", response_class=HTMLResponse)
async def logs_page(request: Request):
    """执行日志页面"""
    return templates.TemplateResponse("logs.html", {"request": request})

@app.get("/api/jobs/{job_id}/progress")
async def get_job_progress(job_id: int, request: Request):
    """SSE端点：实时推送任务执行进度"""
    
    async def event_stream() -> AsyncGenerator[str, None]:
        queue = asyncio.Queue(maxsize=100)
        
        try:
            # 添加客户端
            progress_manager.add_client(job_id, queue)
            
            # 发送当前进度
            current_progress = progress_manager.get_current_progress(job_id)
            if current_progress:
                yield f"data: {json.dumps(current_progress)}\n\n"
            
            # 持续监听进度更新
            while True:
                try:
                    # 等待进度更新，设置超时避免长时间阻塞
                    progress_data = await asyncio.wait_for(queue.get(), timeout=30.0)
                    yield f"data: {json.dumps(progress_data)}\n\n"
                    
                    # 如果任务完成，发送完成事件并结束
                    if progress_data.get('status') in ['completed', 'failed']:
                        yield f"event: complete\ndata: {json.dumps(progress_data)}\n\n"
                        break
                        
                except asyncio.TimeoutError:
                    # 发送心跳保持连接
                    yield f"data: {json.dumps({'type': 'heartbeat', 'timestamp': asyncio.get_event_loop().time()})}\n\n"
                    
        except Exception as e:
            logger.error(f"SSE stream error for job {job_id}: {e}")
        finally:
            # 清理客户端
            progress_manager.remove_client(job_id, queue)
    
    return StreamingResponse(
        event_stream(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Cache-Control"
        }
    )

@app.get("/api/health")
async def health_check():
    """健康检查端点"""
    try:
        # 检查调度器状态
        scheduler_running = scheduler_manager.is_running()
        
        # 检查数据库连接
        
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        
        return {
            "status": "healthy",
            "timestamp": datetime.utcnow().isoformat(),
            "scheduler_running": scheduler_running,
            "database_connected": True,
            "version": "1.0.0"
        }
    except Exception as e:
        logger.error(f"Health check failed: {e}")
        return JSONResponse(
            status_code=503,
            content={
                "status": "unhealthy",
                "timestamp": datetime.utcnow().isoformat(),
                "error": str(e),
                "version": "1.0.0"
            }
        )

if __name__ == "__main__":
    uvicorn.run(
        "main:app",
        host=settings.app_host,
        port=settings.app_port,
        reload=settings.app_debug,
        workers=1
    )
