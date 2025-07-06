"""
PostgreSQL数据库同步与备份自动化平台
主应用入口
"""
import logging
import uvicorn
from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates
from fastapi.responses import HTMLResponse, JSONResponse
from contextlib import asynccontextmanager
from datetime import datetime

from sqlalchemy import text

from config import settings
from database import init_db
from scheduler import scheduler_manager
from routers import connections, jobs, logs
from database import engine
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
