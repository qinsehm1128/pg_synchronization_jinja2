"""
应用配置管理
"""
import os
from pydantic_settings import BaseSettings
from dotenv import load_dotenv

# 加载环境变量
load_dotenv()

class Settings(BaseSettings):
    """应用配置类"""
    
    # 应用配置
    app_host: str = "127.0.0.1"
    app_port: int = 8000
    app_debug: bool = False
    log_level: str = "INFO"
    
    # 数据库配置
    database_url: str = "postgresql://username:password@localhost:5432/pg_sync_platform"
    
    # 安全配置
    encryption_key: str = ""
    
    # 调度器配置
    scheduler_timezone: str = "Asia/Shanghai"
    max_workers: int = 4
    
    # 日志配置
    log_file: str = "logs/app.log"
    log_max_size: str = "10MB"
    log_backup_count: int = 5
    
    # 连接池配置
    db_pool_size: int = 10
    db_max_overflow: int = 20
    db_pool_timeout: int = 30
    
    # 同步配置
    batch_size: int = 1000
    max_retry_attempts: int = 3
    retry_delay: int = 5
    
    class Config:
        env_file = ".env"

# 全局配置实例
settings = Settings()

# 验证必要的配置
if not settings.encryption_key:
    raise ValueError("ENCRYPTION_KEY environment variable is required")
