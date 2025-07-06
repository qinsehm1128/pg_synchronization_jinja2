"""
数据库连接模型
"""
from sqlalchemy import Column, Integer, String, DateTime, Text, Boolean
from sqlalchemy.sql import func
from database import Base

class DatabaseConnection(Base):
    """数据库连接模型"""
    
    __tablename__ = "database_connections"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String(100), nullable=False, comment="连接名称")
    description = Column(Text, comment="连接描述")
    host = Column(String(255), nullable=False, comment="数据库主机")
    port = Column(Integer, default=5432, comment="数据库端口")
    database_name = Column(String(100), nullable=False, comment="数据库名称")
    username = Column(String(100), nullable=False, comment="用户名")
    encrypted_password = Column(Text, nullable=False, comment="加密后的密码")
    connection_string_encrypted = Column(Text, nullable=False, comment="加密后的完整连接字符串")
    is_active = Column(Boolean, default=True, comment="是否启用")
    created_at = Column(DateTime(timezone=True), server_default=func.now(), comment="创建时间")
    updated_at = Column(DateTime(timezone=True), server_default=func.now(), onupdate=func.now(), comment="更新时间")
    
    def __repr__(self):
        return f"<DatabaseConnection(id={self.id}, name='{self.name}', host='{self.host}')>"
