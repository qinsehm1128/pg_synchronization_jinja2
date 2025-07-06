"""
数据库连接管理API路由
"""
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session
from pydantic import BaseModel
import logging

from database import get_db
from models.database_connections import DatabaseConnection
from utils.encryption import encrypt_connection_string, decrypt_connection_string

router = APIRouter()
logger = logging.getLogger(__name__)

# Pydantic模型
class DatabaseConnectionCreate(BaseModel):
    name: str
    description: Optional[str] = None
    host: str
    port: int = 5432
    database_name: str
    username: str
    password: str

class DatabaseConnectionUpdate(BaseModel):
    name: Optional[str] = None
    description: Optional[str] = None
    host: Optional[str] = None
    port: Optional[int] = None
    database_name: Optional[str] = None
    username: Optional[str] = None
    password: Optional[str] = None
    is_active: Optional[bool] = None

class DatabaseConnectionResponse(BaseModel):
    id: int
    name: str
    description: Optional[str]
    host: str
    port: int
    database_name: str
    username: str
    is_active: bool
    created_at: str
    updated_at: str
    
    class Config:
        from_attributes = True

@router.get("/", response_model=List[DatabaseConnectionResponse])
async def list_connections(
    skip: int = 0,
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """获取数据库连接列表"""
    connections = db.query(DatabaseConnection).offset(skip).limit(limit).all()
    
    # 转换为响应格式
    result = []
    for conn in connections:
        result.append(DatabaseConnectionResponse(
            id=conn.id,
            name=conn.name,
            description=conn.description,
            host=conn.host,
            port=conn.port,
            database_name=conn.database_name,
            username=conn.username,
            is_active=conn.is_active,
            created_at=conn.created_at.isoformat(),
            updated_at=conn.updated_at.isoformat()
        ))
    
    return result

@router.get("/{connection_id}", response_model=DatabaseConnectionResponse)
async def get_connection(connection_id: int, db: Session = Depends(get_db)):
    """获取单个数据库连接"""
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.id == connection_id
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found"
        )
    
    return DatabaseConnectionResponse(
        id=connection.id,
        name=connection.name,
        description=connection.description,
        host=connection.host,
        port=connection.port,
        database_name=connection.database_name,
        username=connection.username,
        is_active=connection.is_active,
        created_at=connection.created_at.isoformat(),
        updated_at=connection.updated_at.isoformat()
    )

@router.post("/", response_model=DatabaseConnectionResponse)
async def create_connection(
    connection_data: DatabaseConnectionCreate,
    db: Session = Depends(get_db)
):
    """创建新的数据库连接"""
    try:
        # 构建连接字符串
        connection_string = (
            f"postgresql://{connection_data.username}:{connection_data.password}@"
            f"{connection_data.host}:{connection_data.port}/{connection_data.database_name}"
        )
        
        # 加密敏感信息
        encrypted_password = encrypt_connection_string(connection_data.password)
        encrypted_connection_string = encrypt_connection_string(connection_string)
        
        # 创建数据库记录
        db_connection = DatabaseConnection(
            name=connection_data.name,
            description=connection_data.description,
            host=connection_data.host,
            port=connection_data.port,
            database_name=connection_data.database_name,
            username=connection_data.username,
            encrypted_password=encrypted_password,
            connection_string_encrypted=encrypted_connection_string
        )
        
        db.add(db_connection)
        db.commit()
        db.refresh(db_connection)
        
        return DatabaseConnectionResponse(
            id=db_connection.id,
            name=db_connection.name,
            description=db_connection.description,
            host=db_connection.host,
            port=db_connection.port,
            database_name=db_connection.database_name,
            username=db_connection.username,
            is_active=db_connection.is_active,
            created_at=db_connection.created_at.isoformat(),
            updated_at=db_connection.updated_at.isoformat()
        )
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to create database connection: {str(e)}"
        )

@router.put("/{connection_id}", response_model=DatabaseConnectionResponse)
async def update_connection(
    connection_id: int,
    connection_data: DatabaseConnectionUpdate,
    db: Session = Depends(get_db)
):
    """更新数据库连接"""
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.id == connection_id
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found"
        )
    
    try:
        # 更新字段
        update_data = connection_data.dict(exclude_unset=True)
        
        # 如果更新了连接相关信息，需要重新加密
        if any(key in update_data for key in ['host', 'port', 'database_name', 'username', 'password']):
            # 获取当前值或使用新值
            host = update_data.get('host', connection.host)
            port = update_data.get('port', connection.port)
            database_name = update_data.get('database_name', connection.database_name)
            username = update_data.get('username', connection.username)
            
            if 'password' in update_data:
                password = update_data['password']
                # 加密新密码
                connection.encrypted_password = encrypt_connection_string(password)
            else:
                # 解密现有密码
                password = decrypt_connection_string(connection.encrypted_password)
            
            # 重新构建并加密连接字符串
            connection_string = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
            connection.connection_string_encrypted = encrypt_connection_string(connection_string)
            
            # 移除密码字段，避免直接赋值
            update_data.pop('password', None)
        
        # 更新其他字段
        for field, value in update_data.items():
            setattr(connection, field, value)
        
        db.commit()
        db.refresh(connection)
        
        return DatabaseConnectionResponse(
            id=connection.id,
            name=connection.name,
            description=connection.description,
            host=connection.host,
            port=connection.port,
            database_name=connection.database_name,
            username=connection.username,
            is_active=connection.is_active,
            created_at=connection.created_at.isoformat(),
            updated_at=connection.updated_at.isoformat()
        )
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to update database connection: {str(e)}"
        )

@router.delete("/{connection_id}")
async def delete_connection(connection_id: int, db: Session = Depends(get_db)):
    """删除数据库连接"""
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.id == connection_id
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found"
        )
    
    try:
        db.delete(connection)
        db.commit()
        return {"message": "Database connection deleted successfully"}
        
    except Exception as e:
        db.rollback()
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Failed to delete database connection: {str(e)}"
        )

@router.post("/test")
async def test_connection_data(connection_data: DatabaseConnectionCreate):
    """测试连接参数"""
    try:
        from sqlalchemy import create_engine, text
        
        # 构建连接字符串
        connection_string = (
            f"postgresql://{connection_data.username}:{connection_data.password}@"
            f"{connection_data.host}:{connection_data.port}/{connection_data.database_name}"
        )
        
        # 创建临时引擎并测试连接
        test_engine = create_engine(connection_string, pool_pre_ping=True)
        
        with test_engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version_info = result.fetchone()[0]
        
        test_engine.dispose()
        
        return {
            "success": True,
            "status": "success",
            "message": "Connection test successful",
            "database_version": version_info
        }
        
    except Exception as e:
        return {
            "success": False,
            "status": "failed",
            "message": f"Connection test failed: {str(e)}"
        }

@router.post("/{connection_id}/test")
async def test_connection(connection_id: int, db: Session = Depends(get_db)):
    """测试数据库连接"""
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.id == connection_id
    ).first()
    
    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found"
        )
    
    try:
        from sqlalchemy import create_engine, text
        
        # 解密连接字符串
        connection_string = decrypt_connection_string(connection.connection_string_encrypted)
        
        # 创建临时引擎并测试连接
        test_engine = create_engine(connection_string, pool_pre_ping=True)
        
        with test_engine.connect() as conn:
            result = conn.execute(text("SELECT version()"))
            version_info = result.fetchone()[0]
        
        test_engine.dispose()
        
        return {
            "success": True,
            "status": "success",
            "message": "Connection test successful",
            "database_version": version_info
        }
        
    except Exception as e:
        return {
            "success": False,
            "status": "failed",
            "message": f"Connection test failed: {str(e)}"
        }


@router.get("/{connection_id}/tables")
async def get_connection_tables(connection_id: int, db: Session = Depends(get_db)):
    """获取数据库连接的表信息"""
    connection = db.query(DatabaseConnection).filter(
        DatabaseConnection.id == connection_id
    ).first()

    if not connection:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Database connection not found"
        )

    try:
        from sqlalchemy import create_engine, text

        # 尝试解密连接字符串
        try:
            connection_string = decrypt_connection_string(connection.connection_string_encrypted)
        except Exception as decrypt_error:
            # 如果解密失败，尝试手动构建连接字符串
            logger.warning(
                f"Decryption failed for connection {connection_id}, building connection string manually: {decrypt_error}")
            try:
                # 解密密码
                decrypted_password = decrypt_connection_string(connection.encrypted_password)
                connection_string = (
                    f"postgresql://{connection.username}:{decrypted_password}@"
                    f"{connection.host}:{connection.port}/{connection.database_name}"
                )
            except Exception as password_decrypt_error:
                logger.error(f"Failed to decrypt password for connection {connection_id}: {password_decrypt_error}")
                raise HTTPException(
                    status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
                    detail="Failed to decrypt connection credentials"
                )

        # 创建临时引擎并查询表信息
        test_engine = create_engine(connection_string, pool_pre_ping=True)

        with test_engine.connect() as conn:
            # 查询所有表
            result = conn.execute(text("""
                SELECT 
                    schemaname as schema_name,
                    tablename as table_name,
                    tableowner as table_owner
                FROM pg_tables 
                WHERE schemaname NOT IN ('information_schema', 'pg_catalog')
                ORDER BY schemaname, tablename
            """))

            tables = []
            for row in result:
                tables.append({
                    "schema_name": row.schema_name,
                    "table_name": row.table_name,
                    "table_owner": row.table_owner,
                    "full_name": f"{row.schema_name}.{row.table_name}"
                })

        test_engine.dispose()

        return {
            "success": True,
            "tables": tables,
            "count": len(tables)
        }

    except Exception as e:
        logger.error(f"Error getting tables for connection {connection_id}: {e}")
        return {
            "success": False,
            "error": str(e),
            "tables": [],
            "count": 0
        }
