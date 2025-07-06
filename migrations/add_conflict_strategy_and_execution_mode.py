"""
添加冲突处理策略和执行模式字段的数据库迁移脚本
"""
import os
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlalchemy import create_engine, text
from config import settings

def upgrade():
    """升级数据库结构"""
    engine = create_engine(settings.database_url)
    
    with engine.connect() as conn:
        with conn.begin():
            print("开始添加新字段...")
            
            # 添加冲突处理策略字段
            conn.execute(text("""
                ALTER TABLE backup_jobs 
                ADD COLUMN IF NOT EXISTS conflict_strategy VARCHAR(20) DEFAULT 'error';
            """))
            print("✓ 添加 conflict_strategy 字段")
            
            # 添加执行模式字段
            conn.execute(text("""
                ALTER TABLE backup_jobs 
                ADD COLUMN IF NOT EXISTS execution_mode VARCHAR(20) DEFAULT 'scheduled';
            """))
            print("✓ 添加 execution_mode 字段")
            
            # 修改cron_expression为可选
            conn.execute(text("""
                ALTER TABLE backup_jobs 
                ALTER COLUMN cron_expression DROP NOT NULL;
            """))
            print("✓ 修改 cron_expression 为可选字段")
            
            # 添加字段注释
            conn.execute(text("""
                COMMENT ON COLUMN backup_jobs.conflict_strategy IS '数据冲突处理策略';
            """))
            
            conn.execute(text("""
                COMMENT ON COLUMN backup_jobs.execution_mode IS '执行模式';
            """))
            
            conn.execute(text("""
                COMMENT ON COLUMN backup_jobs.cron_expression IS 'Cron表达式（定时执行时必填）';
            """))
            print("✓ 添加字段注释")
            
            print("数据库迁移完成！")

def downgrade():
    """降级数据库结构"""
    engine = create_engine(settings.database_url)
    
    with engine.connect() as conn:
        with conn.begin():
            print("开始回滚数据库结构...")
            
            # 删除新添加的字段
            conn.execute(text("""
                ALTER TABLE backup_jobs 
                DROP COLUMN IF EXISTS conflict_strategy;
            """))
            print("✓ 删除 conflict_strategy 字段")
            
            conn.execute(text("""
                ALTER TABLE backup_jobs 
                DROP COLUMN IF EXISTS execution_mode;
            """))
            print("✓ 删除 execution_mode 字段")
            
            # 恢复cron_expression为必填
            conn.execute(text("""
                ALTER TABLE backup_jobs 
                ALTER COLUMN cron_expression SET NOT NULL;
            """))
            print("✓ 恢复 cron_expression 为必填字段")
            
            print("数据库回滚完成！")

if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='数据库迁移脚本')
    parser.add_argument('action', choices=['upgrade', 'downgrade'], help='迁移操作')
    
    args = parser.parse_args()
    
    if args.action == 'upgrade':
        upgrade()
    else:
        downgrade()
