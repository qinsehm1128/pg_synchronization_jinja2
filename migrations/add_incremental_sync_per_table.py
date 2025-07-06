"""
添加每表独立增量同步配置
migration: add_incremental_sync_per_table
"""

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql


def upgrade():
    """升级数据库结构"""
    
    # 添加增量同步策略枚举类型
    incremental_strategy_enum = postgresql.ENUM(
        'none', 'auto_id', 'auto_timestamp', 'custom_condition',
        name='incrementalstrategy'
    )
    incremental_strategy_enum.create(op.get_bind())
    
    # 为job_target_tables表添加新字段
    op.add_column('job_target_tables', 
                  sa.Column('incremental_strategy', 
                           sa.Enum('none', 'auto_id', 'auto_timestamp', 'custom_condition', name='incrementalstrategy'),
                           server_default='none',
                           comment='增量同步策略'))
    
    op.add_column('job_target_tables', 
                  sa.Column('incremental_field', 
                           sa.String(100), 
                           nullable=True,
                           comment='增量字段名（id、created_at等）'))
    
    op.add_column('job_target_tables', 
                  sa.Column('custom_condition', 
                           sa.Text(),
                           nullable=True,
                           comment='自定义WHERE条件'))
    
    op.add_column('job_target_tables', 
                  sa.Column('last_sync_value', 
                           sa.String(255),
                           nullable=True,
                           comment='上次同步的最大值（用于下次增量同步）'))


def downgrade():
    """降级数据库结构"""
    
    # 删除新添加的字段
    op.drop_column('job_target_tables', 'last_sync_value')
    op.drop_column('job_target_tables', 'custom_condition')
    op.drop_column('job_target_tables', 'incremental_field')
    op.drop_column('job_target_tables', 'incremental_strategy')
    
    # 删除枚举类型
    incremental_strategy_enum = postgresql.ENUM(
        'none', 'auto_id', 'auto_timestamp', 'custom_condition',
        name='incrementalstrategy'
    )
    incremental_strategy_enum.drop(op.get_bind())


if __name__ == "__main__":
    print("请通过 alembic 运行此迁移文件")
    print("命令: alembic upgrade head")
