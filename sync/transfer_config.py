# sync/transfer_config.py
"""
数据传输策略配置文件
在这里可以轻松切换不同的传输模式
"""

from enum import Enum
from typing import Dict, Any

class TransferMode(Enum):
    """数据传输模式枚举"""
    COPY = "copy"          # 高性能COPY模式 (推荐大数据量)
    STREAM = "stream"      # 传统流式模式 (推荐小数据量或需要复杂处理)

class TransferConfig:
    """传输配置类"""
    
    # ============ 主要配置 ============
    # 在这里修改传输模式
    DEFAULT_MODE = TransferMode.COPY  # 默认使用COPY模式
    
    # ============ COPY模式配置 ============
    COPY_CONFIG = {
        'batch_size': 50000,              # COPY批量大小（建议50000-100000）
        'progress_update_interval': 10,    # 进度更新间隔（每N个批次）
        'use_binary_copy': False,         # 是否使用二进制COPY（实验性功能）
        'enable_fallback': True,          # COPY失败时是否回退到INSERT
        'copy_timeout': 300,              # COPY操作超时时间（秒）
    }
    
    # ============ STREAM模式配置 ============
    STREAM_CONFIG = {
        'batch_size': 5000,               # 流式批量大小（建议1000-10000）
        'progress_update_interval': 1,     # 进度更新间隔（每N个批次）
        'enable_preprocessing': True,      # 是否启用数据预处理
        'conflict_strategy': 'IGNORE',     # 冲突处理策略：ERROR/IGNORE/REPLACE/SKIP
    }
    
    # ============ 性能优化配置 ============
    PERFORMANCE_CONFIG = {
        'connection_pool_size': 10,        # 连接池大小
        'max_overflow': 20,               # 连接池最大溢出
        'pool_timeout': 30,               # 连接池超时
        'enable_query_cache': True,       # 是否启用查询缓存
        'log_slow_queries': True,         # 是否记录慢查询
        'slow_query_threshold': 5.0,      # 慢查询阈值（秒）
    }
    
    # ============ 自动模式选择规则 ============
    AUTO_MODE_RULES = {
        'large_table_threshold': 100000,   # 大表记录数阈值
        'use_copy_for_large_tables': True, # 大表自动使用COPY模式
        'use_stream_for_complex_data': True, # 复杂数据类型使用STREAM模式
    }
    
    @classmethod
    def get_mode_config(cls, mode: TransferMode) -> Dict[str, Any]:
        """获取指定模式的配置"""
        if mode == TransferMode.COPY:
            return cls.COPY_CONFIG
        elif mode == TransferMode.STREAM:
            return cls.STREAM_CONFIG
        else:
            raise ValueError(f"不支持的传输模式: {mode}")
    
    @classmethod
    def auto_select_mode(cls, table_info: Dict[str, Any]) -> TransferMode:
        """根据表信息自动选择传输模式"""
        record_count = table_info.get('record_count', 0)
        has_complex_types = table_info.get('has_complex_types', False)
        
        # 大表优先使用COPY模式
        if (record_count > cls.AUTO_MODE_RULES['large_table_threshold'] and 
            cls.AUTO_MODE_RULES['use_copy_for_large_tables']):
            return TransferMode.COPY
        
        # 复杂数据类型优先使用STREAM模式
        if (has_complex_types and 
            cls.AUTO_MODE_RULES['use_stream_for_complex_data']):
            return TransferMode.STREAM
        
        # 默认模式
        return cls.DEFAULT_MODE
    
    @classmethod
    def get_performance_tips(cls, mode: TransferMode) -> str:
        """获取性能优化建议"""
        if mode == TransferMode.COPY:
            return """
COPY模式性能建议：
1. 适用于大批量数据传输（>10万条记录）
2. 性能比STREAM模式快10-40倍
3. 数据格式要求较严格，复杂类型支持有限
4. 建议batch_size设置为50000-100000
5. 如果有大量JSON/数组字段，考虑使用STREAM模式
            """.strip()
        else:
            return """
STREAM模式性能建议：
1. 适用于中小批量数据或复杂数据处理
2. 支持完整的数据预处理和冲突处理
3. 内存占用较低，适合长时间运行
4. 建议batch_size设置为1000-10000
5. 可以处理复杂的JSON、数组等数据类型
            """.strip()

# ============ 快速配置模板 ============

class QuickConfig:
    """快速配置模板"""
    
    # 高性能模式（适合大数据量）
    HIGH_PERFORMANCE = {
        'mode': TransferMode.COPY,
        'batch_size': 100000,
        'progress_update_interval': 20,
        'use_binary_copy': False,
    }
    
    # 平衡模式（适合中等数据量）
    BALANCED = {
        'mode': TransferMode.COPY,
        'batch_size': 50000,
        'progress_update_interval': 10,
        'use_binary_copy': False,
    }
    
    # 兼容模式（适合复杂数据）
    COMPATIBLE = {
        'mode': TransferMode.STREAM,
        'batch_size': 5000,
        'progress_update_interval': 1,
        'conflict_strategy': 'IGNORE',
    }
    
    # 调试模式（适合开发测试）
    DEBUG = {
        'mode': TransferMode.STREAM,
        'batch_size': 1000,
        'progress_update_interval': 1,
        'conflict_strategy': 'ERROR',
    }

# ============ 使用示例 ============
"""
使用方法：

1. 简单切换模式：
   在 TransferConfig.DEFAULT_MODE 中修改默认模式

2. 使用快速配置：
   config = QuickConfig.HIGH_PERFORMANCE
   
3. 自定义配置：
   TransferConfig.COPY_CONFIG['batch_size'] = 80000
   
4. 自动模式选择：
   table_info = {'record_count': 500000, 'has_complex_types': False}
   mode = TransferConfig.auto_select_mode(table_info)
   
5. 获取性能建议：
   tips = TransferConfig.get_performance_tips(TransferMode.COPY)
   print(tips)
"""