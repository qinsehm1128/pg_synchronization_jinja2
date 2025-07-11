# sync/transfer_integration.py
"""
数据传输策略集成模块
展示如何在现有同步系统中集成COPY传输策略
"""

import logging
from typing import Optional, Dict, Any, Callable
from sqlalchemy import Engine
from sqlalchemy.orm import Session

from models.backup_jobs import BackupJob
from .transfer_config import TransferConfig, TransferMode, QuickConfig
from .copy_data_manager import CopyDataManager, DataTransferStrategy

logger = logging.getLogger(__name__)

class IntegratedTransferManager:
    """集成的数据传输管理器"""
    
    def __init__(self, job: BackupJob, source_engine: Engine, 
                 destination_engine: Engine, db_session: Session):
        self.job = job
        self.source_engine = source_engine
        self.destination_engine = destination_engine
        self.db_session = db_session
        
        # 当前使用的传输模式
        self.current_mode = TransferConfig.DEFAULT_MODE
        self.data_manager = None
        
        # 性能统计
        self.performance_stats = {
            'total_records': 0,
            'total_time': 0,
            'average_speed': 0,
            'mode_used': None
        }
    
    def sync_table_data(self, target_table, progress_tracker: dict, 
                       cancellation_check: callable, 
                       force_mode: Optional[TransferMode] = None) -> int:
        """同步表数据 - 支持自动或手动模式选择"""
        
        # 确定使用的传输模式
        selected_mode = self._select_transfer_mode(target_table, force_mode)
        
        # 创建对应的数据管理器
        self.data_manager = self._create_data_manager(selected_mode)
        
        # 应用模式特定的配置
        self._apply_mode_config(selected_mode)
        
        # 记录性能统计开始时间
        import time
        start_time = time.time()
        
        try:
            # 执行数据同步
            records_synced = self.data_manager.sync_table_data(
                target_table, progress_tracker, cancellation_check
            )
            
            # 记录性能统计
            end_time = time.time()
            self._update_performance_stats(records_synced, end_time - start_time, selected_mode)
            
            logger.info(f"表同步完成 - 模式: {selected_mode.value}, 记录数: {records_synced}, "
                       f"耗时: {end_time - start_time:.2f}秒")
            
            return records_synced
            
        except Exception as e:
            logger.error(f"数据同步失败 - 模式: {selected_mode.value}, 错误: {e}")
            
            # 如果COPY模式失败，可以尝试回退到STREAM模式
            if selected_mode == TransferMode.COPY and not force_mode:
                logger.warning("COPY模式失败，尝试回退到STREAM模式")
                return self.sync_table_data(target_table, progress_tracker, 
                                           cancellation_check, TransferMode.STREAM)
            
            raise
    
    def _select_transfer_mode(self, target_table, force_mode: Optional[TransferMode]) -> TransferMode:
        """选择传输模式"""
        if force_mode:
            logger.info(f"使用强制指定的传输模式: {force_mode.value}")
            return force_mode
        
        # 获取表信息用于自动选择
        table_info = self._analyze_table(target_table)
        
        # 自动选择模式
        auto_mode = TransferConfig.auto_select_mode(table_info)
        
        logger.info(f"自动选择传输模式: {auto_mode.value} (记录数: {table_info['record_count']}, "
                   f"复杂类型: {table_info['has_complex_types']})")
        
        return auto_mode
    
    def _analyze_table(self, target_table) -> Dict[str, Any]:
        """分析表特征"""
        try:
            # 获取记录数
            count_query = f"SELECT COUNT(*) FROM {target_table.schema_name}.{target_table.table_name}"
            with self.source_engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text(count_query))
                record_count = result.scalar() or 0
            
            # 检查是否有复杂数据类型（简化检查）
            has_complex_types = self._has_complex_data_types(target_table)
            
            return {
                'record_count': record_count,
                'has_complex_types': has_complex_types,
                'table_name': f"{target_table.schema_name}.{target_table.table_name}"
            }
            
        except Exception as e:
            logger.warning(f"表分析失败，使用默认配置: {e}")
            return {
                'record_count': 0,
                'has_complex_types': False,
                'table_name': f"{target_table.schema_name}.{target_table.table_name}"
            }
    
    def _has_complex_data_types(self, target_table) -> bool:
        """检查表是否包含复杂数据类型"""
        try:
            query = """
                SELECT COUNT(*) FROM information_schema.columns 
                WHERE table_schema = :schema_name 
                  AND table_name = :table_name 
                  AND data_type IN ('json', 'jsonb', 'array', 'text[]', 'integer[]')
            """
            
            with self.source_engine.connect() as conn:
                from sqlalchemy import text
                result = conn.execute(text(query), {
                    'schema_name': target_table.schema_name,
                    'table_name': target_table.table_name
                })
                complex_columns = result.scalar() or 0
                return complex_columns > 0
                
        except Exception:
            return False
    
    def _create_data_manager(self, mode: TransferMode):
        """创建对应模式的数据管理器"""
        if mode == TransferMode.COPY:
            return CopyDataManager(
                self.job, self.source_engine, 
                self.destination_engine, self.db_session
            )
        else:
            # 导入原有的DataManager
            try:
                from .data_manager import DataManager
                return DataManager(
                    self.job, self.source_engine,
                    self.destination_engine, self.db_session
                )
            except ImportError:
                logger.error("无法导入DataManager，请确保data_manager.py存在")
                raise
    
    def _apply_mode_config(self, mode: TransferMode):
        """应用模式特定的配置"""
        config = TransferConfig.get_mode_config(mode)
        
        if hasattr(self.data_manager, 'batch_size'):
            self.data_manager.batch_size = config.get('batch_size', 5000)
        
        if hasattr(self.data_manager, 'progress_update_interval'):
            self.data_manager.progress_update_interval = config.get('progress_update_interval', 1)
        
        # COPY模式特定配置
        if mode == TransferMode.COPY and hasattr(self.data_manager, 'use_binary_copy'):
            self.data_manager.use_binary_copy = config.get('use_binary_copy', False)
        
        logger.debug(f"应用{mode.value}模式配置: {config}")
    
    def _update_performance_stats(self, records: int, time_taken: float, mode: TransferMode):
        """更新性能统计"""
        self.performance_stats['total_records'] += records
        self.performance_stats['total_time'] += time_taken
        self.performance_stats['mode_used'] = mode.value
        
        if time_taken > 0:
            speed = records / time_taken
            self.performance_stats['average_speed'] = speed
            logger.info(f"传输速度: {speed:.0f} 记录/秒")
    
    def get_performance_report(self) -> str:
        """获取性能报告"""
        stats = self.performance_stats
        return f"""
=== 数据传输性能报告 ===
传输模式: {stats['mode_used']}
总记录数: {stats['total_records']:,}
总耗时: {stats['total_time']:.2f} 秒
平均速度: {stats['average_speed']:.0f} 记录/秒
========================
        """.strip()


# ============ 使用示例和工具函数 ============

def create_transfer_manager_with_config(job: BackupJob, source_engine: Engine,
                                       destination_engine: Engine, db_session: Session,
                                       config_name: str = 'BALANCED') -> IntegratedTransferManager:
    """使用预定义配置创建传输管理器"""
    
    # 应用快速配置
    if hasattr(QuickConfig, config_name):
        config = getattr(QuickConfig, config_name)
        
        # 更新默认模式
        TransferConfig.DEFAULT_MODE = config['mode']
        
        # 更新对应模式的配置
        if config['mode'] == TransferMode.COPY:
            TransferConfig.COPY_CONFIG.update({
                'batch_size': config.get('batch_size', 50000),
                'progress_update_interval': config.get('progress_update_interval', 10),
                'use_binary_copy': config.get('use_binary_copy', False)
            })
        else:
            TransferConfig.STREAM_CONFIG.update({
                'batch_size': config.get('batch_size', 5000),
                'progress_update_interval': config.get('progress_update_interval', 1),
                'conflict_strategy': config.get('conflict_strategy', 'IGNORE')
            })
        
        logger.info(f"应用快速配置: {config_name} - 模式: {config['mode'].value}")
    
    return IntegratedTransferManager(job, source_engine, destination_engine, db_session)


def benchmark_transfer_modes(job: BackupJob, source_engine: Engine,
                            destination_engine: Engine, db_session: Session,
                            target_table, progress_tracker: dict,
                            cancellation_check: callable) -> Dict[str, Any]:
    """对比不同传输模式的性能"""
    
    results = {}
    
    for mode in [TransferMode.STREAM, TransferMode.COPY]:
        try:
            logger.info(f"开始测试 {mode.value} 模式")
            
            manager = IntegratedTransferManager(job, source_engine, destination_engine, db_session)
            
            import time
            start_time = time.time()
            
            records = manager.sync_table_data(
                target_table, progress_tracker.copy(), 
                cancellation_check, force_mode=mode
            )
            
            end_time = time.time()
            time_taken = end_time - start_time
            
            results[mode.value] = {
                'records': records,
                'time': time_taken,
                'speed': records / time_taken if time_taken > 0 else 0,
                'success': True
            }
            
        except Exception as e:
            logger.error(f"{mode.value} 模式测试失败: {e}")
            results[mode.value] = {
                'records': 0,
                'time': 0,
                'speed': 0,
                'success': False,
                'error': str(e)
            }
    
    return results


# ============ 配置切换示例 ============
"""
使用示例：

# 1. 基本使用（自动模式选择）
manager = IntegratedTransferManager(job, source_engine, dest_engine, db_session)
records = manager.sync_table_data(target_table, progress_tracker, cancellation_check)

# 2. 强制使用COPY模式
records = manager.sync_table_data(target_table, progress_tracker, cancellation_check, 
                                 force_mode=TransferMode.COPY)

# 3. 使用预定义配置
manager = create_transfer_manager_with_config(
    job, source_engine, dest_engine, db_session, 'HIGH_PERFORMANCE'
)

# 4. 手动切换默认模式
TransferConfig.DEFAULT_MODE = TransferMode.COPY  # 全局切换到COPY模式

# 5. 调整COPY模式参数
TransferConfig.COPY_CONFIG['batch_size'] = 100000
TransferConfig.COPY_CONFIG['progress_update_interval'] = 20

# 6. 性能对比测试
benchmark_results = benchmark_transfer_modes(
    job, source_engine, dest_engine, db_session,
    target_table, progress_tracker, cancellation_check
)
print(benchmark_results)

# 7. 获取性能报告
print(manager.get_performance_report())
"""