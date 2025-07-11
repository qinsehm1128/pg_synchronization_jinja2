# PostgreSQL COPY 高性能数据传输方案

## 概述

本方案提供了基于PostgreSQL COPY命令的高性能数据传输功能，相比传统的INSERT方式，性能提升10-40倍。

## 文件说明

- `copy_data_manager.py` - COPY传输核心实现
- `transfer_config.py` - 传输策略配置文件
- `transfer_integration.py` - 集成模块，提供统一接口
- `README_COPY_TRANSFER.md` - 本说明文档

## 快速开始

### 1. 基本使用

```python
from sync.transfer_integration import IntegratedTransferManager
from sync.transfer_config import TransferMode

# 创建集成传输管理器
manager = IntegratedTransferManager(job, source_engine, dest_engine, db_session)

# 自动选择最佳传输模式
records = manager.sync_table_data(target_table, progress_tracker, cancellation_check)

# 强制使用COPY模式
records = manager.sync_table_data(target_table, progress_tracker, cancellation_check, 
                                 force_mode=TransferMode.COPY)
```

### 2. 切换传输策略

#### 方法一：修改默认配置

在 `transfer_config.py` 中修改：

```python
class TransferConfig:
    # 修改这里切换默认模式
    DEFAULT_MODE = TransferMode.COPY  # 或 TransferMode.STREAM
```

#### 方法二：运行时切换

```python
from sync.transfer_config import TransferConfig, TransferMode

# 全局切换到COPY模式
TransferConfig.DEFAULT_MODE = TransferMode.COPY

# 或者在每次调用时指定
records = manager.sync_table_data(target_table, progress_tracker, cancellation_check, 
                                 force_mode=TransferMode.COPY)
```

#### 方法三：使用快速配置

```python
from sync.transfer_integration import create_transfer_manager_with_config

# 使用预定义的高性能配置
manager = create_transfer_manager_with_config(
    job, source_engine, dest_engine, db_session, 'HIGH_PERFORMANCE'
)
```

## 传输模式对比

| 特性 | COPY模式 | STREAM模式 |
|------|----------|------------|
| 性能 | 50,000-200,000 记录/秒 | 1,000-5,000 记录/秒 |
| 内存占用 | 中等 | 低 |
| 数据类型支持 | 基础类型 | 完整支持 |
| 冲突处理 | 有限 | 完整支持 |
| 适用场景 | 大批量数据 | 复杂数据处理 |

## 配置选项

### COPY模式配置

```python
COPY_CONFIG = {
    'batch_size': 50000,              # 批量大小（建议50000-100000）
    'progress_update_interval': 10,    # 进度更新间隔
    'use_binary_copy': False,         # 二进制COPY（实验性）
    'enable_fallback': True,          # 失败时回退到INSERT
    'copy_timeout': 300,              # 超时时间（秒）
}
```

### STREAM模式配置

```python
STREAM_CONFIG = {
    'batch_size': 5000,               # 批量大小（建议1000-10000）
    'progress_update_interval': 1,     # 进度更新间隔
    'enable_preprocessing': True,      # 数据预处理
    'conflict_strategy': 'IGNORE',     # 冲突策略：ERROR/IGNORE/REPLACE/SKIP
}
```

## 自动模式选择

系统会根据以下规则自动选择传输模式：

1. **大表（>10万记录）** → COPY模式
2. **包含复杂数据类型** → STREAM模式
3. **其他情况** → 使用默认模式

```python
# 自定义自动选择规则
AUTO_MODE_RULES = {
    'large_table_threshold': 100000,   # 大表阈值
    'use_copy_for_large_tables': True, # 大表使用COPY
    'use_stream_for_complex_data': True, # 复杂数据使用STREAM
}
```

## 性能优化建议

### COPY模式优化

1. **批量大小**：设置为50,000-100,000
2. **进度更新**：降低更新频率（每10-20个批次）
3. **数据预处理**：简化JSON和数组处理
4. **连接池**：使用连接池减少连接开销

```python
# 高性能配置示例
TransferConfig.COPY_CONFIG.update({
    'batch_size': 100000,
    'progress_update_interval': 20,
    'use_binary_copy': False,  # 暂时保持False
})
```

### STREAM模式优化

1. **批量大小**：设置为5,000-10,000
2. **冲突策略**：选择合适的冲突处理策略
3. **数据预处理**：根据需要启用/禁用

## 故障处理

### COPY模式失败回退

```python
# 启用自动回退
TransferConfig.COPY_CONFIG['enable_fallback'] = True

# 手动回退示例
try:
    records = manager.sync_table_data(target_table, progress_tracker, 
                                     cancellation_check, TransferMode.COPY)
except Exception as e:
    logger.warning(f"COPY模式失败，回退到STREAM模式: {e}")
    records = manager.sync_table_data(target_table, progress_tracker, 
                                     cancellation_check, TransferMode.STREAM)
```

### 常见问题

1. **COPY命令失败**
   - 检查数据格式是否符合PostgreSQL要求
   - 确认目标表结构正确
   - 查看错误日志获取详细信息

2. **性能不如预期**
   - 增加batch_size
   - 减少progress_update_interval
   - 检查网络和磁盘I/O

3. **内存占用过高**
   - 减少batch_size
   - 使用STREAM模式

## 性能监控

```python
# 获取性能报告
print(manager.get_performance_report())

# 性能对比测试
from sync.transfer_integration import benchmark_transfer_modes

benchmark_results = benchmark_transfer_modes(
    job, source_engine, dest_engine, db_session,
    target_table, progress_tracker, cancellation_check
)

for mode, stats in benchmark_results.items():
    print(f"{mode}: {stats['speed']:.0f} 记录/秒")
```

## 最佳实践

1. **数据量评估**
   - < 1万记录：使用STREAM模式
   - 1万-10万记录：两种模式都可以
   - > 10万记录：优先使用COPY模式

2. **数据类型考虑**
   - 简单类型（数字、字符串、日期）：COPY模式
   - 复杂类型（JSON、数组、自定义类型）：STREAM模式

3. **网络环境**
   - 本地网络：COPY模式
   - 跨网络传输：考虑网络延迟影响

4. **监控和调优**
   - 定期检查性能报告
   - 根据实际情况调整配置
   - 使用基准测试验证效果

## 示例代码

完整的使用示例：

```python
from sync.transfer_integration import IntegratedTransferManager
from sync.transfer_config import TransferConfig, TransferMode, QuickConfig

# 1. 使用默认配置
manager = IntegratedTransferManager(job, source_engine, dest_engine, db_session)

# 2. 切换到高性能模式
TransferConfig.DEFAULT_MODE = TransferMode.COPY
TransferConfig.COPY_CONFIG['batch_size'] = 100000

# 3. 执行同步
for target_table in target_tables:
    try:
        records = manager.sync_table_data(
            target_table, progress_tracker, cancellation_check
        )
        logger.info(f"表 {target_table.table_name} 同步完成: {records} 条记录")
    except Exception as e:
        logger.error(f"表 {target_table.table_name} 同步失败: {e}")

# 4. 查看性能报告
print(manager.get_performance_report())
```

---

**注意**：首次使用建议先在测试环境验证，确认配置和性能符合预期后再在生产环境使用。