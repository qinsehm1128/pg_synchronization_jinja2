# engine/__init__.py
"""
初始化 'engine' 包，并定义其公共API。
"""

# 从 executor 模块中导入 execute_sync_job 函数，
# 这样其他程序可以直接通过 'from engine import execute_sync_job' 来调用它。
from .executor import execute_sync_job

# __all__ 定义了当其他模块使用 'from engine import *' 时，哪些符号会被导入。
# 这是一种明确包公共接口的好习惯。
__all__ = [
    'execute_sync_job'
]