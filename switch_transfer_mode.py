#!/usr/bin/env python3
# switch_transfer_mode.py
"""
传输模式切换脚本
直接运行此脚本来切换数据传输策略

使用方法:
    python switch_transfer_mode.py copy          # 切换到COPY模式
    python switch_transfer_mode.py stream        # 切换到STREAM模式
    python switch_transfer_mode.py high          # 切换到高性能模式
    python switch_transfer_mode.py debug         # 切换到调试模式
    python switch_transfer_mode.py status        # 查看当前配置
    python switch_transfer_mode.py benchmark     # 性能基准测试
"""

import sys
import os
import json
from pathlib import Path

# 添加项目根目录到Python路径
project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

try:
    from sync.transfer_config import TransferConfig, TransferMode, QuickConfig
    from sync.switch_config_example import SyncConfigSwitcher
except ImportError as e:
    print(f"导入错误: {e}")
    print("请确保在项目根目录下运行此脚本")
    sys.exit(1)

def print_banner():
    """打印横幅"""
    print("="*60)
    print("    PostgreSQL 数据传输策略切换工具")
    print("="*60)

def print_current_config():
    """显示当前配置"""
    switcher = SyncConfigSwitcher()
    config = switcher.get_current_config()
    
    print("\n📊 当前配置状态:")
    print(f"   传输模式: {config['mode'].upper()}")
    print(f"   配置参数: {json.dumps(config['config'], indent=14, ensure_ascii=False)}")
    print("\n💡 性能建议:")
    for line in config['description'].split('\n'):
        if line.strip():
            print(f"   {line.strip()}")

def switch_to_copy():
    """切换到COPY模式"""
    SyncConfigSwitcher.switch_to_copy_mode()
    print("\n✅ 已切换到 COPY 高性能模式")
    print("   - 适用于大批量数据传输")
    print("   - 性能提升10-40倍")
    print("   - 批量大小: 50,000条")
    print_current_config()

def switch_to_stream():
    """切换到STREAM模式"""
    SyncConfigSwitcher.switch_to_stream_mode()
    print("\n✅ 已切换到 STREAM 兼容模式")
    print("   - 适用于复杂数据处理")
    print("   - 完整的冲突处理支持")
    print("   - 批量大小: 5,000条")
    print_current_config()

def switch_to_high_performance():
    """切换到高性能模式"""
    SyncConfigSwitcher.switch_to_high_performance()
    print("\n🚀 已切换到 极高性能模式")
    print("   - 最大化传输速度")
    print("   - 适用于超大数据量")
    print("   - 批量大小: 100,000条")
    print_current_config()

def switch_to_debug():
    """切换到调试模式"""
    SyncConfigSwitcher.switch_to_debug_mode()
    print("\n🔍 已切换到 调试模式")
    print("   - 小批量处理")
    print("   - 详细错误报告")
    print("   - 批量大小: 1,000条")
    print_current_config()

def show_benchmark_info():
    """显示性能基准信息"""
    print("\n📈 性能基准对比:")
    print("""
    ┌─────────────────┬─────────────────┬─────────────────┬─────────────────┐
    │     模式        │   传输速度      │   批量大小      │   适用场景      │
    ├─────────────────┼─────────────────┼─────────────────┼─────────────────┤
    │ COPY模式        │ 50K-200K条/秒   │ 50,000条        │ 大批量数据      │
    │ STREAM模式      │ 1K-5K条/秒      │ 5,000条         │ 复杂数据处理    │
    │ 高性能模式      │ 100K-300K条/秒  │ 100,000条       │ 超大数据量      │
    │ 调试模式        │ 500-1K条/秒     │ 1,000条         │ 开发测试        │
    └─────────────────┴─────────────────┴─────────────────┴─────────────────┘
    """)
    
    print("\n🎯 选择建议:")
    print("   • 数据量 < 1万条     → 使用 STREAM 模式")
    print("   • 数据量 1万-10万条  → 两种模式都可以")
    print("   • 数据量 > 10万条    → 优先使用 COPY 模式")
    print("   • 包含复杂数据类型   → 使用 STREAM 模式")
    print("   • 开发测试阶段       → 使用 DEBUG 模式")

def create_config_backup():
    """创建配置备份"""
    backup_file = project_root / "config_backup.json"
    
    switcher = SyncConfigSwitcher()
    config = switcher.get_current_config()
    
    backup_data = {
        'mode': config['mode'],
        'copy_config': TransferConfig.COPY_CONFIG,
        'stream_config': TransferConfig.STREAM_CONFIG,
        'timestamp': str(Path(__file__).stat().st_mtime)
    }
    
    with open(backup_file, 'w', encoding='utf-8') as f:
        json.dump(backup_data, f, indent=2, ensure_ascii=False)
    
    print(f"\n💾 配置已备份到: {backup_file}")

def show_usage():
    """显示使用说明"""
    print("\n📖 使用说明:")
    print("   python switch_transfer_mode.py copy      # 切换到COPY模式")
    print("   python switch_transfer_mode.py stream    # 切换到STREAM模式")
    print("   python switch_transfer_mode.py high      # 切换到高性能模式")
    print("   python switch_transfer_mode.py debug     # 切换到调试模式")
    print("   python switch_transfer_mode.py status    # 查看当前配置")
    print("   python switch_transfer_mode.py benchmark # 性能基准信息")
    print("   python switch_transfer_mode.py backup    # 备份当前配置")
    print("   python switch_transfer_mode.py help      # 显示帮助")

def main():
    """主函数"""
    print_banner()
    
    if len(sys.argv) < 2:
        print("\n❌ 请指定操作模式")
        show_usage()
        return
    
    command = sys.argv[1].lower()
    
    try:
        if command == 'copy':
            switch_to_copy()
        elif command == 'stream':
            switch_to_stream()
        elif command in ['high', 'high_performance', 'turbo']:
            switch_to_high_performance()
        elif command == 'debug':
            switch_to_debug()
        elif command == 'status':
            print_current_config()
        elif command in ['benchmark', 'bench', 'compare']:
            show_benchmark_info()
        elif command == 'backup':
            create_config_backup()
        elif command in ['help', '-h', '--help']:
            show_usage()
        else:
            print(f"\n❌ 未知命令: {command}")
            show_usage()
            return
        
        print("\n" + "="*60)
        print("✨ 操作完成！配置已生效。")
        print("💡 提示: 重启应用程序以确保配置完全生效。")
        print("="*60)
        
    except Exception as e:
        print(f"\n❌ 操作失败: {e}")
        print("请检查错误信息并重试。")
        sys.exit(1)

if __name__ == "__main__":
    main()