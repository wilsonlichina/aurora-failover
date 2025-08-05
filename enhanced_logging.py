#!/usr/bin/env python3
"""
增强日志功能的补丁文件
为 connection_tester.py 添加详细的过程日志
"""

import logging
import sys
from datetime import datetime

# 配置日志格式
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%H:%M:%S.%f'
)

def setup_enhanced_logging():
    """设置增强的日志功能"""
    logger = logging.getLogger('aurora_failover')
    
    # 创建控制台处理器
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.INFO)
    
    # 创建文件处理器
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    file_handler = logging.FileHandler(f'results/test_log_{timestamp}.log')
    file_handler.setLevel(logging.DEBUG)
    
    # 设置格式
    formatter = logging.Formatter(
        '%(asctime)s.%(msecs)03d [%(levelname)s] [%(name)s] %(message)s',
        datefmt='%H:%M:%S'
    )
    
    console_handler.setFormatter(formatter)
    file_handler.setFormatter(formatter)
    
    logger.addHandler(console_handler)
    logger.addHandler(file_handler)
    
    return logger

def log_operation_start(connection_type, operation_type, operation_id, details=""):
    """记录操作开始"""
    logger = logging.getLogger('aurora_failover')
    emoji_map = {
        'read': '🔍',
        'write': '✏️',
        'transaction': '🔄'
    }
    emoji = emoji_map.get(operation_type, '🔧')
    logger.info(f"[{connection_type}] {emoji} 开始{operation_type}操作 {operation_id} {details}")

def log_operation_success(connection_type, operation_type, operation_id, response_time, affected_rows=0):
    """记录操作成功"""
    logger = logging.getLogger('aurora_failover')
    logger.info(f"[{connection_type}] ✅ {operation_type}操作 {operation_id} 成功，"
               f"响应时间: {response_time:.3f}s，影响行数: {affected_rows}")

def log_operation_failure(connection_type, operation_type, operation_id, error_message):
    """记录操作失败"""
    logger = logging.getLogger('aurora_failover')
    logger.error(f"[{connection_type}] ❌ {operation_type}操作 {operation_id} 失败: {error_message}")

def log_connection_status(connection_type, status, details=""):
    """记录连接状态"""
    logger = logging.getLogger('aurora_failover')
    status_emoji = {
        'connected': '🟢',
        'disconnected': '🔴',
        'reconnecting': '🟡',
        'failed': '❌'
    }
    emoji = status_emoji.get(status, '⚪')
    logger.info(f"[{connection_type}] {emoji} 连接状态: {status} {details}")

def log_downtime_event(connection_type, event_type, duration=None):
    """记录停机事件"""
    logger = logging.getLogger('aurora_failover')
    if event_type == 'start':
        logger.warning(f"[{connection_type}] 🚨 检测到连接中断，开始记录停机时间")
    elif event_type == 'end':
        logger.info(f"[{connection_type}] ✅ 连接恢复，停机时长: {duration:.3f}秒")

def log_test_progress(connection_type, total_ops, success_rate, current_tps=None):
    """记录测试进度"""
    logger = logging.getLogger('aurora_failover')
    message = f"[{connection_type}] 📊 已执行 {total_ops} 个操作，成功率: {success_rate:.1f}%"
    if current_tps:
        message += f"，当前TPS: {current_tps:.1f}"
    logger.info(message)

def log_pgbench_status(connection_type, tps, latency_ms, errors=0):
    """记录 pgbench 状态"""
    logger = logging.getLogger('aurora_failover')
    logger.info(f"[{connection_type}] 📈 pgbench - TPS: {tps:.1f}, 延迟: {latency_ms:.2f}ms, 错误: {errors}")

if __name__ == "__main__":
    # 测试日志功能
    logger = setup_enhanced_logging()
    
    # 模拟一些日志输出
    log_connection_status('direct', 'connected', '初始连接成功')
    log_operation_start('direct', 'read', 'abc123', '(用户列表查询)')
    log_operation_success('direct', 'read', 'abc123', 0.045, 20)
    log_downtime_event('direct', 'start')
    log_downtime_event('direct', 'end', 12.345)
    log_test_progress('direct', 1250, 97.2, 156.7)
    log_pgbench_status('proxy', 1234.5, 8.12, 2)
    
    print("✅ 增强日志功能测试完成")
