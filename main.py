#!/usr/bin/env python3
"""
Aurora PostgreSQL 故障转移测试主程序
"""

import argparse
import sys
import os
from src.connection_tester import ConnectionTester
from src.config import TestConfig
from src.reporter import Reporter
from src.pgbench_load_generator import PgbenchConfig
from src.failover_tester import FailoverTester

# 导入增强日志功能
from enhanced_logging import setup_enhanced_logging, log_connection_status, log_test_progress


def parse_arguments():
    """解析命令行参数"""
    parser = argparse.ArgumentParser(description='Aurora PostgreSQL 故障转移测试工具')
    
    # 基本测试参数
    parser.add_argument('--mode', choices=['direct', 'proxy', 'both'], 
                       default='both', help='测试模式')
    parser.add_argument('--duration', type=int, default=300, 
                       help='测试持续时间（秒）')
    parser.add_argument('--interval', type=float, default=0.1, 
                       help='查询间隔（秒）')
    parser.add_argument('--verbose', '-v', action='store_true',
                       help='启用详细日志输出')
    
    # 业务场景测试参数
    parser.add_argument('--concurrent-workers', type=int, default=3,
                       help='并发工作线程数 (默认: 3)')
    parser.add_argument('--read-weight', type=int, default=70,
                       help='读操作权重百分比 (默认: 70)')
    parser.add_argument('--write-weight', type=int, default=20,
                       help='写操作权重百分比 (默认: 20)')
    parser.add_argument('--transaction-weight', type=int, default=10,
                       help='事务操作权重百分比 (默认: 10)')
    
    # pgbench 负载测试参数
    parser.add_argument('--enable-pgbench', action='store_true',
                       help='启用 pgbench 负载测试')
    parser.add_argument('--pgbench-clients', type=int, default=10,
                       help='pgbench 并发客户端数 (默认: 10)')
    parser.add_argument('--pgbench-jobs', type=int, default=2,
                       help='pgbench 工作线程数 (默认: 2)')
    parser.add_argument('--pgbench-scale', type=int, default=10,
                       help='pgbench 数据规模因子 (默认: 10)')
    parser.add_argument('--pgbench-mode', choices=['tpc-b', 'read-only', 'custom'],
                       default='tpc-b', help='pgbench 测试模式 (默认: tpc-b)')
    parser.add_argument('--warmup-time', type=int, default=60,
                       help='负载预热时间，秒 (默认: 60)')
    
    return parser.parse_args()


def main():
    args = parse_arguments()
    
    # 确保结果目录存在
    os.makedirs('results', exist_ok=True)
    
    # 设置增强日志
    if args.verbose:
        logger = setup_enhanced_logging()
        print("🔧 已启用详细日志输出")
    
    print("Aurora PostgreSQL 故障转移测试工具")
    print("=" * 50)
    
    # 验证权重参数
    total_weight = args.read_weight + args.write_weight + args.transaction_weight
    if total_weight != 100:
        print(f"⚠️  警告：操作权重总和为 {total_weight}%，不等于 100%")
        print("   权重将按比例调整")
    
    if args.enable_pgbench:
        # 使用 pgbench 负载测试器
        print("🔧 启用 pgbench 负载测试模式")
        if args.verbose:
            log_connection_status('system', 'connected', 'pgbench 负载测试模式已启用')
        
        # 构建 pgbench 配置
        pgbench_config = PgbenchConfig(
            clients=args.pgbench_clients,
            jobs=args.pgbench_jobs,
            duration=args.duration,
            scale_factor=args.pgbench_scale,
            mode=args.pgbench_mode,
            warmup_time=args.warmup_time,
            connections=None  # 将在 TestConfig 中设置
        )
        
        # 构建测试配置
        config = TestConfig(
            duration=args.duration,
            interval=args.interval,
            mode=args.mode,
            pgbench_config=pgbench_config,
            concurrent_workers=args.concurrent_workers,
            read_weight=args.read_weight,
            write_weight=args.write_weight,
            transaction_weight=args.transaction_weight
        )
        
        # 设置 pgbench 连接配置
        config.pgbench_config.connections = config.get_database_connections_for_pgbench()
        
        # 使用故障转移测试器
        tester = FailoverTester(config)
        
        # 运行集成测试
        tester.run_test()
        
    else:
        # 使用业务场景测试器
        print("🔧 使用业务场景测试模式")
        if args.verbose:
            log_connection_status('system', 'connected', '业务场景测试模式已启用')
        
        # 加载配置
        config = TestConfig(
            duration=args.duration, 
            interval=args.interval, 
            mode=args.mode,
            concurrent_workers=args.concurrent_workers,
            read_weight=args.read_weight,
            write_weight=args.write_weight,
            transaction_weight=args.transaction_weight
        )
        
        if args.mode in ['direct', 'both']:
            print("\n开始直接连接测试...")
            if args.verbose:
                log_connection_status('direct', 'connecting', '开始直接连接测试')
            
            direct_tester = ConnectionTester(config, 'direct')
            direct_result = direct_tester.run_test(args.duration, args.concurrent_workers)
            
            # 生成报告
            reporter = Reporter()
            reporter.save_result('direct', direct_result)
            print(f"直接连接测试完成，结果已保存")
            
            if args.verbose:
                log_test_progress('direct', direct_result.total_attempts, direct_result.success_rate)
        
        if args.mode in ['proxy', 'both']:
            print("\n开始代理连接测试...")
            if args.verbose:
                log_connection_status('proxy', 'connecting', '开始代理连接测试')
            
            proxy_tester = ConnectionTester(config, 'proxy')
            proxy_result = proxy_tester.run_test(args.duration, args.concurrent_workers)
            
            # 生成报告
            reporter = Reporter()
            reporter.save_result('proxy', proxy_result)
            print(f"代理连接测试完成，结果已保存")
            
            if args.verbose:
                log_test_progress('proxy', proxy_result.total_attempts, proxy_result.success_rate)
        
        if args.mode == 'both':
            print("\n生成对比报告...")
            reporter.generate_comparison_report()
    
    print("\n✅ 测试完成！")
    if args.verbose:
        print("📄 详细日志已保存到 results/test_log_*.log")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ 程序被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
