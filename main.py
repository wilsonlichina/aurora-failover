#!/usr/bin/env python3
"""
Aurora PostgreSQL 故障转移测试主程序
"""

import argparse
import sys
from src.connection_tester import ConnectionTester
from src.config import TestConfig
from src.reporter import Reporter
from src.pgbench_load_generator import PgbenchConfig
from src.failover_tester import FailoverTester


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
    
    print("Aurora PostgreSQL 故障转移测试工具")
    print("=" * 50)
    
    if args.enable_pgbench:
        # 使用 pgbench 负载测试器
        print("🔧 启用 pgbench 负载测试模式")
        
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
            pgbench_config=pgbench_config
        )
        
        # 设置 pgbench 连接配置
        config.pgbench_config.connections = config.get_database_connections_for_pgbench()
        
        # 使用故障转移测试器
        tester = FailoverTester(config)
        
        # 运行集成测试
        tester.run_test()
        
    else:
        # 使用原有的连接测试器
        print("🔧 使用标准连接测试模式")
        
        # 加载配置
        config = TestConfig(duration=args.duration, interval=args.interval, mode=args.mode)
        
        if args.mode in ['direct', 'both']:
            print("\n开始直接连接测试...")
            direct_tester = ConnectionTester(config, 'direct')
            direct_result = direct_tester.run_test(args.duration, args.interval)
            
            # 生成报告
            reporter = Reporter()
            reporter.save_result('direct', direct_result)
            print(f"直接连接测试完成，结果已保存")
        
        if args.mode in ['proxy', 'both']:
            print("\n开始代理连接测试...")
            proxy_tester = ConnectionTester(config, 'proxy')
            proxy_result = proxy_tester.run_test(args.duration, args.interval)
            
            # 生成报告
            reporter = Reporter()
            reporter.save_result('proxy', proxy_result)
            print(f"代理连接测试完成，结果已保存")
        
        if args.mode == 'both':
            print("\n生成对比报告...")
            reporter.generate_comparison_report()
    
    print("\n✅ 测试完成！")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n⚠️ 程序被用户中断")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ 程序执行出错: {e}")
        sys.exit(1)
