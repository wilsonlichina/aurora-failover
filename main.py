#!/usr/bin/env python3
"""
Aurora PostgreSQL 故障转移测试主程序
"""

import argparse
import sys
from src.connection_tester import ConnectionTester
from src.config import TestConfig
from src.reporter import Reporter


def main():
    parser = argparse.ArgumentParser(description='Aurora PostgreSQL 故障转移测试')
    parser.add_argument('--mode', choices=['direct', 'proxy', 'both'], 
                       default='both', help='测试模式')
    parser.add_argument('--duration', type=int, default=300, 
                       help='测试持续时间（秒）')
    parser.add_argument('--interval', type=float, default=0.1, 
                       help='查询间隔（秒）')
    
    args = parser.parse_args()
    
    print("Aurora PostgreSQL 故障转移测试工具")
    print("=" * 50)
    
    # 加载配置
    config = TestConfig()
    
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
    
    print("\n测试完成！")


if __name__ == "__main__":
    main()
