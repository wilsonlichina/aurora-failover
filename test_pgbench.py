#!/usr/bin/env python3
"""
pgbench 功能测试脚本
用于验证 pgbench 负载生成器是否正常工作
"""

import sys
import subprocess
from src.pgbench_load_generator import PgbenchConfig
from src.config import TestConfig

def check_pgbench_installation():
    """检查 pgbench 是否已安装"""
    try:
        result = subprocess.run(['pgbench', '--version'], 
                              capture_output=True, text=True, timeout=5)
        if result.returncode == 0:
            print(f"✅ pgbench 已安装: {result.stdout.strip()}")
            return True
        else:
            print("❌ pgbench 未正确安装")
            return False
    except FileNotFoundError:
        print("❌ pgbench 未找到，请先安装 PostgreSQL 客户端")
        print("   macOS: brew install postgresql")
        print("   Ubuntu: sudo apt-get install postgresql-client")
        return False
    except subprocess.TimeoutExpired:
        print("❌ pgbench 命令超时")
        return False

def test_configuration():
    """测试配置是否正确"""
    print("\n🔧 测试配置...")
    
    try:
        # 创建测试配置
        config = TestConfig(mode='both')
        connections = config.get_database_connections_for_pgbench()
        
        print(f"✅ 配置加载成功，找到 {len(connections)} 个连接配置:")
        for conn_type, conn_config in connections.items():
            print(f"   {conn_type}: {conn_config['host']}:{conn_config['port']}")
        
        return True
    except Exception as e:
        print(f"❌ 配置测试失败: {e}")
        return False

def test_database_connectivity():
    """测试数据库连接"""
    print("\n🔍 测试数据库连接...")
    
    try:
        import psycopg2
        config = TestConfig(mode='both')
        connections = config.get_database_connections_for_pgbench()
        
        for conn_type, conn_config in connections.items():
            try:
                conn = psycopg2.connect(
                    host=conn_config['host'],
                    port=conn_config['port'],
                    user=conn_config['user'],
                    password=conn_config['password'],
                    database=conn_config['database'],
                    connect_timeout=5
                )
                conn.close()
                print(f"   ✅ {conn_type} 连接成功")
            except Exception as e:
                print(f"   ❌ {conn_type} 连接失败: {e}")
                return False
        
        return True
    except ImportError:
        print("❌ psycopg2 未安装，请运行: pip install -r requirements.txt")
        return False

def main():
    """主测试函数"""
    print("pgbench 功能测试")
    print("=" * 30)
    
    # 检查 pgbench 安装
    if not check_pgbench_installation():
        sys.exit(1)
    
    # 测试配置
    if not test_configuration():
        sys.exit(1)
    
    # 测试数据库连接
    if not test_database_connectivity():
        print("\n⚠️ 数据库连接测试失败，请检查:")
        print("   1. 数据库服务是否运行")
        print("   2. 网络连接是否正常")
        print("   3. 用户名密码是否正确")
        print("   4. 安全组配置是否允许连接")
        sys.exit(1)
    
    print("\n🎉 所有测试通过！")
    print("\n💡 现在可以运行 pgbench 负载测试:")
    print("   python main.py --enable-pgbench --duration 60 --pgbench-clients 5")

if __name__ == "__main__":
    main()
