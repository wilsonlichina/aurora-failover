"""
配置管理模块
"""

from dataclasses import dataclass
from typing import Dict


@dataclass
class DatabaseConfig:
    """数据库连接配置"""
    host: str
    port: int = 5432
    database: str = "postgres"
    username: str = "postgres"
    password: str = "Guoguo123"
    
    def get_connection_string(self) -> str:
        """获取连接字符串"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class TestConfig:
    """测试配置类"""
    
    def __init__(self, duration=300, interval=0.1, mode='both', pgbench_config=None, 
                 concurrent_workers=3, read_weight=70, write_weight=20, transaction_weight=10):
        # Aurora 直接连接配置
        self.direct_writer = DatabaseConfig(
            host="ards-with-rdsproxy.cluster-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com"
        )
        self.direct_reader = DatabaseConfig(
            host="ards-with-rdsproxy.cluster-ro-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com"
        )
        
        # RDS 代理连接配置
        self.proxy_writer = DatabaseConfig(
            host="proxy-1753874304259-ards-with-rdsproxy.proxy-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com"
        )
        self.proxy_reader = DatabaseConfig(
            host="proxy-1753874304259-ards-with-rdsproxy-read-only.endpoint.proxy-czfhjvjvmivm.ap-southeast-1.rds.amazonaws.com"
        )
        
        # 测试参数
        self.duration = duration
        self.interval = interval
        self.mode = mode
        
        # pgbench 配置
        self.pgbench_config = pgbench_config
        
        # 业务测试配置
        self.concurrent_workers = concurrent_workers
        self.read_weight = read_weight
        self.write_weight = write_weight
        self.transaction_weight = transaction_weight
        
        # 操作间隔配置
        self.min_operation_interval = 0.1  # 最小操作间隔
        self.max_operation_interval = 0.5  # 最大操作间隔
        
        # 连接池配置
        self.connection_pool_size = 5
        
        # 连接参数
        self.connection_timeout = 5  # 连接超时时间（秒）
        self.query_timeout = 3       # 查询超时时间（秒）
        self.retry_attempts = 3      # 重试次数
        self.retry_delay = 0.5       # 重试间隔（秒）
    
    def get_config(self, connection_type: str, endpoint_type: str = 'writer') -> DatabaseConfig:
        """
        获取指定类型的数据库配置
        
        Args:
            connection_type: 'direct' 或 'proxy'
            endpoint_type: 'writer' 或 'reader'
        """
        if connection_type == 'direct':
            return self.direct_writer if endpoint_type == 'writer' else self.direct_reader
        elif connection_type == 'proxy':
            return self.proxy_writer if endpoint_type == 'writer' else self.proxy_reader
        else:
            raise ValueError(f"不支持的连接类型: {connection_type}")
    
    def get_database_connections_for_pgbench(self) -> Dict:
        """获取用于 pgbench 的数据库连接配置"""
        connections = {}
        
        if self.mode in ['direct', 'both']:
            connections['direct'] = {
                'host': self.direct_writer.host,
                'port': self.direct_writer.port,
                'user': self.direct_writer.username,
                'password': self.direct_writer.password,
                'database': self.direct_writer.database
            }
        
        if self.mode in ['proxy', 'both']:
            connections['proxy'] = {
                'host': self.proxy_writer.host,
                'port': self.proxy_writer.port,
                'user': self.proxy_writer.username,
                'password': self.proxy_writer.password,
                'database': self.proxy_writer.database
            }
        
        return connections
