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
    password: str = ""
    
    def get_connection_string(self) -> str:
        """获取连接字符串"""
        return f"postgresql://{self.username}:{self.password}@{self.host}:{self.port}/{self.database}"


class TestConfig:
    """测试配置类"""
    
    def __init__(self):
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
