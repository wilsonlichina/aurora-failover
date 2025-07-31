import subprocess
import threading
import time
import re
import os
from dataclasses import dataclass
from typing import Dict, List, Optional
from queue import Queue

@dataclass
class PgbenchConfig:
    """pgbench 配置类"""
    # pgbench 基础配置
    clients: int = 10           # 并发客户端数
    jobs: int = 2              # 工作线程数
    duration: int = 300        # 测试时长（秒）
    scale_factor: int = 10     # 数据规模因子
    
    # 测试模式
    mode: str = "tpc-b"        # tpc-b, read-only, custom
    custom_script: Optional[str] = None
    
    # 报告配置
    progress_interval: int = 5  # 进度报告间隔
    warmup_time: int = 60      # 预热时间
    
    # 数据库连接配置
    connections: Dict = None   # {'direct': {...}, 'proxy': {...}}

class PgbenchLoadGenerator:
    """pgbench 负载生成器"""
    
    def __init__(self, config: PgbenchConfig):
        self.config = config
        self.processes = {}  # {conn_type: process}
        self.metrics_queue = Queue()
        self.running = False
        self.start_time = None
        self.metrics = {
            'direct': {'tps': [], 'latency': [], 'errors': []},
            'proxy': {'tps': [], 'latency': [], 'errors': []}
        }
    
    def prepare_database(self):
        """准备 pgbench 测试数据"""
        print("🔧 准备 pgbench 测试数据...")
        
        for conn_type, conn_config in self.config.connections.items():
            print(f"   初始化 {conn_type} 连接的测试数据...")
            
            cmd = [
                'pgbench',
                '-i',  # 初始化模式
                '-s', str(self.config.scale_factor),
                '-h', conn_config['host'],
                '-p', str(conn_config['port']),
                '-U', conn_config['user'],
                '-d', conn_config['database']
            ]
            
            # 设置环境变量（如果需要密码）
            env = os.environ.copy()
            if 'password' in conn_config:
                env['PGPASSWORD'] = conn_config['password']
            
            try:
                result = subprocess.run(cmd, capture_output=True, text=True, timeout=300, env=env)
                if result.returncode == 0:
                    print(f"   ✅ {conn_type} 数据初始化完成")
                else:
                    print(f"   ❌ {conn_type} 数据初始化失败: {result.stderr}")
                    raise Exception(f"数据初始化失败: {result.stderr}")
            except subprocess.TimeoutExpired:
                print(f"   ⏰ {conn_type} 数据初始化超时")
                raise Exception("数据初始化超时")
    
    def start_load_generation(self):
        """启动负载生成"""
        print("🚀 启动 pgbench 负载生成...")
        self.running = True
        self.start_time = time.time()
        
        # 为每种连接类型启动 pgbench 进程
        for conn_type, conn_config in self.config.connections.items():
            process = self._start_pgbench_process(conn_type, conn_config)
            self.processes[conn_type] = process
            
            # 启动输出解析线程
            parser_thread = threading.Thread(
                target=self._parse_pgbench_output,
                args=(process, conn_type)
            )
            parser_thread.daemon = True
            parser_thread.start()
        
        print(f"✅ 已启动 {len(self.processes)} 个 pgbench 进程")
    
    def _start_pgbench_process(self, conn_type: str, conn_config: Dict):
        """启动单个 pgbench 进程"""
        cmd = [
            'pgbench',
            '-c', str(self.config.clients),
            '-j', str(self.config.jobs),
            '-T', str(self.config.duration),
            '-P', str(self.config.progress_interval),
            '-h', conn_config['host'],
            '-p', str(conn_config['port']),
            '-U', conn_config['user'],
            '-d', conn_config['database']
        ]
        
        # 根据模式添加参数
        if self.config.mode == "read-only":
            cmd.append('-S')
        elif self.config.mode == "custom" and self.config.custom_script:
            cmd.extend(['-f', self.config.custom_script])
        
        # 设置环境变量（如果需要密码）
        env = os.environ.copy()
        if 'password' in conn_config:
            env['PGPASSWORD'] = conn_config['password']
        
        print(f"   启动 {conn_type} pgbench: {' '.join(cmd[:8])}...")  # 只显示前几个参数
        
        return subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,  # 将 stderr 重定向到 stdout
            text=True,
            env=env
        )
    
    def _parse_pgbench_output(self, process, conn_type: str):
        """解析 pgbench 输出"""
        while self.running and process.poll() is None:
            line = process.stdout.readline()
            if not line:
                continue
                
            line = line.strip()
            
            # 解析进度报告
            # 格式: progress: 5.0 s, 1234.5 tps, lat 8.123 ms stddev 1.456, 0 failed
            if line.startswith('progress:'):
                metrics = self._parse_progress_line(line)
                if metrics:
                    metrics['conn_type'] = conn_type
                    metrics['timestamp'] = time.time()
                    self.metrics_queue.put(metrics)
                    
                    # 存储到内存中用于分析
                    self.metrics[conn_type]['tps'].append(metrics['tps'])
                    self.metrics[conn_type]['latency'].append(metrics['latency_ms'])
            
            # 解析错误信息
            elif 'ERROR' in line or 'FATAL' in line:
                error_info = {
                    'type': 'error',
                    'conn_type': conn_type,
                    'message': line,
                    'timestamp': time.time()
                }
                self.metrics_queue.put(error_info)
                self.metrics[conn_type]['errors'].append(error_info)
        
        # 解析最终结果
        if process.poll() is not None:
            # 读取剩余的输出
            remaining_output = process.stdout.read()
            if remaining_output:
                self._parse_final_results(remaining_output, conn_type)
    
    def _parse_progress_line(self, line: str) -> Optional[Dict]:
        """解析进度行"""
        try:
            # 使用正则表达式解析
            # progress: 5.0 s, 1234.5 tps, lat 8.123 ms stddev 1.456, 0 failed
            pattern = r'progress: ([\d.]+) s, ([\d.]+) tps, lat ([\d.]+) ms stddev ([\d.]+)(?:, (\d+) failed)?'
            match = re.search(pattern, line)
            
            if match:
                failed_count = int(match.group(5)) if match.group(5) else 0
                return {
                    'type': 'progress',
                    'elapsed_time': float(match.group(1)),
                    'tps': float(match.group(2)),
                    'latency_ms': float(match.group(3)),
                    'latency_stddev': float(match.group(4)),
                    'failed_count': failed_count
                }
        except Exception as e:
            print(f"解析进度行失败: {line}, 错误: {e}")
        
        return None
    
    def _parse_final_results(self, output: str, conn_type: str):
        """解析最终结果"""
        try:
            # 查找最终的 tps 结果行
            # 格式: tps = 1234.567890 (including connections establishing)
            tps_pattern = r'tps = ([\d.]+)'
            tps_match = re.search(tps_pattern, output)
            
            if tps_match:
                final_tps = float(tps_match.group(1))
                final_result = {
                    'type': 'final',
                    'conn_type': conn_type,
                    'final_tps': final_tps,
                    'timestamp': time.time()
                }
                self.metrics_queue.put(final_result)
        except Exception as e:
            print(f"解析最终结果失败: {e}")
    
    def stop_load_generation(self):
        """停止负载生成"""
        print("🛑 停止 pgbench 负载生成...")
        self.running = False
        
        for conn_type, process in self.processes.items():
            if process.poll() is None:  # 进程还在运行
                process.terminate()
                try:
                    process.wait(timeout=10)
                    print(f"   ✅ {conn_type} pgbench 进程已停止")
                except subprocess.TimeoutExpired:
                    process.kill()
                    print(f"   ⚠️ {conn_type} pgbench 进程被强制终止")
    
    def get_current_metrics(self) -> Dict:
        """获取当前性能指标"""
        current_metrics = {}
        
        for conn_type in self.config.connections.keys():
            metrics = self.metrics[conn_type]
            if metrics['tps']:
                current_metrics[conn_type] = {
                    'avg_tps': sum(metrics['tps']) / len(metrics['tps']),
                    'max_tps': max(metrics['tps']),
                    'min_tps': min(metrics['tps']),
                    'avg_latency_ms': sum(metrics['latency']) / len(metrics['latency']),
                    'max_latency_ms': max(metrics['latency']),
                    'min_latency_ms': min(metrics['latency']),
                    'error_count': len(metrics['errors']),
                    'sample_count': len(metrics['tps'])
                }
            else:
                current_metrics[conn_type] = {
                    'avg_tps': 0,
                    'max_tps': 0,
                    'min_tps': 0,
                    'avg_latency_ms': 0,
                    'max_latency_ms': 0,
                    'min_latency_ms': 0,
                    'error_count': 0,
                    'sample_count': 0
                }
        
        return current_metrics
    
    def get_detailed_metrics(self) -> Dict:
        """获取详细的性能指标数据"""
        return {
            'raw_metrics': self.metrics,
            'queue_size': self.metrics_queue.qsize(),
            'running': self.running,
            'start_time': self.start_time,
            'processes_status': {
                conn_type: process.poll() is None 
                for conn_type, process in self.processes.items()
            }
        }
