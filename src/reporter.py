"""
结果报告模块
"""

import json
import os
from datetime import datetime
from typing import Dict, Any, Optional
from .connection_tester import TestResult


class Reporter:
    """测试结果报告器"""
    
    def __init__(self, results_dir: str = "results"):
        self.results_dir = results_dir
        self._ensure_results_dir()
        self.results: Dict[str, TestResult] = {}
    
    def _ensure_results_dir(self):
        """确保结果目录存在"""
        if not os.path.exists(self.results_dir):
            os.makedirs(self.results_dir)
    
    def save_result(self, test_type: str, result: TestResult):
        """保存测试结果"""
        self.results[test_type] = result
        
        # 保存详细结果到JSON文件
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{self.results_dir}/{test_type}_result_{timestamp}.json"
        
        result_data = {
            'connection_type': result.connection_type,
            'start_time': result.start_time.isoformat(),
            'end_time': result.end_time.isoformat(),
            'total_attempts': result.total_attempts,
            'successful_attempts': result.successful_attempts,
            'failed_attempts': result.failed_attempts,
            'success_rate': result.success_rate,
            'total_downtime': result.total_downtime,
            'downtime_periods': [
                {
                    'start': period['start'].isoformat(),
                    'end': period['end'].isoformat(),
                    'duration': period['duration']
                }
                for period in result.downtime_periods
            ],
            'events': [
                {
                    'timestamp': event.timestamp.isoformat(),
                    'event_type': event.event_type,
                    'message': event.message,
                    'response_time': event.response_time
                }
                for event in result.events
            ]
        }
        
        with open(filename, 'w', encoding='utf-8') as f:
            json.dump(result_data, f, indent=2, ensure_ascii=False)
        
        print(f"详细结果已保存到: {filename}")
    
    def generate_comparison_report(self):
        """生成对比报告"""
        if 'direct' not in self.results or 'proxy' not in self.results:
            print("警告: 需要同时有直接连接和代理连接的测试结果才能生成对比报告")
            return
        
        direct_result = self.results['direct']
        proxy_result = self.results['proxy']
        
        # 生成对比报告
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        report_filename = f"{self.results_dir}/comparison_report_{timestamp}.txt"
        
        with open(report_filename, 'w', encoding='utf-8') as f:
            f.write("Aurora PostgreSQL 故障转移测试对比报告\n")
            f.write("=" * 50 + "\n\n")
            
            f.write(f"报告生成时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
            
            # 基本统计信息
            f.write("基本统计信息\n")
            f.write("-" * 20 + "\n")
            f.write(f"{'指标':<20} {'直接连接':<15} {'代理连接':<15} {'差异':<15}\n")
            f.write("-" * 70 + "\n")
            
            f.write(f"{'总尝试次数':<20} {direct_result.total_attempts:<15} {proxy_result.total_attempts:<15} {proxy_result.total_attempts - direct_result.total_attempts:<15}\n")
            f.write(f"{'成功次数':<20} {direct_result.successful_attempts:<15} {proxy_result.successful_attempts:<15} {proxy_result.successful_attempts - direct_result.successful_attempts:<15}\n")
            f.write(f"{'失败次数':<20} {direct_result.failed_attempts:<15} {proxy_result.failed_attempts:<15} {proxy_result.failed_attempts - direct_result.failed_attempts:<15}\n")
            f.write(f"{'成功率(%)':<20} {direct_result.success_rate:<15.2f} {proxy_result.success_rate:<15.2f} {proxy_result.success_rate - direct_result.success_rate:<15.2f}\n")
            
            # 停机时间分析
            f.write(f"\n停机时间分析\n")
            f.write("-" * 20 + "\n")
            f.write(f"{'指标':<20} {'直接连接':<15} {'代理连接':<15} {'改善':<15}\n")
            f.write("-" * 70 + "\n")
            
            f.write(f"{'总停机时间(秒)':<20} {direct_result.total_downtime:<15.3f} {proxy_result.total_downtime:<15.3f} {direct_result.total_downtime - proxy_result.total_downtime:<15.3f}\n")
            f.write(f"{'停机次数':<20} {len(direct_result.downtime_periods):<15} {len(proxy_result.downtime_periods):<15} {len(direct_result.downtime_periods) - len(proxy_result.downtime_periods):<15}\n")
            
            if direct_result.downtime_periods:
                avg_direct_downtime = direct_result.total_downtime / len(direct_result.downtime_periods)
            else:
                avg_direct_downtime = 0
                
            if proxy_result.downtime_periods:
                avg_proxy_downtime = proxy_result.total_downtime / len(proxy_result.downtime_periods)
            else:
                avg_proxy_downtime = 0
            
            f.write(f"{'平均停机时间(秒)':<20} {avg_direct_downtime:<15.3f} {avg_proxy_downtime:<15.3f} {avg_direct_downtime - avg_proxy_downtime:<15.3f}\n")
            
            # 性能改善分析
            if direct_result.total_downtime > 0:
                improvement_percentage = ((direct_result.total_downtime - proxy_result.total_downtime) / direct_result.total_downtime) * 100
                f.write(f"\n性能改善\n")
                f.write("-" * 20 + "\n")
                f.write(f"RDS 代理相比直接连接减少停机时间: {improvement_percentage:.2f}%\n")
                
                if improvement_percentage > 0:
                    f.write("结论: RDS 代理有效减少了故障转移停机时间\n")
                elif improvement_percentage < 0:
                    f.write("结论: RDS 代理增加了故障转移停机时间\n")
                else:
                    f.write("结论: RDS 代理对故障转移停机时间无明显影响\n")
            
            # 详细停机时间记录
            f.write(f"\n详细停机时间记录\n")
            f.write("-" * 30 + "\n")
            
            f.write("直接连接停机记录:\n")
            for i, period in enumerate(direct_result.downtime_periods, 1):
                f.write(f"  {i}. {period['start'].strftime('%H:%M:%S')} - {period['end'].strftime('%H:%M:%S')} (持续 {period['duration']:.3f}秒)\n")
            
            f.write("\n代理连接停机记录:\n")
            for i, period in enumerate(proxy_result.downtime_periods, 1):
                f.write(f"  {i}. {period['start'].strftime('%H:%M:%S')} - {period['end'].strftime('%H:%M:%S')} (持续 {period['duration']:.3f}秒)\n")
        
        print(f"对比报告已保存到: {report_filename}")
        
        # 在控制台也显示简要对比
        print("\n" + "=" * 50)
        print("测试结果对比")
        print("=" * 50)
        print(f"直接连接总停机时间: {direct_result.total_downtime:.3f}秒")
        print(f"代理连接总停机时间: {proxy_result.total_downtime:.3f}秒")
        
        if direct_result.total_downtime > 0:
            improvement = ((direct_result.total_downtime - proxy_result.total_downtime) / direct_result.total_downtime) * 100
            print(f"性能改善: {improvement:.2f}%")
            
            if improvement > 0:
                print("✅ RDS 代理有效减少了故障转移停机时间")
            elif improvement < 0:
                print("❌ RDS 代理增加了故障转移停机时间")
            else:
                print("➖ RDS 代理对故障转移停机时间无明显影响")
        
        print("=" * 50)
