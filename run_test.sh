#!/bin/bash

# Aurora PostgreSQL 故障转移测试脚本
# 使用增强版测试器，能够精确监控每种连接类型的downtime

echo "🎯 Aurora PostgreSQL 故障转移测试"
echo "=================================="

# 检查虚拟环境
if [ ! -d ".venv" ]; then
    echo "❌ 虚拟环境不存在，请先运行 python -m venv .venv"
    exit 1
fi

# 激活虚拟环境
source .venv/bin/activate

# 检查依赖
echo "🔍 检查依赖..."
pip install -r requirements.txt > /dev/null 2>&1

# 运行故障转移测试
echo "🚀 启动故障转移测试..."
echo ""
echo "测试配置:"
echo "  - 测试时长: 300秒 (5分钟)"
echo "  - pgbench 客户端数: 10"
echo "  - pgbench 作业数: 2"
echo "  - 数据规模因子: 10"
echo "  - 预热时间: 60秒"
echo "  - 测试模式: both (direct + proxy)"
echo ""
echo "💡 请在测试开始后手动触发故障转移:"
echo "   aws rds failover-db-cluster --db-cluster-identifier ards-with-rdsproxy --region ap-southeast-1"
echo ""

# 运行测试
python main.py \
    --enable-pgbench \
    --mode both \
    --duration 300 \
    --pgbench-clients 10 \
    --pgbench-jobs 2 \
    --pgbench-scale 10 \
    --warmup-time 60

echo ""
echo "✅ 测试完成！"
echo "📄 查看结果文件: results/pgbench_failover_report_*.txt"
