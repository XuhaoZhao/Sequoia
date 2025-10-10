#!/usr/bin/env python3
"""
实时数据收集器
每分钟获取实时数据并存入SQLite数据库
"""

import time
import schedule
from datetime import datetime
from industry_analysis import IndustryDataCollector
import signal
import sys
from industry_analysis import IndustryAnalyzer
import functools
import builtins
# 让所有 print 自动刷新
print = functools.partial(builtins.print, flush=True)

class RealtimeDataCollector:
    """实时数据收集服务"""
    
    def __init__(self):
        self.collector = IndustryDataCollector()
        self.is_running = True

        
        # 注册信号处理器，优雅退出
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)
    
    def _signal_handler(self, signum, frame):
        """信号处理器：优雅退出"""
        print(f"\n收到退出信号 {signum}，正在停止数据收集...")
        self.is_running = False
        # 立即退出，避免重复信号处理
        sys.exit(0)
    
    def collect_data(self):
        """收集一次实时数据"""
        try:
            print(f"开始收集实时数据 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            self.collector.collect_realtime_data()
        except Exception as e:
            print(f"收集实时数据失败: {e}")
    
    
    def collect_historical_data(self):
        """单次历史数据收集"""
        print("=" * 50)
        print("执行单次历史数据收集")
        print("=" * 50)
        print(f"开始时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        try:
            # 收集历史数据
            print("正在收集历史数据...")
            self.collector.collect_all_historical_data()
            print("历史数据收集完成")
        except Exception as e:
            print(f"历史数据收集失败: {e}")
        
        print("=" * 50)
    
    def start_realtime_collection(self):
        """启动定时每分钟实时数据收集"""
        print("=" * 50)
        print("启动定时实时数据收集服务")
        print("=" * 50)
        print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("收集频率: 每分钟")
        print("交易时间: 9:30-11:30, 13:00-15:00")
        print("按 Ctrl+C 停止服务")
        print("=" * 50)
        
        # 清除所有之前的任务
        schedule.clear()
        # 设置定时任务：每分钟执行一次实时数据收集
        schedule.every().minute.do(self.collect_data)
        
        # 立即执行一次（如果在交易时间内）
        if self.collector._is_trading_time():
            print("当前在交易时间内，立即执行一次数据收集...")
            self.collect_data()
        else:
            print("当前不在交易时间内，等待交易时间开始...")
        
        # 主循环
        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(10)  # 减少睡眠时间，提高响应速度
        except KeyboardInterrupt:
            print("\n收到键盘中断信号...")
            self.is_running = False
        
        print("实时数据收集服务已停止")
    
    def analyze_data(self):
        """执行一次数据分析"""
        try:
            print(f"开始数据分析 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
            analyzer = IndustryAnalyzer()
            analyzer.analyze_all_boards()
            print("数据分析完成")
        except Exception as e:
            print(f"数据分析失败: {e}")
    
    def start_analysis_scheduler(self):
        """启动定时每小时分析功能"""
        print("=" * 50)
        print("启动定时数据分析服务")
        print("=" * 50)
        print(f"启动时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("分析频率: 每小时")
        print("按 Ctrl+C 停止服务")
        print("=" * 50)
        
        # 清除所有之前的任务
        schedule.clear()
        # 设置定时任务：每小时执行一次数据分析
        schedule.every().hour.do(self.analyze_data)
        
        # 立即执行一次分析
        print("立即执行一次数据分析...")
        self.analyze_data()
        
        # 主循环
        try:
            while self.is_running:
                schedule.run_pending()
                time.sleep(60)  # 分析模式可以使用较长的睡眠间隔
        except KeyboardInterrupt:
            print("\n收到键盘中断信号...")
            self.is_running = False
        
        print("数据分析服务已停止")
    


def main():
    """主函数"""
    import argparse
    
    parser = argparse.ArgumentParser(description='实时数据收集器')
    parser.add_argument('--mode', type=str, choices=['historical', 'realtime', 'analysis'], 
                       help='运行模式: historical(单次历史数据收集), realtime(定时实时数据收集), analysis(定时分析)')
    args = parser.parse_args()
    
    collector = RealtimeDataCollector()
    

    if args.mode == 'historical':
        collector.collect_historical_data()
    elif args.mode == 'realtime':
        collector.start_realtime_collection()
    elif args.mode == 'analysis':
        collector.start_analysis_scheduler()
    else:
        if args.daemon:
            print("以守护进程模式启动...")
        collector.start()


if __name__ == '__main__':
    main()