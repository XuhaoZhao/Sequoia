#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试 IndustryDataCollector 类的功能
"""

import sys
import os
import time
from datetime import datetime
from industry_analysis import IndustryDataCollector,IndustryAnalyzer

def test_basic_functions():
    """测试基本功能"""
    print("=== 测试基本功能 ===")
    
    collector = IndustryDataCollector()
    
    # 测试获取板块列表
    print("1. 测试获取板块列表...")
    boards = collector.get_all_boards()
    print(f"获取到 {len(boards)} 个板块")
    if boards:
        print(f"前5个板块: {boards[:5]}")
    
    # 测试获取单个板块历史数据
    if boards:
        test_board = boards[0]
        print(f"\n2. 测试获取 {test_board} 的历史数据...")
        hist_data = collector.get_historical_data(test_board)
        if hist_data is not None:
            print(f"获取到 {len(hist_data)} 条历史数据")
            print("最新5条数据:")
            print(hist_data.tail()[['日期时间', '开盘', '收盘', '最高', '最低']])
        else:
            print("获取历史数据失败")


def test_realtime_data():
    """测试实时数据功能"""
    print("\n=== 测试实时数据功能 ===")
    
    collector = IndustryDataCollector()
    
    # 测试获取实时数据
    print("1. 测试获取实时数据...")
    realtime_df = collector.get_realtime_data()
    print(realtime_df)
    period_start = collector.aggregate_to_5min(realtime_df)
    time.sleep(60)
    realtime_df = collector.get_realtime_data()
    period_start = collector.aggregate_to_5min(realtime_df)
    time.sleep(60)
    realtime_df = collector.get_realtime_data()
    if realtime_df is not None:
        print(f"获取到 {len(realtime_df)} 个板块的实时数据")
        print("前5个板块实时数据:")
        print(realtime_df.head()[['板块名称', '最新价', '涨跌幅']])
        
        # 测试聚合到5分钟
        print("\n2. 测试聚合到5分钟数据...")
        period_start = collector.aggregate_to_5min(realtime_df)
        print(f"当前5分钟周期开始时间: {period_start}")
        
        # 显示聚合后的数据
        if collector.realtime_data:
            print("已聚合的板块数量:", len(collector.realtime_data))
            sample_board = list(collector.realtime_data.keys())[0]
            print(collector.realtime_data[sample_board])
            sample_data = collector.realtime_data[sample_board][-1]
            print(f"{sample_board} 最新聚合数据:")
            print(f"  开盘: {sample_data['open']}")
            print(f"  收盘: {sample_data['close']}")
            print(f"  最高: {sample_data['high']}")
            print(f"  最低: {sample_data['low']}")
    else:
        print("获取实时数据失败")


def test_data_persistence():
    """测试数据持久化功能"""
    print("\n=== 测试数据持久化功能 ===")
    
    collector = IndustryDataCollector()
    
    # 先获取一些实时数据
    print("1. 获取实时数据...")
    realtime_df = collector.get_realtime_data()
    if realtime_df is not None:
        collector.aggregate_to_5min(realtime_df)
        
        # 测试保存到磁盘
        print("2. 测试保存实时数据到磁盘...")
        collector.save_realtime_data_to_disk()
        
        # 测试从磁盘加载
        if collector.realtime_data:
            test_board = list(collector.realtime_data.keys())[0]
            print(f"3. 测试从磁盘加载 {test_board} 的数据...")
            loaded_data = collector.load_realtime_data_from_disk(test_board)
            if loaded_data is not None:
                print(f"从磁盘加载到 {len(loaded_data)} 条数据")
                print("最新3条数据:")
                print(loaded_data.tail(3)[['日期时间', '开盘', '收盘', '最高', '最低']])
            else:
                print("从磁盘加载数据失败")


def test_data_combination():
    """测试数据合并功能"""
    print("\n=== 测试数据合并功能 ===")
    
    collector = IndustryDataCollector()
    
    # 选择一个板块进行测试
    boards = collector.get_all_boards()
    if boards:
        test_board = boards[0]
        print(f"测试板块: {test_board}")
        
        # 先获取历史数据并保存
        print("1. 获取并保存历史数据...")
        hist_data = collector.get_historical_data(test_board)
        if hist_data is not None:
            collector.save_historical_data(test_board, hist_data)
            print(f"保存了 {len(hist_data)} 条历史数据")
        
        # 获取实时数据
        print("2. 获取实时数据...")
        realtime_df = collector.get_realtime_data()
        if realtime_df is not None:
            collector.aggregate_to_5min(realtime_df)
            collector.save_realtime_data_to_disk()
        
        # 测试合并数据
        print("3. 测试合并历史和实时数据...")
        combined_data = collector.combine_historical_and_realtime(test_board)
        if combined_data is not None:
            print(f"合并后共有 {len(combined_data)} 条数据")
            print("最新5条数据:")
            print(combined_data.tail()[['日期时间', '开盘', '收盘', '最高', '最低']])
        else:
            print("数据合并失败")


def test_monitoring_simulation():
    """模拟监控模式测试"""
    print("\n=== 模拟监控模式测试 ===")
    
    collector = IndustryDataCollector()
    
    print("模拟运行3次数据收集...")
    for i in range(3):
        print(f"\n第 {i+1} 次数据收集:")
        collector.collect_realtime_data()
        
        # 显示当前内存中的数据状态
        if collector.realtime_data:
            print(f"  内存中有 {len(collector.realtime_data)} 个板块的数据")
            sample_board = list(collector.realtime_data.keys())[0]
            periods_count = len(collector.realtime_data[sample_board])
            print(f"  {sample_board} 有 {periods_count} 个时间段的数据")
        
        time.sleep(2)  # 等待2秒
    
    # 最终保存数据
    print("\n最终保存数据到磁盘...")
    collector.save_realtime_data_to_disk()


def test_directory_structure():
    """测试目录结构"""
    print("\n=== 检查目录结构 ===")
    
    collector = IndustryDataCollector()
    
    print(f"数据主目录: {collector.data_dir}")
    print(f"历史数据目录: {collector.historical_dir}")
    print(f"实时数据目录: {collector.realtime_dir}")
    
    # 检查目录是否存在
    for dir_name, dir_path in [
        ("主目录", collector.data_dir),
        ("历史数据目录", collector.historical_dir),
        ("实时数据目录", collector.realtime_dir)
    ]:
        if os.path.exists(dir_path):
            files = os.listdir(dir_path)
            print(f"{dir_name} 存在，包含 {len(files)} 个文件")
            if files:
                print(f"  前5个文件: {files[:5]}")
        else:
            print(f"{dir_name} 不存在")
def test_analysis():
    data_collector = IndustryDataCollector()
    analyzer = IndustryAnalyzer(data_collector)
    analyzer.analyze_all_boards()
# test_analysis()
# 测试实时数据
test_realtime_data()
# def main():
#     """主测试函数"""
#     print("IndustryDataCollector 测试程序")
#     print("=" * 50)
#     print(f"测试时间: {datetime.now()}")
#     print()
    
#     try:
#         # 检查目录结构
#         test_directory_structure()
        
#         # 测试基本功能
#         test_basic_functions()
        
#         # 测试实时数据
#         test_realtime_data()
        
#         # 测试数据持久化
#         test_data_persistence()
        
#         # 测试数据合并
#         test_data_combination()
        
#         # 模拟监控模式
#         if len(sys.argv) > 1 and sys.argv[1] == "monitor":
#             test_monitoring_simulation()
        
#         print("\n" + "=" * 50)
#         print("所有测试完成！")
        
#     except KeyboardInterrupt:
#         print("\n测试被用户中断")
#     except Exception as e:
#         print(f"\n测试过程中发生错误: {e}")
#         import traceback
#         traceback.print_exc()


# if __name__ == "__main__":
#     if len(sys.argv) > 1 and sys.argv[1] == "help":
#         print("使用方法:")
#         print("  python test_data_collector.py          # 运行基本测试")
#         print("  python test_data_collector.py monitor  # 包含监控模式测试")
#     else:
#         main()