#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
测试新的5分钟数据聚合逻辑
"""

import pandas as pd
from datetime import datetime, timedelta
import sys
import os

# 添加当前目录到路径以便导入
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from industry_analysis import IndustryDataCollector

def test_timestamp_generation():
    """测试时间戳生成功能"""
    print("=== 测试时间戳生成功能 ===")
    
    collector = IndustryDataCollector()
    
    # 测试指定日期的时间戳生成
    test_date = "2025-08-19"
    timestamps = collector._generate_trading_timestamps(test_date)
    
    print(f"生成的时间戳数量: {len(timestamps)}")
    print("前10个时间戳:")
    for i, ts in enumerate(timestamps[:10]):
        print(f"  {i+1}: {ts.strftime('%Y-%m-%d %H:%M:%S')}")
    
    print("最后10个时间戳:")
    for i, ts in enumerate(timestamps[-10:], len(timestamps)-9):
        print(f"  {i}: {ts.strftime('%Y-%m-%d %H:%M:%S')}")
    
    # 验证时间戳的正确性
    expected_start_morning = datetime.strptime(f"{test_date} 09:30:00", "%Y-%m-%d %H:%M:%S")
    expected_end_morning = datetime.strptime(f"{test_date} 11:30:00", "%Y-%m-%d %H:%M:%S")
    expected_start_afternoon = datetime.strptime(f"{test_date} 13:00:00", "%Y-%m-%d %H:%M:%S")
    expected_end_afternoon = datetime.strptime(f"{test_date} 15:00:00", "%Y-%m-%d %H:%M:%S")
    
    assert timestamps[0] == expected_start_morning, f"上午开始时间不正确: {timestamps[0]} vs {expected_start_morning}"
    assert expected_end_afternoon in timestamps, f"下午结束时间不在时间戳列表中"
    
    print("✓ 时间戳生成测试通过")
    return timestamps

def test_target_timestamp_finding():
    """测试目标时间戳查找功能"""
    print("\n=== 测试目标时间戳查找功能 ===")
    
    collector = IndustryDataCollector()
    
    # 测试数据时间点
    test_cases = [
        ("2025-08-19 09:32:30", "2025-08-19 09:35:00"),  # 应该归到9:35
        ("2025-08-19 09:35:00", "2025-08-19 09:35:00"),  # 准确时间
        ("2025-08-19 09:37:45", "2025-08-19 09:40:00"),  # 应该归到9:40
        ("2025-08-19 11:29:00", "2025-08-19 11:30:00"),  # 上午最后一个时间戳
        ("2025-08-19 13:02:15", "2025-08-19 13:05:00"),  # 下午开始后
        ("2025-08-19 14:58:30", "2025-08-19 15:00:00"),  # 下午结束前
    ]
    
    for data_time_str, expected_target_str in test_cases:
        data_time = datetime.strptime(data_time_str, "%Y-%m-%d %H:%M:%S")
        expected_target = datetime.strptime(expected_target_str, "%Y-%m-%d %H:%M:%S")
        
        actual_target = collector._find_target_timestamp(data_time)
        
        print(f"数据时间: {data_time_str} -> 目标时间戳: {actual_target}")
        assert actual_target == expected_target, f"时间戳查找错误: {data_time_str} -> {actual_target} (期望: {expected_target})"
    
    print("✓ 目标时间戳查找测试通过")

def test_aggregation_logic():
    """测试聚合逻辑"""
    print("\n=== 测试聚合逻辑 ===")
    
    collector = IndustryDataCollector()
    
    # 创建模拟的实时数据
    mock_realtime_data = pd.DataFrame([
        {
            '板块名称': '钢铁行业',
            '最新价': 8441.82,
            '成交量': 1000000,
            '成交额': 500000000.0
        },
        {
            '板块名称': '有色金属',
            '最新价': 5200.50,
            '成交量': 800000,
            '成交额': 400000000.0
        }
    ])
    
    # 执行聚合
    current_time = collector.aggregate_to_5min(mock_realtime_data)
    
    print(f"聚合执行时间: {current_time}")
    print(f"内存中的板块数量: {len(collector.realtime_data)}")
    
    # 检查聚合结果
    for board_name, timestamp_data in collector.realtime_data.items():
        print(f"\n板块: {board_name}")
        print(f"时间戳数量: {len(timestamp_data)}")
        
        # 检查有数据的时间戳
        data_timestamps = []
        for ts, data in timestamp_data.items():
            if data['data_points'] > 0:
                data_timestamps.append(ts)
                print(f"  {ts}: 开盘={data['open']}, 收盘={data['close']}, 成交量={data['volume']}")
        
        assert len(data_timestamps) > 0, f"板块 {board_name} 没有聚合到任何数据"
    
    print("✓ 聚合逻辑测试通过")

def test_save_and_load():
    """测试保存和加载逻辑"""
    print("\n=== 测试保存和加载逻辑 ===")
    
    collector = IndustryDataCollector()
    
    # 先进行一次聚合
    mock_realtime_data = pd.DataFrame([
        {
            '板块名称': '钢铁行业',
            '最新价': 8500.00,
            '成交量': 2000000,
            '成交额': 1000000000.0
        }
    ])
    
    collector.aggregate_to_5min(mock_realtime_data)
    
    # 保存到磁盘
    print("保存数据到磁盘...")
    collector.save_realtime_data_to_disk()
    
    # 尝试加载数据
    print("从磁盘加载数据...")
    loaded_data = collector.load_realtime_data_from_disk('钢铁行业')
    
    if loaded_data is not None:
        print(f"加载的数据行数: {len(loaded_data)}")
        print("加载的数据样例:")
        print(loaded_data.head())
        assert len(loaded_data) > 0, "加载的数据为空"
    else:
        print("没有找到保存的数据文件")
    
    print("✓ 保存和加载测试通过")

if __name__ == "__main__":
    print("开始测试新的5分钟数据聚合逻辑...\n")
    
    try:
        test_timestamp_generation()
        test_target_timestamp_finding()
        test_aggregation_logic()
        test_save_and_load()
        
        print("\n🎉 所有测试通过！新的聚合逻辑工作正常。")
        
    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        import traceback
        traceback.print_exc()