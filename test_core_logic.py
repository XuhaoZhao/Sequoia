#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
简单测试新的5分钟数据聚合逻辑的核心功能
"""

from datetime import datetime, timedelta

def generate_trading_timestamps(date=None):
    """生成A股交易时间的5分钟时间戳序列"""
    if date is None:
        date = datetime.now().date()
    elif isinstance(date, str):
        date = datetime.strptime(date, '%Y-%m-%d').date()
    
    timestamps = []
    
    # 上午交易时间：9:30-11:30
    current_time = datetime.combine(date, datetime.strptime("09:30", "%H:%M").time())
    end_morning = datetime.combine(date, datetime.strptime("11:30", "%H:%M").time())
    
    while current_time <= end_morning:
        timestamps.append(current_time)
        current_time += timedelta(minutes=5)
    
    # 下午交易时间：13:00-15:00
    current_time = datetime.combine(date, datetime.strptime("13:00", "%H:%M").time())
    end_afternoon = datetime.combine(date, datetime.strptime("15:00", "%H:%M").time())
    
    while current_time <= end_afternoon:
        timestamps.append(current_time)
        current_time += timedelta(minutes=5)
    
    return timestamps

def find_target_timestamp(data_time, trading_timestamps):
    """根据数据时间找到对应的目标时间戳（5分钟周期）"""
    # 找到数据应该归属的5分钟时间戳
    for i, timestamp in enumerate(trading_timestamps):
        # 如果是第一个时间戳，从开盘开始的数据都归到第一个时间戳
        if i == 0 and data_time <= timestamp:
            return timestamp
        
        # 如果不是第一个时间戳，找到前一个和当前时间戳之间的范围
        if i > 0:
            prev_timestamp = trading_timestamps[i-1]
            if prev_timestamp < data_time <= timestamp:
                return timestamp
    
    # 如果数据时间超过了最后一个时间戳，归到最后一个时间戳
    if trading_timestamps and data_time > trading_timestamps[-1]:
        return trading_timestamps[-1]
    
    return None

def test_timestamp_generation():
    """测试时间戳生成功能"""
    print("=== 测试时间戳生成功能 ===")
    
    # 测试指定日期的时间戳生成
    test_date = "2025-08-19"
    timestamps = generate_trading_timestamps(test_date)
    
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
    
    # 计算预期的时间戳数量
    # 上午：9:30-11:30 = 2小时 = 120分钟 = 24个5分钟时间戳 + 1（包含起始点） = 25个
    # 下午：13:00-15:00 = 2小时 = 120分钟 = 24个5分钟时间戳 + 1（包含起始点） = 25个
    # 总计：25 + 25 = 50个时间戳
    expected_count = 25 + 25
    assert len(timestamps) == expected_count, f"时间戳数量不正确: {len(timestamps)} vs {expected_count}"
    
    print("SUCCESS: 时间戳生成测试通过")
    return timestamps

def test_target_timestamp_finding():
    """测试目标时间戳查找功能"""
    print("\n=== 测试目标时间戳查找功能 ===")
    
    test_date = "2025-08-19"
    timestamps = generate_trading_timestamps(test_date)
    
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
        
        actual_target = find_target_timestamp(data_time, timestamps)
        
        print(f"数据时间: {data_time_str} -> 目标时间戳: {actual_target}")
        assert actual_target == expected_target, f"时间戳查找错误: {data_time_str} -> {actual_target} (期望: {expected_target})"
    
    print("SUCCESS: 目标时间戳查找测试通过")

def test_edge_cases():
    """测试边界情况"""
    print("\n=== 测试边界情况 ===")
    
    test_date = "2025-08-19"
    timestamps = generate_trading_timestamps(test_date)
    
    # 测试非交易时间
    non_trading_times = [
        "2025-08-19 08:00:00",  # 开盘前
        "2025-08-19 12:00:00",  # 午休时间
        "2025-08-19 16:00:00",  # 收盘后
    ]
    
    for time_str in non_trading_times:
        data_time = datetime.strptime(time_str, "%Y-%m-%d %H:%M:%S")
        result = find_target_timestamp(data_time, timestamps)
        print(f"非交易时间 {time_str} -> {result}")
    
    # 测试开盘前的数据应该归到第一个时间戳
    early_time = datetime.strptime("2025-08-19 09:25:00", "%Y-%m-%d %H:%M:%S")
    result = find_target_timestamp(early_time, timestamps)
    expected = datetime.strptime("2025-08-19 09:30:00", "%Y-%m-%d %H:%M:%S")
    assert result == expected, f"开盘前数据归属错误: {result} vs {expected}"
    
    # 测试收盘后的数据应该归到最后一个时间戳
    late_time = datetime.strptime("2025-08-19 15:30:00", "%Y-%m-%d %H:%M:%S")
    result = find_target_timestamp(late_time, timestamps)
    expected = datetime.strptime("2025-08-19 15:00:00", "%Y-%m-%d %H:%M:%S")
    assert result == expected, f"收盘后数据归属错误: {result} vs {expected}"
    
    print("SUCCESS: 边界情况测试通过")

if __name__ == "__main__":
    print("开始测试新的5分钟数据聚合逻辑核心功能...\n")
    
    try:
        test_timestamp_generation()
        test_target_timestamp_finding()
        test_edge_cases()
        
        print("\nSUCCESS: 所有核心功能测试通过！")
        print("\n新的聚合逻辑主要改进：")
        print("1. SUCCESS: 预定义A股交易时间的5分钟时间戳（9:30, 9:35, ..., 15:00）")
        print("2. SUCCESS: 按整5分钟时间戳对数据进行分片和聚合")
        print("3. SUCCESS: 处理实时数据的bad case（可归到预定义时间戳）")
        print("4. SUCCESS: 确定性的时间戳，适合A股市场时间")
        
    except Exception as e:
        print(f"\nFAILED: 测试失败: {e}")
        import traceback
        traceback.print_exc()