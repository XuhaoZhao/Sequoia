#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
读取MACD金叉信号文件的示例
"""

import pandas as pd
from financial_framework.file_path_generator import generate_macd_signal_path

def read_macd_signals(instrument_type, period="30m", date=None):
    """
    读取MACD金叉信号数据

    Args:
        instrument_type: 产品类型 ('stock', 'etf' 等)
        period: 数据周期，默认 '30m'
        date: 日期字符串，格式 YYYY-MM-DD，如果为None则使用今天

    Returns:
        DataFrame: 金叉信号数据
    """
    # 使用统一的路径生成函数
    filepath = generate_macd_signal_path(instrument_type, period, date)

    try:
        df = pd.read_csv(filepath)
        print(f"成功读取文件: {filepath}")
        print(f"共有 {len(df)} 条金叉信号记录\n")
        return df
    except FileNotFoundError:
        print(f"文件不存在: {filepath}")
        return None
    except Exception as e:
        print(f"读取文件失败: {e}")
        return None


if __name__ == "__main__":
    # 示例1: 读取今天的股票金叉信号
    print("=== 读取今天的股票金叉信号 ===")
    stock_signals = read_macd_signals('stock')
    if stock_signals is not None:
        print(stock_signals)

    print("\n" + "="*50 + "\n")

    # 示例2: 读取今天的ETF金叉信号
    print("=== 读取今天的ETF金叉信号 ===")
    etf_signals = read_macd_signals('etf')
    if etf_signals is not None:
        print(etf_signals)

    print("\n" + "="*50 + "\n")

    # 示例3: 读取指定日期的信号
    print("=== 读取指定日期的股票金叉信号 ===")
    historical_signals = read_macd_signals('stock', date='2025-10-22')
    if historical_signals is not None:
        print(historical_signals)
