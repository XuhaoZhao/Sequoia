#!/usr/bin/env python
# -*- coding:utf-8 -*-
"""
分时数据分析示例
演示如何使用Stock类的analyze_intraday_tick_data方法
"""

import pandas as pd
import json
from financial_framework.stock import Stock

# 示例数据（根据您提供的数据格式）
sample_data = {
    '时间': ['09:15:00', '09:15:09', '09:15:18', '09:15:27', '09:15:36',
             '14:56:51', '14:56:54', '14:56:57', '14:57:00', '15:00:00'],
    '成交价': [10.60, 10.57, 10.57, 10.57, 10.57,
               10.50, 10.49, 10.49, 10.50, 10.50],
    '手数': [11, 1547, 1549, 1549, 1552,
            30, 109, 27, 68, 8338],
    '买卖盘性质': ['中性盘', '中性盘', '中性盘', '中性盘', '中性盘',
                   '买盘', '卖盘', '卖盘', '买盘', '买盘']
}

def print_section(title, data, indent=0):
    """格式化打印分析结果的某个部分"""
    indent_str = "  " * indent
    print(f"\n{indent_str}{'='*60}")
    print(f"{indent_str}{title}")
    print(f"{indent_str}{'='*60}")

    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, (dict, list)) and value:
                print(f"{indent_str}{key}:")
                print_section("", value, indent + 1)
            elif isinstance(value, list) and not value:
                print(f"{indent_str}{key}: []")
            else:
                print(f"{indent_str}{key}: {value}")
    elif isinstance(data, list):
        for i, item in enumerate(data, 1):
            if isinstance(item, dict):
                print(f"{indent_str}[{i}]")
                for k, v in item.items():
                    print(f"{indent_str}  {k}: {v}")
            else:
                print(f"{indent_str}[{i}] {item}")
    else:
        print(f"{indent_str}{data}")

def main():
    """主函数"""
    print("="*80)
    print("股票分时数据分析示例")
    print("="*80)

    # 创建DataFrame
    df = pd.DataFrame(sample_data)

    print(f"\n原始数据样本（共{len(df)}条记录）:")
    print(df.to_string())

    # 创建Stock实例
    stock = Stock()

    # 执行分析（设置大单阈值为100手）
    print("\n\n开始执行分析...")
    analysis_result = stock.analyze_intraday_tick_data(df, big_order_threshold=100)

    if not analysis_result:
        print("分析失败！")
        return

    # 打印分析结果
    print("\n\n" + "="*80)
    print("分析结果")
    print("="*80)

    # 1. 综合摘要
    if 'summary' in analysis_result:
        print_section("【一、综合摘要】", analysis_result['summary'])

    # 2. 资金流向分析
    if 'capital_flow' in analysis_result:
        print_section("【二、资金流向分析】", analysis_result['capital_flow'])

    # 3. 关键时段分析
    if 'key_periods' in analysis_result:
        print_section("【三、关键时段分析】", analysis_result['key_periods'])

    # 4. 大单追踪
    if 'big_orders' in analysis_result:
        print_section("【四、大单追踪】", analysis_result['big_orders'])

    # 5. 价格波动分析
    if 'price_volatility' in analysis_result:
        print_section("【五、价格波动分析】", analysis_result['price_volatility'])

    # 6. 买卖盘力量对比
    if 'trading_power' in analysis_result:
        print_section("【六、买卖盘力量对比】", analysis_result['trading_power'])

    # 7. 交易策略信号
    if 'strategy_signals' in analysis_result:
        print_section("【七、交易策略信号】", analysis_result['strategy_signals'])

    # 保存完整结果到JSON文件
    output_file = 'intraday_analysis_result.json'
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(analysis_result, f, ensure_ascii=False, indent=2, default=str)

    print(f"\n\n完整分析结果已保存到: {output_file}")
    print("="*80)

if __name__ == "__main__":
    main()
