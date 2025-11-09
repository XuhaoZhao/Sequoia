#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票MACD分析 - 快速运行脚本
简化的MACD分析运行脚本
"""

import os
from stock_macd_analysis import StockMACDAnalysis

def main():
    print("=== 股票MACD金叉死叉分析系统 ===\n")

    # 检查必要的文件
    if not os.path.exists('data/stock_data_2025-11-07.csv'):
        print("错误: 找不到股票数据文件 'data/stock_data_2025-11-07.csv'")
        print("请确保CSV文件存在于data目录中")
        return

    # 检查数据库文件
    if not os.path.exists('industry_data.db'):
        print("错误: 找不到数据库文件 'industry_data.db'")
        print("请确保数据库文件存在于当前目录中")
        return

    # 创建分析器
    analyzer = StockMACDAnalysis(db_path='industry_data.db')

    # 选择运行模式
    print("请选择运行模式:")
    print("1. 快速测试 (分析10只股票)")
    print("2. 中等规模 (分析50只股票)")
    print("3. 完整分析 (分析所有股票)")
    print("4. 自定义股票数量")

    try:
        choice = input("请输入选择 (1-4): ").strip()

        if choice == '1':
            max_stocks = 10
            print("\n开始快速测试分析...")
        elif choice == '2':
            max_stocks = 50
            print("\n开始中等规模分析...")
        elif choice == '3':
            max_stocks = None
            print("\n开始完整分析...")
        elif choice == '4':
            try:
                max_stocks = int(input("请输入要分析的股票数量: "))
                if max_stocks <= 0:
                    print("股票数量必须大于0")
                    return
                print(f"\n开始分析 {max_stocks} 只股票...")
            except ValueError:
                print("请输入有效的数字")
                return
        else:
            print("无效选择，默认进行快速测试")
            max_stocks = 10
            print("\n开始快速测试分析...")

    except KeyboardInterrupt:
        print("\n\n用户取消操作")
        return
    except:
        print("输入错误，默认进行快速测试")
        max_stocks = 10
        print("\n开始快速测试分析...")

    # 运行分析
    try:
        results = analyzer.run_analysis(max_stocks=max_stocks, csv_path='data/stock_data_2025-11-07.csv')

        if results and len(results) > 0:
            print(f"\n分析完成！共分析了 {len(results)} 只股票")

            # 生成可视化图表
            print("\n生成分析图表...")
            analyzer.visualize_results(results)

            print("\n=== 分析完成 ===")
            print("生成的文件:")
            print("- macd_intervals_analysis.csv: 详细分析结果")
            print("- macd_analysis_results.png: 分析图表")
        else:
            print("分析失败或没有有效结果")

    except Exception as e:
        print(f"分析过程中出现错误: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()