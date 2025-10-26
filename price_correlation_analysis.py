import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from rewrite_ak_share.rewrite_index_zh_em import index_zh_a_hist

import adata as ad
# from datetime import datetime
# import seaborn as sns

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

def get_index_data(symbol, start_date, end_date):
    """获取指数数据"""
    df = index_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date)
    # 转换日期格式
    df['日期'] = pd.to_datetime(df['日期'])
    df = df.rename(columns={'日期': 'date', '收盘': 'close'})
    # 确保close列是数值类型
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    return df[['date', 'close']].copy()

def get_etf_data(fund_code, start_date, end_date):
    """获取ETF数据"""
    df = ad.fund.market.get_market_etf(fund_code, 1, start_date, end_date)
    # 转换日期格式
    df['trade_time'] = pd.to_datetime(df['trade_time'])
    df = df.rename(columns={'trade_time': 'date', 'close': 'close'})
    # 确保close列是数值类型
    df['close'] = pd.to_numeric(df['close'], errors='coerce')
    return df[['date', 'close']].copy()

def normalize_prices(prices):
    """标准化价格数据，以第一个交易日为基准"""
    return (prices / prices.iloc[0]) * 100

def calculate_correlation(df1, df2):
    """计算两个数据序列的相关性"""
    # 确保日期对齐
    merged = pd.merge(df1, df2, on='date', how='inner', suffixes=('_index', '_etf'))
    if len(merged) == 0:
        return None, None, None

    # 去除close列中的NaN值
    merged = merged.dropna(subset=['close_index', 'close_etf'])

    # 确保数据类型正确
    merged['close_index'] = pd.to_numeric(merged['close_index'], errors='coerce')
    merged['close_etf'] = pd.to_numeric(merged['close_etf'], errors='coerce')

    # 再次去除可能的NaN值
    merged = merged.dropna(subset=['close_index', 'close_etf'])

    # 计算日价格变动
    merged['change_index'] = merged['close_index'].pct_change()
    merged['change_etf'] = merged['close_etf'].pct_change()

    # 去除第一行的NaN和任何无效的价格变动
    merged = merged.dropna()

    # 计算相关系数
    if len(merged) < 2:
        return None, None, None

    price_correlation = merged['close_index'].corr(merged['close_etf'])
    change_correlation = merged['change_index'].corr(merged['change_etf'])

    return price_correlation, change_correlation, merged

def plot_price_comparison(index_df, etf_df, correlation_info):
    """绘制价格对比图"""
    fig, axes = plt.subplots(2, 2, figsize=(15, 10))
    fig.suptitle('上证指数(000001) vs 国泰上证5年期国债ETF(511090) 相关性分析', fontsize=16, fontweight='bold')

    # 使用合并后的数据来绘制标准化价格，确保时间轴一致
    if correlation_info[2] is not None:
        merged = correlation_info[2]
        # 基于合并数据的第一个交易日进行标准化
        index_base_price = merged['close_index'].iloc[0]
        etf_base_price = merged['close_etf'].iloc[0]

        merged['index_normalized'] = (merged['close_index'] / index_base_price) * 100
        merged['etf_normalized'] = (merged['close_etf'] / etf_base_price) * 100

        print(f"合并数据日期范围: {merged['date'].min()} 到 {merged['date'].max()}")
        print(f"上证指数基准价格: {index_base_price}")
        print(f"国债ETF基准价格: {etf_base_price}")
    else:
        merged = None

    # 1. 价格走势对比图（标准化）
    ax1 = axes[0, 0]
    if merged is not None:
        ax1.plot(merged['date'], merged['index_normalized'], label='上证指数', linewidth=2, color='red')
        ax1.plot(merged['date'], merged['etf_normalized'], label='国债ETF', linewidth=2, color='blue')
    ax1.set_title('价格走势对比 (标准化到100)', fontsize=12)
    ax1.set_ylabel('标准化价格')
    ax1.legend()
    ax1.grid(True, alpha=0.3)
    ax1.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
    ax1.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
    plt.setp(ax1.xaxis.get_majorticklabels(), rotation=45)

    # 2. 散点图（价格相关性）
    ax2 = axes[0, 1]
    if correlation_info[2] is not None:
        merged = correlation_info[2]
        ax2.scatter(merged['close_index'], merged['close_etf'], alpha=0.6, s=30)
        ax2.set_title(f'价格相关性散点图\n相关系数: {correlation_info[0]:.4f}', fontsize=12)
        ax2.set_xlabel('上证指数收盘价')
        ax2.set_ylabel('国债ETF收盘价')
        ax2.grid(True, alpha=0.3)

        # 添加趋势线
        z = np.polyfit(merged['close_index'], merged['close_etf'], 1)
        p = np.poly1d(z)
        ax2.plot(merged['close_index'], p(merged['close_index']), "r--", alpha=0.8)

    # 3. 价格变动对比图
    ax3 = axes[1, 0]
    if correlation_info[2] is not None:
        merged = correlation_info[2]
        ax3.plot(merged['date'], merged['change_index'] * 100, label='上证指数价格变动', alpha=0.7, linewidth=1)
        ax3.plot(merged['date'], merged['change_etf'] * 100, label='国债ETF价格变动', alpha=0.7, linewidth=1)
        ax3.set_title('日价格变动对比', fontsize=12)
        ax3.set_ylabel('价格变动 (%)')
        ax3.set_xlabel('日期')
        ax3.legend()
        ax3.grid(True, alpha=0.3)
        ax3.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m'))
        ax3.xaxis.set_major_locator(mdates.MonthLocator(interval=3))
        plt.setp(ax3.xaxis.get_majorticklabels(), rotation=45)

    # 4. 价格变动散点图
    ax4 = axes[1, 1]
    if correlation_info[2] is not None:
        merged = correlation_info[2]
        ax4.scatter(merged['change_index'] * 100, merged['change_etf'] * 100, alpha=0.6, s=30)
        ax4.set_title(f'价格变动相关性散点图\n相关系数: {correlation_info[1]:.4f}', fontsize=12)
        ax4.set_xlabel('上证指数价格变动 (%)')
        ax4.set_ylabel('国债ETF价格变动 (%)')
        ax4.grid(True, alpha=0.3)

        # 添加趋势线
        z = np.polyfit(merged['change_index'], merged['change_etf'], 1)
        p = np.poly1d(z)
        ax4.plot(merged['change_index'] * 100, p(merged['change_index']) * 100, "r--", alpha=0.8)

    plt.tight_layout()
    return fig

def main():
    """主函数"""
    print("开始获取数据...")

    # 获取上证指数数据
    print("获取上证指数数据...")
    index_df = get_index_data("000001", "20240102", "20251024")
    print(f"上证指数数据: {len(index_df)} 条记录")
    print(index_df.head())

    # 获取国债ETF数据
    print("\n获取国债ETF数据...")
    etf_df = get_etf_data('511090', '20240102', '20251024')
    print(f"国债ETF数据: {len(etf_df)} 条记录")
    print(etf_df.head())

    # 计算相关性
    print("\n计算相关性...")
    price_corr, change_corr, merged_data = calculate_correlation(index_df, etf_df)

    if price_corr is not None:
        print(f"\n=== 相关性分析结果 ===")
        print(f"价格相关系数: {price_corr:.4f}")
        print(f"价格变动相关系数: {change_corr:.4f}")
        print(f"有效交易日数: {len(merged_data)}")

        # 分析相关性强度
        if abs(price_corr) >= 0.7:
            corr_strength = "强"
        elif abs(price_corr) >= 0.3:
            corr_strength = "中等"
        else:
            corr_strength = "弱"

        print(f"相关性强度: {corr_strength}")

        if price_corr < 0:
            print("注意: 两个资产呈现负相关关系")

    # 绘制图表
    print("\n绘制价格对比图...")
    correlation_info = (price_corr, change_corr, merged_data)
    fig = plot_price_comparison(index_df, etf_df, correlation_info)

    plt.show()

    # 保存数据到CSV
    if merged_data is not None:
        merged_data.to_csv('/Users/zxh/code-repo/Sequoia/merged_data_analysis.csv', index=False)
        print("合并数据已保存到: merged_data_analysis.csv")

if __name__ == "__main__":
    main()