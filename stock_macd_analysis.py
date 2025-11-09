#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票日K MACD分析脚本
功能：
1. 从数据库获取股票列表和日K数据
2. 计算MACD指标和金叉死叉
3. 分析金叉死叉时间差值统计
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
import warnings
from db_manager import IndustryDataDB

warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class StockMACDAnalysis:
    def __init__(self, db_path='industry_data.db'):
        self.db = IndustryDataDB(db_path)
        self.stock_list = None
        self.macd_data = {}

    def load_stock_list(self, csv_path='data/stock_data_2025-11-07.csv'):
        """从CSV文件加载股票列表"""
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            # 提取股票代码和名称
            self.stock_list = df[['SECURITY_CODE', 'SECURITY_SHORT_NAME']].copy()
            self.stock_list.columns = ['stock_code', 'stock_name']
            print(f"成功加载 {len(self.stock_list)} 只股票")
            return True
        except Exception as e:
            print(f"加载股票列表失败: {e}")
            return False

    def get_daily_k_data(self, stock_code, start_date=None, end_date=None):
        """从数据库获取单只股票的日K数据"""
        try:
            # 使用现有的数据库管理器查询日K数据
            df = self.db.query_kline_data(
                period='1d',
                code=stock_code,
                start_date=start_date,
                end_date=end_date
            )

            if df.empty:
                print(f"股票 {stock_code} 没有日K数据")
                return None

            # 转换列名以适配分析逻辑
            df = df.rename(columns={
                'datetime': 'date',
                'open_price': 'open',
                'high_price': 'high',
                'low_price': 'low',
                'close_price': 'close'
            })

            # 转换日期格式
            df['date'] = pd.to_datetime(df['date'])
            df.set_index('date', inplace=True)

            return df
        except Exception as e:
            print(f"获取股票 {stock_code} 日K数据失败: {e}")
            return None

    def calculate_macd(self, df, fast_period=12, slow_period=26, signal_period=9):
        """计算MACD指标"""
        # 计算EMA
        ema_fast = df['close'].ewm(span=fast_period).mean()
        ema_slow = df['close'].ewm(span=slow_period).mean()

        # 计算MACD线 (DIF)
        macd_line = ema_fast - ema_slow

        # 计算信号线 (DEA)
        signal_line = macd_line.ewm(span=signal_period).mean()

        # 计算MACD柱状图 (MACD Bar)
        macd_histogram = macd_line - signal_line

        # 添加到DataFrame
        df['MACD'] = macd_line
        df['Signal'] = signal_line
        df['Histogram'] = macd_histogram

        return df

    def identify_cross_signals(self, df):
        """识别金叉和死叉信号"""
        # 金叉：MACD线上穿信号线 (Histogram从负变正)
        df['GoldenCross'] = (df['Histogram'] > 0) & (df['Histogram'].shift(1) <= 0)

        # 死叉：MACD线下穿信号线 (Histogram从正变负)
        df['DeathCross'] = (df['Histogram'] < 0) & (df['Histogram'].shift(1) >= 0)

        # 标记信号类型
        df['SignalType'] = 0
        df.loc[df['GoldenCross'], 'SignalType'] = 1  # 金叉
        df.loc[df['DeathCross'], 'SignalType'] = -1  # 死叉

        return df

    def analyze_cross_intervals(self, df):
        """分析金叉死叉间隔时间"""
        signals = df[df['SignalType'] != 0].copy()

        if len(signals) < 2:
            return None

        intervals = []

        for i in range(1, len(signals)):
            prev_signal = signals.iloc[i-1]
            curr_signal = signals.iloc[i]

            # 计算间隔天数
            interval_days = (curr_signal.name - prev_signal.name).days

            # 识别间隔类型
            if prev_signal['SignalType'] == 1 and curr_signal['SignalType'] == -1:
                interval_type = '金叉到死叉'
            elif prev_signal['SignalType'] == -1 and curr_signal['SignalType'] == 1:
                interval_type = '死叉到金叉'
            else:
                continue

            intervals.append({
                'start_date': prev_signal.name,
                'end_date': curr_signal.name,
                'interval_days': interval_days,
                'interval_type': interval_type,
                'start_signal': '金叉' if prev_signal['SignalType'] == 1 else '死叉',
                'end_signal': '死叉' if curr_signal['SignalType'] == -1 else '金叉',
                'start_price': prev_signal['close'],
                'end_price': curr_signal['close'],
                'price_change': (curr_signal['close'] - prev_signal['close']) / prev_signal['close'] * 100
            })

        return pd.DataFrame(intervals)

    def analyze_single_stock(self, stock_code, stock_name):
        """分析单只股票的MACD"""
        print(f"正在分析股票: {stock_code} - {stock_name}")

        # 获取日K数据
        df = self.get_daily_k_data(stock_code)
        if df is None:
            return None

        # 计算MACD
        df = self.calculate_macd(df)

        # 识别金叉死叉
        df = self.identify_cross_signals(df)

        # 分析间隔
        intervals = self.analyze_cross_intervals(df)

        result = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'data_count': len(df),
            'golden_cross_count': df['GoldenCross'].sum(),
            'death_cross_count': df['DeathCross'].sum(),
            'intervals': intervals
        }

        if intervals is not None and len(intervals) > 0:
            result['interval_stats'] = {
                'total_intervals': len(intervals),
                'golden_to_death_intervals': intervals[intervals['interval_type'] == '金叉到死叉'],
                'death_to_golden_intervals': intervals[intervals['interval_type'] == '死叉到金叉']
            }

            # 计算统计数据
            for interval_type in ['金叉到死叉', '死叉到金叉']:
                type_intervals = intervals[intervals['interval_type'] == interval_type]
                if len(type_intervals) > 0:
                    result['interval_stats'][f'{interval_type}_stats'] = {
                        'count': len(type_intervals),
                        'min_days': type_intervals['interval_days'].min(),
                        'max_days': type_intervals['interval_days'].max(),
                        'median_days': type_intervals['interval_days'].median(),
                        'mean_days': type_intervals['interval_days'].mean(),
                        'std_days': type_intervals['interval_days'].std(),
                        'mean_price_change': type_intervals['price_change'].mean(),
                        'median_price_change': type_intervals['price_change'].median()
                    }

        return result

    def run_analysis(self, max_stocks=None, csv_path='data/stock_data_2025-11-07.csv'):
        """运行完整分析"""
        print("开始股票MACD分析...")

        # 加载股票列表
        if not self.load_stock_list(csv_path):
            return

        # 限制分析股票数量（用于测试）
        stocks_to_analyze = self.stock_list
        if max_stocks:
            stocks_to_analyze = self.stock_list.head(max_stocks)

        all_results = []

        for idx, stock in stocks_to_analyze.iterrows():
            result = self.analyze_single_stock(stock['stock_code'], stock['stock_name'])
            if result:
                all_results.append(result)

        # 生成汇总统计
        self.generate_summary_statistics(all_results)

        return all_results

    def generate_summary_statistics(self, results):
        """生成汇总统计"""
        print("\n=== 汇总统计 ===")
        print(f"共分析 {len(results)} 只股票")

        # 收集所有间隔数据
        all_intervals = []
        for result in results:
            if result.get('intervals') is not None and len(result['intervals']) > 0:
                intervals_with_stock = result['intervals'].copy()
                intervals_with_stock['stock_code'] = result['stock_code']
                intervals_with_stock['stock_name'] = result['stock_name']
                all_intervals.append(intervals_with_stock)

        if all_intervals:
            all_intervals_df = pd.concat(all_intervals, ignore_index=True)

            print(f"\n总共发现 {len(all_intervals_df)} 个间隔")
            print(f"包含间隔数据的股票数量: {all_intervals_df['stock_code'].nunique()}")

            # 按间隔类型统计
            print("\n=== 按间隔类型统计 ===")
            for interval_type in ['金叉到死叉', '死叉到金叉']:
                type_data = all_intervals_df[all_intervals_df['interval_type'] == interval_type]
                if len(type_data) > 0:
                    print(f"\n{interval_type}:")
                    print(f"  样本数: {len(type_data)}")
                    print(f"  平均天数: {type_data['interval_days'].mean():.1f}")
                    print(f"  中位数天数: {type_data['interval_days'].median():.1f}")
                    print(f"  最短天数: {type_data['interval_days'].min()}")
                    print(f"  最长天数: {type_data['interval_days'].max()}")
                    print(f"  标准差: {type_data['interval_days'].std():.1f}")

                    # 按股票统计
                    stock_stats = type_data.groupby('stock_code')['interval_days'].agg(['count', 'mean', 'median'])
                    print(f"  涉及股票数: {len(stock_stats)}")

                    # 找出间隔最长和最短的股票
                    if len(stock_stats) > 0:
                        max_interval_stock = stock_stats['mean'].idxmax()
                        min_interval_stock = stock_stats['mean'].idxmin()
                        max_interval_stock_name = type_data[type_data['stock_code'] == max_interval_stock]['stock_name'].iloc[0]
                        min_interval_stock_name = type_data[type_data['stock_code'] == min_interval_stock]['stock_name'].iloc[0]

                        print(f"  平均间隔最长的股票: {max_interval_stock_name} ({stock_stats.loc[max_interval_stock, 'mean']:.1f}天)")
                        print(f"  平均间隔最短的股票: {min_interval_stock_name} ({stock_stats.loc[min_interval_stock, 'mean']:.1f}天)")

    def visualize_results(self, results, save_plots=True):
        """可视化分析结果"""
        # 收集所有间隔数据
        all_intervals = []
        for result in results:
            if result.get('intervals') is not None and len(result['intervals']) > 0:
                intervals_with_stock = result['intervals'].copy()
                intervals_with_stock['stock_code'] = result['stock_code']
                intervals_with_stock['stock_name'] = result['stock_name']
                all_intervals.append(intervals_with_stock)

        if not all_intervals:
            print("没有数据可用于可视化")
            return

        all_intervals_df = pd.concat(all_intervals, ignore_index=True)

        # 分离不同类型的间隔数据
        golden_to_death = all_intervals_df[all_intervals_df['interval_type'] == '金叉到死叉']
        death_to_golden = all_intervals_df[all_intervals_df['interval_type'] == '死叉到金叉']

        # 保存所有图表文件名
        saved_files = []

        # 图表1: 金叉到死叉分析
        if len(golden_to_death) > 0:
            self._plot_golden_to_death_analysis(golden_to_death, save_plots, saved_files)

        # 图表2: 死叉到金叉分析
        if len(death_to_golden) > 0:
            self._plot_death_to_golden_analysis(death_to_golden, save_plots, saved_files)

        # 图表3: 综合对比分析
        if len(golden_to_death) > 0 and len(death_to_golden) > 0:
            self._plot_comparison_analysis(all_intervals_df, save_plots, saved_files)

        # 图表4: 股票分布分析
        self._plot_stock_distribution_analysis(all_intervals_df, save_plots, saved_files)

        if save_plots and saved_files:
            print(f"已保存 {len(saved_files)} 个图表:")
            for file in saved_files:
                print(f"  - {file}")

    def _plot_golden_to_death_analysis(self, data, save_plots, saved_files):
        """绘制金叉到死叉分析图表"""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('金叉到死叉分析', fontsize=16, color='red')

        # 1. 间隔天数分布
        ax1 = axes[0, 0]
        ax1.hist(data['interval_days'], bins=20, alpha=0.7, color='red', edgecolor='black')
        ax1.set_xlabel('间隔天数')
        ax1.set_ylabel('频次')
        ax1.set_title('金叉到死叉间隔天数分布')
        ax1.grid(True, alpha=0.3)

        # 添加统计线
        mean_days = data['interval_days'].mean()
        median_days = data['interval_days'].median()
        ax1.axvline(mean_days, color='darkred', linestyle='--', label=f'平均: {mean_days:.1f}天')
        ax1.axvline(median_days, color='orange', linestyle='--', label=f'中位数: {median_days:.1f}天')
        ax1.legend()

        # 2. 价格变化分布
        ax2 = axes[0, 1]
        ax2.hist(data['price_change'], bins=20, alpha=0.7, color='red', edgecolor='black')
        ax2.set_xlabel('价格变化 (%)')
        ax2.set_ylabel('频次')
        ax2.set_title('金叉到死叉期间价格变化分布')
        ax2.grid(True, alpha=0.3)

        # 添加盈亏统计
        profit_rate = (data['price_change'] > 0).mean() * 100
        mean_change = data['price_change'].mean()
        ax2.axvline(0, color='black', linestyle='-', alpha=0.5)
        ax2.axvline(mean_change, color='darkred', linestyle='--',
                   label=f'平均: {mean_change:.2f}%')
        ax2.text(0.05, 0.95, f'盈利概率: {profit_rate:.1f}%',
                transform=ax2.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

        # 3. 间隔天数 vs 价格变化散点图
        ax3 = axes[1, 0]
        scatter = ax3.scatter(data['interval_days'], data['price_change'],
                            alpha=0.6, color='red', s=30)
        ax3.set_xlabel('间隔天数')
        ax3.set_ylabel('价格变化 (%)')
        ax3.set_title('间隔天数与价格变化关系')
        ax3.grid(True, alpha=0.3)
        ax3.axhline(y=0, color='black', linestyle='-', alpha=0.5)

        # 添加趋势线
        if len(data) > 1:
            z = np.polyfit(data['interval_days'], data['price_change'], 1)
            p = np.poly1d(z)
            ax3.plot(data['interval_days'], p(data['interval_days']),
                    "r--", alpha=0.8, label=f'趋势: y={z[0]:.3f}x+{z[1]:.2f}')
            ax3.legend()

        # 4. 股票表现统计
        ax4 = axes[1, 1]
        stock_stats = data.groupby('stock_name')['interval_days'].agg(['count', 'mean']).reset_index()
        stock_stats = stock_stats.sort_values('count', ascending=False).head(10)

        if len(stock_stats) > 0:
            bars = ax4.bar(range(len(stock_stats)), stock_stats['count'], color='red', alpha=0.7)
            ax4.set_xlabel('股票')
            ax4.set_ylabel('金叉到死叉次数')
            ax4.set_title('股票金叉到死叉频率排行')
            ax4.set_xticks(range(len(stock_stats)))
            ax4.set_xticklabels(stock_stats['stock_name'], rotation=45, ha='right')
            ax4.grid(True, alpha=0.3)

            # 添加数值标签
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                        f'{int(height)}', ha='center', va='bottom')

        plt.tight_layout()

        if save_plots:
            filename = 'macd_golden_to_death_analysis.png'
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            saved_files.append(filename)

        plt.show()

    def _plot_death_to_golden_analysis(self, data, save_plots, saved_files):
        """绘制死叉到金叉分析图表"""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('死叉到金叉分析', fontsize=16, color='green')

        # 1. 间隔天数分布
        ax1 = axes[0, 0]
        ax1.hist(data['interval_days'], bins=20, alpha=0.7, color='green', edgecolor='black')
        ax1.set_xlabel('间隔天数')
        ax1.set_ylabel('频次')
        ax1.set_title('死叉到金叉间隔天数分布')
        ax1.grid(True, alpha=0.3)

        # 添加统计线
        mean_days = data['interval_days'].mean()
        median_days = data['interval_days'].median()
        ax1.axvline(mean_days, color='darkgreen', linestyle='--', label=f'平均: {mean_days:.1f}天')
        ax1.axvline(median_days, color='lightgreen', linestyle='--', label=f'中位数: {median_days:.1f}天')
        ax1.legend()

        # 2. 价格变化分布
        ax2 = axes[0, 1]
        ax2.hist(data['price_change'], bins=20, alpha=0.7, color='green', edgecolor='black')
        ax2.set_xlabel('价格变化 (%)')
        ax2.set_ylabel('频次')
        ax2.set_title('死叉到金叉期间价格变化分布')
        ax2.grid(True, alpha=0.3)

        # 添加盈亏统计
        profit_rate = (data['price_change'] > 0).mean() * 100
        mean_change = data['price_change'].mean()
        ax2.axvline(0, color='black', linestyle='-', alpha=0.5)
        ax2.axvline(mean_change, color='darkgreen', linestyle='--',
                   label=f'平均: {mean_change:.2f}%')
        ax2.text(0.05, 0.95, f'盈利概率: {profit_rate:.1f}%',
                transform=ax2.transAxes, verticalalignment='top',
                bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

        # 3. 间隔天数 vs 价格变化散点图
        ax3 = axes[1, 0]
        scatter = ax3.scatter(data['interval_days'], data['price_change'],
                            alpha=0.6, color='green', s=30)
        ax3.set_xlabel('间隔天数')
        ax3.set_ylabel('价格变化 (%)')
        ax3.set_title('间隔天数与价格变化关系')
        ax3.grid(True, alpha=0.3)
        ax3.axhline(y=0, color='black', linestyle='-', alpha=0.5)

        # 添加趋势线
        if len(data) > 1:
            z = np.polyfit(data['interval_days'], data['price_change'], 1)
            p = np.poly1d(z)
            ax3.plot(data['interval_days'], p(data['interval_days']),
                    "g--", alpha=0.8, label=f'趋势: y={z[0]:.3f}x+{z[1]:.2f}')
            ax3.legend()

        # 4. 股票表现统计
        ax4 = axes[1, 1]
        stock_stats = data.groupby('stock_name')['interval_days'].agg(['count', 'mean']).reset_index()
        stock_stats = stock_stats.sort_values('count', ascending=False).head(10)

        if len(stock_stats) > 0:
            bars = ax4.bar(range(len(stock_stats)), stock_stats['count'], color='green', alpha=0.7)
            ax4.set_xlabel('股票')
            ax4.set_ylabel('死叉到金叉次数')
            ax4.set_title('股票死叉到金叉频率排行')
            ax4.set_xticks(range(len(stock_stats)))
            ax4.set_xticklabels(stock_stats['stock_name'], rotation=45, ha='right')
            ax4.grid(True, alpha=0.3)

            # 添加数值标签
            for i, bar in enumerate(bars):
                height = bar.get_height()
                ax4.text(bar.get_x() + bar.get_width()/2., height + 0.05,
                        f'{int(height)}', ha='center', va='bottom')

        plt.tight_layout()

        if save_plots:
            filename = 'macd_death_to_golden_analysis.png'
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            saved_files.append(filename)

        plt.show()

    def _plot_comparison_analysis(self, data, save_plots, saved_files):
        """绘制综合对比分析图表"""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('金叉死叉对比分析', fontsize=16)

        # 1. 间隔天数对比直方图
        ax1 = axes[0, 0]
        golden_to_death = data[data['interval_type'] == '金叉到死叉']
        death_to_golden = data[data['interval_type'] == '死叉到金叉']

        ax1.hist(golden_to_death['interval_days'], alpha=0.6, label='金叉到死叉',
                bins=15, color='red', density=True)
        ax1.hist(death_to_golden['interval_days'], alpha=0.6, label='死叉到金叉',
                bins=15, color='green', density=True)
        ax1.set_xlabel('间隔天数')
        ax1.set_ylabel('密度')
        ax1.set_title('间隔天数分布对比')
        ax1.legend()
        ax1.grid(True, alpha=0.3)

        # 2. 价格变化对比直方图
        ax2 = axes[0, 1]
        ax2.hist(golden_to_death['price_change'], alpha=0.6, label='金叉到死叉',
                bins=15, color='red', density=True)
        ax2.hist(death_to_golden['price_change'], alpha=0.6, label='死叉到金叉',
                bins=15, color='green', density=True)
        ax2.set_xlabel('价格变化 (%)')
        ax2.set_ylabel('密度')
        ax2.set_title('价格变化分布对比')
        ax2.legend()
        ax2.grid(True, alpha=0.3)
        ax2.axvline(0, color='black', linestyle='-', alpha=0.5)

        # 3. 统计指标对比
        ax3 = axes[1, 0]
        stats_data = {
            '指标': ['平均天数', '中位数天数', '标准差', '平均价格变化', '盈利概率'],
            '金叉到死叉': [
                golden_to_death['interval_days'].mean(),
                golden_to_death['interval_days'].median(),
                golden_to_death['interval_days'].std(),
                golden_to_death['price_change'].mean(),
                (golden_to_death['price_change'] > 0).mean() * 100
            ],
            '死叉到金叉': [
                death_to_golden['interval_days'].mean(),
                death_to_golden['interval_days'].median(),
                death_to_golden['interval_days'].std(),
                death_to_golden['price_change'].mean(),
                (death_to_golden['price_change'] > 0).mean() * 100
            ]
        }

        x = np.arange(len(stats_data['指标']))
        width = 0.35

        bars1 = ax3.bar(x - width/2, stats_data['金叉到死叉'], width,
                       label='金叉到死叉', color='red', alpha=0.7)
        bars2 = ax3.bar(x + width/2, stats_data['死叉到金叉'], width,
                       label='死叉到金叉', color='green', alpha=0.7)

        ax3.set_xlabel('统计指标')
        ax3.set_ylabel('数值')
        ax3.set_title('关键指标对比')
        ax3.set_xticks(x)
        ax3.set_xticklabels(stats_data['指标'], rotation=45, ha='right')
        ax3.legend()
        ax3.grid(True, alpha=0.3)

        # 添加数值标签
        for bars in [bars1, bars2]:
            for bar in bars:
                height = bar.get_height()
                if not np.isnan(height):
                    ax3.text(bar.get_x() + bar.get_width()/2., height + height*0.01,
                            f'{height:.2f}', ha='center', va='bottom', fontsize=8)

        # 4. 盈利分布饼图对比
        ax4 = axes[1, 1]
        profit_golden = (golden_to_death['price_change'] > 0).sum()
        loss_golden = len(golden_to_death) - profit_golden
        profit_death = (death_to_golden['price_change'] > 0).sum()
        loss_death = len(death_to_golden) - profit_death

        # 创建子图
        ax4.pie([profit_golden, loss_golden], labels=['盈利', '亏损'],
                autopct='%1.1f%%', colors=['lightcoral', 'darkred'],
                startangle=90, wedgeprops=dict(width=0.3))
        ax4.set_title('金叉到死叉盈亏分布')

        # 在右侧添加死叉到金叉的饼图
        ax4_pie = ax4.inset_axes([0.6, 0.1, 0.4, 0.8])
        ax4_pie.pie([profit_death, loss_death], labels=['盈利', '亏损'],
                   autopct='%1.1f%%', colors=['lightgreen', 'darkgreen'],
                   startangle=90, wedgeprops=dict(width=0.3))
        ax4_pie.set_title('死叉到金叉盈亏分布')

        plt.tight_layout()

        if save_plots:
            filename = 'macd_comparison_analysis.png'
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            saved_files.append(filename)

        plt.show()

    def _plot_stock_distribution_analysis(self, data, save_plots, saved_files):
        """绘制股票分布分析图表"""
        fig, axes = plt.subplots(2, 2, figsize=(14, 10))
        fig.suptitle('股票分布分析', fontsize=16)

        # 1. 不同股票的间隔天数箱线图
        ax1 = axes[0, 0]
        top_stocks = data['stock_code'].value_counts().head(8).index
        top_stock_data = data[data['stock_code'].isin(top_stocks)]

        if len(top_stock_data) > 0:
            sns.boxplot(data=top_stock_data, x='stock_name', y='interval_days', ax=ax1)
            ax1.set_xlabel('股票')
            ax1.set_ylabel('间隔天数')
            ax1.set_title('主要股票间隔天数分布')
            ax1.tick_params(axis='x', rotation=45)
            ax1.grid(True, alpha=0.3)

        # 2. 股票间隔频率统计
        ax2 = axes[0, 1]
        stock_interval_counts = data['stock_name'].value_counts().head(15)

        bars = ax2.bar(range(len(stock_interval_counts)), stock_interval_counts.values,
                      color='steelblue', alpha=0.7)
        ax2.set_xlabel('股票')
        ax2.set_ylabel('总间隔次数')
        ax2.set_title('股票MACD信号频率排行')
        ax2.set_xticks(range(len(stock_interval_counts)))
        ax2.set_xticklabels(stock_interval_counts.index, rotation=45, ha='right')
        ax2.grid(True, alpha=0.3)

        # 添加数值标签
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax2.text(bar.get_x() + bar.get_width()/2., height + 0.1,
                    f'{int(height)}', ha='center', va='bottom')

        # 3. 股票平均间隔天数
        ax3 = axes[1, 0]
        stock_avg_intervals = data.groupby('stock_name')['interval_days'].mean().sort_values(ascending=False).head(15)

        bars = ax3.bar(range(len(stock_avg_intervals)), stock_avg_intervals.values,
                      color='orange', alpha=0.7)
        ax3.set_xlabel('股票')
        ax3.set_ylabel('平均间隔天数')
        ax3.set_title('股票平均间隔天数排行')
        ax3.set_xticks(range(len(stock_avg_intervals)))
        ax3.set_xticklabels(stock_avg_intervals.index, rotation=45, ha='right')
        ax3.grid(True, alpha=0.3)

        # 添加数值标签
        for i, bar in enumerate(bars):
            height = bar.get_height()
            ax3.text(bar.get_x() + bar.get_width()/2., height + 0.5,
                    f'{height:.1f}', ha='center', va='bottom')

        # 4. 间隔类型分布饼图
        ax4 = axes[1, 1]
        type_counts = data['interval_type'].value_counts()
        colors = ['red', 'green'][:len(type_counts)]

        wedges, texts, autotexts = ax4.pie(type_counts.values, labels=type_counts.index,
                                          autopct='%1.1f%%', colors=colors, startangle=90)
        ax4.set_title('间隔类型分布')

        # 在中心添加总数
        ax4.text(0, 0, f'总计\n{len(data)}个间隔', ha='center', va='center',
                fontsize=12, bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

        plt.tight_layout()

        if save_plots:
            filename = 'macd_stock_distribution_analysis.png'
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            saved_files.append(filename)

        plt.show()

if __name__ == "__main__":
    # 创建分析实例
    analyzer = StockMACDAnalysis()

    # 运行分析（可以设置max_stocks参数限制分析的股票数量用于测试）
    results = analyzer.run_analysis(max_stocks=10)  # 先分析10只股票进行测试

    if results:
        # 生成可视化图表
        analyzer.visualize_results(results)

        # 保存详细结果到CSV
        all_intervals = []
        for result in results:
            if result.get('intervals') is not None and len(result['intervals']) > 0:
                intervals_with_stock = result['intervals'].copy()
                intervals_with_stock['stock_code'] = result['stock_code']
                intervals_with_stock['stock_name'] = result['stock_name']
                all_intervals.append(intervals_with_stock)

        if all_intervals:
            all_intervals_df = pd.concat(all_intervals, ignore_index=True)
            all_intervals_df.to_csv('macd_intervals_analysis.csv', index=False, encoding='utf-8-sig')
            print("详细分析结果已保存为 macd_intervals_analysis.csv")