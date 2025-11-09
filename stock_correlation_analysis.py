#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
股票价格相关性分析脚本
⚠️ 重要声明：本工具仅供学习和研究使用，不构成任何投资建议！
⚠️ 股票投资存在风险，历史相关性不代表未来表现！
⚠️ 请勿根据本分析结果做出投资决策，投资需谨慎！

功能：
1. 从数据库读取多只股票的日K数据
2. 计算股票间的相关性矩阵
3. 找出相似度高的股票对
4. 提供多种相关性分析方法
5. 生成可视化图表展示相关性结果

使用风险提示：
- 本分析基于历史数据，不能预测未来走势
- 高相关性股票可能同向波动，增加组合风险
- 投资决策请咨询专业人士，切勿盲目跟风
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime
import warnings
from scipy.spatial.distance import cosine
from sklearn.metrics.pairwise import pairwise_distances
from db_manager import IndustryDataDB

warnings.filterwarnings('ignore')

# 设置中文字体
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS']
plt.rcParams['axes.unicode_minus'] = False

class StockCorrelationAnalysis:
    def __init__(self, db_path='industry_data.db'):
        self.db = IndustryDataDB(db_path)
        self.stock_list = None
        self.price_data = {}
        self.correlation_matrix = None

    def load_stock_list(self, csv_path='data/stock_data_2025-11-08.csv'):
        """从CSV文件加载股票列表"""
        try:
            df = pd.read_csv(csv_path, encoding='utf-8-sig')
            # 提取股票代码和名称，将股票代码作为字符串读取以保留前导0
            self.stock_list = df[['SECURITY_CODE', 'SECURITY_SHORT_NAME']].copy()
            self.stock_list.columns = ['stock_code', 'stock_name']

            # 将股票代码转换为字符串，保留前导0
            self.stock_list['stock_code'] = self.stock_list['stock_code'].astype(str)

            # 格式化股票代码
            self.stock_list['stock_code'] = self.stock_list['stock_code'].apply(self._format_stock_code)

            print(f"成功加载 {len(self.stock_list)} 只股票")
            return True
        except Exception as e:
            print(f"加载股票列表失败: {e}")
            return False

    def _format_stock_code(self, stock_code):
        """
        格式化股票代码，确保有正确的前导0

        Args:
            stock_code: 原始股票代码

        Returns:
            格式化后的股票代码
        """
        # 如果是数字，转换为字符串
        if isinstance(stock_code, (int, float)):
            stock_code = str(int(stock_code))

        # 移除可能存在的前缀
        stock_code = stock_code.replace('SH', '').replace('SZ', '').replace('.SH', '').replace('.SZ', '')

        # 移除小数点（如果存在）
        if '.' in stock_code:
            stock_code = stock_code.split('.')[0]

        # 根据长度补全前导0
        if len(stock_code) == 4:
            # 4位代码，补全到6位
            if stock_code.startswith('6'):  # 上海证券交易所
                return stock_code.zfill(6)
            elif stock_code.startswith('0') or stock_code.startswith('3'):  # 深圳证券交易所
                return stock_code.zfill(6)
            else:
                # 其他情况，尝试补全到6位
                return stock_code.zfill(6)
        elif len(stock_code) == 5:
            # 5位代码，补全到6位
            if stock_code.startswith('6'):  # 上海证券交易所
                return stock_code.zfill(6)
            elif stock_code.startswith('0') or stock_code.startswith('3'):  # 深圳证券交易所
                return stock_code.zfill(6)
            else:
                # 其他情况，尝试补全到6位
                return stock_code.zfill(6)
        elif len(stock_code) == 6:
            # 已经是6位，直接返回
            return stock_code
        elif len(stock_code) < 4:
            # 少于4位，可能是数据错误，尝试补全到6位
            return stock_code.zfill(6)
        else:
            # 其他长度，直接返回原代码
            return stock_code

    def get_daily_k_data(self, stock_code, start_date=None, end_date=None):
        """从数据库获取单只股票的日K数据"""
        try:
            # 确保股票代码有正确的格式（补全前导0）
            formatted_code = self._format_stock_code(stock_code)

            # 使用现有的数据库管理器查询日K数据
            df = self.db.query_kline_data(
                period='1d',
                code=formatted_code,
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

    def prepare_price_data(self, stock_codes, start_date=None, end_date=None,
                          data_type='close', min_days=30):
        """
        准备多只股票的价格数据

        Args:
            stock_codes: 股票代码列表
            start_date: 开始日期
            end_date: 结束日期
            data_type: 数据类型 ('close', 'open', 'high', 'low', 'returns')
            min_days: 最少交易天数要求

        Returns:
            DataFrame，列为股票代码，行为日期，值为价格数据
        """
        print("正在准备股票价格数据...")

        all_data = {}
        valid_stocks = []

        for stock_code in stock_codes:
            df = self.get_daily_k_data(stock_code, start_date, end_date)
            if df is not None and len(df) >= min_days:
                if data_type == 'returns':
                    # 计算日收益率
                    all_data[stock_code] = df['close'].pct_change().dropna()
                else:
                    # 使用价格数据
                    all_data[stock_code] = df[data_type]
                valid_stocks.append(stock_code)
            else:
                print(f"股票 {stock_code} 数据不足，跳过")

        if not all_data:
            print("没有获取到有效的价格数据")
            return None

        # 对齐日期索引
        price_df = pd.DataFrame(all_data)
        price_df = price_df.dropna()

        print(f"成功获取 {len(valid_stocks)} 只股票的数据，时间范围：{price_df.index.min().date()} 到 {price_df.index.max().date()}")
        print(f"数据矩阵大小: {price_df.shape}")

        return price_df, valid_stocks

    def calculate_correlation_matrix(self, price_df, method='pearson'):
        """
        计算相关性矩阵

        Args:
            price_df: 价格数据DataFrame
            method: 相关性计算方法 ('pearson', 'spearman', 'cosine', 'dtw')

        Returns:
            相关性矩阵
        """
        print(f"正在计算 {method} 相关性矩阵...")

        if method in ['pearson', 'spearman']:
            # 直接使用pandas的corr方法
            corr_matrix = price_df.corr(method=method)
        elif method == 'cosine':
            # 计算余弦相似度
            n_stocks = price_df.shape[1]
            corr_matrix = np.zeros((n_stocks, n_stocks))

            for i, stock1 in enumerate(price_df.columns):
                for j, stock2 in enumerate(price_df.columns):
                    if i <= j:
                        # 计算余弦相似度 (1 - 余弦距离)
                        cosine_sim = 1 - cosine(price_df[stock1].values, price_df[stock2].values)
                        corr_matrix[i, j] = cosine_sim
                        corr_matrix[j, i] = cosine_sim
                    else:
                        corr_matrix[i, j] = corr_matrix[j, i]

            corr_matrix = pd.DataFrame(corr_matrix,
                                     index=price_df.columns,
                                     columns=price_df.columns)
        elif method == 'dtw':
            # 动态时间规整相似度（简化版本）
            corr_matrix = 1 / (1 + pairwise_distances(price_df.T, metric='euclidean'))
            corr_matrix = pd.DataFrame(corr_matrix,
                                     index=price_df.columns,
                                     columns=price_df.columns)
        else:
            raise ValueError(f"不支持的相关性计算方法: {method}")

        self.correlation_matrix = corr_matrix
        return corr_matrix

    def find_highly_correlated_pairs(self, corr_matrix, threshold=0.9):
        """
        找出高相关性的股票对

        Args:
            corr_matrix: 相关性矩阵
            threshold: 相关性阈值
            exclude_self: 是否排除自相关

        Returns:
            高相关性股票对的列表
        """
        high_corr_pairs = []

        for i in range(len(corr_matrix.columns)):
            for j in range(i+1, len(corr_matrix.columns)):  # 只考虑上三角矩阵
                stock1 = corr_matrix.columns[i]
                stock2 = corr_matrix.columns[j]
                correlation = corr_matrix.iloc[i, j]

                if correlation > threshold:  # 只要正相关，不要负相关
                    stock1_name = self.stock_list[self.stock_list['stock_code'] == stock1]['stock_name'].iloc[0] if len(self.stock_list[self.stock_list['stock_code'] == stock1]) > 0 else stock1
                    stock2_name = self.stock_list[self.stock_list['stock_code'] == stock2]['stock_name'].iloc[0] if len(self.stock_list[self.stock_list['stock_code'] == stock2]) > 0 else stock2

                    high_corr_pairs.append({
                        'stock1_code': stock1,
                        'stock1_name': stock1_name,
                        'stock2_code': stock2,
                        'stock2_name': stock2_name,
                        'correlation': correlation,
                        'correlation_type': 'positive'
                    })

        # 按相关性大小排序
        high_corr_pairs.sort(key=lambda x: abs(x['correlation']), reverse=True)

        return high_corr_pairs

    def run_analysis(self, max_stocks=50, start_date=None, end_date=None,
                    correlation_methods=['pearson'], threshold=0.9,
                    data_type='close'):
        """
        运行完整的相关性分析

        Args:
            max_stocks: 最大分析股票数量
            start_date: 开始日期 (格式: YYYY-MM-DD)
            end_date: 结束日期 (格式: YYYY-MM-DD)
            correlation_methods: 相关性计算方法列表
            threshold: 相关性阈值
            data_type: 数据类型 ('close', 'returns')

        Returns:
            分析结果字典
        """
        print("开始股票相关性分析...")
        print("⚠️  重要提醒：本分析仅供学习研究，不构成投资建议！")
        print("⚠️  历史相关性不代表未来走势，投资决策请谨慎！")
        print("⚠️  股市有风险，入市需谨慎！\n")

        # 加载股票列表
        if not self.load_stock_list():
            return None

        # 限制分析的股票数量
        stocks_to_analyze = self.stock_list['stock_code'].head(max_stocks).tolist()
        print(f"将分析 {len(stocks_to_analyze)} 只股票")

        # 准备价格数据
        price_data = self.prepare_price_data(stocks_to_analyze, start_date, end_date, data_type)
        if price_data is None:
            return None

        price_df, valid_stocks = price_data

        # 计算相关性矩阵
        results = {}
        for method in correlation_methods:
            print(f"\n=== 使用 {method} 方法分析 ===")
            corr_matrix = self.calculate_correlation_matrix(price_df, method)

            # 找出高相关性股票对
            high_corr_pairs = self.find_highly_correlated_pairs(corr_matrix, threshold)

            results[method] = {
                'correlation_matrix': corr_matrix,
                'high_correlation_pairs': high_corr_pairs,
                'summary_stats': self._calculate_summary_stats(corr_matrix, high_corr_pairs)
            }

            print(f"发现 {len(high_corr_pairs)} 对相关性大于 {threshold} 的股票")

        results['metadata'] = {
            'analyzed_stocks': valid_stocks,
            'data_type': data_type,
            'date_range': f"{price_df.index.min().date()} to {price_df.index.max().date()}",
            'total_trading_days': len(price_df)
        }

        return results

    def _calculate_summary_stats(self, corr_matrix, high_corr_pairs):
        """计算汇总统计信息"""
        # 移除对角线元素（自相关）
        mask = ~np.eye(corr_matrix.shape[0], dtype=bool)
        correlations = corr_matrix.values[mask]

        stats = {
            'total_pairs': len(correlations) // 2,  # 除以2因为矩阵是对称的
            'mean_correlation': np.nanmean(correlations),
            'median_correlation': np.nanmedian(correlations),
            'std_correlation': np.nanstd(correlations),
            'max_correlation': np.nanmax(correlations),
            'min_correlation': np.nanmin(correlations),
            'high_corr_pairs_count': len(high_corr_pairs),
            'positive_correlations_ratio': np.mean(correlations > 0)
        }

        return stats

    def visualize_correlation_matrix(self, corr_matrix, method='pearson',
                                   save_plot=True, high_corr_pairs=None):
        """
        可视化相关性矩阵

        Args:
            corr_matrix: 相关性矩阵
            method: 计算方法名称
            save_plot: 是否保存图片
            high_corr_pairs: 高相关性股票对列表，用于筛选显示的股票
        """
        # 如果有高相关性股票对，只显示这些股票的相关性矩阵
        if high_corr_pairs and len(high_corr_pairs) > 0:
            # 提取所有参与高相关性的股票代码
            high_corr_stocks = set()
            for pair in high_corr_pairs:
                high_corr_stocks.add(pair['stock1_code'])
                high_corr_stocks.add(pair['stock2_code'])

            high_corr_stocks = list(high_corr_stocks)
            corr_matrix_subset = corr_matrix.loc[high_corr_stocks, high_corr_stocks]
            title_suffix = f" ({len(high_corr_stocks)}只高相关性股票)"
        else:
            # 如果没有高相关性股票对，显示前20只股票
            if corr_matrix.shape[0] > 20:
                mean_corr = corr_matrix.mean(axis=1)
                top_stocks = mean_corr.nlargest(20).index
                corr_matrix_subset = corr_matrix.loc[top_stocks, top_stocks]
                title_suffix = f" (前20只股票)"
            else:
                corr_matrix_subset = corr_matrix
                title_suffix = f" (所有{corr_matrix.shape[0]}只股票)"

        # 根据股票数量调整图形大小
        n_stocks = corr_matrix_subset.shape[0]
        figsize = (max(12, n_stocks * 0.3), max(10, n_stocks * 0.3))
        plt.figure(figsize=figsize)

        # 创建热力图 - 只显示相关性大于0.9的值
        mask = np.triu(np.ones_like(corr_matrix_subset, dtype=bool))
        # 创建一个掩码，隐藏所有小于等于0.9的值
        value_mask = corr_matrix_subset <= 0.9
        # 组合掩码：上三角 + 小于等于0.9的值
        combined_mask = mask | value_mask

        sns.heatmap(corr_matrix_subset,
                   mask=combined_mask,
                   annot=True,
                   cmap='Reds',  # 只用红色系，因为都是正相关
                   vmin=0.9,
                   vmax=1.0,
                   square=True,
                   fmt='.3f',
                   cbar_kws={"shrink": .8, "label": "相关系数"},
                   annot_kws={'size': max(6, 48 // n_stocks)})

        plt.title(f'股票 {method} 相关性矩阵热力图（相关性>0.9）{title_suffix}', fontsize=16)
        plt.xticks(rotation=45, ha='right', fontsize=max(8, 48 // n_stocks))
        plt.yticks(rotation=0, fontsize=max(8, 48 // n_stocks))
        plt.tight_layout()

        if save_plot:
            filename = f'stock_correlation_heatmap_{method}.png'
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"相关性热力图已保存为: {filename}")

        plt.show()

    def visualize_correlation_distribution(self, corr_matrix, method='pearson', save_plot=True):
        """可视化相关性分布"""
        # 移除对角线元素
        mask = ~np.eye(corr_matrix.shape[0], dtype=bool)
        correlations = corr_matrix.values[mask]
        correlations = correlations[~np.isnan(correlations)]

        plt.figure(figsize=(15, 5))

        # 1. 相关性分布直方图
        plt.subplot(1, 3, 1)
        plt.hist(correlations, bins=50, alpha=0.7, color='skyblue', edgecolor='black')
        plt.axvline(0, color='red', linestyle='--', alpha=0.7)
        plt.axvline(np.mean(correlations), color='green', linestyle='--',
                   alpha=0.7, label=f'平均值: {np.mean(correlations):.3f}')
        plt.xlabel('相关系数')
        plt.ylabel('频次')
        plt.title(f'{method} 相关性分布')
        plt.legend()
        plt.grid(True, alpha=0.3)

        # 2. 箱线图
        plt.subplot(1, 3, 2)
        plt.boxplot(correlations, vert=True)
        plt.axhline(y=0, color='red', linestyle='--', alpha=0.7)
        plt.ylabel('相关系数')
        plt.title(f'{method} 相关性箱线图')
        plt.grid(True, alpha=0.3)

        # 3. 相关性累积分布
        plt.subplot(1, 3, 3)
        sorted_corr = np.sort(correlations)
        cumulative = np.arange(1, len(sorted_corr) + 1) / len(sorted_corr)
        plt.plot(sorted_corr, cumulative, linewidth=2)
        plt.axvline(0, color='red', linestyle='--', alpha=0.7)
        plt.axhline(y=0.5, color='orange', linestyle='--', alpha=0.7, label='中位数')
        plt.xlabel('相关系数')
        plt.ylabel('累积概率')
        plt.title(f'{method} 相关性累积分布')
        plt.legend()
        plt.grid(True, alpha=0.3)

        plt.tight_layout()

        if save_plot:
            filename = f'stock_correlation_distribution_{method}.png'
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"相关性分布图已保存为: {filename}")

        plt.show()

    def visualize_high_correlation_pairs(self, high_corr_pairs, method='pearson',
                                      save_plot=True):
        """可视化高相关性股票对"""
        if not high_corr_pairs:
            print("没有高相关性股票对可以可视化")
            return

        # 显示所有高相关性股票对
        pairs_to_show = high_corr_pairs

        # 创建标签
        pair_labels = [f"{pair['stock1_name']}\n{pair['stock2_name']}"
                      for pair in pairs_to_show]
        correlations = [pair['correlation'] for pair in pairs_to_show]
        colors = ['green' if corr > 0 else 'red' for corr in correlations]

        # 根据股票对数量调整图形大小
        n_pairs = len(pairs_to_show)
        figsize = (14, max(8, n_pairs * 0.25))
        plt.figure(figsize=figsize)
        bars = plt.barh(range(len(pair_labels)), correlations, color=colors, alpha=0.7)

        plt.yticks(range(len(pair_labels)), pair_labels)
        plt.xlabel('相关系数')
        plt.title(f'所有{len(pairs_to_show)}对高相关性股票 ({method}方法)', fontsize=16)
        plt.grid(True, alpha=0.3)

        # 添加数值标签
        for bar, corr in zip(bars, correlations):
            plt.text(bar.get_width() + 0.01 if corr > 0 else bar.get_width() - 0.01,
                    bar.get_y() + bar.get_height()/2,
                    f'{corr:.3f}',
                    ha='left' if corr > 0 else 'right',
                    va='center')

        # 添加图例
        from matplotlib.patches import Patch
        legend_elements = [Patch(facecolor='green', alpha=0.7, label='正相关'),
                          Patch(facecolor='red', alpha=0.7, label='负相关')]
        plt.legend(handles=legend_elements)

        plt.tight_layout()

        if save_plot:
            filename = f'high_correlation_pairs_{method}.png'
            plt.savefig(filename, dpi=300, bbox_inches='tight')
            print(f"高相关性股票对图已保存为: {filename}")

        plt.show()

    def export_results(self, results, filename_prefix='stock_correlation_analysis'):
        """导出分析结果到文件"""
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        for method, result in results.items():
            if method == 'metadata':
                continue

            # 导出相关性矩阵
            corr_matrix = result['correlation_matrix']
            corr_matrix.to_csv(f'{filename_prefix}_correlation_matrix_{method}_{timestamp}.csv',
                             encoding='utf-8-sig')

            # 导出高相关性股票对
            if result['high_correlation_pairs']:
                pairs_df = pd.DataFrame(result['high_correlation_pairs'])
                pairs_df.to_csv(f'{filename_prefix}_high_corr_pairs_{method}_{timestamp}.csv',
                              index=False, encoding='utf-8-sig')

            print(f"{method} 方法的结果已导出")

        print(f"所有分析结果已导出，时间戳: {timestamp}")

    def run_full_analysis(self, max_stocks=50, start_date=None, end_date=None,
                         threshold=0.9, correlation_methods=['pearson', 'spearman'],
                         data_type='close', save_plots=True, export_results=True):
        """运行完整分析流程"""
        # 运行分析
        results = self.run_analysis(
            max_stocks=max_stocks,
            start_date=start_date,
            end_date=end_date,
            correlation_methods=correlation_methods,
            threshold=threshold,
            data_type=data_type
        )

        if not results:
            print("分析失败")
            return

        # 显示汇总信息
        print("\n" + "="*50)
        print("分析汇总")
        print("="*50)

        metadata = results['metadata']
        print(f"分析股票数量: {len(metadata['analyzed_stocks'])}")
        print(f"数据类型: {metadata['data_type']}")
        print(f"时间范围: {metadata['date_range']}")
        print(f"交易天数: {metadata['total_trading_days']}")

        for method in correlation_methods:
            if method in results:
                stats = results[method]['summary_stats']
                print(f"\n{method} 方法统计:")
                print(f"  股票对总数: {stats['total_pairs']}")
                print(f"  平均相关性: {stats['mean_correlation']:.3f}")
                print(f"  相关性标准差: {stats['std_correlation']:.3f}")
                print(f"  最大相关性: {stats['max_correlation']:.3f}")
                print(f"  最小相关性: {stats['min_correlation']:.3f}")
                print(f"  高相关性股票对数 (阈值{threshold}): {stats['high_corr_pairs_count']}")
                print(f"  正相关比例: {stats['positive_correlations_ratio']:.1%}")

        # 生成可视化图表
        print("\n生成可视化图表...")
        for method in correlation_methods:
            if method in results:
                result = results[method]
                corr_matrix = result['correlation_matrix']
                high_corr_pairs = result['high_correlation_pairs']

                print(f"\n生成 {method} 方法的图表...")

                # 相关性矩阵热力图（只显示高相关性股票）
                self.visualize_correlation_matrix(corr_matrix, method, save_plots, high_corr_pairs)

                # 相关性分布图
                self.visualize_correlation_distribution(corr_matrix, method, save_plots)

                # 高相关性股票对（显示所有）
                if high_corr_pairs:
                    self.visualize_high_correlation_pairs(high_corr_pairs, method, save_plots)

        # 导出结果
        if export_results:
            print("\n导出分析结果...")
            self.export_results(results)

        return results

if __name__ == "__main__":
    # 创建分析实例
    analyzer = StockCorrelationAnalysis()

    # 运行完整分析
    # 重要提醒：股票相关性分析仅供参考，不构成投资建议！
    # 投资有风险，决策需谨慎！
    #
    # 参数说明：
    # max_stocks: 分析的最大股票数量（None表示分析所有股票）
    # start_date: 分析开始日期 (格式: '2024-01-01')，None表示使用所有可用数据
    # end_date: 分析结束日期 (格式: '2024-12-31')，None表示使用最新数据
    # threshold: 相关性阈值，大于此值被认为是高相关性
    # correlation_methods: 相关性计算方法列表 ['pearson', 'spearman', 'cosine']
    # data_type: 'close' 使用收盘价，'returns' 使用收益率

    print("=" * 60)
    print("股票相关性分析工具")
    print("=" * 60)
    print("⚠️  重要提醒：本分析仅供学习和研究参考！")
    print("⚠️  历史相关性不代表未来表现！")
    print("⚠️  投资有风险，决策需谨慎，请勿据此做出投资决策！")
    print("=" * 60)

    # 默认分析所有可用股票和最新数据，查找高相关性股票
    results = analyzer.run_full_analysis(
        max_stocks=None,            # 分析所有股票
        start_date=None,            # 使用所有可用历史数据
        end_date=None,              # 使用最新数据
        threshold=0.9,              # 高相关性阈值，查找相关性>0.9的股票
        correlation_methods=['pearson', 'spearman'],  # 使用两种方法
        data_type='close',          # 使用收盘价
        save_plots=True,            # 保存图表
        export_results=True         # 导出结果
    )

    if results:
        print("\n股票相关性分析完成！")
        print("生成的文件包括：")
        print("- 相关性热力图")
        print("- 相关性分布图")
        print("- 高相关性股票对图表")
        print("- CSV格式的详细数据文件")

        # 详细展示相关性>0.9的股票对
        print("\n" + "="*60)
        print("高相关性股票对分析（相关性大于 0.9）")
        print("="*60)

        for method in ['pearson', 'spearman']:
            if method in results:
                high_corr_pairs = results[method]['high_correlation_pairs']
                if high_corr_pairs:
                    print(f"\n📊 {method.upper()} 方法 - 发现 {len(high_corr_pairs)} 对相关性大于0.9的股票:")
                    print("-" * 50)

                    for i, pair in enumerate(high_corr_pairs, 1):
                        print(f"{i:3d}. {pair['stock1_name']} ({pair['stock1_code']}) ↔ {pair['stock2_name']} ({pair['stock2_code']})")
                        print(f"     相关性: {pair['correlation']:.4f} ({pair['correlation_type']})")
                else:
                    print(f"\n📊 {method.upper()} 方法 - 未发现相关性大于0.9的股票对")

        print("\n" + "⚠️" * 20)
        print("警告：高相关性股票可能同涨同跌，增加投资组合风险！")
        print("建议：分散投资，避免持仓过于集中的股票！")
        print("⚠️" * 20)
    else:
        print("分析失败，请检查数据库连接和数据可用性")