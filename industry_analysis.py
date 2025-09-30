import akshare as ak
import pandas as pd
import talib
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import schedule
import time
import os
import json
from datetime import datetime
import threading
import numpy as np



# #所有行业板块实时
# stock_board_industry_name_em = ak.stock_board_industry_name_em()

# print("所有板块名称:")
# print(stock_board_industry_name_em['板块名称'].tolist())
# # ['能源金属', '小金属', '航天航空', '半导体', '有色金属', '电池', '电源设备', '光伏设备', '船舶制造', '医疗服务', '汽车整车', '贵金属']
# print(stock_board_industry_name_em)
# #     排名  板块名称    板块代码       最新价      涨跌额   涨跌幅             总市值   换手率  上涨家数  下跌家数   领涨股票  领涨股票-涨跌幅
# # 0    1  能源金属  BK1015    694.63    33.40  5.05    542864800000  9.08    13     0   盛屯矿业     10.02
# # 1    2   小金属  BK1027   3061.87    80.24  2.69   1189946448000  3.61    34     5   锡业股份      9.98
# # 2    3  航天航空  BK0480  53990.25  1324.85  2.52   1312159792000  2.50    41     5   中航沈飞     10.00
# #单行业板块分时历史
# stock_board_industry_hist_min_em_df = ak.stock_board_industry_hist_min_em(symbol="银行", period="5")
# print(stock_board_industry_hist_min_em_df)
# #                   日期时间       开盘       收盘       最高       最低   涨跌幅    涨跌额      成交量           成交额    振幅   换手率
# # 0     2025-08-18 09:35  4242.80  4249.01  4250.60  4227.13  0.12   5.03  4829616  3.743659e+09  0.55  0.04
# # 1     2025-08-18 09:40  4248.05  4259.35  4261.86  4244.46  0.24  10.34  2856570  2.054416e+09  0.41  0.02
# # 2     2025-08-18 09:45  4259.94  4263.56  4270.19  4258.19  0.10   4.21  2079865  1.459746e+09  0.28  0.02


class IndustryAnalysis:
    def __init__(self):
        self.data_dir = "industry_data"
        self.realtime_data = {}
        self.historical_data = {}
        
        if not os.path.exists(self.data_dir):
            os.makedirs(self.data_dir)
    
    def get_all_boards(self):
        """获取所有板块名称"""
        try:
            boards_df = ak.stock_board_industry_name_em()
            return boards_df['板块名称'].tolist()
        except Exception as e:
            print(f"获取板块列表失败: {e}")
            return []
    
    def get_historical_data(self, board_name, period="5"):
        """获取指定板块的历史数据"""
        try:
            hist_data = ak.stock_board_industry_hist_min_em(symbol=board_name, period=period)
            hist_data['日期时间'] = pd.to_datetime(hist_data['日期时间'])
            return hist_data
        except Exception as e:
            print(f"获取{board_name}历史数据失败: {e}")
            return None
    
    def save_historical_data(self, board_name, data, period="5"):
        """保存历史数据到文件"""
        filename = os.path.join(self.data_dir, f"{board_name}_{period}min_historical.csv")
        data.to_csv(filename, index=False, encoding='utf-8')
        print(f"已保存{board_name}历史数据到{filename}")
    
    def load_historical_data(self, board_name, period="5"):
        """从文件加载历史数据"""
        filename = os.path.join(self.data_dir, f"{board_name}_{period}min_historical.csv")
        if os.path.exists(filename):
            data = pd.read_csv(filename, encoding='utf-8')
            data['日期时间'] = pd.to_datetime(data['日期时间'])
            return data
        return None
    
    def clean_historical_data_dir(self):
        """清理历史数据文件夹"""
        if os.path.exists(self.data_dir):
            import shutil
            shutil.rmtree(self.data_dir)
            print(f"已清理历史数据目录: {self.data_dir}")
        
        os.makedirs(self.data_dir)
        print(f"已重建历史数据目录: {self.data_dir}")
    
    def collect_all_historical_data(self, delay_seconds=None):
        """每天早晨8点定时获取所有板块的历史数据"""
        print(f"开始获取所有板块历史数据 - {datetime.now()}")
        
        # 先清理历史数据文件夹
        self.clean_historical_data_dir()
        
        boards = self.get_all_boards()
        total_boards = len(boards)
        
        # 如果没有指定延迟时间，按照1小时完成所有板块来计算平均延迟
        if delay_seconds is None:
            delay_seconds = 3600 / total_boards if total_boards > 0 else 1
            print(f"未指定延迟时间，按1小时完成{total_boards}个板块计算，每个板块延迟{delay_seconds:.2f}秒")
        else:
            estimated_total_time = delay_seconds * total_boards
            print(f"使用指定延迟时间{delay_seconds}秒，预计总耗时{estimated_total_time/60:.1f}分钟")
        
        for i, board in enumerate(boards, 1):
            print(f"正在获取{board}的历史数据... ({i}/{total_boards})")
            hist_data = self.get_historical_data(board, "5")
            if hist_data is not None:
                self.save_historical_data(board, hist_data, "5")
            
            # 最后一个板块不需要延迟
            if i < total_boards:
                time.sleep(delay_seconds)
        
        print(f"所有板块历史数据获取完成 - {datetime.now()}")
    
    def get_realtime_data(self):
        """获取实时数据"""
        try:
            realtime_df = ak.stock_board_industry_name_em()
            return realtime_df
        except Exception as e:
            print(f"获取实时数据失败: {e}")
            return None
    
    def aggregate_to_5min(self, realtime_df):
        """将实时数据聚合成5分钟数据"""
        current_time = datetime.now()
        minute = current_time.minute
        
        # 计算5分钟周期的起始时间
        period_start_minute = (minute // 5) * 5
        period_start = current_time.replace(minute=period_start_minute, second=0, microsecond=0)
        
        aggregated_data = []
        
        for _, row in realtime_df.iterrows():
            board_name = row['板块名称']
            price = row['最新价']
            
            # 如果该板块还没有5分钟聚合数据，初始化
            if board_name not in self.realtime_data:
                self.realtime_data[board_name] = []
            
            # 检查是否需要新的5分钟周期
            if not self.realtime_data[board_name] or \
               self.realtime_data[board_name][-1]['period_start'] != period_start:
                
                self.realtime_data[board_name].append({
                    'period_start': period_start,
                    'open': price,
                    'high': price,
                    'low': price,
                    'close': price,
                    'volume': row.get('成交量', 0),
                    'amount': row.get('成交额', 0)
                })
            else:
                # 更新当前5分钟周期的数据
                current_period = self.realtime_data[board_name][-1]
                current_period['high'] = max(current_period['high'], price)
                current_period['low'] = min(current_period['low'], price)
                current_period['close'] = price
                current_period['volume'] += row.get('成交量', 0)
                current_period['amount'] += row.get('成交额', 0)
        
        return period_start
    
    def combine_historical_and_realtime(self, board_name):
        """合并历史数据和实时数据"""
        hist_data = self.load_historical_data(board_name)
        
        if hist_data is None:
            return None
        
        if board_name not in self.realtime_data or not self.realtime_data[board_name]:
            return hist_data
        
        # 转换实时数据格式
        realtime_rows = []
        for period in self.realtime_data[board_name]:
            realtime_rows.append({
                '日期时间': period['period_start'],
                '开盘': period['open'],
                '收盘': period['close'],
                '最高': period['high'],
                '最低': period['low'],
                '成交量': period['volume'],
                '成交额': period['amount']
            })
        
        realtime_df = pd.DataFrame(realtime_rows)
        
        # 合并数据
        combined = pd.concat([hist_data, realtime_df], ignore_index=True)
        combined = combined.sort_values('日期时间').reset_index(drop=True)
        
        return combined
    
    def resample_data(self, data, period):
        """重采样数据到指定周期"""
        if data is None or data.empty:
            return None
        
        data = data.set_index('日期时间')
        
        resampled = data.resample(period).agg({
            '开盘': 'first',
            '最高': 'max',
            '最低': 'min',
            '收盘': 'last',
            '成交量': 'sum',
            '成交额': 'sum'
        }).dropna()
        
        return resampled.reset_index()
    
    def calculate_macd(self, close_prices, fast=12, slow=26, signal=9):
        """使用talib计算MACD指标"""
        if len(close_prices) < slow:
            return None, None, None
        
        close_array = close_prices.values.astype(float)
        
        macd_line, signal_line, histogram = talib.MACD(close_array, 
                                                      fastperiod=fast, 
                                                      slowperiod=slow, 
                                                      signalperiod=signal)
        
        return pd.Series(macd_line, index=close_prices.index), \
               pd.Series(signal_line, index=close_prices.index), \
               pd.Series(histogram, index=close_prices.index)
    
    def detect_macd_signals(self, macd_line, signal_line):
        """检测MACD金叉死叉信号"""
        if macd_line is None or signal_line is None or len(macd_line) < 2:
            return []
        
        signals = []
        
        for i in range(1, len(macd_line)):
            if pd.isna(macd_line.iloc[i]) or pd.isna(signal_line.iloc[i]) or \
               pd.isna(macd_line.iloc[i-1]) or pd.isna(signal_line.iloc[i-1]):
                continue
            
            # 金叉：MACD线从下方穿越信号线
            if macd_line.iloc[i-1] <= signal_line.iloc[i-1] and macd_line.iloc[i] > signal_line.iloc[i]:
                signals.append({
                    'type': '金叉',
                    'index': i,
                    'macd': macd_line.iloc[i],
                    'signal': signal_line.iloc[i]
                })
            
            # 死叉：MACD线从上方穿越信号线
            elif macd_line.iloc[i-1] >= signal_line.iloc[i-1] and macd_line.iloc[i] < signal_line.iloc[i]:
                signals.append({
                    'type': '死叉',
                    'index': i,
                    'macd': macd_line.iloc[i],
                    'signal': signal_line.iloc[i]
                })
        
        return signals
    
    def analyze_board_macd(self, board_name):
        """分析单个板块的MACD"""
        base_data = self.combine_historical_and_realtime(board_name)
        
        if base_data is None or len(base_data) < 26:
            print(f"{board_name}: 数据不足，无法计算MACD")
            return
        
        results = {}
        periods = {
            '5分钟': '5T',
            '15分钟': '15T', 
            '30分钟': '30T',
            '60分钟': '60T'
        }
        
        for period_name, period_code in periods.items():
            if period_name == '5分钟':
                period_data = base_data
            else:
                period_data = self.resample_data(base_data, period_code)
            
            if period_data is None or len(period_data) < 26:
                continue
            
            close_prices = period_data['收盘']
            macd_line, signal_line, histogram = self.calculate_macd(close_prices)
            
            if macd_line is not None:
                signals = self.detect_macd_signals(macd_line, signal_line)
                
                if signals:
                    results[period_name] = []
                    for signal in signals:
                        timestamp = period_data.iloc[signal['index']]['日期时间']
                        results[period_name].append({
                            'time': timestamp,
                            'type': signal['type'],
                            'macd': signal['macd'],
                            'signal': signal['signal']
                        })
        
        if results:
            print(f"\n{board_name} MACD信号:")
            for period, signals in results.items():
                print(f"  {period}:")
                for signal in signals[-3:]:  # 只显示最近3个信号
                    print(f"    {signal['time']} - {signal['type']}: MACD={signal['macd']:.4f}, Signal={signal['signal']:.4f}")
    
    def collect_realtime_data(self):
        """每分钟收集实时数据"""
        realtime_df = self.get_realtime_data()
        if realtime_df is not None:
            period_start = self.aggregate_to_5min(realtime_df)
            print(f"实时数据已更新 - {datetime.now()}")
    
    def analyze_all_boards(self):
        """分析所有板块的MACD"""
        boards = self.get_all_boards()
        
        for board in boards:
            try:
                self.analyze_board_macd(board)
            except Exception as e:
                print(f"分析{board}失败: {e}")
    
    def start_monitoring(self):
        """启动监控系统"""
        # 定时任务
        schedule.every().day.at("08:00").do(self.collect_all_historical_data)
        schedule.every().minute.do(self.collect_realtime_data)
        
        print("板块监控系统已启动...")
        print("- 每天8:00获取历史数据")
        print("- 每分钟获取实时数据")
        
        while True:
            schedule.run_pending()
            time.sleep(1)
    
    def run_analysis(self):
        """运行分析（手动触发）"""
        print("开始分析所有板块...")
        self.analyze_all_boards()


if __name__ == "__main__":
    analyzer = IndustryAnalysis()
    
    # 可以选择启动监控模式或手动分析模式
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "monitor":
        analyzer.start_monitoring()
    elif len(sys.argv) > 1 and sys.argv[1] == "collect":
        delay_seconds = None
        if len(sys.argv) > 2:
            try:
                delay_seconds = float(sys.argv[2])
                print(f"使用指定延迟时间: {delay_seconds}秒")
            except ValueError:
                print("延迟时间参数无效，使用默认计算方式")
        analyzer.collect_all_historical_data(delay_seconds)
    elif len(sys.argv) > 1 and sys.argv[1] == "analyze":
        analyzer.run_analysis()
    else:
        # 交互式菜单，方便在VSCode中运行
        print("=== 板块MACD分析系统 ===")
        print("1. 启动监控模式（定时任务）")
        print("2. 手动获取历史数据")
        print("3. 手动分析所有板块")
        print("4. 退出")
        print()
        print("命令行使用方法:")
        print("  python industry_analysis.py monitor             # 启动监控模式")
        print("  python industry_analysis.py collect             # 手动获取历史数据（自动计算延迟）")
        print("  python industry_analysis.py collect <延迟秒数>    # 手动获取历史数据（指定延迟）")
        print("  python industry_analysis.py analyze             # 手动分析")
        print()
        
        while True:
            try:
                choice = input("请选择功能 (1-4): ").strip()
                
                if choice == "1":
                    print("启动监控模式...")
                    analyzer.start_monitoring()
                    break
                elif choice == "2":
                    delay_input = input("请输入延迟时间（秒，回车使用自动计算）: ").strip()
                    delay_seconds = None
                    if delay_input:
                        try:
                            delay_seconds = float(delay_input)
                        except ValueError:
                            print("输入的延迟时间无效，将使用自动计算")
                    print("开始获取历史数据...")
                    analyzer.collect_all_historical_data(delay_seconds)
                    break
                elif choice == "3":
                    print("开始分析所有板块...")
                    analyzer.run_analysis()
                    break
                elif choice == "4":
                    print("退出程序")
                    break
                else:
                    print("无效选择，请输入1-4")
            except KeyboardInterrupt:
                print("\n程序已退出")
                break
            except Exception as e:
                print(f"发生错误: {e}")
                break

