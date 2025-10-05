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
from datetime import datetime, timedelta
import threading
import numpy as np
import push

class IndustryDataCollector:
    """数据收集器，负责历史数据和实时数据的获取和保存"""
    def __init__(self):
        self.data_dir = "industry_data"
        self.realtime_dir = os.path.join(self.data_dir, "realtime")
        self.historical_dir = os.path.join(self.data_dir, "historical")
        
        for dir_path in [self.data_dir, self.realtime_dir, self.historical_dir]:
            if not os.path.exists(dir_path):
                os.makedirs(dir_path)
        
        self.realtime_data = {}
        self.current_date = datetime.now().strftime('%Y-%m-%d')
        
        # 初始化A股交易时间的5分钟时间戳
        self.trading_timestamps = self._generate_trading_timestamps()
    
    def _generate_trading_timestamps(self, date=None):
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
    
    def _find_target_timestamp(self, data_time):
        """根据数据时间找到对应的目标时间戳（5分钟周期）"""
        date = data_time.date()
        
        # 如果日期变了，重新生成当天的时间戳
        if not hasattr(self, '_cached_date') or self._cached_date != date:
            self.trading_timestamps = self._generate_trading_timestamps(date)
            self._cached_date = date
        
        # 找到数据应该归属的5分钟时间戳
        for i, timestamp in enumerate(self.trading_timestamps):
            # 如果是第一个时间戳，从开盘开始的数据都归到第一个时间戳
            if i == 0 and data_time <= timestamp:
                return timestamp
            
            # 如果不是第一个时间戳，找到前一个和当前时间戳之间的范围
            if i > 0:
                prev_timestamp = self.trading_timestamps[i-1]
                if prev_timestamp < data_time <= timestamp:
                    return timestamp
        
        # 如果数据时间超过了最后一个时间戳，归到最后一个时间戳
        if self.trading_timestamps and data_time > self.trading_timestamps[-1]:
            return self.trading_timestamps[-1]
        
        return None
    
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
        filename = os.path.join(self.historical_dir, f"{board_name}_{period}min_historical.csv")
        data.to_csv(filename, index=False, encoding='utf-8')
        print(f"已保存{board_name}历史数据到{filename}")
    
    def load_historical_data(self, board_name, period="5"):
        """从文件加载历史数据"""
        filename = os.path.join(self.historical_dir, f"{board_name}_{period}min_historical.csv")
        if os.path.exists(filename):
            data = pd.read_csv(filename, encoding='utf-8')
            data['日期时间'] = pd.to_datetime(data['日期时间'])
            return data
        return None
    
    def clean_historical_data_dir(self):
        """清理历史数据文件夹"""
        if os.path.exists(self.historical_dir):
            import shutil
            shutil.rmtree(self.historical_dir)
            print(f"已清理历史数据目录: {self.historical_dir}")
        
        os.makedirs(self.historical_dir)
        print(f"已重建历史数据目录: {self.historical_dir}")
    
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
        """将实时数据聚合成5分钟数据，使用预定义的交易时间戳"""
        current_time = datetime.now()
        current_date = current_time.date()
        
        # 确保我们有当天的时间戳
        if not hasattr(self, '_cached_date') or self._cached_date != current_date:
            self.trading_timestamps = self._generate_trading_timestamps(current_date)
            self._cached_date = current_date
        
        for _, row in realtime_df.iterrows():
            board_name = row['板块名称']
            price = row['最新价']
            volume = row.get('成交量', 0)
            amount = row.get('成交额', 0)
            
            # 找到当前数据应该归属的5分钟时间戳
            target_timestamp = self._find_target_timestamp(current_time)
            if target_timestamp is None:
                continue  # 不在交易时间内
            
            # 如果该板块还没有数据，初始化所有时间戳的数据结构
            if board_name not in self.realtime_data:
                self.realtime_data[board_name] = {}
                
                # 为所有交易时间戳初始化数据结构
                for ts in self.trading_timestamps:
                    self.realtime_data[board_name][ts] = {
                        'timestamp': ts,
                        'open': None,
                        'high': None,
                        'low': None,
                        'close': None,
                        'volume': 0,
                        'amount': 0,
                        'data_points': 0  # 记录该时间段内收到的数据点数量
                    }
            
            # 获取目标时间戳的数据
            target_data = self.realtime_data[board_name][target_timestamp]
            
            # 更新该时间戳的数据
            if target_data['open'] is None:
                # 第一次收到该时间段的数据
                target_data['open'] = price
                target_data['high'] = price
                target_data['low'] = price
                target_data['close'] = price
            else:
                # 更新高低价
                target_data['high'] = max(target_data['high'], price)
                target_data['low'] = min(target_data['low'], price)
                target_data['close'] = price  # 收盘价始终是最新价
            
            # 累计成交量和成交额
            target_data['volume'] += volume
            target_data['amount'] += amount
            target_data['data_points'] += 1
        
        return current_time
    
    def save_realtime_data_to_disk(self):
        """将内存中的实时数据保存到磁盘"""
        if not self.realtime_data:
            return
        
        for board_name, timestamp_data in self.realtime_data.items():
            if not timestamp_data:
                continue
            
            # 按日期保存实时数据
            filename = os.path.join(self.realtime_dir, f"{board_name}_{self.current_date}_realtime.csv")
            
            # 转换为DataFrame格式，只保存有数据的时间戳
            rows = []
            for timestamp, data in timestamp_data.items():
                # 只保存有实际数据的时间戳（至少收到过一次数据）
                if data['open'] is not None and data['data_points'] > 0:
                    rows.append({
                        '日期时间': timestamp,
                        '开盘': data['open'],
                        '收盘': data['close'],
                        '最高': data['high'],
                        '最低': data['low'],
                        '成交量': data['volume'],
                        '成交额': data['amount']
                    })
            
            if rows:
                df = pd.DataFrame(rows)
                # 按时间排序
                df = df.sort_values('日期时间')
                
                # 如果文件已存在，追加数据（去重）
                if os.path.exists(filename):
                    existing_df = pd.read_csv(filename, encoding='utf-8')
                    existing_df['日期时间'] = pd.to_datetime(existing_df['日期时间'])
                    df['日期时间'] = pd.to_datetime(df['日期时间'])
                    
                    # 合并并去重
                    combined_df = pd.concat([existing_df, df], ignore_index=True)
                    combined_df = combined_df.drop_duplicates(subset=['日期时间']).sort_values('日期时间')
                    combined_df.to_csv(filename, index=False, encoding='utf-8')
                else:
                    df.to_csv(filename, index=False, encoding='utf-8')
                
                print(f"已保存{board_name}实时数据到{filename}，共{len(rows)}个时间点")
    
    def load_realtime_data_from_disk(self, board_name, date=None):
        """从磁盘加载实时数据"""
        if date is None:
            date = self.current_date
        
        filename = os.path.join(self.realtime_dir, f"{board_name}_{date}_realtime.csv")
        if os.path.exists(filename):
            data = pd.read_csv(filename, encoding='utf-8')
            data['日期时间'] = pd.to_datetime(data['日期时间'])
            return data
        return None
    
    def collect_realtime_data(self):
        """每分钟收集实时数据并保存到磁盘"""
        current_date_str = datetime.now().strftime('%Y-%m-%d')
        
        # 如果日期变了，先保存昨天的数据，然后清空内存
        if current_date_str != self.current_date:
            print(f"日期变更：{self.current_date} -> {current_date_str}")
            self.save_realtime_data_to_disk()
            self.realtime_data = {}
            self.current_date = current_date_str
        
        realtime_df = self.get_realtime_data()
        if realtime_df is not None:
            current_time = self.aggregate_to_5min(realtime_df)
            
            # 每个整5分钟时刻保存一次数据到磁盘
            # 检查当前时间是否是整5分钟（例如9:30, 9:35, 9:40等）
            if current_time.minute % 5 == 0 and current_time.second < 10:
                self.save_realtime_data_to_disk()
            
            print(f"实时数据已更新 - {datetime.now()}")
    
    def combine_historical_and_realtime(self, board_name):
        """合并历史数据和实时数据"""
        hist_data = self.load_historical_data(board_name)
        
        if hist_data is None:
            return None
        
        # 加载今天的实时数据（从磁盘）
        today_realtime = self.load_realtime_data_from_disk(board_name)
        
        # 加载内存中的实时数据
        memory_realtime_rows = []
        if board_name in self.realtime_data and self.realtime_data[board_name]:
            for timestamp, data in self.realtime_data[board_name].items():
                # 只包含有实际数据的时间戳
                if data['open'] is not None and data['data_points'] > 0:
                    memory_realtime_rows.append({
                        '日期时间': timestamp,
                        '开盘': data['open'],
                        '收盘': data['close'],
                        '最高': data['high'],
                        '最低': data['low'],
                        '成交量': data['volume'],
                        '成交额': data['amount']
                    })
        
        # 合并所有数据
        all_data = [hist_data]
        
        if today_realtime is not None:
            all_data.append(today_realtime)
        
        if memory_realtime_rows:
            memory_realtime_df = pd.DataFrame(memory_realtime_rows)
            all_data.append(memory_realtime_df)
        
        if len(all_data) == 1:
            return hist_data
        
        combined = pd.concat(all_data, ignore_index=True)
        combined = combined.drop_duplicates(subset=['日期时间']).sort_values('日期时间').reset_index(drop=True)
        
        return combined
    
    def start_monitoring(self):
        """启动数据收集监控系统"""
        # 定时任务
        schedule.every().day.at("08:00").do(self.collect_all_historical_data)
        schedule.every().minute.do(self.collect_realtime_data)
        # 每15分钟强制保存一次实时数据
        schedule.every(15).minutes.do(self.save_realtime_data_to_disk)
        
        print("数据收集系统已启动...")
        print("- 每天8:00获取历史数据")
        print("- 每分钟获取实时数据")
        print("- 每15分钟保存实时数据到磁盘")
        print("- 程序停止时会自动保存当前实时数据")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("\n收到停止信号，正在保存数据...")
            self.save_realtime_data_to_disk()
            print("数据已保存，程序退出")


class IndustryAnalyzer:
    """数据分析器，负责技术指标分析"""
    def __init__(self, data_collector=None):
        if data_collector is None:
            self.data_collector = IndustryDataCollector()
        else:
            self.data_collector = data_collector
    
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
        base_data = self.data_collector.combine_historical_and_realtime(board_name)
        
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
            macd_line, signal_line, histogram = self.calculate_macd(close_prices,5,13,5)
            
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
                if board_name == "保险":
                    for signal in signals: 
                        print(f"    {signal['time']} - {signal['type']}: MACD={signal['macd']:.4f}, Signal={signal['signal']:.4f}")
                        # 推送保险板块60分钟金叉信号
                        if period == "60分钟" and signal['type'] == "金叉":
                            print("hello")
                            message = f"保险板块60分钟MACD金叉信号\n时间: {signal['time']}\nMACD: {signal['macd']:.4f}\nSignal:{signal['signal']:.4f}"
                            push.strategy(message)
                            
        
    def analyze_all_boards(self):
        """分析所有板块的MACD"""
        boards = self.data_collector.get_all_boards()
        
        for board in boards:
            try:
                self.analyze_board_macd(board)
            except Exception as e:
                print(f"分析{board}失败: {e}")
    
    def run_analysis(self):
        """运行分析（手动触发）"""
        print("开始分析所有板块...")
        self.analyze_all_boards()


class IndustryDataReader:
    """数据读取器，负责为分析提供数据"""
    def __init__(self, data_dir="industry_data"):
        self.data_dir = data_dir
        self.realtime_dir = os.path.join(data_dir, "realtime")
        self.historical_dir = os.path.join(data_dir, "historical")
    
    def get_available_boards(self):
        """获取有数据的板块列表"""
        boards = set()
        
        # 从历史数据目录获取
        if os.path.exists(self.historical_dir):
            for filename in os.listdir(self.historical_dir):
                if filename.endswith('_5min_historical.csv'):
                    board_name = filename.replace('_5min_historical.csv', '')
                    boards.add(board_name)
        
        # 从实时数据目录获取
        if os.path.exists(self.realtime_dir):
            for filename in os.listdir(self.realtime_dir):
                if filename.endswith('_realtime.csv'):
                    parts = filename.replace('_realtime.csv', '').split('_')
                    if len(parts) >= 2:
                        board_name = '_'.join(parts[:-1])  # 移除日期部分
                        boards.add(board_name)
        
        return list(boards)
    
    def get_available_dates(self, board_name):
        """获取指定板块可用的日期列表"""
        dates = set()
        
        if os.path.exists(self.realtime_dir):
            for filename in os.listdir(self.realtime_dir):
                if filename.startswith(f"{board_name}_") and filename.endswith('_realtime.csv'):
                    # 提取日期
                    date_part = filename.replace(f"{board_name}_", '').replace('_realtime.csv', '')
                    try:
                        datetime.strptime(date_part, '%Y-%m-%d')
                        dates.add(date_part)
                    except ValueError:
                        continue
        
        return sorted(list(dates))
    
    def load_board_data(self, board_name, start_date=None, end_date=None):
        """加载指定板块的完整数据（历史+实时）"""
        # 加载历史数据
        hist_file = os.path.join(self.historical_dir, f"{board_name}_5min_historical.csv")
        all_data = []
        
        if os.path.exists(hist_file):
            hist_data = pd.read_csv(hist_file, encoding='utf-8')
            hist_data['日期时间'] = pd.to_datetime(hist_data['日期时间'])
            all_data.append(hist_data)
        
        # 加载实时数据
        available_dates = self.get_available_dates(board_name)
        for date in available_dates:
            if start_date and date < start_date:
                continue
            if end_date and date > end_date:
                continue
            
            rt_file = os.path.join(self.realtime_dir, f"{board_name}_{date}_realtime.csv")
            if os.path.exists(rt_file):
                rt_data = pd.read_csv(rt_file, encoding='utf-8')
                rt_data['日期时间'] = pd.to_datetime(rt_data['日期时间'])
                all_data.append(rt_data)
        
        if not all_data:
            return None
        
        # 合并所有数据
        combined = pd.concat(all_data, ignore_index=True)
        combined = combined.drop_duplicates(subset=['日期时间']).sort_values('日期时间').reset_index(drop=True)
        
        # 按时间范围过滤
        if start_date:
            combined = combined[combined['日期时间'] >= start_date]
        if end_date:
            combined = combined[combined['日期时间'] <= end_date]
        
        return combined
    
    def get_latest_data(self, board_name, periods=100):
        """获取指定板块最新的N个周期数据"""
        data = self.load_board_data(board_name)
        if data is not None and len(data) > 0:
            return data.tail(periods)
        return None


if __name__ == "__main__":
    data_collector = IndustryDataCollector()
    analyzer = IndustryAnalyzer(data_collector)
    data_reader = IndustryDataReader()
    
    # 可以选择启动监控模式或手动分析模式
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == "monitor":
        data_collector.start_monitoring()
    elif len(sys.argv) > 1 and sys.argv[1] == "collect":
        delay_seconds = None
        if len(sys.argv) > 2:
            try:
                delay_seconds = float(sys.argv[2])
                print(f"使用指定延迟时间: {delay_seconds}秒")
            except ValueError:
                print("延迟时间参数无效，使用默认计算方式")
        data_collector.collect_all_historical_data(delay_seconds)
    elif len(sys.argv) > 1 and sys.argv[1] == "analyze":
        analyzer.run_analysis()
    elif len(sys.argv) > 1 and sys.argv[1] == "info":
        print("=== 数据信息 ===")
        boards = data_reader.get_available_boards()
        print(f"可用板块数量: {len(boards)}")
        print(f"板块列表: {boards[:10]}{'...' if len(boards) > 10 else ''}")
        
        if boards:
            sample_board = boards[0]
            dates = data_reader.get_available_dates(sample_board)
            print(f"\n{sample_board} 可用日期: {dates}")
            
            latest_data = data_reader.get_latest_data(sample_board, 5)
            if latest_data is not None:
                print(f"\n{sample_board} 最新5条数据:")
                print(latest_data[['日期时间', '开盘', '收盘', '最高', '最低']].to_string())
    else:
        # 交互式菜单，方便在VSCode中运行
        print("=== 板块数据收集与分析系统 ===")
        print("1. 启动数据收集监控模式")
        print("2. 手动获取历史数据")
        print("3. 手动分析所有板块")
        print("4. 查看数据信息")
        print("5. 退出")
        print()
        print("命令行使用方法:")
        print("  python industry_analysis.py monitor             # 启动数据收集监控")
        print("  python industry_analysis.py collect             # 手动获取历史数据")
        print("  python industry_analysis.py collect <延迟秒数>    # 指定延迟获取历史数据")
        print("  python industry_analysis.py analyze             # 手动分析")
        print("  python industry_analysis.py info                # 查看数据信息")
        print()
        
        while True:
            try:
                choice = input("请选择功能 (1-5): ").strip()
                
                if choice == "1":
                    print("启动数据收集监控模式...")
                    data_collector.start_monitoring()
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
                    data_collector.collect_all_historical_data(delay_seconds)
                    break
                elif choice == "3":
                    print("开始分析所有板块...")
                    analyzer.run_analysis()
                    break
                elif choice == "4":
                    print("查看数据信息...")
                    boards = data_reader.get_available_boards()
                    print(f"可用板块数量: {len(boards)}")
                    if boards:
                        print(f"板块列表: {boards[:10]}{'...' if len(boards) > 10 else ''}")
                        sample_board = boards[0]
                        dates = data_reader.get_available_dates(sample_board)
                        print(f"\n{sample_board} 可用日期: {dates}")
                        
                        latest_data = data_reader.get_latest_data(sample_board, 5)
                        if latest_data is not None:
                            print(f"\n{sample_board} 最新5条数据:")
                            print(latest_data[['日期时间', '开盘', '收盘', '最高', '最低']].to_string())
                    break
                elif choice == "5":
                    print("退出程序")
                    break
                else:
                    print("无效选择，请输入1-5")
            except KeyboardInterrupt:
                print("\n程序已退出")
                break
            except Exception as e:
                print(f"发生错误: {e}")
                break