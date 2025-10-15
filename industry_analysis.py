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
import settings
from db_manager import IndustryDataDB

class IndustryDataCollector:
    """数据收集器，负责历史数据和实时数据的获取和保存"""
    def __init__(self):
        # 初始化SQLite数据库
        self.db = IndustryDataDB("industry_data.db")
    
    
    def _is_trading_time(self, check_time=None):
        """检查是否在A股交易时间内"""
        print("hello4")
        if check_time is None:
            check_time = datetime.now()
        
        # 获取时间部分
        current_time = check_time.time()
        
        # # 检查是否为工作日（周一到周五）
        # if check_time.weekday() >= 5:  # 周六=5, 周日=6
        #     return False
        
        # 上午交易时间：9:30-11:30
        morning_start = datetime.strptime("09:25", "%H:%M").time()
        morning_end = datetime.strptime("11:30", "%H:%M").time()
        
        # 下午交易时间：13:00-15:00
        afternoon_start = datetime.strptime("12:59", "%H:%M").time()
        afternoon_end = datetime.strptime("15:00", "%H:%M").time()
        
        return (morning_start <= current_time <= morning_end) or \
               (afternoon_start <= current_time <= afternoon_end)
    
    
    def get_all_boards(self):
        """获取所有板块名称和代码"""
        try:
            boards_df = ak.stock_board_industry_name_em()
            # 返回包含板块名称和代码的列表，每个元素是一个字典
            return boards_df[['板块名称', '板块代码']].to_dict('records')
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
    
    def save_historical_data(self, board_info, data, period="5"):
        """保存历史数据到数据库"""
        # 保存到SQLite数据库（避免重复数据）
        try:
            board_name = board_info['板块名称'] 
            data_records = []
            for _, row in data.iterrows():
                data_records.append({
                    'code': str(board_info['板块代码']),
                    'name': board_info['板块名称'],
                    'datetime': str(row['日期时间']),
                    'open': float(row['开盘']),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'close': float(row['收盘']),
                    'volume': int(row.get('成交量', 0)),
                    'amount': float(row.get('成交额', 0))
                })
            
            # 添加股票信息
            self.db.add_or_update_stock_info(board_name, board_name, "板块", "行业板块")
            
            # 插入数据（使用INSERT OR REPLACE避免重复）
            inserted_count = self.db.insert_kline_data('5m', data_records)
            print(f"已保存{board_name}历史数据到数据库，共{inserted_count}条记录")
            
        except Exception as e:
            print(f"保存{board_name}历史数据到数据库失败: {e}")
    

    
    def collect_all_historical_data(self, delay_seconds=None):
        """每天早晨8点定时获取所有板块的历史数据"""
        print(f"开始获取所有板块历史数据 - {datetime.now()}")    
        boards = self.get_all_boards()
        total_boards = len(boards)
        
        # 如果没有指定延迟时间，按照2小时完成所有板块来计算平均延迟
        if delay_seconds is None:
            delay_seconds = 7200 / total_boards if total_boards > 0 else 1
            print(f"未指定延迟时间，按1小时完成{total_boards}个板块计算，每个板块延迟{delay_seconds:.2f}秒")
        else:
            estimated_total_time = delay_seconds * total_boards
            print(f"使用指定延迟时间{delay_seconds}秒，预计总耗时{estimated_total_time/60:.1f}分钟")
        boards = list(reversed(boards))  # 倒序排列
        for i, board_info in enumerate(boards, 1):
            board_name = board_info['板块名称']
            board_code = board_info['板块代码']
            print(f"正在获取{board_name}({board_code})的历史数据... ({i}/{total_boards})")
            hist_data = self.get_historical_data(board_name, "5")
            if hist_data is not None:
                self.save_historical_data(board_info, hist_data, "5")
            
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
    
    def collect_realtime_data(self):
        """每分钟收集实时数据并保存到数据库"""
        # 检查是否在交易时间内
        if not self._is_trading_time():
            return
        
        current_time = datetime.now()
        
        realtime_df = self.get_realtime_data()
        if realtime_df is not None:
            # 保存1分钟级别的实时数据到数据库
            try:
                print("heuddc")
                db_records_1m = []
                for _, row in realtime_df.iterrows():
                    board_name = row['板块名称']
                    board_code = row['板块代码']
                    
                    # 准备1分钟数据记录
                    db_records_1m.append({
                        'code': board_code,
                        'name': board_name,
                        'datetime': current_time.strftime('%Y-%m-%d %H:%M:00'),  # 取整到分钟
                        'open': float(row['最新价']),
                        'high': float(row['最新价']),
                        'low': float(row['最新价']),
                        'close': float(row['最新价']),
                        'volume': int(row.get('成交量', 0)),
                        'amount': float(row.get('成交额', 0))
                    })
                    
                # 插入1分钟数据到数据库
                if db_records_1m:
                    inserted_count = self.db.insert_kline_data('1m', db_records_1m)
                    print(f"已保存{len(db_records_1m)}个板块的1分钟数据到数据库，共{inserted_count}条记录")
                
            except Exception as e:
                print(f"保存1分钟数据到数据库失败: {e}")
            
            print(f"实时数据已更新 - {datetime.now()}")
    
    def _aggregate_1m_to_5m(self, df_1m):
        """将1分钟数据聚合为5分钟数据，只基于已有数据的时间范围"""
        if df_1m.empty:
            return pd.DataFrame()
        
        # 确保时间列是datetime类型
        df_1m['datetime'] = pd.to_datetime(df_1m['datetime'])
        df_1m = df_1m.sort_values('datetime')
        
        # 获取数据的实际时间范围
        min_time = df_1m['datetime'].min()
        max_time = df_1m['datetime'].max()
        start_date = min_time.date()
        
        # 定义5分钟边界时间点（从9:30开始）
        base_times = []
        
        # 上午时段：9:30开始每5分钟一个点
        morning_start = pd.Timestamp.combine(start_date, pd.Timestamp('09:30:00').time())
        current = morning_start
        while current <= pd.Timestamp.combine(start_date, pd.Timestamp('11:30:00').time()):
            base_times.append(current)
            current += pd.Timedelta(minutes=5)
        
        # 下午时段：13:00开始每5分钟一个点
        afternoon_start = pd.Timestamp.combine(start_date, pd.Timestamp('13:00:00').time())
        current = afternoon_start
        while current <= pd.Timestamp.combine(start_date, pd.Timestamp('15:00:00').time()):
            base_times.append(current)
            current += pd.Timedelta(minutes=5)
        
        # 只保留在数据时间范围内的5分钟边界点
        valid_times = [t for t in base_times if min_time <= t <= max_time]

        print(valid_times)
        
        # 为每个有效的5分钟时间点聚合数据
        aggregated_data = []
        
        for i, target_time in enumerate(valid_times):
            # 确定这个5分钟区间的开始时间
            if i == 0:
                # 第一个区间：从数据开始时间到第一个5分钟边界
                start_time = min_time
            else:
                # 其他区间：从上一个5分钟边界的下一分钟开始
                start_time = valid_times[i-1] + pd.Timedelta(minutes=1)
            
            # 获取这个时间区间的数据
            mask = (df_1m['datetime'] >= start_time) & (df_1m['datetime'] <= target_time)
            period_data = df_1m[mask]
            
            if not period_data.empty:
                # 聚合OHLCV数据
                open_price = period_data['open_price'].iloc[0]  # 第一条记录的开盘价
                high_price = period_data['high_price'].max()    # 最高价
                low_price = period_data['low_price'].min()      # 最低价
                close_price = period_data['close_price'].iloc[-1]  # 最后一条记录的收盘价
                volume = period_data['volume'].sum()            # 成交量求和
                amount = period_data['amount'].sum()            # 成交额求和
                
                aggregated_data.append({
                    'datetime': target_time,
                    'open_price': open_price,
                    'high_price': high_price,
                    'low_price': low_price,
                    'close_price': close_price,
                    'volume': volume,
                    'amount': amount,
                    'code': period_data['code'].iloc[0],
                    'name': period_data['name'].iloc[0]
                })
        return pd.DataFrame(aggregated_data)
    
    def combine_historical_and_realtime(self, board_info):
        """从数据库获取并合并历史和实时数据，返回5分钟K线数据"""
        board_code = board_info['板块代码']
        board_name = board_info['板块名称']
        try:
            # 第一步：获取当天1分钟数据并聚合为5分钟
            today = datetime.now().strftime('%Y-%m-%d')
            df_1m_today = self.db.query_kline_data('1m', code=board_code, start_date=today, end_date=today)
            
            today_5m_data = pd.DataFrame()
            if not df_1m_today.empty:
                today_5m_data = self._aggregate_1m_to_5m(df_1m_today)
            
            # 第二步：获取历史5分钟数据（排除今天）
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            df_5m_hist = self.db.query_kline_data('5m', code=board_code, end_date=yesterday)
            
            # 第三步：合并所有数据
            all_data = []
            
            if not df_5m_hist.empty:
                # 重命名列以匹配原有格式
                df_5m_hist_renamed = df_5m_hist.rename(columns={
                    'datetime': '日期时间',
                    'open_price': '开盘',
                    'high_price': '最高',
                    'low_price': '最低',
                    'close_price': '收盘',
                    'volume': '成交量',
                    'amount': '成交额'
                })
                all_data.append(df_5m_hist_renamed)
            
            if not today_5m_data.empty:
                # 重命名列以匹配原有格式
                today_5m_renamed = today_5m_data.rename(columns={
                    'datetime': '日期时间',
                    'open_price': '开盘',
                    'high_price': '最高',
                    'low_price': '最低',
                    'close_price': '收盘',
                    'volume': '成交量',
                    'amount': '成交额'
                })
                all_data.append(today_5m_renamed)
            
            if not all_data:
                return None
            
            # 合并并排序
            combined = pd.concat(all_data, ignore_index=True)
            combined['日期时间'] = pd.to_datetime(combined['日期时间'])
            combined = combined.sort_values('日期时间').reset_index(drop=True)
            
            # 去重（保留最后一条记录）
            combined = combined.drop_duplicates(subset=['日期时间'], keep='last')
            
            return combined
            
        except Exception as e:
            print(f"从数据库获取{board_name}数据失败: {e}")
            # 回退到原有逻辑
            return self._combine_historical_and_realtime_fallback(board_name)
    
    def start_monitoring(self):
        """启动数据收集监控系统"""
        # 定时任务
        schedule.every().day.at("08:00").do(self.collect_all_historical_data)
        # 只在交易时间内每分钟执行实时数据收集
        schedule.every().minute.do(self.collect_realtime_data)
        
        print("数据收集系统已启动...")
        print("- 每天8:00获取历史数据")
        print("- 交易时间内每分钟获取实时数据（9:30-11:30, 13:00-15:00）")
        print("- 每15分钟保存实时数据到磁盘")
        print("- 程序停止时会自动保存当前实时数据")
        
        try:
            while True:
                schedule.run_pending()
                time.sleep(1)
        except KeyboardInterrupt:
            print("数据已保存，程序退出")


class IndustryAnalyzer:
    """数据分析器，负责技术指标分析"""
    def __init__(self, data_collector=None):
        settings.init()
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
    
    def analyze_board_macd(self, board_info):
        """分析单个板块的MACD"""
        base_data = self.data_collector.combine_historical_and_realtime(board_info)
        board_name = board_info['板块名称']
        
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
            # 只打印当天的MACD金叉信号，且只打印30分钟和60分钟周期
            from datetime import datetime
            today = datetime.now().strftime('%Y-%m-%d')
            
            print(f"\n{board_name} 当天MACD金叉信号:")
            for period, signals in results.items():
                if period in ['30分钟', '60分钟']:  # 只处理30分钟和60分钟周期
                    today_signals = [s for s in signals if s['time'].strftime('%Y-%m-%d') == today and s['type'] == '金叉']  # 只处理金叉信号
                    if today_signals:
                        print(f"  {period}:")
                        for signal in today_signals: 
                            message = f"{board_name} {period}MACD{signal['type']}信号\n时间: {signal['time']}\nMACD: {signal['macd']:.4f}\nSignal: {signal['signal']:.4f}"
                            print(message)
                            push.strategy(message)
                        
        
    def analyze_all_boards(self):
        """分析所有板块的MACD"""
        boards = self.data_collector.get_all_boards()
        
        for board_info in boards:
            board_name = board_info['板块名称']
            try:
                self.analyze_board_macd(board_info)
            except Exception as e:
                print(f"分析{board_name}失败: {e}")
    
    def is_price_at_monthly_high_drawdown_5pct(self, board_name, current_price=None):
        """
        计算给定股票当前价格是否是当前时间向前推一个月的最高点回撤5%
        
        Args:
            board_name: 板块名称
            current_price: 当前价格，如果为None则使用最新的收盘价
            
        Returns:
            dict: {
                'is_at_drawdown_5pct': bool,  # 是否在5%回撤位置
                'current_price': float,        # 当前价格
                'monthly_high': float,         # 一个月内最高价
                'drawdown_5pct_price': float,  # 5%回撤价格
                'actual_drawdown_pct': float,  # 实际回撤百分比
                'days_from_high': int          # 距离最高点的天数
            }
        """
        # 获取板块数据
        data = self.data_collector.combine_historical_and_realtime(board_name)
        
        if data is None or data.empty:
            print(f"{board_name}: 无法获取数据")
            return None
        
        # 计算一个月前的日期
        current_time = datetime.now()
        one_month_ago = current_time - timedelta(days=30)
        
        # 过滤出一个月内的数据
        data['日期时间'] = pd.to_datetime(data['日期时间'])
        monthly_data = data[data['日期时间'] >= one_month_ago].copy()
        
        if monthly_data.empty:
            print(f"{board_name}: 一个月内没有数据")
            return None
        
        # 获取当前价格
        if current_price is None:
            current_price = monthly_data['收盘'].iloc[-1]
        
        # 找到一个月内的最高价和对应的日期
        monthly_high = monthly_data['最高'].max()
        high_date_idx = monthly_data['最高'].idxmax()
        high_date = monthly_data.loc[high_date_idx, '日期时间']
        
        # 计算5%回撤价格
        drawdown_5pct_price = monthly_high * 0.95
        
        # 计算实际回撤百分比
        actual_drawdown_pct = ((monthly_high - current_price) / monthly_high) * 100
        
        # 计算距离最高点的天数
        days_from_high = (current_time - high_date).days
        
        # 判断是否在5%回撤位置（允许一定误差范围，比如±1%）
        is_at_drawdown_5pct = abs(actual_drawdown_pct - 5.0) <= 1.0 and actual_drawdown_pct >= 4.0
        
        result = {
            'is_at_drawdown_5pct': is_at_drawdown_5pct,
            'current_price': current_price,
            'monthly_high': monthly_high,
            'drawdown_5pct_price': drawdown_5pct_price,
            'actual_drawdown_pct': actual_drawdown_pct,
            'days_from_high': days_from_high,
            'high_date': high_date
        }
        
        return result

    def run_analysis(self):
        """运行分析（手动触发）"""
        print("开始分析所有板块...")
        settings.init()
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
