import akshare as ak
import pandas as pd
import talib
from abc import ABC, abstractmethod
from datetime import datetime, timedelta
import time
from db_manager import IndustryDataDB
from .logger_config import LoggerMixin, log_method_call, log_data_operation


class FinancialInstrument(ABC, LoggerMixin):
    """金融产品基类"""
    
    def __init__(self, db, code=None, name=None):
        """
        Args:
            db: IndustryDataDB 数据库实例（依赖注入）
            code: 产品代码
            name: 产品名称
        """
        super().__init__()
        self.db = db
        self.code = code
        self.name = name
        self.log_info(f"初始化{self.get_instrument_type()}产品: {name or code or 'Unknown'}")
    
    @abstractmethod
    def get_all_instruments(self):
        """获取所有产品列表"""
        pass
    
    @abstractmethod
    def get_historical_min_data(self, instrument_info, period="5", delay_seconds=1.0):
        """获取历史分时数据

        Args:
            instrument_info: 产品信息字典（包含 code 和 name）
            period: 数据周期，可选 "1", "5", "15", "30", "60" 等（单位：分钟）
            delay_seconds: 获取数据后的延迟时间（秒），防止被封禁IP，默认1.0秒

        Returns:
            字典列表格式的数据
        """
        pass
    
    @abstractmethod
    def get_realtime_1min_data(self):
        """获取实时1分钟分时数据"""
        pass
    
    @abstractmethod
    def get_daily_data(self, symbol, start_date=None, end_date=None):
        """获取日K数据"""
        pass
    
    @abstractmethod
    def _get_data_api_params(self, symbol):
        """获取API参数，子类需要实现具体的参数映射"""
        pass

    
    @log_data_operation('保存历史分时数据')
    def save_historical_min_data(self, instrument_info, data, period="5"):
        """保存历史分时数据到数据库

        Args:
            instrument_info: 产品信息字典
            data: 字典列表格式的数据
            period: 数据周期（"1", "5", "30"等，单位：分钟）
        """
        try:
            instrument_name = instrument_info.get('name', self.name)
            # 根据周期确定数据库存储的period标识
            period_map = {
                "1": "1m",
                "5": "5m",
                "15": "15m",
                "30": "30m",
                "60": "60m"
            }
            db_period = period_map.get(str(period), f"{period}m")

            self.log_info(f"开始保存{instrument_name}的{period}分钟历史数据")

            # 添加产品信息
            self.db.add_or_update_stock_info(
                instrument_info.get('code', self.code),
                instrument_info.get('name', self.name),
                self.__class__.__name__,
                self.get_instrument_type()
            )

            # 插入数据（data应该是字典列表）
            inserted_count = self.db.insert_kline_data(db_period, data)
            self.log_info(f"已保存{instrument_info.get('name', self.name)}{period}分钟历史数据到数据库，共{inserted_count}条记录")

        except Exception as e:
            self.log_error(f"保存{instrument_info.get('name', self.name)}{period}分钟历史数据到数据库失败: {e}", exc_info=True)
            raise
    
    @log_data_operation('收集1分钟实时数据')
    def collect_realtime_1min_data(self):
        """收集1分钟实时数据并保存到数据库"""
        # if not self._is_trading_time():
        #     self.log_error("非交易时间，跳过实时数据收集")
        #     return
        
        current_time = datetime.now()
        realtime_df = self.get_realtime_1min_data()
        
        if realtime_df is not None:
            try:
            
                db_records_1m = []
                
                for _, row in realtime_df.iterrows():
                    db_records_1m.append({
                        'code': row.get('code', self.code),
                        'name': row.get('name', self.name),
                        'datetime': current_time.strftime('%Y-%m-%d %H:%M:00'),
                        'open': float(row.get('close', row.get('open', 0))),
                        'high': float(row.get('close', row.get('high', 0))),
                        'low': float(row.get('close', row.get('low', 0))),
                        'close': float(row.get('close', 0)),
                        'volume': int(row.get('volume', 0)),
                        'amount': float(row.get('amount', 0))
                    })
                
                if db_records_1m:
                    inserted_count = self.db.insert_kline_data('1m', db_records_1m)
                    self.log_info(f"已保存{len(db_records_1m)}个{self.get_instrument_type()}的1分钟数据到数据库，共{inserted_count}条记录")
                
            except Exception as e:
                self.log_error(f"保存1分钟数据到数据库失败: {e}", exc_info=True)
    
    @log_method_call(include_args=False)
    def combine_historical_and_realtime(self, instrument_info):
        """从数据库获取并合并历史和实时数据，返回5分钟K线数据"""
        code = instrument_info.get('code', self.code)
        name = instrument_info.get('name', self.name)
        self.log_debug(f"开始合并{name}({code})的历史和实时数据")
        
        try:
            # 获取当天1分钟数据并聚合为5分钟
            today = datetime.now().strftime('%Y-%m-%d')
            df_1m_today = self.db.query_kline_data('1m', code=code, start_date=today, end_date=today)
            
            today_5m_data = pd.DataFrame()
            if not df_1m_today.empty:
                today_5m_data = self._aggregate_1m_to_5m(df_1m_today)
            
            # 获取历史5分钟数据（排除今天）
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            df_5m_hist = self.db.query_kline_data('5m', code=code, end_date=yesterday)
            
            # 合并所有数据
            all_data = []
            
            if not df_5m_hist.empty:
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
                self.log_warning(f"{name}无数据可合并")
                return None
            
            # 合并并排序
            combined = pd.concat(all_data, ignore_index=True)
            combined['日期时间'] = pd.to_datetime(combined['日期时间'])
            combined = combined.sort_values('日期时间').reset_index(drop=True)
            
            # 去重
            combined = combined.drop_duplicates(subset=['日期时间'], keep='last')
            
            self.log_debug(f"数据合并完成，共{len(combined)}条记录")
            return combined
            
        except Exception as e:
            self.log_error(f"从数据库获取{name}数据失败: {e}", exc_info=True)
            return None
    
    def _aggregate_1m_to_5m(self, df_1m):
        """将1分钟数据聚合为5分钟数据"""
        if df_1m.empty:
            return pd.DataFrame()
        
        df_1m['datetime'] = pd.to_datetime(df_1m['datetime'])
        df_1m = df_1m.sort_values('datetime')
        
        min_time = df_1m['datetime'].min()
        max_time = df_1m['datetime'].max()
        start_date = min_time.date()
        
        # 定义5分钟边界时间点
        base_times = []
        
        # 上午时段：9:30开始每5分钟
        morning_start = pd.Timestamp.combine(start_date, pd.Timestamp('09:30:00').time())
        current = morning_start
        while current <= pd.Timestamp.combine(start_date, pd.Timestamp('11:30:00').time()):
            base_times.append(current)
            current += pd.Timedelta(minutes=5)
        
        # 下午时段：13:00开始每5分钟
        afternoon_start = pd.Timestamp.combine(start_date, pd.Timestamp('13:00:00').time())
        current = afternoon_start
        while current <= pd.Timestamp.combine(start_date, pd.Timestamp('15:00:00').time()):
            base_times.append(current)
            current += pd.Timedelta(minutes=5)
        
        valid_times = [t for t in base_times if min_time <= t <= max_time]
        
        aggregated_data = []
        
        for i, target_time in enumerate(valid_times):
            if i == 0:
                start_time = min_time
            else:
                start_time = valid_times[i-1] + pd.Timedelta(minutes=1)
            
            mask = (df_1m['datetime'] >= start_time) & (df_1m['datetime'] <= target_time)
            period_data = df_1m[mask]
            
            if not period_data.empty:
                aggregated_data.append({
                    'datetime': target_time,
                    'open_price': period_data['open_price'].iloc[0],
                    'high_price': period_data['high_price'].max(),
                    'low_price': period_data['low_price'].min(),
                    'close_price': period_data['close_price'].iloc[-1],
                    'volume': period_data['volume'].sum(),
                    'amount': period_data['amount'].sum(),
                    'code': period_data['code'].iloc[0],
                    'name': period_data['name'].iloc[0]
                })
        
        return pd.DataFrame(aggregated_data)
    
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
        """计算MACD指标"""
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
            
            # 金叉
            if macd_line.iloc[i-1] <= signal_line.iloc[i-1] and macd_line.iloc[i] > signal_line.iloc[i]:
                signals.append({
                    'type': '金叉',
                    'index': i,
                    'macd': macd_line.iloc[i],
                    'signal': signal_line.iloc[i]
                })
            
            # 死叉
            elif macd_line.iloc[i-1] >= signal_line.iloc[i-1] and macd_line.iloc[i] < signal_line.iloc[i]:
                signals.append({
                    'type': '死叉',
                    'index': i,
                    'macd': macd_line.iloc[i],
                    'signal': signal_line.iloc[i]
                })
        
        return signals
    
    def analyze_macd(self, instrument_info):
        """分析30分钟级别MACD，接收已经处理好的30分钟数据"""
        code = instrument_info.get('code', self.code)
        name = instrument_info.get('name', self.name)

        # 从数据库获取30分钟数据
        data_30m = self.db.query_kline_data('30m', code=code)

        if data_30m is None or len(data_30m) < 26:
            print(f"{name}: 30分钟数据不足，无法计算MACD")
            return

        # 重命名列以匹配计算所需格式
        data_30m = data_30m.rename(columns={
            'datetime': '日期时间',
            'close_price': '收盘'
        })
        data_30m['日期时间'] = pd.to_datetime(data_30m['日期时间'])

        close_prices = data_30m['收盘']
        macd_line, signal_line, _ = self.calculate_macd(close_prices, 5, 13, 5)

        if macd_line is None:
            return

        signals = self.detect_macd_signals(macd_line, signal_line)

        if not signals:
            return

        # 筛选当天的金叉信号
        today = datetime.now().strftime('%Y-%m-%d')
        today_golden_cross_signals = []

        for signal in signals:
            timestamp = data_30m.iloc[signal['index']]['日期时间']
            if timestamp.strftime('%Y-%m-%d') == today and signal['type'] == '金叉':
                today_golden_cross_signals.append({
                    'time': timestamp,
                    'type': signal['type'],
                    'macd': signal['macd'],
                    'signal': signal['signal']
                })

        if today_golden_cross_signals:
            print(f"\n{name} 当天30分钟MACD金叉信号:")
            for signal in today_golden_cross_signals:
                message = f"{name} 30分钟MACD{signal['type']}信号\n时间: {signal['time']}\nMACD: {signal['macd']:.4f}\nSignal: {signal['signal']:.4f}"
                print(message)
    
    def _is_trading_time(self, check_time=None):
        """检查是否在A股交易时间内"""
        if check_time is None:
            check_time = datetime.now()
        
        current_time = check_time.time()
        
        # 上午交易时间：9:30-11:30
        morning_start = datetime.strptime("09:25", "%H:%M").time()
        morning_end = datetime.strptime("11:30", "%H:%M").time()
        
        # 下午交易时间：13:00-15:00
        afternoon_start = datetime.strptime("12:59", "%H:%M").time()
        afternoon_end = datetime.strptime("15:00", "%H:%M").time()
        
        return (morning_start <= current_time <= morning_end) or \
               (afternoon_start <= current_time <= afternoon_end)
    
    @abstractmethod
    def get_instrument_type(self):
        """获取产品类型"""
        pass
    
    def collect_all_historical_min_data(self, period="5", delay_seconds=None):
        """获取所有产品的历史分时数据

        Args:
            period: 数据周期（"1", "5", "30"等，单位：分钟）
            delay_seconds: 延迟秒数，如果为None则使用类的默认值
        """
        print(f"开始获取所有{self.get_instrument_type()}{period}分钟历史数据 - {datetime.now()}")
        instruments = self.get_all_instruments()
        total_instruments = len(instruments)

        if delay_seconds is None:
            # 使用实现类自定义的延迟参数
            delay_seconds = self.__class__.delay_seconds
            print(f"使用{self.get_instrument_type()}的默认延迟时间: {delay_seconds}秒")

        estimated_total_time = delay_seconds * total_instruments
        print(f"预计总耗时{estimated_total_time/60:.1f}分钟，共{total_instruments}个{self.get_instrument_type()}")

        instruments = list(reversed(instruments))
        for i, instrument_info in enumerate(instruments, 1):
            name = instrument_info.get('name', instrument_info.get('板块名称', ''))
            code = instrument_info.get('code', instrument_info.get('板块代码', ''))
            print(f"正在获取{name}({code})的{period}分钟历史数据... ({i}/{total_instruments})")

            hist_data = self.get_historical_min_data(instrument_info, period)
            if hist_data is not None:
                self.save_historical_min_data(instrument_info, hist_data, period)

            if i < total_instruments:
                time.sleep(delay_seconds)

        print(f"所有{self.get_instrument_type()}{period}分钟历史数据获取完成 - {datetime.now()}")