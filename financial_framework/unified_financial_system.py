from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time
from datetime import datetime, timedelta
from .industry_sector import IndustrySector
from .stock import Stock
from .etf import ETF
from .concept_sector import ConceptSector
from .index import Index
from .logger_config import LoggerMixin, log_method_call, FinancialLogger
from .file_path_generator import FilePathGenerator
from db_manager import IndustryDataDB
import settings
import push
import pandas as pd
import os
import akshare as ak
import talib
import numpy as np
import json


class UnifiedDataCollector(LoggerMixin):
    """统一数据收集器"""

    def __init__(self, db=None):
        """
        Args:
            db: IndustryDataDB 数据库实例（依赖注入），如果为None则创建新实例
        """
        super().__init__()
        # 初始化日志系统
        FinancialLogger.setup_logging()

        # 初始化数据库实例（依赖注入）
        self.db = db if db is not None else IndustryDataDB("industry_data.db")
        self.log_info(f"数据库实例: {self.db}")

        # 初始化各种金融产品实例，注入同一个数据库实例
        self.industry_sector = IndustrySector(self.db)
        self.stock = Stock(self.db)
        self.etf = ETF(self.db)
        self.concept_sector = ConceptSector(self.db)
        self.index = Index(self.db)

        # 初始化APScheduler
        self.scheduler = BackgroundScheduler()

        self.log_info("统一数据收集器初始化完成")
    
    @log_method_call(include_args=False)
    def collect_all_historical_min_data(self, instrument_type='industry_sector', period="5", delay_seconds=None):
        """收集指定类型产品的历史分时数据（遍历该类型下所有子项）

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
            period: 数据周期（"1", "5", "30"等，单位：分钟）
            delay_seconds: 延迟秒数（批量收集时使用），如果为None则使用各类的默认延迟参数
        """
        instruments_map = {
            'industry_sector': self.industry_sector,
            'stock': self.stock,
            'etf': self.etf,
            'concept_sector': self.concept_sector,
            'index': self.index
        }

        if instrument_type not in instruments_map:
            self.log_warning(f"未知的产品类型: {instrument_type}")
            return

        instrument = instruments_map[instrument_type]

        # 如果没有指定延迟时间，使用类的默认延迟参数
        if delay_seconds is None:
            delay_seconds = instrument.__class__.delay_seconds
            self.log_info(f"使用{instrument.get_instrument_type()}的默认延迟时间: {delay_seconds}秒")

        # 调用基类的 collect_all_historical_min_data 方法
        instrument.collect_all_historical_min_data(period, delay_seconds)

    # 保持向后兼容的方法
    def collect_all_historical_5min_data(self, instrument_type='industry_sector', delay_seconds=None):
        """收集指定类型产品的5分钟历史数据（向后兼容方法）

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
            delay_seconds: 延迟秒数（批量收集时使用），如果为None则使用各类的默认延迟参数
        """
        return self.collect_all_historical_min_data(instrument_type, "5", delay_seconds)

    @log_method_call(include_args=False)
    def collect_all_daily_data(self, instrument_type='stock', delay_seconds=None):
        """收集指定类型产品的日K数据（遍历该类型下所有子项）

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
            delay_seconds: 延迟秒数（批量收集时使用），如果为None则使用各类的默认延迟参数
        """
        instruments_map = {
            'industry_sector': self.industry_sector,
            'stock': self.stock,
            'etf': self.etf,
            'concept_sector': self.concept_sector,
            'index': self.index
        }

        if instrument_type not in instruments_map:
            self.log_warning(f"未知的产品类型: {instrument_type}")
            return

        instrument = instruments_map[instrument_type]

        # 如果没有指定延迟时间，使用类的默认延迟参数
        if delay_seconds is None:
            delay_seconds = instrument.__class__.delay_seconds
            self.log_info(f"使用{instrument.get_instrument_type()}的默认延迟时间: {delay_seconds}秒")

        # 调用基类的 collect_all_daily_data 方法
        instrument.collect_all_daily_data(delay_seconds)
    
    @log_method_call(include_args=False)
    def collect_realtime_1min_data(self, instrument_type):
        """收集指定类型的1分钟实时数据

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
        """
        instruments_map = {
            'industry_sector': self.industry_sector,
            'stock': self.stock,
            'etf': self.etf,
            'concept_sector': self.concept_sector,
            'index': self.index
        }

        if instrument_type not in instruments_map:
            error_msg = f"未知的产品类型: {instrument_type}，必须是以下类型之一: {list(instruments_map.keys())}"
            self.log_error(error_msg)
            raise ValueError(error_msg)

        self.log_info(f"开始收集{instrument_type}的1分钟实时数据 - {datetime.now()}")
        try:
            instruments_map[instrument_type].collect_realtime_1min_data()
            self.log_info(f"{instrument_type}的1分钟实时数据收集完成 - {datetime.now()}")
        except Exception as e:
            self.log_error(f"{instrument_type}的1分钟实时数据收集失败: {e}", exc_info=True)
            raise
    
    def start_monitoring(self):
        """启动监控系统"""
        # 配置定时任务
        # 每天早上8:00收集历史数据（可以根据需要修改为收集特定类型的数据）
        self.scheduler.add_job(
            func=self.collect_all_historical_5min_data,
            trigger=CronTrigger(hour=8, minute=0),
            id='collect_historical_data',
            name='收集5分钟历史数据',
            replace_existing=True,
            kwargs={'instrument_type': 'industry_sector'}
        )

        # 每2分钟收集实时数据
        self.scheduler.add_job(
            func=self.collect_realtime_1min_data,
            trigger=CronTrigger(minute='*/2'),
            id='collect_realtime_data',
            name='收集1分钟实时数据',
            replace_existing=True,
            kwargs={'instrument_type': 'index'}
        )

        # 启动调度器
        self.scheduler.start()

        self.log_info("统一数据收集系统已启动...")
        self.log_info("- 每天8:00获取所有产品5分钟历史数据")
        self.log_info("- 交易时间内每分钟获取所有产品1分钟实时数据")
        self.log_info(f"- 调度器状态: {'运行中' if self.scheduler.running else '已停止'}")

        try:
            # 保持程序运行
            while True:
                time.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            self.log_info("正在停止调度器...")
            self.scheduler.shutdown()
            self.log_info("数据已保存，程序退出")

    def add_scheduled_job(self, instrument_type, hour=8, minute=0):
        """添加自定义定时任务

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
            hour: 执行小时
            minute: 执行分钟
        """
        job_id = f'collect_{instrument_type}_data'
        self.scheduler.add_job(
            func=self.collect_all_historical_5min_data,
            trigger=CronTrigger(hour=hour, minute=minute),
            id=job_id,
            name=f'收集{instrument_type}数据',
            replace_existing=True,
            kwargs={'instrument_type': instrument_type}
        )
        self.log_info(f"已添加定时任务: {job_id}，执行时间 {hour}:{minute:02d}")

    def stop_monitoring(self):
        """停止监控系统"""
        if self.scheduler.running:
            self.scheduler.shutdown()
            self.log_info("调度器已停止")


class UnifiedAnalyzer:
    """统一分析器"""

    def __init__(self, db=None):
        """
        Args:
            db: IndustryDataDB 数据库实例（依赖注入），如果为None则创建新实例
        """
        settings.init()

        # 初始化数据库实例（依赖注入）
        self.db = db if db is not None else IndustryDataDB("industry_data.db")

        # 初始化各种金融产品实例，注入同一个数据库实例
        self.industry_sector = IndustrySector(self.db)
        self.stock = Stock(self.db)
        self.etf = ETF(self.db)
        self.concept_sector = ConceptSector(self.db)
        self.index = Index(self.db)
    
    def analyze_instrument(self, instrument_type, instrument_info):
        """分析指定产品"""
        instruments_map = {
            'industry_sector': self.industry_sector,
            'stock': self.stock,
            'etf': self.etf,
            'concept_sector': self.concept_sector,
            'index': self.index
        }
        
        if instrument_type in instruments_map:
            try:
                instruments_map[instrument_type].analyze_macd(instrument_info)
            except Exception as e:
                print(f"分析{instrument_info.get('name', '')}失败: {e}")
    
    def analyze_all_instruments(self, instrument_type='industry_sector'):
        """分析指定类型的所有产品，收集所有金叉信号后统一保存

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
        """
        instruments_map = {
            'industry_sector': self.industry_sector,
            'stock': self.stock,
            'etf': self.etf,
            'concept_sector': self.concept_sector,
            'index': self.index
        }

        if instrument_type not in instruments_map:
            print(f"未知的产品类型: {instrument_type}")
            return

        instrument = instruments_map[instrument_type]
        print(f"开始分析{instrument.get_instrument_type()}...")

        # 收集所有金叉信号数据
        all_golden_cross_data = []

        all_instruments = instrument.get_all_instruments()
        for instrument_info in all_instruments:
            try:
                # analyze_macd 现在返回金叉信号数据列表
                golden_cross_data = instrument.analyze_macd(instrument_info, instrument_type)
                if golden_cross_data:
                    all_golden_cross_data.extend(golden_cross_data)
            except Exception as e:
                print(f"分析{instrument_info.get('name', '')}失败: {e}")

        # 统一保存所有金叉信号到CSV
        if all_golden_cross_data:
            today = datetime.now().strftime('%Y-%m-%d')
            self._save_golden_cross_to_csv(all_golden_cross_data, instrument_type, today)
            print(f"共收集到 {len(all_golden_cross_data)} 个金叉信号，已保存到CSV")
        else:
            print("未发现金叉信号")

        print(f"{instrument.get_instrument_type()}分析完成")

    def _save_golden_cross_to_csv(self, data, instrument_type, date_str):
        """将金叉信号保存到CSV文件

        Args:
            data: 金叉信号数据列表
            instrument_type: 产品类型
            date_str: 日期字符串
        """
        try:
            # 使用FilePathGenerator生成文件路径
            filepath = FilePathGenerator.generate_macd_signal_path(
                instrument_type=instrument_type,
                period="30m",
                date=date_str
            )

            # 确保目录存在
            FilePathGenerator.ensure_directory_exists(filepath)

            # 创建DataFrame
            df = pd.DataFrame(data)

            # 如果文件已存在，追加数据；否则创建新文件
            if os.path.exists(filepath):
                # 读取现有数据
                existing_df = pd.read_csv(filepath)
                # 合并数据并去重(基于code和time)
                df = pd.concat([existing_df, df], ignore_index=True)
                df = df.drop_duplicates(subset=['code', 'time'], keep='last')

            # 保存到CSV
            df.to_csv(filepath, index=False, encoding='utf-8-sig')
            print(f"金叉信号已保存到文件: {filepath}")

        except Exception as e:
            print(f"保存金叉信号到CSV失败: {e}")

    def run_analysis(self, instrument_type='industry_sector'):
        """运行分析

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
        """
        print("开始统一分析...")
        self.analyze_all_instruments(instrument_type)
        print("统一分析完成")


class TechnicalAnalyzer:
    """
    技术分析类（从 industry_daliyK_analysis.py 整合）
    提供综合技术分析功能，包括：
    - ZigZag分析
    - 分形分析
    - 斐波那契回撤
    - 布林带分析
    - 移动平均线分析
    - 转折点检测
    """

    def __init__(self, db=None, default_symbol="人形机器人", default_days_back=250, default_data_source="industry"):
        """
        初始化分析器

        Args:
            db: IndustryDataDB 数据库实例（依赖注入），如果为None则创建新实例
            default_symbol: 默认分析的板块名称或股票代码
            default_days_back: 默认分析天数
            default_data_source: 默认数据来源 ("industry", "stock", "concept")
        """
        # 初始化数据库实例（依赖注入）
        self.db = db if db is not None else IndustryDataDB("industry_data.db")
        self.default_symbol = default_symbol
        self.default_days_back = default_days_back
        self.default_data_source = default_data_source

    def zigzag(self, high, low, close, deviation=0.05):
        """
        ZigZag算法识别高低点

        Args:
            high, low, close: 价格数组
            deviation: 最小变化幅度（默认5%）

        Returns:
            list: [(index, price, type)] type为'high'或'low'
        """
        peaks = []
        if len(close) < 3:
            return peaks

        trend = None
        last_peak_idx = 0
        last_peak_price = close[0]

        for i in range(1, len(close)):
            if trend is None:
                if close[i] > close[i-1] * (1 + deviation):
                    trend = 'up'
                    last_peak_idx = i-1
                    last_peak_price = close[i-1]
                    peaks.append((i-1, close[i-1], 'low'))
                elif close[i] < close[i-1] * (1 - deviation):
                    trend = 'down'
                    last_peak_idx = i-1
                    last_peak_price = close[i-1]
                    peaks.append((i-1, close[i-1], 'high'))

            elif trend == 'up':
                if close[i] > last_peak_price:
                    last_peak_idx = i
                    last_peak_price = close[i]
                elif close[i] < last_peak_price * (1 - deviation):
                    peaks.append((last_peak_idx, last_peak_price, 'high'))
                    trend = 'down'
                    last_peak_idx = i
                    last_peak_price = close[i]

            elif trend == 'down':
                if close[i] < last_peak_price:
                    last_peak_idx = i
                    last_peak_price = close[i]
                elif close[i] > last_peak_price * (1 + deviation):
                    peaks.append((last_peak_idx, last_peak_price, 'low'))
                    trend = 'up'
                    last_peak_idx = i
                    last_peak_price = close[i]

        if trend and len(peaks) > 0:
            peaks.append((last_peak_idx, last_peak_price, 'high' if trend == 'up' else 'low'))

        return peaks


    def fractal_highs_lows(self, high, low, period=2):
        """
        分形算法识别局部高低点

        Args:
            high, low: 价格数组
            period: 分形周期（默认2，即前后2个点）

        Returns:
            dict: {'highs': [(index, price)], 'lows': [(index, price)]}
        """
        fractal_highs = []
        fractal_lows = []

        for i in range(period, len(high) - period):
            is_high = True
            is_low = True

            for j in range(i - period, i + period + 1):
                if j == i:
                    continue
                if high[j] >= high[i]:
                    is_high = False
                if low[j] <= low[i]:
                    is_low = False

            if is_high:
                fractal_highs.append((i, high[i]))
            if is_low:
                fractal_lows.append((i, low[i]))

        return {'highs': fractal_highs, 'lows': fractal_lows}


    def fibonacci_retracement(self, high_price, low_price):
        """
        计算斐波那契回撤位

        Args:
            high_price: 高点价格
            low_price: 低点价格

        Returns:
            dict: 各个斐波那契回撤位
        """
        price_range = high_price - low_price

        fib_levels = {
            '0%': high_price,
            '23.6%': high_price - price_range * 0.236,
            '38.2%': high_price - price_range * 0.382,
            '50%': high_price - price_range * 0.5,
            '61.8%': high_price - price_range * 0.618,
            '78.6%': high_price - price_range * 0.786,
            '100%': low_price
        }

        return fib_levels


    def analyze_comprehensive_technical(self, code=None, symbol=None, days_back=None, data_source=None):
        """
        综合技术分析：布林带 + 斐波那契回撤 + ZigZag + 分形

        Args:
            code: 板块代码或股票代码（优先使用）
            symbol: 板块名称或股票代码（当code为None时使用，默认使用实例化时的默认值）
            days_back: 分析天数（默认使用实例化时的默认值）
            data_source: 数据来源，可选值：
                - "industry": 行业板块数据
                - "stock": 个股数据
                - "concept": 概念板块数据
                （默认使用实例化时的默认值）

        Returns:
            dict: 包含所有技术分析结果的字典
        """
        # 参数处理
        if code is None and symbol is None:
            symbol = self.default_symbol
        if days_back is None:
            days_back = self.default_days_back
        if data_source is None:
            data_source = self.default_data_source

        # 计算日期范围
        end_date = datetime.now().strftime("%Y-%m-%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y-%m-%d")

        # 从数据库获取日K数据
        try:
            # 如果提供了code，直接使用code查询
            query_code = code if code is not None else symbol

            # 从数据库查询日K数据
            df = self.db.query_kline_data('1d', code=query_code, start_date=start_date, end_date=end_date)

            if df is None or df.empty:
                return {"error": f"无法从数据库获取日K数据，code/symbol: {query_code}"}

            # 重命名列以匹配后续处理
            df = df.rename(columns={
                'datetime': '日期',
                'open_price': '开盘',
                'high_price': '最高',
                'low_price': '最低',
                'close_price': '收盘',
                'volume': '成交量'
            })

            # 转换日期格式
            df['日期'] = pd.to_datetime(df['日期'])

        except Exception as e:
            return {"error": f"获取数据库日K数据失败: {str(e)}"}

        df = df.sort_values('日期').reset_index(drop=True)

        high_prices = df['最高'].values.astype(float)
        low_prices = df['最低'].values.astype(float)
        close_prices = df['收盘'].values.astype(float)
        volumes = df['成交量'].values.astype(float)

        upper_band, middle_band, lower_band = talib.BBANDS(
            close_prices,
            timeperiod=20,
            nbdevup=2,
            nbdevdn=2,
            matype=0
        )

        df['上轨'] = upper_band
        df['中轨'] = middle_band
        df['下轨'] = lower_band

        ma_data = self.calculate_moving_averages(close_prices)
        for ma_name, ma_values in ma_data.items():
            df[ma_name] = ma_values

        # 计算成交量分析
        volume_analysis = self.analyze_volume_status(volumes, lookback_days=60)

        zigzag_points = self.zigzag(high_prices, low_prices, close_prices, deviation=0.08)

        fractals = self.fractal_highs_lows(high_prices, low_prices, period=3)

        latest_data = df.iloc[-1]
        latest_close = float(latest_data['收盘'])
        latest_lower_band = float(latest_data['下轨'])
        latest_middle_band = float(latest_data['中轨'])
        latest_upper_band = float(latest_data['上轨'])

        ma_arrangement = self.analyze_ma_arrangement(ma_data, latest_close)
        crossover_signals = self.detect_ma_crossover_signals(ma_data, lookback=5)
        turning_points = self.detect_turning_points(close_prices, ma_data, latest_close)

        bb_is_oversold = latest_close < latest_lower_band

        distance_to_lower = ((latest_close - latest_lower_band) / latest_lower_band) * 100
        bb_position = ((latest_close - latest_lower_band) / (latest_upper_band - latest_lower_band)) * 100

        recent_highs = [point for point in zigzag_points if point[2] == 'high'][-3:]
        recent_lows = [point for point in zigzag_points if point[2] == 'low'][-3:]

        fib_analysis = {}
        if recent_highs and recent_lows:
            last_high = max(recent_highs, key=lambda x: x[1])
            last_low = min(recent_lows, key=lambda x: x[1])

            if last_high[0] > last_low[0]:
                swing_high = last_high[1]
                swing_low = last_low[1]
                fib_levels = self.fibonacci_retracement(swing_high, swing_low)

                fib_support_levels = []
                for level, price in fib_levels.items():
                    if abs(latest_close - price) / price < 0.02:
                        fib_support_levels.append(level)

                fib_analysis = {
                    "摆动高点": swing_high,
                    "摆动低点": swing_low,
                    "斐波那契回撤位": fib_levels,
                    "当前位置接近的回撤位": fib_support_levels,
                    "回撤百分比": ((swing_high - latest_close) / (swing_high - swing_low)) * 100 if swing_high != swing_low else 0
                }

        fractal_recent_highs = fractals['highs'][-5:] if len(fractals['highs']) >= 5 else fractals['highs']
        fractal_recent_lows = fractals['lows'][-5:] if len(fractals['lows']) >= 5 else fractals['lows']

        综合分析信号 = []

        if bb_is_oversold:
            综合分析信号.append("布林带下轨超跌")

        if bb_position < 20:
            综合分析信号.append("布林带底部区域")

        if fib_analysis.get("回撤百分比", 0) > 50:
            综合分析信号.append("斐波那契深度回撤")

        if fib_analysis.get("当前位置接近的回撤位"):
            综合分析信号.append(f"接近斐波那契支撑位: {', '.join(fib_analysis['当前位置接近的回撤位'])}")

        if len(recent_lows) > 0:
            last_zigzag_low = min(recent_lows, key=lambda x: x[1])[1]
            if latest_close <= last_zigzag_low * 1.05:
                综合分析信号.append("接近ZigZag关键低点")

        if len(fractal_recent_lows) > 0:
            last_fractal_low = min(fractal_recent_lows, key=lambda x: x[1])[1]
            if latest_close <= last_fractal_low * 1.03:
                综合分析信号.append("接近分形关键低点")

        if ma_arrangement["排列状态"] in ["完美多头排列", "多头排列"]:
            综合分析信号.append(f"均线呈{ma_arrangement['排列状态']}")
        elif ma_arrangement["排列状态"] in ["完美空头排列", "空头排列"]:
            综合分析信号.append(f"均线呈{ma_arrangement['排列状态']}")

        for signal in crossover_signals:
            if signal["天数前"] <= 3:
                综合分析信号.append(f"{signal['天数前']}天前{signal['快线']}{signal['类型']}{signal['慢线']}")

        if turning_points["综合判断"] == "关键转折点":
            综合分析信号.append("检测到关键转折点信号")

        # 添加成交量相关的分析信号
        if "error" not in volume_analysis:
            if volume_analysis["成交量状态"] == "极低":
                综合分析信号.append(f"成交量处于{volume_analysis['成交量百分位']:.1f}分位，极度萎缩")
            elif volume_analysis["成交量状态"] == "低":
                综合分析信号.append(f"成交量处于{volume_analysis['成交量百分位']:.1f}分位，明显萎缩")

            if volume_analysis["成交量趋势"] in ["明显放量", "放量"] and volume_analysis["成交量等级"] <= 2:
                综合分析信号.append(f"底部区域出现{volume_analysis['成交量趋势']}({volume_analysis['成交量变化率']:+.1f}%)")

        综合评级 = "强烈超跌" if len(综合分析信号) >= 3 else "可能超跌" if len(综合分析信号) >= 2 else "观望" if len(综合分析信号) >= 1 else "正常"

        return {
            "板块名称": query_code,
            "最新日期": latest_data['日期'],
            "最新收盘价": latest_close,

            "均线分析": ma_arrangement,

            "均线交叉信号": crossover_signals,

            "转折点分析": turning_points,

            "成交量分析": volume_analysis,

            "布林带分析": {
                "上轨": latest_upper_band,
                "中轨": latest_middle_band,
                "下轨": latest_lower_band,
                "是否超跌": bb_is_oversold,
                "距离下轨百分比": round(distance_to_lower, 2),
                "布林带位置": round(bb_position, 2)
            },

            "ZigZag分析": {
                "最近高点": recent_highs,
                "最近低点": recent_lows,
                "关键点数量": len(zigzag_points)
            },

            "分形分析": {
                "分形高点": fractal_recent_highs,
                "分形低点": fractal_recent_lows
            },

            "斐波那契分析": fib_analysis,

            "综合分析信号": 综合分析信号,
            "综合评级": 综合评级,
            "投资建议": self.get_investment_advice(综合评级, len(综合分析信号))
        }


    def calculate_moving_averages(self, prices, periods=[5, 10, 20, 30, 60]):
        """
        计算多周期移动平均线

        Args:
            prices: 价格序列
            periods: 均线周期列表

        Returns:
            dict: 各周期均线数据
        """
        ma_data = {}
        for period in periods:
            ma_data[f'MA{period}'] = talib.SMA(prices, timeperiod=period)
        return ma_data


    def analyze_ma_arrangement(self, ma_data, current_price):
        """
        分析均线排列状态

        Args:
            ma_data: 均线数据字典
            current_price: 当前价格

        Returns:
            dict: 排列分析结果
        """
        periods = [5, 10, 20, 30, 60]
        ma_values = []

        for period in periods:
            ma_key = f'MA{period}'
            if ma_key in ma_data and not np.isnan(ma_data[ma_key][-1]):
                ma_values.append((period, ma_data[ma_key][-1]))

        if len(ma_values) < 3:
            return {"排列状态": "数据不足", "信号强度": 0}

        ma_values_only = [value for _, value in ma_values]

        is_bullish = all(ma_values_only[i] >= ma_values_only[i+1] for i in range(len(ma_values_only)-1))
        is_bearish = all(ma_values_only[i] <= ma_values_only[i+1] for i in range(len(ma_values_only)-1))

        price_above_all = current_price > max(ma_values_only)
        price_below_all = current_price < min(ma_values_only)

        if is_bullish and price_above_all:
            arrangement = "完美多头排列"
            signal_strength = 5
        elif is_bullish:
            arrangement = "多头排列"
            signal_strength = 4
        elif is_bearish and price_below_all:
            arrangement = "完美空头排列"
            signal_strength = -5
        elif is_bearish:
            arrangement = "空头排列"
            signal_strength = -4
        else:
            arrangement = "混乱排列"
            signal_strength = 0

        return {
            "排列状态": arrangement,
            "信号强度": signal_strength,
            "价格位置": "多头" if current_price > ma_values_only[0] else "空头",
            "均线数值": {f'MA{period}': round(value, 2) for period, value in ma_values}
        }


    def detect_ma_crossover_signals(self, ma_data, lookback=5):
        """
        检测均线交叉信号

        Args:
            ma_data: 均线数据字典
            lookback: 回看天数

        Returns:
            list: 交叉信号列表
        """
        signals = []
        periods = [5, 10, 20, 30, 60]

        for i in range(len(periods)):
            for j in range(i+1, len(periods)):
                fast_period = periods[i]
                slow_period = periods[j]

                fast_ma = ma_data[f'MA{fast_period}']
                slow_ma = ma_data[f'MA{slow_period}']

                if len(fast_ma) < lookback or len(slow_ma) < lookback:
                    continue

                for k in range(1, min(lookback, len(fast_ma))):
                    if (fast_ma[-k-1] <= slow_ma[-k-1] and fast_ma[-k] > slow_ma[-k]):
                        signals.append({
                            "类型": "金叉",
                            "快线": f"MA{fast_period}",
                            "慢线": f"MA{slow_period}",
                            "发生位置": len(fast_ma) - k,
                            "天数前": k,
                            "信号强度": "强" if fast_period <= 10 and slow_period >= 20 else "中"
                        })
                    elif (fast_ma[-k-1] >= slow_ma[-k-1] and fast_ma[-k] < slow_ma[-k]):
                        signals.append({
                            "类型": "死叉",
                            "快线": f"MA{fast_period}",
                            "慢线": f"MA{slow_period}",
                            "发生位置": len(fast_ma) - k,
                            "天数前": k,
                            "信号强度": "强" if fast_period <= 10 and slow_period >= 20 else "中"
                        })

        return sorted(signals, key=lambda x: x["天数前"])


    def detect_turning_points(self, prices, ma_data, current_price):
        """
        检测潜在转折点

        Args:
            prices: 价格序列
            ma_data: 均线数据
            current_price: 当前价格

        Returns:
            dict: 转折点分析结果
        """
        signals = []

        ma5 = ma_data.get('MA5', [])
        ma10 = ma_data.get('MA10', [])
        ma20 = ma_data.get('MA20', [])

        if len(ma5) < 3 or len(ma10) < 3 or len(ma20) < 3:
            return {"转折信号": [], "综合判断": "数据不足"}

        ma5_slope = (ma5[-1] - ma5[-3]) / 2
        ma10_slope = (ma10[-1] - ma10[-3]) / 2
        ma20_slope = (ma20[-1] - ma20[-3]) / 2

        if ma5_slope > 0 and ma10_slope > 0 and current_price > ma5[-1]:
            if ma20_slope <= 0:
                signals.append("短期均线向上，可能形成底部")
            else:
                signals.append("多均线向上，上升趋势确认")

        if ma5_slope < 0 and ma10_slope < 0 and current_price < ma5[-1]:
            if ma20_slope >= 0:
                signals.append("短期均线向下，可能形成顶部")
            else:
                signals.append("多均线向下，下降趋势确认")

        price_volatility = np.std(prices[-10:]) / np.mean(prices[-10:])
        if price_volatility > 0.05:
            signals.append("价格波动加剧，注意趋势转换")

        ma_convergence = abs(ma5[-1] - ma20[-1]) / ma20[-1]
        if ma_convergence < 0.02:
            signals.append("均线收敛，关注突破方向")

        if len(signals) >= 2:
            trend_judgment = "关键转折点"
        elif len(signals) == 1:
            trend_judgment = "潜在转折"
        else:
            trend_judgment = "趋势延续"

        return {
            "转折信号": signals,
            "综合判断": trend_judgment,
            "均线斜率": {
                "MA5斜率": round(ma5_slope, 4),
                "MA10斜率": round(ma10_slope, 4),
                "MA20斜率": round(ma20_slope, 4)
            }
        }


    def calculate_volume_ma(self, volumes, periods=[5, 10, 20]):
        """
        计算成交量移动平均线

        Args:
            volumes: 成交量序列
            periods: 均线周期列表，默认[5, 10, 20]

        Returns:
            dict: 各周期成交量均线数据
        """
        volume_ma_data = {}
        for period in periods:
            volume_ma_data[f'VMA{period}'] = talib.SMA(volumes, timeperiod=period)
        return volume_ma_data


    def analyze_volume_status(self, volumes, lookback_days=60):
        """
        分析成交量状态，判断当前5日成交量均线是处于低点还是高点

        Args:
            volumes: 成交量序列
            lookback_days: 回看天数，用于判断高低点，默认60天

        Returns:
            dict: 成交量分析结果，包含：
                - current_vma5: 当前5日成交量均线
                - vma5_percentile: 5日成交量均线在回看期内的百分位
                - volume_status: 成交量状态（极低、低、中等、高、极高）
                - volume_trend: 成交量趋势（放量、缩量、平稳）
                - max_vma5: 回看期内5日均线最大值
                - min_vma5: 回看期内5日均线最小值
        """
        if len(volumes) < 5:
            return {"error": "数据不足，无法计算5日成交量均线"}

        # 计算5日成交量均线
        vma5 = talib.SMA(volumes, timeperiod=5)

        if len(vma5) < lookback_days:
            lookback_days = len(vma5)

        # 获取回看期内的数据
        recent_vma5 = vma5[-lookback_days:]
        current_vma5 = vma5[-1]

        # 去除NaN值
        valid_vma5 = recent_vma5[~np.isnan(recent_vma5)]

        if len(valid_vma5) == 0:
            return {"error": "有效数据不足"}

        # 计算统计指标
        max_vma5 = np.max(valid_vma5)
        min_vma5 = np.min(valid_vma5)
        mean_vma5 = np.mean(valid_vma5)
        std_vma5 = np.std(valid_vma5)

        # 计算当前值的百分位（在回看期内的位置）
        percentile = np.sum(valid_vma5 <= current_vma5) / len(valid_vma5) * 100

        # 判断成交量状态
        if percentile <= 20:
            volume_status = "极低"
            volume_level = 1
        elif percentile <= 40:
            volume_status = "低"
            volume_level = 2
        elif percentile <= 60:
            volume_status = "中等"
            volume_level = 3
        elif percentile <= 80:
            volume_status = "高"
            volume_level = 4
        else:
            volume_status = "极高"
            volume_level = 5

        # 分析成交量趋势（对比前一个5日均线）
        if len(vma5) >= 2 and not np.isnan(vma5[-2]):
            prev_vma5 = vma5[-2]
            volume_change_pct = ((current_vma5 - prev_vma5) / prev_vma5) * 100

            if volume_change_pct > 10:
                volume_trend = "明显放量"
            elif volume_change_pct > 3:
                volume_trend = "放量"
            elif volume_change_pct < -10:
                volume_trend = "明显缩量"
            elif volume_change_pct < -3:
                volume_trend = "缩量"
            else:
                volume_trend = "平稳"
        else:
            volume_change_pct = 0
            volume_trend = "平稳"

        # 计算与均值的偏离程度
        if std_vma5 > 0:
            z_score = (current_vma5 - mean_vma5) / std_vma5
        else:
            z_score = 0

        return {
            "当前5日成交量均线": round(current_vma5, 2),
            "成交量百分位": round(percentile, 2),
            "成交量状态": volume_status,
            "成交量等级": volume_level,  # 1-5，数字越大成交量越高
            "成交量趋势": volume_trend,
            "成交量变化率": round(volume_change_pct, 2),
            "回看期最大值": round(max_vma5, 2),
            "回看期最小值": round(min_vma5, 2),
            "回看期均值": round(mean_vma5, 2),
            "Z分数": round(z_score, 2),  # 标准分数，反映偏离均值的程度
            "距离最高点": round(((max_vma5 - current_vma5) / max_vma5) * 100, 2),
            "距离最低点": round(((current_vma5 - min_vma5) / min_vma5) * 100, 2),
        }


    def get_investment_advice(self, rating, signal_count):
        """根据综合评级给出投资建议"""
        if rating == "强烈超跌":
            return "🔥 多重技术指标显示强烈超跌，可考虑分批建仓，但需注意风险控制"
        elif rating == "可能超跌":
            return "⚠️ 技术指标显示可能超跌，可小量试探建仓，密切关注后续走势"
        elif rating == "观望":
            return "👀 部分技术指标显示调整，建议观望等待更好机会"
        else:
            return "✅ 技术指标相对正常，可按既定策略操作"

    def analyze_instruments_from_macd_file(self, instrument_type, date_str=None):
        """
        从MACD信号文件读取数据并执行综合技术分析

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
            date_str: 日期字符串，格式为 YYYY-MM-DD，如果为None则使用今天

        Returns:
            dict: 包含所有分析结果的字典
        """
        from datetime import datetime

        if date_str is None:
            date_str = datetime.now().strftime("%Y-%m-%d")

        # 使用FilePathGenerator生成文件路径
        filepath = FilePathGenerator.generate_macd_signal_path(
            instrument_type=instrument_type,
            period="30m",
            date=date_str
        )

        print(f"读取MACD信号文件: {filepath}")

        # 检查文件是否存在
        if not os.path.exists(filepath):
            return {"error": f"MACD信号文件不存在: {filepath}"}

        try:
            # 读取CSV文件，指定code列为字符串以保留前导零
            macd_data = pd.read_csv(filepath, dtype={'code': str})
            if macd_data.empty:
                return {"error": f"MACD信号文件为空: {filepath}"}

            print(f"成功读取 {len(macd_data)} 条MACD信号数据")

            # 获取所有独特的股票代码作为列表变量
            if 'code' in macd_data.columns:
                instrument_codes = macd_data['code'].unique().tolist()
                print(f"发现 {len(instrument_codes)} 个独特的金融产品代码")
            else:
                return {"error": "MACD信号文件中没有找到'code'列"}

            # 为每个代码执行综合技术分析
            all_analysis_results = []
            successful_analyses = 0
            failed_analyses = 0

            for code in instrument_codes:
                try:
                    print(f"正在分析: {code}")

                    # 执行综合技术分析
                    analysis_result = self.analyze_comprehensive_technical(
                        code=code,
                        data_source=instrument_type.replace('_sector', '')  # 转换为数据源格式
                    )

                    if "error" not in analysis_result:
                        analysis_result["分析来源"] = "MACD信号文件"
                        analysis_result["MACD信号日期"] = date_str
                        analysis_result["产品类型"] = instrument_type
                        all_analysis_results.append(analysis_result)
                        successful_analyses += 1
                        print(f"✓ {code} 分析完成")
                    else:
                        print(f"✗ {code} 分析失败: {analysis_result['error']}")
                        failed_analyses += 1

                except Exception as e:
                    print(f"✗ {code} 分析异常: {str(e)}")
                    failed_analyses += 1
                    continue

            # 生成结果摘要
            summary = {
                "分析日期": date_str,
                "产品类型": instrument_type,
                "总产品数量": len(instrument_codes),
                "成功分析数量": successful_analyses,
                "失败分析数量": failed_analyses,
                "分析成功率": f"{(successful_analyses / len(instrument_codes) * 100):.1f}%" if instrument_codes else "0%"
            }

            # 将完整结果保存到JSON文件
            result_data = {
                "摘要": summary,
                "分析结果": all_analysis_results
            }

            # 生成JSON文件路径
            json_filepath = f"data/{instrument_type}_comprehensive_analysis_{date_str}.json"

            # 确保目录存在
            FilePathGenerator.ensure_directory_exists(json_filepath)

            # 保存到JSON文件
            with open(json_filepath, 'w', encoding='utf-8') as f:
                json.dump(result_data, f, ensure_ascii=False, indent=2, default=str)

            print(f"分析结果已保存到JSON文件: {json_filepath}")
            print(f"分析完成: 成功 {successful_analyses} 个，失败 {failed_analyses} 个")

            return result_data

        except Exception as e:
            return {"error": f"处理MACD信号文件失败: {str(e)}"}


# 兼容性类，保持原有接口
class IndustryDataCollector(UnifiedDataCollector):
    """行业数据收集器（兼容性类）"""
    
    def __init__(self):
        super().__init__()
        # 保持原有方法兼容性
        self.db = self.industry_sector.db
    
    def get_all_boards(self):
        """获取所有板块名称和代码（兼容性方法）"""
        boards = self.industry_sector.get_all_instruments()
        # 转换为原有格式
        return [{'板块名称': board['name'], '板块代码': board['code']} for board in boards]
    
    def get_historical_min_data(self, board_name, period="5"):
        """获取指定板块的历史分时数据（兼容性方法）"""
        return self.industry_sector.get_historical_min_data({'name': board_name}, period)

    def get_historical_5min_data(self, board_name, period="5"):
        """获取指定板块的5分钟历史数据（兼容性方法）"""
        return self.get_historical_min_data(board_name, period)

    def save_historical_min_data(self, board_info, data, period="5"):
        """保存历史分时数据到数据库（兼容性方法）"""
        return self.industry_sector.save_historical_min_data(board_info, data, period)

    def save_historical_5min_data(self, board_info, data, period="5"):
        """保存5分钟历史数据到数据库（兼容性方法）"""
        return self.save_historical_min_data(board_info, data, period)
    
    def get_realtime_1min_data(self):
        """获取1分钟实时数据（兼容性方法）"""
        return self.industry_sector.get_realtime_1min_data()
    
    def collect_all_historical_min_data(self, period="5", delay_seconds=None):
        """收集所有板块历史分时数据（兼容性方法）"""
        return self.industry_sector.collect_all_historical_min_data(period, delay_seconds)

    def collect_all_historical_5min_data(self, delay_seconds=None):
        """收集所有板块5分钟历史数据（兼容性方法）"""
        return self.collect_all_historical_min_data("5", delay_seconds)

    def collect_all_daily_data(self, delay_seconds=None):
        """收集所有板块日K数据（兼容性方法）"""
        return self.industry_sector.collect_all_daily_data(delay_seconds)
    
    # 保持原有方法名称以兼容旧代码
    def get_historical_data(self, board_name, period="5"):
        """获取指定板块的历史数据（兼容性方法）"""
        return self.get_historical_min_data(board_name, period)

    def save_historical_data(self, board_info, data, period="5"):
        """保存历史数据到数据库（兼容性方法）"""
        return self.save_historical_min_data(board_info, data, period)

    def get_realtime_data(self):
        """获取实时数据（兼容性方法）"""
        return self.get_realtime_1min_data()

    def collect_all_historical_data(self, period="5", delay_seconds=None):
        """收集所有板块历史数据（兼容性方法）"""
        return self.collect_all_historical_min_data(period, delay_seconds)


class IndustryAnalyzer(UnifiedAnalyzer):
    """行业分析器（兼容性类）"""
    
    def __init__(self, data_collector=None):
        super().__init__()
        if data_collector is None:
            self.data_collector = IndustryDataCollector()
        else:
            self.data_collector = data_collector
    
    def resample_data(self, data, period):
        """重采样数据到指定周期（兼容性方法）"""
        return self.industry_sector.resample_data(data, period)
    
    def calculate_macd(self, close_prices, fast=12, slow=26, signal=9):
        """计算MACD指标（兼容性方法）"""
        return self.industry_sector.calculate_macd(close_prices, fast, slow, signal)
    
    def detect_macd_signals(self, macd_line, signal_line):
        """检测MACD信号（兼容性方法）"""
        return self.industry_sector.detect_macd_signals(macd_line, signal_line)
    
    def analyze_board_macd(self, board_info):
        """分析单个板块的MACD（兼容性方法）"""
        return self.industry_sector.analyze_macd(board_info)
    
    def analyze_all_boards(self):
        """分析所有板块的MACD（兼容性方法）"""
        return self.analyze_all_instruments('industry_sector')
    
    def is_price_at_monthly_high_drawdown_5pct(self, board_name, current_price=None):
        """计算月度回撤（兼容性方法）"""
        # 创建临时board_info
        board_info = {'name': board_name, 'code': board_name}
        
        data = self.industry_sector.combine_historical_and_realtime(board_info)
        
        if data is None or data.empty:
            print(f"{board_name}: 无法获取数据")
            return None
        
        from datetime import timedelta
        current_time = datetime.now()
        one_month_ago = current_time - timedelta(days=30)
        
        data['日期时间'] = pd.to_datetime(data['日期时间'])
        monthly_data = data[data['日期时间'] >= one_month_ago].copy()
        
        if monthly_data.empty:
            print(f"{board_name}: 一个月内没有数据")
            return None
        
        if current_price is None:
            current_price = monthly_data['收盘'].iloc[-1]
        
        monthly_high = monthly_data['最高'].max()
        high_date_idx = monthly_data['最高'].idxmax()
        high_date = monthly_data.loc[high_date_idx, '日期时间']
        
        drawdown_5pct_price = monthly_high * 0.95
        actual_drawdown_pct = ((monthly_high - current_price) / monthly_high) * 100
        days_from_high = (current_time - high_date).days
        
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