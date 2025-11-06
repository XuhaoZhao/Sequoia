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
                # 使用UnifiedAnalyzer的analyze_macd方法返回金叉信号数据列表
                golden_cross_data = self.analyze_macd(instrument_info, instrument_type)

                if golden_cross_data:
                    all_golden_cross_data.extend(golden_cross_data)
            except Exception as e:
                print(f"分析{instrument_info.get('name', '')}失败: {e}")

        # 统一保存所有金叉信号到数据库并发送通知
        if all_golden_cross_data:
            # 保存到数据库
            saved_count = self.db.insert_macd_data(all_golden_cross_data, instrument_type, "金叉")
            print(f"已保存 {saved_count} 条MACD金叉信号到数据库")

            # 发送通知
            for signal_data in all_golden_cross_data:
                self.send_macd_notification(
                    name=signal_data['name'],
                    signal_data={
                        'time': signal_data['time'],
                        'macd': signal_data['macd'],
                        'signal': signal_data['signal']
                    },
                    code=signal_data['code'],
                    instrument_type=instrument_type,
                    signal_type="金叉"
                )

            print(f"共收集到 {len(all_golden_cross_data)} 个金叉信号，已保存到数据库并发送通知")
        else:
            print("未发现金叉信号")

        print(f"{instrument.get_instrument_type()}分析完成")

  
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

    def detect_macd_signals(self, macd_line, signal_line, timestamps):
        """检测MACD金叉死叉信号"""
        if macd_line is None or signal_line is None or len(macd_line) < 2:
            return []

        signals = []

        for i in range(1, len(macd_line)):
            if pd.isna(macd_line.iloc[i]) or pd.isna(signal_line.iloc[i]) or \
               pd.isna(macd_line.iloc[i-1]) or pd.isna(signal_line.iloc[i-1]):
                continue

            # 获取对应的时间戳
            timestamp = timestamps.iloc[i] if i < len(timestamps) else None

            # 金叉
            if macd_line.iloc[i-1] <= signal_line.iloc[i-1] and macd_line.iloc[i] > signal_line.iloc[i]:
                signals.append({
                    'type': '金叉',
                    'index': i,
                    'timestamp': timestamp,
                    'macd': macd_line.iloc[i],
                    'signal': signal_line.iloc[i]
                })

            # 死叉
            elif macd_line.iloc[i-1] >= signal_line.iloc[i-1] and macd_line.iloc[i] < signal_line.iloc[i]:
                signals.append({
                    'type': '死叉',
                    'index': i,
                    'timestamp': timestamp,
                    'macd': macd_line.iloc[i],
                    'signal': signal_line.iloc[i]
                })

        return signals

    def merge_30m_data_with_priority(self, data_historical, data_today, instrument_name):
        """
        合并历史30分钟数据和今日30分钟数据，处理时间重复问题

        Args:
            data_historical: 历史数据DataFrame (data_30m)
            data_today: 今日数据DataFrame (data_30m_toady)
            instrument_name: 产品名称，用于日志输出

        Returns:
            合并后的DataFrame，以历史数据为准
        """
        if data_historical is None or data_historical.empty:
            if data_today is None or data_today.empty:
                print(f"{instrument_name}: 无法获取任何30分钟数据")
                return None
            else:
                print(f"{instrument_name}: 仅使用今日30分钟数据")
                return data_today

        if data_today is None or data_today.empty:
            print(f"{instrument_name}: 仅使用历史30分钟数据")
            return data_historical

        # 转换datetime列为datetime类型以便比较
        data_historical = data_historical.copy()
        data_today = data_today.copy()

        data_historical['datetime'] = pd.to_datetime(data_historical['datetime'])
        data_today['datetime'] = pd.to_datetime(data_today['datetime'])

        # 找出今日数据中不在历史数据中的时间点
        historical_times = set(data_historical['datetime'])
        new_data_mask = ~data_today['datetime'].isin(historical_times)
        new_data = data_today[new_data_mask]

        if not new_data.empty:
            print(f"{instrument_name}: 从今日数据中补充 {len(new_data)} 条新的30分钟数据")
            # 合并数据，历史数据在前，新增数据在后
            combined_data = pd.concat([data_historical, new_data], ignore_index=True)
        else:
            print(f"{instrument_name}: 今日数据已包含在历史数据中，无需补充")
            combined_data = data_historical

        # 按时间排序合并后的数据
        combined_data = combined_data.sort_values('datetime').reset_index(drop=True)
        return combined_data

    def send_macd_notification(self, name, signal_data, code, instrument_type, signal_type):
        """发送MACD信号通知，先查询是否已发送过

        Args:
            name: 产品名称
            signal_data: 信号数据字典，包含time, macd, signal等信息
            code: 产品代码
            instrument_type: 产品类型 (stock, etf, index等)
            signal_type: 信号类型 (金叉, 死叉, 底部收敛等)
        """
        try:
            # 查询数据库是否已经发送过通知
            time_str = signal_data['time'].strftime('%Y-%m-%d %H:%M:%S') if hasattr(signal_data['time'], 'strftime') else str(signal_data['time'])

            existing_data = self.db.query_macd_data(
                code=code,
                start_time=time_str,
                end_time=time_str,
                instrument_type=instrument_type,
                signal_type=signal_type,
                limit=1
            )

            # 检查是否已经发送过通知
            if not existing_data.empty and existing_data.iloc[0]['notification_sent'] == 1:
                print(f"{name} 的{signal_type}信号已发送过通知，跳过")
                return

            # 构建通知消息
            message = f"{name} 30分钟MACD{signal_type}信号\n时间: {signal_data['time']}\nMACD: {signal_data['macd']:.4f}\nSignal: {signal_data['signal']:.4f}"

            # 打印消息
            print(message)

            # 发送推送通知
            if hasattr(settings, 'ENABLE_PUSH_NOTIFICATION') and settings.ENABLE_PUSH_NOTIFICATION:
                push.send(message)
                print(f"已发送{signal_type}通知: {name}")
                # 更新数据库中的通知状态
                self.db.update_notification_status(
                    code=code,
                    time=time_str,
                    instrument_type=instrument_type,
                    signal_type=signal_type,
                    sent=True
                )

        except Exception as e:
            print(f"发送MACD通知失败: {e}")

    def analyze_macd(self, instrument_info, instrument_type="unknown"):
        """分析30分钟级别MACD，返回金叉信号数据

        Args:
            instrument_info: 产品信息字典
            instrument_type: 产品类型(如: stock, etf等)，用于文件命名

        Returns:
            list: 金叉信号数据列表，如果没有信号则返回空列表
        """
        code = instrument_info.get('code')
        name = instrument_info.get('name')

        # 从数据库获取30分钟数据
        data_30m = self.db.query_kline_data('30m', code=code)
        data_30m_toady = self.db.get_today_30m_data(code=code)

        # 合并数据，处理时间重复问题（以历史数据为准）
        combined_data = self.merge_30m_data_with_priority(data_30m, data_30m_toady, name)

        if combined_data is None or len(combined_data) < 26:
            print(f"{name}: 30分钟数据不足，无法计算MACD")
            return []

        # 重命名列以匹配计算所需格式
        combined_data = combined_data.rename(columns={
            'datetime': '日期时间',
            'close_price': '收盘'
        })
        combined_data['日期时间'] = pd.to_datetime(combined_data['日期时间'])

        close_prices = combined_data['收盘']
        macd_line, signal_line, _ = self.calculate_macd(close_prices, 5, 13, 5)

        if macd_line is None:
            return []

        signals = self.detect_macd_signals(macd_line, signal_line, combined_data['日期时间'])
        if not signals:
            return []

        # 筛选当天的金叉信号
        today = datetime.now().strftime('%Y-%m-%d')
        # today = (datetime.now() - timedelta(days=2)).strftime('%Y-%m-%d')
        today_golden_cross_signals = []

        for signal in signals:
            timestamp = signal['timestamp']
            if timestamp is not None and timestamp.strftime('%Y-%m-%d') == today and signal['type'] == '金叉':
                today_golden_cross_signals.append({
                    'time': timestamp,
                    'type': signal['type'],
                    'macd': signal['macd'],
                    'signal': signal['signal']
                })

        if today_golden_cross_signals:
            print(f"\n{name} 当天30分钟MACD金叉信号:")
            # 准备返回的数据
            csv_data = []
            for signal in today_golden_cross_signals:
                message = f"{name} 30分钟MACD{signal['type']}信号\n时间: {signal['time']}\nMACD: {signal['macd']:.4f}\nSignal: {signal['signal']:.4f}"
                print(message)

                # 添加到数据列表
                csv_data.append({
                    'code': code,
                    'name': name,
                    'time': signal['time'].strftime('%Y-%m-%d %H:%M:%S'),
                    'macd': round(signal['macd'], 4),
                    'signal': round(signal['signal'], 4)
                })

            return csv_data

        return []

    def detect_macd_bottom_convergence(self, instrument_info, instrument_type="unknown"):
        """检测MACD底部收敛模式：DIF在0轴下方且在DEA下方，但差距在缩小

        Args:
            instrument_info: 产品信息字典
            instrument_type: 产品类型(如: stock, etf等)，用于文件命名

        Returns:
            list: 底部收敛信号数据列表，如果没有信号则返回空列表
        """
        code = instrument_info.get('code')
        name = instrument_info.get('name')

        # 从数据库获取30分钟数据
        data_30m = self.db.query_kline_data('30m', code=code)
        data_30m_toady = self.db.get_today_30m_data(code=code)
        
        # 合并数据，处理时间重复问题（以历史数据为准）
        combined_data = self.merge_30m_data_with_priority(data_30m, data_30m_toady, name)

        if combined_data is None or len(combined_data) < 30:
            return []

        # 重命名列以匹配计算所需格式
        combined_data = combined_data.rename(columns={
            'datetime': '日期时间',
            'close_price': '收盘'
        })
        combined_data['日期时间'] = pd.to_datetime(combined_data['日期时间'])

        close_prices = combined_data['收盘']
        print(close_prices)
        macd_line, signal_line, _ = self.calculate_macd(close_prices, 5, 13, 5)
        print(macd_line)
        print(signal_line)

        if macd_line is None or len(macd_line) < 3:
            return []

        # 检查最近3个数据点
        recent_points = 3
        if len(macd_line) < recent_points:
            return []

        convergence_signals = []

        # 检查最近3个点
        for i in range(len(macd_line) - recent_points + 1, len(macd_line)):
            if (pd.isna(macd_line.iloc[i]) or pd.isna(signal_line.iloc[i]) or
                pd.isna(macd_line.iloc[i-1]) or pd.isna(signal_line.iloc[i-1]) or
                pd.isna(macd_line.iloc[i-2]) or pd.isna(signal_line.iloc[i-2])):
                continue

            dif_current = macd_line.iloc[i]
            dea_current = signal_line.iloc[i]
            dif_prev = macd_line.iloc[i-1]
            dea_prev = signal_line.iloc[i-1]
            dif_prev2 = macd_line.iloc[i-2]
            dea_prev2 = signal_line.iloc[i-2]

            # 检查条件：
            # 1. DIF < 0 (在0轴下方)
            # 2. DIF < DEA (DIF在DEA下方)
            # 3. 最近3个点的DIF-DEA差值在逐渐缩小（差距绝对值减小）
            if (dif_current < 0 and
                dif_current < dea_current and
                dif_prev < 0 and
                dif_prev < dea_prev and
                dif_prev2 < 0 and
                dif_prev2 < dea_prev2):

                # 计算差值的绝对值
                diff_current = abs(dif_current - dea_current)
                diff_prev = abs(dif_prev - dea_prev)
                diff_prev2 = abs(dif_prev2 - dea_prev2)

                # 检查差值是否在逐渐缩小
                if diff_current < diff_prev and diff_prev < diff_prev2:
                    timestamp = combined_data.iloc[i]['日期时间']

                    convergence_signals.append({
                        'code': code,
                        'name': name,
                        'time': timestamp.strftime('%Y-%m-%d %H:%M:%S'),
                        'macd': round(dif_current, 4),
                        'signal': round(dea_current, 4)
                    })

        if convergence_signals:
            print(f"\n{name} 检测到MACD底部收敛信号:")
            for signal in convergence_signals:
                print(f"{name} MACD底部收敛信号\n时间: {signal['time']}\nMACD: {signal['macd']:.4f}\nSignal: {signal['signal']:.4f}")

        return convergence_signals

    def analyze_macd_convergence_patterns(self, instrument_type='industry_sector'):
        """分析指定类型所有产品的MACD底部收敛模式

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
        print(f"开始分析{instrument.get_instrument_type()}的MACD底部收敛模式...")

        # 收集所有底部收敛信号数据
        all_convergence_data = []

        all_instruments = instrument.get_all_instruments()
        for instrument_info in all_instruments:
            try:
                convergence_data = self.detect_macd_bottom_convergence(instrument_info, instrument_type)
                if convergence_data:
                    all_convergence_data.extend(convergence_data)
            except Exception as e:
                print(f"分析{instrument_info.get('name', '')}的底部收敛模式失败: {e}")

        # 统一保存所有底部收敛信号到数据库
        if all_convergence_data:
            # 保存到数据库
            saved_count = self.db.insert_macd_data(all_convergence_data, instrument_type, "底部收敛")
            print(f"已保存 {saved_count} 条MACD底部收敛信号到数据库")

            # 发送通知
            for signal_data in all_convergence_data:
                self.send_macd_notification(
                    name=signal_data['name'],
                    signal_data={
                        'time': signal_data['time'],
                        'macd': signal_data['macd'],
                        'signal': signal_data['signal']
                    },
                    code=signal_data['code'],
                    instrument_type=instrument_type,
                    signal_type="底部收敛"
                )

            print(f"共收集到 {len(all_convergence_data)} 个底部收敛信号，已保存到数据库并发送通知")
        else:
            print("未发现底部收敛信号")

        print(f"{instrument.get_instrument_type()}底部收敛模式分析完成")

    
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


    def calculate_first_derivative(self, series):
        """
        计算序列的一阶导数（斜率）

        Args:
            series: 价格或均线序列

        Returns:
            numpy.ndarray: 一阶导数数组
        """
        if len(series) < 2:
            return np.array([])

        # 使用numpy的梯度函数计算一阶导数
        derivatives = np.gradient(series)
        return derivatives

    def detect_turning_points_by_derivative(self, series):
        """
        基于一阶导数检测转折点（仅判断正负）

        Args:
            series: 价格或均线序列

        Returns:
            dict: 转折点信息 {'bottoms': [], 'tops': [], 'changes': []}
        """
        if len(series) < 3:
            return {'bottoms': [], 'tops': [], 'changes': []}

        derivatives = self.calculate_first_derivative(series)
        turning_points = {'bottoms': [], 'tops': [], 'changes': []}

        # 检测导数符号变化（从负到正为底部，从正到负为顶部）
        for i in range(1, len(derivatives)):
            prev_derivative = derivatives[i-1]
            curr_derivative = derivatives[i]

            # 检测是否发生符号变化
            if prev_derivative * curr_derivative < 0:  # 符号相反
                # 从负变正：底部转折点
                if prev_derivative < 0 and curr_derivative > 0:
                    turning_points['bottoms'].append(i)
                    turning_points['changes'].append(i)

                # 从正变负：顶部转折点
                elif prev_derivative > 0 and curr_derivative < 0:
                    turning_points['tops'].append(i)
                    turning_points['changes'].append(i)

        return turning_points

    def detect_turning_points(self, prices, ma_data, current_price):
        """
        检测潜在转折点（基于一阶导数方法）

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

        if len(ma5) < 5 or len(ma10) < 5 or len(ma20) < 5:
            return {"转折信号": [], "综合判断": "数据不足"}

        # 计算各均线的一阶导数
        ma5_derivatives = self.calculate_first_derivative(ma5)
        ma10_derivatives = self.calculate_first_derivative(ma10)
        ma20_derivatives = self.calculate_first_derivative(ma20)

        # 获取最近的导数值（最后一个点的导数）
        recent_ma5_deriv = ma5_derivatives[-1]
        recent_ma10_deriv = ma10_derivatives[-1]
        recent_ma20_deriv = ma20_derivatives[-1]

        # 检测各均线的转折点
        ma5_turning = self.detect_turning_points_by_derivative(ma5)
        ma10_turning = self.detect_turning_points_by_derivative(ma10)
        ma20_turning = self.detect_turning_points_by_derivative(ma20)

        # 基于导数分析生成信号
        # 1. 短期均线向上突破信号
        if recent_ma5_deriv > 0 and recent_ma10_deriv > 0:
            if current_price > ma5[-1]:
                if recent_ma20_deriv <= 0:
                    signals.append("短期均线导数为正，可能形成底部")
                else:
                    signals.append("多周期均线导数为正，上升趋势确认")

        # 2. 短期均线向下转弱信号
        if recent_ma5_deriv < 0 and recent_ma10_deriv < 0:
            if current_price < ma5[-1]:
                if recent_ma20_deriv >= 0:
                    signals.append("短期均线导数为负，可能形成顶部")
                else:
                    signals.append("多周期均线导数为负，下降趋势确认")

        # 3. 检测最近的转折点（最近5个点内）
        recent_turning_signals = []

        if ma5_turning['changes']:
            last_ma5_turn = ma5_turning['changes'][-1]
            if len(ma5) - last_ma5_turn <= 5:
                if last_ma5_turn in ma5_turning['bottoms']:
                    recent_turning_signals.append("MA5近期底部转折")
                else:
                    recent_turning_signals.append("MA5近期顶部转折")

        if ma10_turning['changes']:
            last_ma10_turn = ma10_turning['changes'][-1]
            if len(ma10) - last_ma10_turn <= 5:
                if last_ma10_turn in ma10_turning['bottoms']:
                    recent_turning_signals.append("MA10近期底部转折")
                else:
                    recent_turning_signals.append("MA10近期顶部转折")

        signals.extend(recent_turning_signals)

        # 4. 价格波动性分析（保留原有逻辑）
        price_volatility = np.std(prices[-10:]) / np.mean(prices[-10:])
        if price_volatility > 0.05:
            signals.append("价格波动加剧，注意趋势转换")

        # 5. 均线收敛分析（基于导数）
        if abs(recent_ma5_deriv - recent_ma20_deriv) < 0.0001:
            signals.append("均线变化速率接近，关注突破方向")

        # 6. 综合判断
        if len(signals) >= 3:
            trend_judgment = "关键转折点"
        elif len(signals) >= 2:
            trend_judgment = "潜在转折"
        elif len(signals) == 1:
            trend_judgment = "趋势变化迹象"
        else:
            trend_judgment = "趋势延续"

        return {
            "转折信号": signals,
            "综合判断": trend_judgment,
            "导数分析": {
                "MA5导数": round(recent_ma5_deriv, 6),
                "MA10导数": round(recent_ma10_deriv, 6),
                "MA20导数": round(recent_ma20_deriv, 6)
            },
            "转折点检测": {
                "MA5转折点": ma5_turning,
                "MA10转折点": ma10_turning,
                "MA20转折点": ma20_turning
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

    def analyze_instruments_from_macd_data(self, instrument_type, date_str=None):
        """
        从MACD数据表读取数据并执行综合技术分析

        Args:
            instrument_type: 产品类型 ('industry_sector', 'stock', 'etf', 'concept_sector', 'index')
            date_str: 日期字符串，格式为 YYYY-MM-DD，如果为None则使用前天

        Returns:
            dict: 包含所有分析结果的字典
        """
        from datetime import datetime, timedelta

        if date_str is None:
            # 默认使用前天的数据（与financial_instruments.py保持一致）
            # date_str = (datetime.now() - timedelta(days=2)).strftime("%Y-%m-%d")
            date_str = (datetime.now()).strftime("%Y-%m-%d")

        print(f"从macd_data表读取数据，产品类型: {instrument_type}, 日期: {date_str}")

        try:
            # 从macd_data表查询指定日期和产品类型的数据
            macd_df = self.db.query_macd_data(
                start_time=f"{date_str} 00:00:00",
                end_time=f"{date_str} 23:59:59",
                instrument_type=instrument_type
            )

            if macd_df.empty:
                return {"error": f"macd_data表中没有找到{instrument_type}类型在{date_str}的数据"}

            print(f"成功读取 {len(macd_df)} 条MACD信号数据")

            # 获取所有独特的产品代码作为列表变量
            if 'code' in macd_df.columns:
                instrument_codes = macd_df['code'].unique().tolist()
                print(f"发现 {len(instrument_codes)} 个独特的金融产品代码")
            else:
                return {"error": "macd_data中没有找到'code'列"}

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
                        analysis_result["分析来源"] = "MACD数据表"
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
            return {"error": f"处理MACD数据失败: {str(e)}"}


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