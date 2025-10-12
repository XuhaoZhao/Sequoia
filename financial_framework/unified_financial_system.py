from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
import time
from datetime import datetime
from .industry_sector import IndustrySector
from .stock import Stock
from .etf import ETF
from .concept_sector import ConceptSector
from .index import Index
from .logger_config import LoggerMixin, log_method_call, FinancialLogger
from db_manager import IndustryDataDB
import settings
import push


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
    def collect_all_historical_5min_data(self, instrument_type='industry_sector', delay_seconds=None):
        """收集指定类型产品的5分钟历史数据（遍历该类型下所有子项）

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

        # 调用基类的 collect_all_historical_5min_data 方法
        instrument.collect_all_historical_5min_data(delay_seconds)
    
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

        # 每分钟收集实时数据
        self.scheduler.add_job(
            func=self.collect_realtime_1min_data,
            trigger=CronTrigger(minute='*'),
            id='collect_realtime_data',
            name='收集1分钟实时数据',
            replace_existing=True,
            kwargs={'instrument_type': 'concept_sector'} 
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
    
    def analyze_all_instruments(self, instrument_types=None):
        """分析所有产品"""
        if instrument_types is None:
            instrument_types = ['industry_sector', 'concept_sector', 'index']  # 默认只分析板块和指数
        
        instruments_map = {
            'industry_sector': self.industry_sector,
            'stock': self.stock,
            'etf': self.etf,
            'concept_sector': self.concept_sector,
            'index': self.index
        }
        
        for instrument_type in instrument_types:
            if instrument_type in instruments_map:
                instrument = instruments_map[instrument_type]
                print(f"开始分析{instrument.get_instrument_type()}...")
                
                all_instruments = instrument.get_all_instruments()
                for instrument_info in all_instruments:
                    try:
                        instrument.analyze_macd(instrument_info)
                    except Exception as e:
                        print(f"分析{instrument_info.get('name', '')}失败: {e}")
                
                print(f"{instrument.get_instrument_type()}分析完成")
    
    def run_analysis(self, instrument_types=None):
        """运行分析"""
        print("开始统一分析...")
        self.analyze_all_instruments(instrument_types)
        print("统一分析完成")


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
    
    def get_historical_5min_data(self, board_name, period="5"):
        """获取指定板块的5分钟历史数据（兼容性方法）"""
        return self.industry_sector.get_historical_5min_data(board_name, period)
    
    def save_historical_5min_data(self, board_info, data, period="5"):
        """保存5分钟历史数据到数据库（兼容性方法）"""
        return self.industry_sector.save_historical_5min_data(board_info, data, period)
    
    def get_realtime_1min_data(self):
        """获取1分钟实时数据（兼容性方法）"""
        return self.industry_sector.get_realtime_1min_data()
    
    def collect_all_historical_5min_data(self, delay_seconds=None):
        """收集所有板块5分钟历史数据（兼容性方法）"""
        return self.industry_sector.collect_all_historical_5min_data(delay_seconds)
    
    # 保持原有方法名称以兼容旧代码
    def get_historical_data(self, board_name, period="5"):
        """获取指定板块的历史数据（兼容性方法）"""
        return self.get_historical_5min_data(board_name, period)
    
    def save_historical_data(self, board_info, data, period="5"):
        """保存历史数据到数据库（兼容性方法）"""
        return self.save_historical_5min_data(board_info, data, period)
    
    def get_realtime_data(self):
        """获取实时数据（兼容性方法）"""
        return self.get_realtime_1min_data()
    
    def collect_all_historical_data(self, delay_seconds=None):
        """收集所有板块历史数据（兼容性方法）"""
        return self.collect_all_historical_5min_data(delay_seconds)


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
        return self.analyze_all_instruments(['industry_sector'])
    
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