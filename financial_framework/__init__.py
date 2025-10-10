"""
金融产品框架

这个包提供了统一的金融产品数据收集和分析框架，支持：
- 股票 (Stock)
- ETF
- 行业板块 (IndustrySector) 
- 概念板块 (ConceptSector)
- 指数 (Index)

主要功能：
- get_historical_5min_data(): 获取5分钟分时历史数据
- get_realtime_1min_data(): 获取1分钟实时数据
- get_daily_data(): 获取日K数据
- 统一的数据收集和分析框架
- 完整的日志系统，支持多级别日志记录、文件轮转、性能监控

日志功能：
- 自动记录所有数据操作和错误
- 性能监控，记录执行时间超过1秒的操作
- 分类日志文件：主日志、错误日志、性能日志、数据操作日志
- 支持日志文件自动轮转，防止文件过大

使用示例:
    from financial_framework.unified_financial_system import UnifiedDataCollector, UnifiedAnalyzer
    from financial_framework.logger_config import FinancialLogger, get_logger
    
    # 初始化日志系统（可选，会自动初始化）
    FinancialLogger.setup_logging(log_level=logging.INFO)
    
    # 数据收集
    collector = UnifiedDataCollector()
    collector.collect_all_historical_5min_data(['stock', 'industry_sector'])
    collector.collect_realtime_1min_data()
    
    # 数据分析
    analyzer = UnifiedAnalyzer()
    analyzer.analyze_all_instruments(['industry_sector'])
    
    # 直接使用日志
    logger = get_logger('my_module')
    logger.info('自定义日志消息')
"""

from .financial_instruments import FinancialInstrument
from .stock import Stock
from .etf import ETF
from .industry_sector import IndustrySector
from .concept_sector import ConceptSector
from .index import Index
from .unified_financial_system import UnifiedDataCollector, UnifiedAnalyzer, IndustryDataCollector, IndustryAnalyzer
from .logger_config import FinancialLogger, get_logger, LoggerMixin, log_method_call, log_data_operation

__all__ = [
    'FinancialInstrument',
    'Stock',
    'ETF',
    'IndustrySector', 
    'ConceptSector',
    'Index',
    'UnifiedDataCollector',
    'UnifiedAnalyzer',
    'IndustryDataCollector',
    'IndustryAnalyzer',
    'FinancialLogger',
    'get_logger',
    'LoggerMixin',
    'log_method_call',
    'log_data_operation'
]