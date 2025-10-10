"""
统一日志配置模块

提供高质量的日志配置，包括：
- 多级别日志记录
- 文件轮转
- 格式化输出
- 性能监控
- 错误追踪
"""

import logging
import logging.handlers
import os
import sys
from datetime import datetime
from pathlib import Path
import functools
import time


class FinancialLogger:
    """金融框架统一日志器"""
    
    _loggers = {}
    _configured = False
    
    @classmethod
    def setup_logging(cls, 
                     log_dir="logs", 
                     log_level=logging.INFO,
                     max_file_size=10*1024*1024,  # 10MB
                     backup_count=5,
                     console_output=True):
        """
        设置统一日志配置
        
        Args:
            log_dir: 日志文件目录
            log_level: 日志级别
            max_file_size: 单个日志文件最大大小（字节）
            backup_count: 保留的备份文件数量
            console_output: 是否输出到控制台
        """
        if cls._configured:
            return
            
        # 创建日志目录
        log_path = Path(log_dir)
        log_path.mkdir(exist_ok=True)
        
        # 设置根日志器
        root_logger = logging.getLogger()
        root_logger.setLevel(log_level)
        
        # 清除已有的处理器
        for handler in root_logger.handlers[:]:
            root_logger.removeHandler(handler)
        
        # 创建格式化器
        detailed_formatter = logging.Formatter(
            '%(asctime)s | %(name)s | %(levelname)s | %(filename)s:%(lineno)d | %(funcName)s | %(message)s',
            datefmt='%Y-%m-%d %H:%M:%S'
        )
        
        simple_formatter = logging.Formatter(
            '%(asctime)s | %(levelname)s | %(name)s | %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # 1. 主应用日志文件（轮转）
        main_log_file = log_path / "financial_framework.log"
        file_handler = logging.handlers.RotatingFileHandler(
            main_log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(file_handler)
        
        # 2. 错误日志文件（单独记录错误）
        error_log_file = log_path / "errors.log"
        error_handler = logging.handlers.RotatingFileHandler(
            error_log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        error_handler.setLevel(logging.ERROR)
        error_handler.setFormatter(detailed_formatter)
        root_logger.addHandler(error_handler)
        
        # 3. 性能日志文件
        perf_log_file = log_path / "performance.log"
        perf_handler = logging.handlers.RotatingFileHandler(
            perf_log_file,
            maxBytes=max_file_size,
            backupCount=backup_count,
            encoding='utf-8'
        )
        perf_handler.setLevel(logging.INFO)
        perf_handler.setFormatter(detailed_formatter)
        
        # 4. 控制台输出
        if console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setLevel(log_level)
            console_handler.setFormatter(simple_formatter)
            root_logger.addHandler(console_handler)
        
        # 5. 数据操作日志（按日期分文件）
        today = datetime.now().strftime('%Y-%m-%d')
        data_log_file = log_path / f"data_operations_{today}.log"
        data_handler = logging.FileHandler(data_log_file, encoding='utf-8')
        data_handler.setLevel(logging.INFO)
        data_handler.setFormatter(detailed_formatter)
        
        # 为特定模块设置数据操作日志
        data_logger = logging.getLogger('financial_framework.data')
        data_logger.addHandler(data_handler)
        
        # 为性能监控设置专用日志
        perf_logger = logging.getLogger('financial_framework.performance')
        perf_logger.addHandler(perf_handler)
        
        cls._configured = True
        
        # 记录启动信息
        startup_logger = cls.get_logger('financial_framework.startup')
        startup_logger.info(f"日志系统初始化完成 - 日志目录: {log_path.absolute()}")
        startup_logger.info(f"日志级别: {logging.getLevelName(log_level)}")
        startup_logger.info(f"文件大小限制: {max_file_size / (1024*1024):.1f}MB")
        startup_logger.info(f"备份文件数量: {backup_count}")
    
    @classmethod
    def get_logger(cls, name):
        """获取指定名称的日志器"""
        if not cls._configured:
            cls.setup_logging()
        
        if name not in cls._loggers:
            cls._loggers[name] = logging.getLogger(name)
        
        return cls._loggers[name]
    
    @classmethod
    def get_data_logger(cls):
        """获取数据操作专用日志器"""
        return cls.get_logger('financial_framework.data')
    
    @classmethod
    def get_performance_logger(cls):
        """获取性能监控专用日志器"""
        return cls.get_logger('financial_framework.performance')


def log_method_call(include_args=True, include_result=False):
    """
    方法调用日志装饰器
    
    Args:
        include_args: 是否记录参数
        include_result: 是否记录返回值
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            logger = getattr(self, 'logger', FinancialLogger.get_logger(self.__class__.__name__))
            
            # 记录方法调用开始
            if include_args:
                args_str = ", ".join([str(arg)[:100] for arg in args])  # 限制参数长度
                kwargs_str = ", ".join([f"{k}={str(v)[:100]}" for k, v in kwargs.items()])
                params = f"({args_str}{', ' + kwargs_str if kwargs_str else ''})"
                logger.debug(f"调用 {func.__name__}{params}")
            else:
                logger.debug(f"调用 {func.__name__}")
            
            start_time = time.time()
            try:
                result = func(self, *args, **kwargs)
                
                # 记录执行时间
                execution_time = time.time() - start_time
                if execution_time > 1.0:  # 超过1秒的操作记录到性能日志
                    perf_logger = FinancialLogger.get_performance_logger()
                    perf_logger.warning(f"{self.__class__.__name__}.{func.__name__} 执行耗时 {execution_time:.2f}秒")
                
                # 记录返回值（如果需要）
                if include_result and result is not None:
                    result_str = str(result)[:200] if hasattr(result, '__str__') else str(type(result))
                    logger.debug(f"{func.__name__} 返回: {result_str}")
                
                logger.debug(f"{func.__name__} 执行成功 (耗时: {execution_time:.3f}秒)")
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                logger.error(f"{func.__name__} 执行失败 (耗时: {execution_time:.3f}秒): {str(e)}", exc_info=True)
                raise
        
        return wrapper
    return decorator


def log_data_operation(operation_type):
    """
    数据操作日志装饰器
    
    Args:
        operation_type: 操作类型 (如: 'fetch', 'save', 'process')
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(self, *args, **kwargs):
            data_logger = FinancialLogger.get_data_logger()
            
            # 获取产品信息用于记录
            instrument_info = ""
            if hasattr(self, 'get_instrument_type'):
                instrument_info = f"[{self.get_instrument_type()}] "
            
            start_time = time.time()
            data_logger.info(f"{instrument_info}开始{operation_type}操作: {func.__name__}")
            
            try:
                result = func(self, *args, **kwargs)
                execution_time = time.time() - start_time
                
                # 记录数据量信息
                data_info = ""
                if hasattr(result, '__len__'):
                    data_info = f" (数据量: {len(result)}条)"
                elif hasattr(result, 'shape'):
                    data_info = f" (数据形状: {result.shape})"
                
                data_logger.info(f"{instrument_info}{operation_type}操作完成: {func.__name__}{data_info} (耗时: {execution_time:.2f}秒)")
                return result
                
            except Exception as e:
                execution_time = time.time() - start_time
                data_logger.error(f"{instrument_info}{operation_type}操作失败: {func.__name__} (耗时: {execution_time:.2f}秒) - {str(e)}")
                raise
        
        return wrapper
    return decorator


class LoggerMixin:
    """日志器混入类，为其他类提供日志功能"""
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.logger = FinancialLogger.get_logger(f"financial_framework.{self.__class__.__name__}")
    
    def log_info(self, message):
        """记录信息日志"""
        self.logger.info(message)
    
    def log_warning(self, message):
        """记录警告日志"""
        self.logger.warning(message)
    
    def log_error(self, message, exc_info=False):
        """记录错误日志"""
        self.logger.error(message, exc_info=exc_info)
    
    def log_debug(self, message):
        """记录调试日志"""
        self.logger.debug(message)
    
    def log_data_operation(self, operation, details=""):
        """记录数据操作"""
        data_logger = FinancialLogger.get_data_logger()
        instrument_type = getattr(self, 'get_instrument_type', lambda: 'Unknown')()
        data_logger.info(f"[{instrument_type}] {operation} {details}")


# 初始化日志系统（首次导入时）
def initialize_logging(log_level=logging.INFO, console_output=True):
    """初始化日志系统"""
    FinancialLogger.setup_logging(
        log_level=log_level,
        console_output=console_output
    )


# 导出的便捷函数
def get_logger(name):
    """获取日志器的便捷函数"""
    return FinancialLogger.get_logger(name)


# 模块级别的日志器
module_logger = FinancialLogger.get_logger('financial_framework.logger_config')
module_logger.info("日志配置模块加载完成")