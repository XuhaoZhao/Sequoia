# -*- encoding: UTF-8 -*-

import os
import datetime
from typing import Optional


class FilePathGenerator:
    """
    文件路径生成器类
    统一管理各种文件的路径生成规则
    """

    @staticmethod
    def generate_data_path(data_type: str, date: Optional[str] = None, extension: str = "csv", base_dir: str = "data") -> str:
        """
        生成数据文件路径

        Args:
            data_type (str): 数据类型，如 'etf', 'stock', 'industry' 等
            date (Optional[str]): 日期字符串，格式为 YYYY-MM-DD，如果为None则使用今天
            extension (str): 文件扩展名，默认为 'csv'
            base_dir (str): 基础目录，默认为 'data'

        Returns:
            str: 生成的文件路径

        Examples:
            >>> FilePathGenerator.generate_data_path('etf')
            'data/etf_data_2023-10-20.csv'

            >>> FilePathGenerator.generate_data_path('stock', '2023-10-19')
            'data/stock_data_2023-10-19.csv'

            >>> FilePathGenerator.generate_data_path('industry', extension='json')
            'data/industry_data_2023-10-20.json'
        """
        if date is None:
            date = datetime.datetime.now().strftime("%Y-%m-%d")

        filename = f"{data_type}_data_{date}.{extension}"
        return os.path.join(base_dir, filename)

    @staticmethod
    def generate_xuangu_path(date: Optional[str] = None, base_dir: str = "data", filename: str = "xuangu_data.csv") -> str:
        """
        生成选股数据文件路径

        Args:
            date (Optional[str]): 日期字符串，如果提供则添加到文件名中
            base_dir (str): 基础目录
            filename (str): 基础文件名

        Returns:
            str: 生成的文件路径
        """
        if date is None:
            return os.path.join(base_dir, filename)
        else:
            name, ext = os.path.splitext(filename)
            dated_filename = f"{name}_{date}{ext}"
            return os.path.join(base_dir, dated_filename)

    @staticmethod
    def generate_log_path(log_name: str, date: Optional[str] = None, base_dir: str = "logs", extension: str = "log") -> str:
        """
        生成日志文件路径

        Args:
            log_name (str): 日志名称
            date (Optional[str]): 日期字符串，如果为None则使用今天
            base_dir (str): 基础目录
            extension (str): 文件扩展名

        Returns:
            str: 生成的日志文件路径
        """
        if date is None:
            date = datetime.datetime.now().strftime("%Y-%m-%d")

        filename = f"{log_name}_{date}.{extension}"
        return os.path.join(base_dir, filename)

    @staticmethod
    def generate_backup_path(original_path: str, timestamp: Optional[str] = None, backup_dir: str = "backup") -> str:
        """
        生成备份文件路径

        Args:
            original_path (str): 原始文件路径
            timestamp (Optional[str]): 时间戳，如果为None则使用当前时间
            backup_dir (str): 备份目录

        Returns:
            str: 生成的备份文件路径
        """
        if timestamp is None:
            timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")

        basename = os.path.basename(original_path)
        name, ext = os.path.splitext(basename)
        backup_filename = f"{name}_backup_{timestamp}{ext}"
        return os.path.join(backup_dir, backup_filename)

    @staticmethod
    def ensure_directory_exists(file_path: str) -> None:
        """
        确保文件路径的目录存在

        Args:
            file_path (str): 文件路径
        """
        directory = os.path.dirname(file_path)
        if directory and not os.path.exists(directory):
            os.makedirs(directory, exist_ok=True)

    @staticmethod
    def get_data_file_pattern(data_type: str, extension: str = "csv", base_dir: str = "data") -> str:
        """
        获取数据文件的匹配模式

        Args:
            data_type (str): 数据类型
            extension (str): 文件扩展名
            base_dir (str): 基础目录

        Returns:
            str: 文件匹配模式，可用于glob等文件搜索
        """
        return os.path.join(base_dir, f"{data_type}_data_*.{extension}")

    @staticmethod
    def generate_macd_signal_path(instrument_type: str, period: str = "30m", date: Optional[str] = None, base_dir: str = "data") -> str:
        """
        生成MACD金叉信号文件路径

        Args:
            instrument_type (str): 产品类型，如 'stock', 'etf' 等
            period (str): 数据周期，默认为 '30m'
            date (Optional[str]): 日期字符串，格式为 YYYY-MM-DD，如果为None则使用今天
            base_dir (str): 基础目录，默认为 'data'

        Returns:
            str: 生成的文件路径

        Examples:
            >>> FilePathGenerator.generate_macd_signal_path('stock')
            'data/stock_macd_30m_2025-10-23.csv'

            >>> FilePathGenerator.generate_macd_signal_path('etf', '30m', '2025-10-22')
            'data/etf_macd_30m_2025-10-22.csv'
        """
        if date is None:
            date = datetime.datetime.now().strftime("%Y-%m-%d")

        filename = f"{instrument_type}_macd_{period}_{date}.csv"
        return os.path.join(base_dir, filename)


# 便捷函数，保持向后兼容性
def generate_etf_data_path(date: Optional[str] = None) -> str:
    """生成ETF数据文件路径"""
    return FilePathGenerator.generate_data_path("etf", date)


def generate_stock_data_path(date: Optional[str] = None) -> str:
    """生成股票数据文件路径"""
    return FilePathGenerator.generate_data_path("stock", date)


def generate_industry_data_path(date: Optional[str] = None) -> str:
    """生成行业数据文件路径"""
    return FilePathGenerator.generate_data_path("industry", date)


def generate_concept_data_path(date: Optional[str] = None) -> str:
    """生成概念数据文件路径"""
    return FilePathGenerator.generate_data_path("concept", date)


def generate_macd_signal_path(instrument_type: str, period: str = "30m", date: Optional[str] = None) -> str:
    """
    生成MACD金叉信号文件路径的便捷函数

    Args:
        instrument_type (str): 产品类型，如 'stock', 'etf' 等
        period (str): 数据周期，默认为 '30m'
        date (Optional[str]): 日期字符串，格式为 YYYY-MM-DD，如果为None则使用今天

    Returns:
        str: 生成的文件路径

    Examples:
        >>> generate_macd_signal_path('stock')
        'data/stock_macd_30m_2025-10-23.csv'

        >>> generate_macd_signal_path('etf', '30m', '2025-10-22')
        'data/etf_macd_30m_2025-10-22.csv'
    """
    return FilePathGenerator.generate_macd_signal_path(instrument_type, period, date)