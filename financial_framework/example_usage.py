# -*- encoding: UTF-8 -*-

"""
文件路径生成器使用示例
展示如何在不同的金融数据处理模块中使用FilePathGenerator
"""

from financial_framework.file_path_generator import (
    FilePathGenerator,
    generate_etf_data_path,
    generate_stock_data_path,
    generate_industry_data_path,
    generate_concept_data_path
)


def example_usage():
    """使用示例"""

    # 1. 使用便捷函数生成数据文件路径
    etf_path = generate_etf_data_path()
    stock_path = generate_stock_data_path()
    print(f"ETF数据路径: {etf_path}")  # 输出: data/etf_data_2023-10-20.csv
    print(f"股票数据路径: {stock_path}")  # 输出: data/stock_data_2023-10-20.csv

    # 2. 使用FilePathGenerator类生成自定义路径
    industry_path = FilePathGenerator.generate_data_path(
        data_type="industry",
        date="2023-10-19",
        extension="csv",
        base_dir="data"
    )
    print(f"行业数据路径: {industry_path}")  # 输出: data/industry_data_2023-10-19.csv

    # 3. 生成JSON格式数据路径
    config_path = FilePathGenerator.generate_data_path(
        data_type="config",
        extension="json",
        base_dir="config"
    )
    print(f"配置文件路径: {config_path}")  # 输出: config/config_data_2023-10-20.json

    # 4. 生成日志文件路径
    log_path = FilePathGenerator.generate_log_path(
        log_name="trading",
        base_dir="logs"
    )
    print(f"交易日志路径: {log_path}")  # 输出: logs/trading_2023-10-20.log

    # 5. 生成备份文件路径
    backup_path = FilePathGenerator.generate_backup_path(
        original_path="data/stock_data_2023-10-20.csv",
        timestamp="20231020_153005"
    )
    print(f"备份文件路径: {backup_path}")  # 输出: backup/stock_data_2023-10-20_backup_20231020_153005.csv

    # 6. 确保目录存在并创建文件
    FilePathGenerator.ensure_directory_exists("data/custom/subdir")
    print("目录已创建")

    # 7. 获取文件匹配模式，用于搜索特定类型的文件
    pattern = FilePathGenerator.get_data_file_pattern("etf")
    print(f"ETF文件匹配模式: {pattern}")  # 输出: data/etf_data_*.csv

    # 8. 批量生成不同日期的数据路径
    dates = ["2023-10-18", "2023-10-19", "2023-10-20"]
    for date in dates:
        path = generate_stock_data_path(date)
        print(f"{date} 的股票数据路径: {path}")


# 在金融数据处理模块中的应用示例
class ETFDataProcessor:
    """ETF数据处理器示例"""

    def __init__(self):
        self.base_dir = "data"

    def get_today_data_path(self):
        """获取今日数据文件路径"""
        return FilePathGenerator.generate_data_path("etf", base_dir=self.base_dir)

    def get_historical_data_path(self, date):
        """获取历史数据文件路径"""
        return FilePathGenerator.generate_data_path("etf", date=date, base_dir=self.base_dir)

    def backup_current_data(self):
        """备份当前数据"""
        current_path = self.get_today_data_path()
        backup_path = FilePathGenerator.generate_backup_path(current_path)
        FilePathGenerator.ensure_directory_exists(backup_path)
        return backup_path


class StockAnalysisSystem:
    """股票分析系统示例"""

    def __init__(self):
        self.data_dir = "data"
        self.log_dir = "logs"
        self.backup_dir = "backup"

    def setup_directories(self):
        """设置所需目录结构"""
        # 确保所有必要的目录都存在
        FilePathGenerator.ensure_directory_exists(
            FilePathGenerator.generate_data_path("stock", base_dir=self.data_dir)
        )
        FilePathGenerator.ensure_directory_exists(
            FilePathGenerator.generate_log_path("analysis", base_dir=self.log_dir)
        )
        FilePathGenerator.ensure_directory_exists(self.backup_dir)

    def get_analysis_paths(self, date=None):
        """获取分析相关的所有文件路径"""
        return {
            "stock_data": FilePathGenerator.generate_data_path("stock", date, base_dir=self.data_dir),
            "etf_data": FilePathGenerator.generate_data_path("etf", date, base_dir=self.data_dir),
            "analysis_log": FilePathGenerator.generate_log_path("analysis", date, base_dir=self.log_dir),
            "backup_pattern": FilePathGenerator.get_data_file_pattern("stock", base_dir=self.backup_dir)
        }


if __name__ == "__main__":
    example_usage()