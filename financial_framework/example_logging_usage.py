# 金融框架日志系统使用示例

"""
演示如何在financial_framework中使用日志功能
"""

import logging
from financial_framework import (
    UnifiedDataCollector, 
    FinancialLogger, 
    get_logger,
    Stock
)

def main():
    # 1. 初始化日志系统（可选配置）
    FinancialLogger.setup_logging(
        log_dir="logs",
        log_level=logging.INFO,
        console_output=True
    )
    
    # 2. 获取自定义日志器
    logger = get_logger('example_usage')
    logger.info("开始运行金融框架示例")
    
    try:
        # 3. 使用数据收集器（自动包含日志）
        collector = UnifiedDataCollector()
        
        # 收集股票数据（会自动记录详细日志）
        collector.collect_all_historical_5min_data(['stock'])
        
        # 4. 单独使用股票类（自动包含日志）
        stock = Stock()
        
        # 获取股票列表（会记录操作日志）
        stocks = stock.get_all_instruments()
        logger.info(f"获取到{len(stocks)}个股票")
        
        # 获取单个股票数据（会记录数据操作日志）
        if stocks:
            sample_stock = stocks[0]
            data = stock.get_historical_5min_data(sample_stock['code'])
            if data is not None:
                logger.info(f"成功获取{sample_stock['name']}的数据，共{len(data)}条记录")
        
    except Exception as e:
        logger.error(f"示例运行失败: {e}", exc_info=True)
    
    logger.info("示例运行完成")

if __name__ == "__main__":
    main()