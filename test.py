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
from datetime import datetime
import threading
import numpy as np
import push as push
import settings
from financial_framework.index import Index
from financial_framework.concept_sector import ConceptSector
from financial_framework.etf import ETF
from financial_framework.stock import Stock
from financial_framework.unified_financial_system import UnifiedDataCollector,UnifiedAnalyzer,TechnicalAnalyzer
from data_collect.stock_chip_race import stock_chip_race_open,stock_chip_race_end,stock_large_cap_filter
import adata as ad


# hh = ak.index_csindex_all()

# print(hh)

# # 方法1: 使用 isin() 检查单个或多个代码
# target_code = "930721"  # 要查找的代码
# exists = hh["指数代码"].isin([target_code]).any()
# print(f"代码 {target_code} 是否存在: {exists}")


# stock_zh_index_hist_csindex_df = ak.stock_zh_index_hist_csindex(symbol="H30590", start_date="20100101", end_date="20240604")
# print(stock_zh_index_hist_csindex_df)


# stock_zh_index_spot_em_df = ak.stock_zh_index_spot_em(symbol="中证系列指数")
# print(stock_zh_index_spot_em_df)

# stock_zh_index_spot_sina_df = ak.stock_zh_index_spot_sina()
# print(stock_zh_index_spot_sina_df)

# index_zh_a_hist_min_em_df = ak.index_zh_a_hist_min_em(symbol="H30590", period="1", start_date="2024-12-11 09:30:00", end_date="2025-12-11 19:00:00")
# print(index_zh_a_hist_min_em_df)
# settings.init()
# push.push("哈哈哈")

# boards_df = ak.stock_board_industry_name_em()

# print('2025-10-01' > '2025-10')

# indexc = Index()
# print(indexc.get_realtime_1min_data())

# index_zh_a_hist_min_em_df = ak.index_zh_a_hist_min_em(symbol="000300", period="5", start_date="2025-09-11 09:30:00", end_date="2025-12-11 19:00:00")
# print(index_zh_a_hist_min_em_df)

# boards_df = ak.stock_board_industry_name_em()
# print(boards_df)

# stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
# print(stock_board_concept_name_em_df)

# concept = ConceptSector()
# print(concept.get_historical_5min_data({'code': 'BK0968', 'name': '固态电池'}))

# concept = ConceptSector()
# print(concept.get_realtime_1min_data())

# concept = ConceptSector()
# print(concept.get_daily_data({'code': 'BK0968', 'name': '固态电池'}))

# stock_board_concept_hist_em_df = ak.stock_board_concept_hist_em(symbol="固态电池", period="daily", start_date="20250101", end_date="20250227", adjust="")
# print(stock_board_concept_hist_em_df)


# fund_etf_spot_em_df = ak.fund_etf_spot_em()
# print(fund_etf_spot_em_df)
# uu = UnifiedDataCollector()
# # uu.collect_realtime_1min_data(instrument_type='concept_sector')
# uu.start_monitoring()

# temp_df = stock_large_cap_filter(debug=True)
# print(temp_df)

# 注意：该接口返回的数据只有最近一个交易日的有开盘价，其他日期开盘价为 0
# stock_intraday_em_df = ak.stock_intraday_em(symbol="000977")
# print(stock_intraday_em_df)


# fund_etf_hist_min_em_df = ak.fund_etf_hist_min_em(symbol="515170", period="1", adjust="", start_date="2025-10-15 09:00:00", end_date="2026-03-20 17:40:00")
# print(fund_etf_hist_min_em_df)

# stock = Stock(db=1)
# stock_value = stock.get_all_instruments()
# print(stock_value)

# uu = UnifiedDataCollector()
# uu.collect_all_historical_min_data(instrument_type='etf',period = '30')

# fund_etf_hist_em_df = ak.fund_etf_hist_em(symbol="513500", period="daily", start_date="20000101", end_date="20230201", adjust="")
# print(fund_etf_hist_em_df)
# data = ad.fund.market.get_market_etf('512690',1,'2025-01-01','2026-01-01')

# print(data)

# # 注意：该接口返回的数据只有最近一个交易日的有开盘价，其他日期开盘价为 0
# stock_zh_a_hist_min_em_df = ak.stock_zh_a_hist_min_em(symbol="002463", start_date="2025-08-20 09:30:00", end_date="2026-03-20 15:00:00", period="30", adjust="")
# print(stock_zh_a_hist_min_em_df)

# ff = UnifiedAnalyzer()
# ff.analyze_all_instruments('stock')

# ee = TechnicalAnalyzer()
# ee.analyze_instruments_from_macd_file('stock')

data = ad.fund.info.all_etf_exchange_traded_info()
print(data)