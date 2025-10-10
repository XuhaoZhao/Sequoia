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



# index_zh_a_hist_min_em_df = ak.index_zh_a_hist_min_em(symbol="H30590", period="1", start_date="2024-12-11 09:30:00", end_date="2025-12-11 19:00:00")
# print(index_zh_a_hist_min_em_df)
# settings.init()
# push.push("哈哈哈")

# boards_df = ak.stock_board_industry_name_em()

# print('2025-10-01' > '2025-10')

# indexc = Index()
# print(indexc.get_realtime_1min_data())

index_zh_a_hist_min_em_df = ak.index_zh_a_hist_min_em(symbol="BK0125", period="5", start_date="2025-09-11 09:30:00", end_date="2025-12-11 19:00:00")
print(index_zh_a_hist_min_em_df)