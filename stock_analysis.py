import akshare as ak
import pandas as pd
import talib
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# # 个股分时数据
# stock_zh_a_minute_df = ak.stock_zh_a_minute(symbol='sz000333', period='1', adjust="qfq")

# # 将day列转换为datetime类型
# stock_zh_a_minute_df['day'] = pd.to_datetime(stock_zh_a_minute_df['day'])

# # 获取当前日期（不包含时间）
# today = pd.Timestamp.now().date()

# # 筛选昨日及之前的数据
# historical_data = stock_zh_a_minute_df[stock_zh_a_minute_df['day'].dt.date < today]

# print(f"总数据行数: {len(stock_zh_a_minute_df)}")
# print(f"昨日及之前数据行数: {len(historical_data)}")
# print(f"今日数据行数: {len(stock_zh_a_minute_df) - len(historical_data)}")
# print("\n昨日及之前数据最后100行:")
# print(historical_data.tail(100))

# stock_zh_a_spot_df = ak.stock_zh_a_spot()
# print(stock_zh_a_spot_df)