import akshare as ak
import pandas as pd
import talib
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots



#所有概念板块实时
stock_board_concept_name_em_df = ak.stock_board_concept_name_em()
print("所有板块名称:")
print(stock_board_concept_name_em_df['板块名称'].tolist())

#单概念板块分时历史
# stock_board_concept_hist_min_em_df = ak.stock_board_concept_hist_min_em(symbol="银行", period="5")
# print(stock_board_concept_hist_min_em_df)

# ['存储芯片', '锂矿概念', 'AI芯片', '小金属概念', 'Kimi概念', 'Sora概念', '低碳冶金', '麒麟电池', 'Chiplet概念', '上海自贸', '昨日涨停_含一字', '同步磁阻电机', '稀缺资源', '汽车芯片', '星闪概念', '磷化工', '昨日连板_含一字', '黄金概念', '氟化工', '多模态AI', '华为昇腾', '航母概念', 'AI眼镜', '科创板做市股', '大飞机', 'Web3.0', '空间站概念']