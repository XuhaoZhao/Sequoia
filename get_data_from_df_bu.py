# -*- coding: utf-8 -*-
import akshare as ak
import pandas as pd
import talib
from dash import Dash, dcc, html
from dash.dependencies import Input, Output
import plotly.graph_objects as go
from plotly.subplots import make_subplots

# ======== 数据获取与处理 ========
def get_intraday(symbol="000333"):
    df = ak.stock_intraday_em(symbol=symbol)
    df.rename(columns={"时间": "time", "成交价": "price", "手数": "volume"}, inplace=True)
    df['time'] = pd.to_datetime(df['time'], errors="coerce")
    df = df.dropna(subset=['time'])
    df.set_index('time', inplace=True) 
    return df

def resample_and_macd(df, rule):
    """按周期聚合，并计算MACD"""
    ohlcv = df['price'].resample(rule).ohlc()
    ohlcv['volume'] = df['volume'].resample(rule).sum()
    ohlcv.dropna(inplace=True)
    print(f"[resample_and_macd] OHLCV数据行数: {len(ohlcv)} (周期: {rule})")
    
    close = ohlcv['close'].values
    if len(close) < 20:  # 数据太少就不计算
        return ohlcv
    fast, slow, signal = 12, 26, 9

    dif, dea, hist = talib.MACD(close, fastperiod=fast, slowperiod=slow, signalperiod=signal)
    print(f"[resample_and_macd] MACD计算后DIF数据行数: {len(dif)} (周期: {rule})")
    print(dif)


    ohlcv['DIF'] = dif
    ohlcv['DEA'] = dea
    ohlcv['HIST'] = hist
    return ohlcv

# ======== Dash App ========
app = Dash(__name__)
app.title = "多周期 MACD 可视化"

app.layout = html.Div([
    html.H2("股票多周期 MACD 可视化"),
    html.Div([
        html.Label("股票代码:"),
        dcc.Input(id="stock-input", type="text", value="000333", style={"marginRight":"20px"}),
        html.Label("选择周期:"),
        dcc.Checklist(
            id="period-checklist",
            options=[
                {"label": "5分钟", "value": "5T"},
                {"label": "30分钟", "value": "30T"},
                {"label": "60分钟", "value": "60T"},
                {"label": "120分钟", "value": "120T"},
                {"label": "240分钟", "value": "240T"},
            ],
            value=["5T", "30T", "60T"]
        ),
    ], style={"marginBottom": "20px"}),
    dcc.Graph(id="macd-graph"),
    dcc.Interval(
        id='interval-component',
        interval=5*60*1000,  # 5分钟刷新一次
        n_intervals=0
    )
])

@app.callback(
    Output("macd-graph", "figure"),
    Input("stock-input", "value"),
    Input("period-checklist", "value"),
    Input("interval-component", "n_intervals")
)
def update_macd(symbol, periods, n_intervals):
    df = get_intraday(symbol)
    print(f"[update_macd] 原始分时数据行数: {len(df)} (股票: {symbol})")
    fig = make_subplots(
        rows=len(periods)*2, cols=1, shared_xaxes=True,
        vertical_spacing=0.02,
        row_heights=[0.6, 0.4]*len(periods),
        subplot_titles=[f"{rule} 收盘价" if i%2==0 else f"{rule} MACD" for i, rule in enumerate(periods*2)]
    )

    row = 1
    for rule in periods:
        dfr = resample_and_macd(df, rule)
        if dfr.empty:
            row += 2
            continue

        # 上图：收盘价
        fig.add_trace(
            go.Scatter(x=dfr.index, y=dfr['close'], name=f"{rule} 收盘价", line=dict(color="black")),
            row=row, col=1
        )

        # 下图：MACD
        row += 1
        if 'DIF' in dfr:
            fig.add_trace(
                go.Scatter(x=dfr.index, y=dfr['DIF'], name=f"{rule} DIF", line=dict(color="blue")),
                row=row, col=1
            )
            fig.add_trace(
                go.Scatter(x=dfr.index, y=dfr['DEA'], name=f"{rule} DEA", line=dict(color="red")),
                row=row, col=1
            )
            # MACD 柱
            colors = ['green' if val >=0 else 'red' for val in dfr['HIST']]
            fig.add_trace(
                go.Bar(x=dfr.index, y=dfr['HIST'], name=f"{rule} MACD柱", marker_color=colors, opacity=0.5),
                row=row, col=1
            )
        row += 1

    fig.update_layout(height=300*len(periods), width=1200, title_text=f"{symbol} 多周期 MACD", showlegend=True)
    return fig

if __name__ == "__main__":
    app.run(debug=True, port=8050)
