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
from datetime import datetime, timedelta
import threading
import numpy as np
import push


class IndustryAnalyzer:
    """
    行业技术分析类
    提供综合技术分析功能，包括：
    - ZigZag分析
    - 分形分析
    - 斐波那契回撤
    - 布林带分析
    - 移动平均线分析
    - 转折点检测
    """
    
    def __init__(self, default_symbol="人形机器人", default_days_back=250, default_data_source="industry"):
        """
        初始化分析器
        
        Args:
            default_symbol: 默认分析的板块名称或股票代码
            default_days_back: 默认分析天数
            default_data_source: 默认数据来源 ("industry", "stock", "concept")
        """
        self.default_symbol = default_symbol
        self.default_days_back = default_days_back
        self.default_data_source = default_data_source
    
    def zigzag(self, high, low, close, deviation=0.05):
        """
        ZigZag算法识别高低点
        
        Args:
            high, low, close: 价格数组
            deviation: 最小变化幅度（默认5%）
        
        Returns:
            list: [(index, price, type)] type为'high'或'low'
        """
        peaks = []
        if len(close) < 3:
            return peaks
        
        trend = None
        last_peak_idx = 0
        last_peak_price = close[0]
        
        for i in range(1, len(close)):
            if trend is None:
                if close[i] > close[i-1] * (1 + deviation):
                    trend = 'up'
                    last_peak_idx = i-1
                    last_peak_price = close[i-1]
                    peaks.append((i-1, close[i-1], 'low'))
                elif close[i] < close[i-1] * (1 - deviation):
                    trend = 'down'
                    last_peak_idx = i-1
                    last_peak_price = close[i-1]
                    peaks.append((i-1, close[i-1], 'high'))
            
            elif trend == 'up':
                if close[i] > last_peak_price:
                    last_peak_idx = i
                    last_peak_price = close[i]
                elif close[i] < last_peak_price * (1 - deviation):
                    peaks.append((last_peak_idx, last_peak_price, 'high'))
                    trend = 'down'
                    last_peak_idx = i
                    last_peak_price = close[i]
            
            elif trend == 'down':
                if close[i] < last_peak_price:
                    last_peak_idx = i
                    last_peak_price = close[i]
                elif close[i] > last_peak_price * (1 + deviation):
                    peaks.append((last_peak_idx, last_peak_price, 'low'))
                    trend = 'up'
                    last_peak_idx = i
                    last_peak_price = close[i]
        
        if trend and len(peaks) > 0:
            peaks.append((last_peak_idx, last_peak_price, 'high' if trend == 'up' else 'low'))
        
        return peaks


    def fractal_highs_lows(self, high, low, period=2):
        """
        分形算法识别局部高低点
        
        Args:
            high, low: 价格数组
            period: 分形周期（默认2，即前后2个点）
        
        Returns:
            dict: {'highs': [(index, price)], 'lows': [(index, price)]}
        """
        fractal_highs = []
        fractal_lows = []
        
        for i in range(period, len(high) - period):
            is_high = True
            is_low = True
            
            for j in range(i - period, i + period + 1):
                if j == i:
                    continue
                if high[j] >= high[i]:
                    is_high = False
                if low[j] <= low[i]:
                    is_low = False
            
            if is_high:
                fractal_highs.append((i, high[i]))
            if is_low:
                fractal_lows.append((i, low[i]))
        
        return {'highs': fractal_highs, 'lows': fractal_lows}


    def fibonacci_retracement(self, high_price, low_price):
        """
        计算斐波那契回撤位
        
        Args:
            high_price: 高点价格
            low_price: 低点价格
        
        Returns:
            dict: 各个斐波那契回撤位
        """
        price_range = high_price - low_price
        
        fib_levels = {
            '0%': high_price,
            '23.6%': high_price - price_range * 0.236,
            '38.2%': high_price - price_range * 0.382,
            '50%': high_price - price_range * 0.5,
            '61.8%': high_price - price_range * 0.618,
            '78.6%': high_price - price_range * 0.786,
            '100%': low_price
        }
        
        return fib_levels


    def analyze_comprehensive_technical(self, symbol=None, days_back=None, data_source=None):
        """
        综合技术分析：布林带 + 斐波那契回撤 + ZigZag + 分形
        
        Args:
            symbol: 板块名称或股票代码（默认使用实例化时的默认值）
            days_back: 分析天数（默认使用实例化时的默认值）
            data_source: 数据来源，可选值：
                - "industry": 行业板块数据
                - "stock": 个股数据  
                - "concept": 概念板块数据
                （默认使用实例化时的默认值）
        
        Returns:
            dict: 包含所有技术分析结果的字典
        """
        if symbol is None:
            symbol = self.default_symbol
        if days_back is None:
            days_back = self.default_days_back
        if data_source is None:
            data_source = self.default_data_source
            
        end_date = datetime.now().strftime("%Y%m%d")
        start_date = (datetime.now() - timedelta(days=days_back)).strftime("%Y%m%d")
        
        # 根据数据源选择获取数据的方式
        try:
            if data_source == "industry":
                # 行业板块数据
                df = ak.stock_board_industry_hist_em(
                    symbol=symbol, 
                    start_date=start_date, 
                    end_date=end_date, 
                    period="日k", 
                    adjust=""
                )
            elif data_source == "stock":
                # 个股数据
                df = ak.stock_zh_a_hist(
                    symbol=symbol, 
                    period="daily", 
                    start_date=start_date, 
                    end_date=end_date, 
                    adjust=""
                )
            elif data_source == "concept":
                # 概念板块数据
                df = ak.stock_board_concept_hist_em(
                    symbol=symbol, 
                    period="daily", 
                    start_date=start_date, 
                    end_date=end_date, 
                    adjust=""
                )
            else:
                return {"error": f"不支持的数据源: {data_source}，请选择 'industry', 'stock', 或 'concept'"}
                
        except Exception as e:
            return {"error": f"获取{data_source}数据失败: {str(e)}"}
        
        if df.empty:
            return {"error": f"无法获取{data_source}数据，请检查symbol参数"}
        
        df = df.sort_values('日期').reset_index(drop=True)
        
        high_prices = df['最高'].values.astype(float)
        low_prices = df['最低'].values.astype(float)
        close_prices = df['收盘'].values.astype(float)
        
        upper_band, middle_band, lower_band = talib.BBANDS(
            close_prices,
            timeperiod=20,
            nbdevup=2,
            nbdevdn=2,
            matype=0
        )
        
        df['上轨'] = upper_band
        df['中轨'] = middle_band
        df['下轨'] = lower_band
        
        ma_data = self.calculate_moving_averages(close_prices)
        for ma_name, ma_values in ma_data.items():
            df[ma_name] = ma_values
        
        zigzag_points = self.zigzag(high_prices, low_prices, close_prices, deviation=0.08)
        
        fractals = self.fractal_highs_lows(high_prices, low_prices, period=3)
        
        latest_data = df.iloc[-1]
        latest_close = float(latest_data['收盘'])
        latest_lower_band = float(latest_data['下轨'])
        latest_middle_band = float(latest_data['中轨'])
        latest_upper_band = float(latest_data['上轨'])
        
        ma_arrangement = self.analyze_ma_arrangement(ma_data, latest_close)
        crossover_signals = self.detect_ma_crossover_signals(ma_data, lookback=5)
        turning_points = self.detect_turning_points(close_prices, ma_data, latest_close)
        
        bb_is_oversold = latest_close < latest_lower_band
        
        distance_to_lower = ((latest_close - latest_lower_band) / latest_lower_band) * 100
        bb_position = ((latest_close - latest_lower_band) / (latest_upper_band - latest_lower_band)) * 100
        
        recent_highs = [point for point in zigzag_points if point[2] == 'high'][-3:]
        recent_lows = [point for point in zigzag_points if point[2] == 'low'][-3:]
        
        fib_analysis = {}
        if recent_highs and recent_lows:
            last_high = max(recent_highs, key=lambda x: x[1])
            last_low = min(recent_lows, key=lambda x: x[1])
            
            if last_high[0] > last_low[0]:  
                swing_high = last_high[1]
                swing_low = last_low[1]
                fib_levels = self.fibonacci_retracement(swing_high, swing_low)
                
                fib_support_levels = []
                for level, price in fib_levels.items():
                    if abs(latest_close - price) / price < 0.02:  
                        fib_support_levels.append(level)
                
                fib_analysis = {
                    "摆动高点": swing_high,
                    "摆动低点": swing_low,
                    "斐波那契回撤位": fib_levels,
                    "当前位置接近的回撤位": fib_support_levels,
                    "回撤百分比": ((swing_high - latest_close) / (swing_high - swing_low)) * 100 if swing_high != swing_low else 0
                }
        
        fractal_recent_highs = fractals['highs'][-5:] if len(fractals['highs']) >= 5 else fractals['highs']
        fractal_recent_lows = fractals['lows'][-5:] if len(fractals['lows']) >= 5 else fractals['lows']
        
        综合分析信号 = []
        
        if bb_is_oversold:
            综合分析信号.append("布林带下轨超跌")
        
        if bb_position < 20:
            综合分析信号.append("布林带底部区域")
        
        if fib_analysis.get("回撤百分比", 0) > 50:
            综合分析信号.append("斐波那契深度回撤")
        
        if fib_analysis.get("当前位置接近的回撤位"):
            综合分析信号.append(f"接近斐波那契支撑位: {', '.join(fib_analysis['当前位置接近的回撤位'])}")
        
        if len(recent_lows) > 0:
            last_zigzag_low = min(recent_lows, key=lambda x: x[1])[1]
            if latest_close <= last_zigzag_low * 1.05:  
                综合分析信号.append("接近ZigZag关键低点")
        
        if len(fractal_recent_lows) > 0:
            last_fractal_low = min(fractal_recent_lows, key=lambda x: x[1])[1]
            if latest_close <= last_fractal_low * 1.03:  
                综合分析信号.append("接近分形关键低点")
        
        if ma_arrangement["排列状态"] in ["完美多头排列", "多头排列"]:
            综合分析信号.append(f"均线呈{ma_arrangement['排列状态']}")
        elif ma_arrangement["排列状态"] in ["完美空头排列", "空头排列"]:
            综合分析信号.append(f"均线呈{ma_arrangement['排列状态']}")
        
        for signal in crossover_signals:
            if signal["天数前"] <= 3:
                综合分析信号.append(f"{signal['天数前']}天前{signal['快线']}{signal['类型']}{signal['慢线']}")
        
        if turning_points["综合判断"] == "关键转折点":
            综合分析信号.append("检测到关键转折点信号")
        
        综合评级 = "强烈超跌" if len(综合分析信号) >= 3 else "可能超跌" if len(综合分析信号) >= 2 else "观望" if len(综合分析信号) >= 1 else "正常"
        
        return {
            "板块名称": symbol,
            "最新日期": latest_data['日期'],
            "最新收盘价": latest_close,
            
            "均线分析": ma_arrangement,
            
            "均线交叉信号": crossover_signals,
            
            "转折点分析": turning_points,
            
            "布林带分析": {
                "上轨": latest_upper_band,
                "中轨": latest_middle_band,
                "下轨": latest_lower_band,
                "是否超跌": bb_is_oversold,
                "距离下轨百分比": round(distance_to_lower, 2),
                "布林带位置": round(bb_position, 2)
            },
            
            "ZigZag分析": {
                "最近高点": recent_highs,
                "最近低点": recent_lows,
                "关键点数量": len(zigzag_points)
            },
            
            "分形分析": {
                "分形高点": fractal_recent_highs,
                "分形低点": fractal_recent_lows
            },
            
            "斐波那契分析": fib_analysis,
            
            "综合分析信号": 综合分析信号,
            "综合评级": 综合评级,
            "投资建议": self.get_investment_advice(综合评级, len(综合分析信号))
        }


    def calculate_moving_averages(self, prices, periods=[5, 10, 20, 30, 60]):
        """
        计算多周期移动平均线
        
        Args:
            prices: 价格序列
            periods: 均线周期列表
        
        Returns:
            dict: 各周期均线数据
        """
        ma_data = {}
        for period in periods:
            ma_data[f'MA{period}'] = talib.SMA(prices, timeperiod=period)
        return ma_data


    def analyze_ma_arrangement(self, ma_data, current_price):
        """
        分析均线排列状态
        
        Args:
            ma_data: 均线数据字典
            current_price: 当前价格
        
        Returns:
            dict: 排列分析结果
        """
        periods = [5, 10, 20, 30, 60]
        ma_values = []
        
        for period in periods:
            ma_key = f'MA{period}'
            if ma_key in ma_data and not np.isnan(ma_data[ma_key][-1]):
                ma_values.append((period, ma_data[ma_key][-1]))
        
        if len(ma_values) < 3:
            return {"排列状态": "数据不足", "信号强度": 0}
        
        ma_values_only = [value for _, value in ma_values]
        
        is_bullish = all(ma_values_only[i] >= ma_values_only[i+1] for i in range(len(ma_values_only)-1))
        is_bearish = all(ma_values_only[i] <= ma_values_only[i+1] for i in range(len(ma_values_only)-1))
        
        price_above_all = current_price > max(ma_values_only)
        price_below_all = current_price < min(ma_values_only)
        
        if is_bullish and price_above_all:
            arrangement = "完美多头排列"
            signal_strength = 5
        elif is_bullish:
            arrangement = "多头排列"
            signal_strength = 4
        elif is_bearish and price_below_all:
            arrangement = "完美空头排列"
            signal_strength = -5
        elif is_bearish:
            arrangement = "空头排列"
            signal_strength = -4
        else:
            arrangement = "混乱排列"
            signal_strength = 0
        
        return {
            "排列状态": arrangement,
            "信号强度": signal_strength,
            "价格位置": "多头" if current_price > ma_values_only[0] else "空头",
            "均线数值": {f'MA{period}': round(value, 2) for period, value in ma_values}
        }


    def detect_ma_crossover_signals(self, ma_data, lookback=5):
        """
        检测均线交叉信号
        
        Args:
            ma_data: 均线数据字典
            lookback: 回看天数
        
        Returns:
            list: 交叉信号列表
        """
        signals = []
        periods = [5, 10, 20, 30, 60]
        
        for i in range(len(periods)):
            for j in range(i+1, len(periods)):
                fast_period = periods[i]
                slow_period = periods[j]
                
                fast_ma = ma_data[f'MA{fast_period}']
                slow_ma = ma_data[f'MA{slow_period}']
                
                if len(fast_ma) < lookback or len(slow_ma) < lookback:
                    continue
                
                for k in range(1, min(lookback, len(fast_ma))):
                    if (fast_ma[-k-1] <= slow_ma[-k-1] and fast_ma[-k] > slow_ma[-k]):
                        signals.append({
                            "类型": "金叉",
                            "快线": f"MA{fast_period}",
                            "慢线": f"MA{slow_period}", 
                            "发生位置": len(fast_ma) - k,
                            "天数前": k,
                            "信号强度": "强" if fast_period <= 10 and slow_period >= 20 else "中"
                        })
                    elif (fast_ma[-k-1] >= slow_ma[-k-1] and fast_ma[-k] < slow_ma[-k]):
                        signals.append({
                            "类型": "死叉",
                            "快线": f"MA{fast_period}",
                            "慢线": f"MA{slow_period}",
                            "发生位置": len(fast_ma) - k,
                            "天数前": k,
                            "信号强度": "强" if fast_period <= 10 and slow_period >= 20 else "中"
                        })
        
        return sorted(signals, key=lambda x: x["天数前"])


    def detect_turning_points(self, prices, ma_data, current_price):
        """
        检测潜在转折点
        
        Args:
            prices: 价格序列
            ma_data: 均线数据
            current_price: 当前价格
        
        Returns:
            dict: 转折点分析结果
        """
        signals = []
        
        ma5 = ma_data.get('MA5', [])
        ma10 = ma_data.get('MA10', [])
        ma20 = ma_data.get('MA20', [])
        
        if len(ma5) < 3 or len(ma10) < 3 or len(ma20) < 3:
            return {"转折信号": [], "综合判断": "数据不足"}
        
        ma5_slope = (ma5[-1] - ma5[-3]) / 2
        ma10_slope = (ma10[-1] - ma10[-3]) / 2
        ma20_slope = (ma20[-1] - ma20[-3]) / 2
        
        if ma5_slope > 0 and ma10_slope > 0 and current_price > ma5[-1]:
            if ma20_slope <= 0:
                signals.append("短期均线向上，可能形成底部")
            else:
                signals.append("多均线向上，上升趋势确认")
        
        if ma5_slope < 0 and ma10_slope < 0 and current_price < ma5[-1]:
            if ma20_slope >= 0:
                signals.append("短期均线向下，可能形成顶部")
            else:
                signals.append("多均线向下，下降趋势确认")
        
        price_volatility = np.std(prices[-10:]) / np.mean(prices[-10:])
        if price_volatility > 0.05:
            signals.append("价格波动加剧，注意趋势转换")
        
        ma_convergence = abs(ma5[-1] - ma20[-1]) / ma20[-1]
        if ma_convergence < 0.02:
            signals.append("均线收敛，关注突破方向")
        
        if len(signals) >= 2:
            trend_judgment = "关键转折点"
        elif len(signals) == 1:
            trend_judgment = "潜在转折"
        else:
            trend_judgment = "趋势延续"
        
        return {
            "转折信号": signals,
            "综合判断": trend_judgment,
            "均线斜率": {
                "MA5斜率": round(ma5_slope, 4),
                "MA10斜率": round(ma10_slope, 4), 
                "MA20斜率": round(ma20_slope, 4)
            }
        }


    def get_investment_advice(self, rating, signal_count):
        """根据综合评级给出投资建议"""
        if rating == "强烈超跌":
            return "🔥 多重技术指标显示强烈超跌，可考虑分批建仓，但需注意风险控制"
        elif rating == "可能超跌":
            return "⚠️ 技术指标显示可能超跌，可小量试探建仓，密切关注后续走势"
        elif rating == "观望":
            return "👀 部分技术指标显示调整，建议观望等待更好机会"
        else:
            return "✅ 技术指标相对正常，可按既定策略操作"


# 使用示例
if __name__ == "__main__":
    # 创建分析器实例 - 使用不同的默认数据源
    print("=== 不同数据源分析示例 ===\n")
    
    # 1. 行业板块分析
    print("1. 行业板块分析：")
    industry_analyzer = IndustryAnalyzer(default_symbol="证券", default_data_source="industry")
    try:
        result = industry_analyzer.analyze_comprehensive_technical()
        print(f"   板块: {result['板块名称']} | 日期: {result['最新日期']} | 收盘价: {result['最新收盘价']}")
        print(f"   综合评级: {result['综合评级']}")
    except Exception as e:
        print(f"   行业板块分析失败: {e}")
    print()
    
    # 2. 个股分析
    print("2. 个股分析：")
    stock_analyzer = IndustryAnalyzer(default_symbol="000001", default_data_source="stock")
    try:
        result = stock_analyzer.analyze_comprehensive_technical()
        print(f"   股票: {result['板块名称']} | 日期: {result['最新日期']} | 收盘价: {result['最新收盘价']}")
        print(f"   综合评级: {result['综合评级']}")
    except Exception as e:
        print(f"   个股分析失败: {e}")
    print()
    
    # 3. 概念板块分析
    print("3. 概念板块分析：")
    concept_analyzer = IndustryAnalyzer(default_symbol="人工智能", default_data_source="concept")
    try:
        result = concept_analyzer.analyze_comprehensive_technical()
        print(f"   概念: {result['板块名称']} | 日期: {result['最新日期']} | 收盘价: {result['最新收盘价']}")
        print(f"   综合评级: {result['综合评级']}")
    except Exception as e:
        print(f"   概念板块分析失败: {e}")
    print()
    
    # 4. 动态指定数据源分析
    print("4. 动态指定数据源分析：")
    analyzer = IndustryAnalyzer()
    
    # 分析不同数据源的相同标的
    symbols_and_sources = [
        ("证券", "industry", "行业板块"),
        ("证券", "concept", "概念板块"),
        ("000001", "stock", "个股")
    ]
    
    for symbol, source, desc in symbols_and_sources:
        try:
            result = analyzer.analyze_comprehensive_technical(symbol=symbol, data_source=source)
            print(f"   {desc}({symbol}): 评级={result['综合评级']}, 信号数={len(result['综合分析信号'])}")
        except Exception as e:
            print(f"   {desc}({symbol}): 分析失败 - {e}")
    
    print("\n" + "="*60)
    print("使用说明：")
    print("- 'industry': 适用于行业板块名称，如'证券'、'银行'、'医药'等")
    print("- 'stock': 适用于个股代码，如'000001'、'000002'等")  
    print("- 'concept': 适用于概念板块名称，如'人工智能'、'新能源'等")
    print("技术分析仅供参考，投资需谨慎！")