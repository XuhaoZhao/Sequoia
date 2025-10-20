import akshare as ak
import pandas as pd
from datetime import datetime
from .financial_instruments import FinancialInstrument
from .logger_config import log_data_operation, log_method_call
from data_collect.stock_chip_race import stock_large_cap_filter
from financial_framework.file_path_generator import (
    generate_stock_data_path
)


class Stock(FinancialInstrument):
    """股票类"""

    # 获取5分钟历史数据的延迟时间（秒），防止被封禁IP
    delay_seconds = 80

    def get_instrument_type(self):
        return "股票"
    
    @log_method_call(include_args=False)
    def get_all_instruments(self):
        """获取所有股票列表"""
        try:
            self.log_info("开始获取所有股票列表")
            # 获取股票数据文件路径
            stock_path = generate_stock_data_path()
            self.log_info(f"读取股票数据文件: {stock_path}")

            # 读取CSV文件
            df = pd.read_csv(stock_path)

            # 从CSV文件中获取股票名称和代码
            result = []
            for _, row in df.iterrows():
                stock_info = {
                    'code': str(row['SECURITY_CODE']),
                    'name': str(row['SECURITY_SHORT_NAME'])
                }
                result.append(stock_info)

            self.log_info(f"成功获取{len(result)}个股票")
            return result
        except Exception as e:
            self.log_error(f"获取股票列表失败: {e}", exc_info=True)
            return []
    
    @log_data_operation('获取股票历史分时数据')
    def get_historical_min_data(self, stock_info, period="5", delay_seconds=1.0):
        """获取股票历史分时数据

        Args:
            stock_info: 股票信息字典（包含 code 和 name）
            period: 数据周期（"1", "5", "15", "30", "60"等，单位：分钟）
            delay_seconds: 延迟时间（秒）

        Returns:
            字典列表格式的数据
        """
        try:
            self.log_debug(f"开始获取股票{stock_info['code']}的{period}分钟历史数据")
            # 使用股票代码获取分钟级历史数据
            hist_data = ak.stock_zh_a_hist_min_em(symbol=stock_info['code'], period=period, adjust="")
            if hist_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in hist_data.iterrows():
                result.append({
                    'code': str(stock_info['code']),
                    'name': stock_info['name'],
                    'datetime': str(row['时间']),
                    'open': float(row['开盘']),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'close': float(row['收盘']),
                    'volume': int(row.get('成交量', 0)),
                    'amount': float(row.get('成交额', 0))
                })
            self.log_info(f"成功获取股票{stock_info['code']}{period}分钟历史数据{len(result)}条")
            return result
        except Exception as e:
            self.log_error(f"获取{stock_info['code']}股票{period}分钟历史数据失败: {e}", exc_info=True)
            return []
    
    @log_data_operation('获取股票实时1分钟数据')
    def get_realtime_1min_data(self):
        """获取股票实时1分钟数据"""
        try:
            self.log_debug("开始获取所有A股实时数据")
            # 获取所有A股实时数据
            realtime_df = ak.stock_zh_a_spot_em()
            if not realtime_df.empty:
                # 重命名列以匹配标准格式
                realtime_df = realtime_df.rename(columns={
                    '代码': 'code',
                    '名称': 'name',
                    '最新价': 'close',
                    '成交量': 'volume',
                    '成交额': 'amount'
                })
                self.log_info(f"成功获取{len(realtime_df)}个股票实时数据")
            return realtime_df
        except Exception as e:
            self.log_error(f"获取股票实时1分钟数据失败: {e}", exc_info=True)
            return None
    
    @log_data_operation('获取股票日K数据')
    def get_daily_data(self, stock_info, start_date=None, end_date=None):
        """获取股票日K数据"""
        try:
            self.log_debug(f"开始获取股票{stock_info['code']}日K数据")
            # 获取股票日K线数据
            daily_data = ak.stock_zh_a_hist(symbol=stock_info['code'], period="daily", start_date=start_date, end_date=end_date, adjust="")
            if daily_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in daily_data.iterrows():
                result.append({
                    'code': str(stock_info['code']),
                    'name': stock_info['name'],
                    'datetime': str(row['日期']),
                    'open': float(row['开盘']),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'close': float(row['收盘']),
                    'volume': int(row.get('成交量', 0)),
                    'amount': float(row.get('成交额', 0))
                })
            self.log_info(f"成功获取股票{stock_info['code']}日K数据{len(result)}条")
            return result
        except Exception as e:
            self.log_error(f"获取{stock_info['code']}股票日K数据失败: {e}", exc_info=True)
            return []
    
    def _get_data_api_params(self, symbol):
        """股票API参数"""
        return {'symbol': symbol}

    @log_data_operation('分析股票分时数据')
    def analyze_intraday_tick_data(self, tick_df, big_order_threshold=500):
        """
        分析股票日内分时数据，输出全面的分析报告

        Args:
            tick_df: DataFrame格式的分时数据，包含列：时间、成交价、手数、买卖盘性质
            big_order_threshold: 大单阈值（手），默认500手

        Returns:
            dict: 包含所有分析结果的字典
        """
        try:
            self.log_info("开始分析分时数据")

            if tick_df.empty:
                self.log_warning("分时数据为空")
                return {}

            # 数据预处理
            df = tick_df.copy()

            # 确保列名统一
            column_mapping = {
                '时间': 'time',
                '成交价': 'price',
                '手数': 'volume',
                '买卖盘性质': 'direction'
            }
            df.rename(columns=column_mapping, inplace=True)

            # 转换时间格式
            if df['time'].dtype == 'object':
                df['time'] = pd.to_datetime(df['time'], format='%H:%M:%S')

            # 计算成交额
            df['amount'] = df['price'] * df['volume'] * 100  # 手转股需乘100

            # 初始化结果字典
            analysis_result = {
                'summary': {},
                'capital_flow': {},
                'key_periods': {},
                'big_orders': {},
                'price_volatility': {},
                'trading_power': {},
                'strategy_signals': {}
            }

            # 1. 资金流向分析
            analysis_result['capital_flow'] = self._analyze_capital_flow(df)

            # 2. 关键时段分析
            analysis_result['key_periods'] = self._analyze_key_periods(df)

            # 3. 大单追踪
            analysis_result['big_orders'] = self._analyze_big_orders(df, big_order_threshold)

            # 4. 价格波动分析
            analysis_result['price_volatility'] = self._analyze_price_volatility(df)

            # 5. 买卖盘力量对比
            analysis_result['trading_power'] = self._analyze_trading_power(df)

            # 6. 交易策略信号识别
            analysis_result['strategy_signals'] = self._analyze_strategy_signals(df)

            # 7. 生成综合摘要
            analysis_result['summary'] = self._generate_summary(df, analysis_result)

            self.log_info("分时数据分析完成")
            return analysis_result

        except Exception as e:
            self.log_error(f"分析分时数据失败: {e}", exc_info=True)
            return {}

    def _analyze_capital_flow(self, df):
        """资金流向分析"""
        try:
            # 分类统计
            buy_mask = df['direction'] == '买盘'
            sell_mask = df['direction'] == '卖盘'
            neutral_mask = df['direction'] == '中性盘'

            buy_amount = df[buy_mask]['amount'].sum()
            sell_amount = df[sell_mask]['amount'].sum()
            neutral_amount = df[neutral_mask]['amount'].sum()
            total_amount = df['amount'].sum()

            net_inflow = buy_amount - sell_amount

            return {
                '买盘金额': round(buy_amount, 2),
                '卖盘金额': round(sell_amount, 2),
                '中性盘金额': round(neutral_amount, 2),
                '总成交额': round(total_amount, 2),
                '净流入': round(net_inflow, 2),
                '买盘占比': round(buy_amount / total_amount * 100, 2) if total_amount > 0 else 0,
                '卖盘占比': round(sell_amount / total_amount * 100, 2) if total_amount > 0 else 0,
                '中性盘占比': round(neutral_amount / total_amount * 100, 2) if total_amount > 0 else 0,
                '买卖比': round(buy_amount / sell_amount, 2) if sell_amount > 0 else float('inf'),
                '主力动向': '主动买入' if net_inflow > 0 else '主动卖出' if net_inflow < 0 else '平衡'
            }
        except Exception as e:
            self.log_error(f"资金流向分析失败: {e}")
            return {}

    def _analyze_key_periods(self, df):
        """关键时段分析"""
        try:
            df['hour'] = df['time'].dt.hour
            df['minute'] = df['time'].dt.minute

            # 定义关键时段
            periods = {
                '开盘竞价(09:15-09:30)': (df['hour'] == 9) & (df['minute'] >= 15) & (df['minute'] < 30),
                '早盘(09:30-10:30)': (df['hour'] == 9) & (df['minute'] >= 30) | (df['hour'] == 10) & (df['minute'] < 30),
                '午盘前(10:30-11:30)': (df['hour'] == 10) & (df['minute'] >= 30) | (df['hour'] == 11) & (df['minute'] < 30),
                '午盘后(13:00-14:00)': (df['hour'] == 13),
                '下午盘(14:00-14:30)': (df['hour'] == 14) & (df['minute'] < 30),
                '尾盘(14:30-15:00)': (df['hour'] == 14) & (df['minute'] >= 30) | (df['hour'] == 15)
            }

            result = {}
            for period_name, mask in periods.items():
                period_df = df[mask]
                if not period_df.empty:
                    buy_amount = period_df[period_df['direction'] == '买盘']['amount'].sum()
                    sell_amount = period_df[period_df['direction'] == '卖盘']['amount'].sum()

                    result[period_name] = {
                        '成交笔数': len(period_df),
                        '成交额': round(period_df['amount'].sum(), 2),
                        '平均价格': round(period_df['price'].mean(), 2),
                        '价格变化': round(period_df['price'].iloc[-1] - period_df['price'].iloc[0], 2) if len(period_df) > 0 else 0,
                        '净流入': round(buy_amount - sell_amount, 2),
                        '资金流向': '流入' if buy_amount > sell_amount else '流出'
                    }

            return result
        except Exception as e:
            self.log_error(f"关键时段分析失败: {e}")
            return {}

    def _analyze_big_orders(self, df, threshold):
        """大单追踪分析"""
        try:
            big_orders = df[df['volume'] >= threshold].copy()

            if big_orders.empty:
                return {
                    '大单总数': 0,
                    '大单阈值(手)': threshold,
                    '说明': f'未发现超过{threshold}手的大单'
                }

            # 按时间排序
            big_orders = big_orders.sort_values('time')

            # 分类统计
            big_buy = big_orders[big_orders['direction'] == '买盘']
            big_sell = big_orders[big_orders['direction'] == '卖盘']

            # 提取大单明细（前10笔）
            big_orders_detail = []
            for _, row in big_orders.head(10).iterrows():
                big_orders_detail.append({
                    '时间': row['time'].strftime('%H:%M:%S'),
                    '价格': row['price'],
                    '手数': row['volume'],
                    '金额': round(row['amount'], 2),
                    '方向': row['direction']
                })

            return {
                '大单总数': len(big_orders),
                '大单阈值(手)': threshold,
                '大买单数量': len(big_buy),
                '大卖单数量': len(big_sell),
                '大买单金额': round(big_buy['amount'].sum(), 2),
                '大卖单金额': round(big_sell['amount'].sum(), 2),
                '最大单笔成交(手)': int(big_orders['volume'].max()),
                '最大单笔金额': round(big_orders['amount'].max(), 2),
                '大单明细(前10笔)': big_orders_detail,
                '大单资金流向': '流入' if len(big_buy) > len(big_sell) else '流出' if len(big_buy) < len(big_sell) else '平衡'
            }
        except Exception as e:
            self.log_error(f"大单追踪分析失败: {e}")
            return {}

    def _analyze_price_volatility(self, df):
        """价格波动分析"""
        try:
            max_price = df['price'].max()
            min_price = df['price'].min()
            open_price = df['price'].iloc[0]
            close_price = df['price'].iloc[-1]

            amplitude = max_price - min_price
            amplitude_pct = (amplitude / open_price * 100) if open_price > 0 else 0
            change = close_price - open_price
            change_pct = (change / open_price * 100) if open_price > 0 else 0

            # 计算价格标准差（波动率）
            volatility = df['price'].std()

            # 找出支撑位和压力位（价格出现频率最高的区间）
            price_counts = df['price'].value_counts().head(5)

            # 计算趋势（线性回归斜率）
            df_indexed = df.reset_index(drop=True)
            x = df_indexed.index.values
            y = df_indexed['price'].values
            if len(x) > 1:
                slope = (len(x) * (x * y).sum() - x.sum() * y.sum()) / (len(x) * (x ** 2).sum() - x.sum() ** 2)
                trend = '上涨' if slope > 0.001 else '下跌' if slope < -0.001 else '震荡'
            else:
                slope = 0
                trend = '震荡'

            return {
                '开盘价': round(open_price, 2),
                '收盘价': round(close_price, 2),
                '最高价': round(max_price, 2),
                '最低价': round(min_price, 2),
                '振幅': round(amplitude, 2),
                '振幅百分比(%)': round(amplitude_pct, 2),
                '涨跌额': round(change, 2),
                '涨跌幅(%)': round(change_pct, 2),
                '价格波动率': round(volatility, 4),
                '价格趋势': trend,
                '趋势斜率': round(slope, 6),
                '高频价格区间': [round(p, 2) for p in price_counts.index.tolist()]
            }
        except Exception as e:
            self.log_error(f"价格波动分析失败: {e}")
            return {}

    def _analyze_trading_power(self, df):
        """买卖盘力量对比分析"""
        try:
            # 连续买盘/卖盘分析
            df['direction_code'] = df['direction'].map({'买盘': 1, '卖盘': -1, '中性盘': 0})

            # 找出连续买盘
            df['is_buy'] = df['direction'] == '买盘'
            df['buy_group'] = (df['is_buy'] != df['is_buy'].shift()).cumsum()
            continuous_buy = df[df['is_buy']].groupby('buy_group').agg({
                'volume': 'sum',
                'amount': 'sum',
                'time': ['first', 'count']
            })

            # 找出连续卖盘
            df['is_sell'] = df['direction'] == '卖盘'
            df['sell_group'] = (df['is_sell'] != df['is_sell'].shift()).cumsum()
            continuous_sell = df[df['is_sell']].groupby('sell_group').agg({
                'volume': 'sum',
                'amount': 'sum',
                'time': ['first', 'count']
            })

            # 计算成交频率
            time_diff = (df['time'].max() - df['time'].min()).total_seconds() / 60  # 分钟
            trade_frequency = len(df) / time_diff if time_diff > 0 else 0

            # 统计买卖笔数
            buy_count = len(df[df['direction'] == '买盘'])
            sell_count = len(df[df['direction'] == '卖盘'])
            neutral_count = len(df[df['direction'] == '中性盘'])

            return {
                '买盘笔数': buy_count,
                '卖盘笔数': sell_count,
                '中性盘笔数': neutral_count,
                '买卖笔数比': round(buy_count / sell_count, 2) if sell_count > 0 else float('inf'),
                '最长连续买盘笔数': int(continuous_buy[('time', 'count')].max()) if not continuous_buy.empty else 0,
                '最长连续卖盘笔数': int(continuous_sell[('time', 'count')].max()) if not continuous_sell.empty else 0,
                '最大连续买盘金额': round(continuous_buy[('amount', 'sum')].max(), 2) if not continuous_buy.empty else 0,
                '最大连续卖盘金额': round(continuous_sell[('amount', 'sum')].max(), 2) if not continuous_sell.empty else 0,
                '平均成交频率(笔/分钟)': round(trade_frequency, 2),
                '盘口强弱': '买方强势' if buy_count > sell_count * 1.2 else '卖方强势' if sell_count > buy_count * 1.2 else '多空平衡'
            }
        except Exception as e:
            self.log_error(f"买卖盘力量对比分析失败: {e}")
            return {}

    def _analyze_strategy_signals(self, df):
        """交易策略信号识别"""
        try:
            signals = []

            # 1. 识别拉升/砸盘（短时间大幅波动）
            df['price_change_1min'] = df['price'].diff()

            # 拉升信号：1分钟内涨幅超过0.5%
            pullup_threshold = df['price'].iloc[0] * 0.005
            pullups = df[df['price_change_1min'] > pullup_threshold]
            if not pullups.empty:
                for _, row in pullups.head(3).iterrows():
                    signals.append({
                        '信号类型': '拉升',
                        '时间': row['time'].strftime('%H:%M:%S'),
                        '价格': round(row['price'], 2),
                        '涨幅': round(row['price_change_1min'], 2),
                        '成交量': row['volume']
                    })

            # 砸盘信号：1分钟内跌幅超过0.5%
            smash_threshold = -pullup_threshold
            smashes = df[df['price_change_1min'] < smash_threshold]
            if not smashes.empty:
                for _, row in smashes.head(3).iterrows():
                    signals.append({
                        '信号类型': '砸盘',
                        '时间': row['time'].strftime('%H:%M:%S'),
                        '价格': round(row['price'], 2),
                        '跌幅': round(row['price_change_1min'], 2),
                        '成交量': row['volume']
                    })

            # 2. 洗盘识别（价格反复震荡在某区间）
            price_std = df['price'].std()
            price_mean = df['price'].mean()
            if price_std < price_mean * 0.005:  # 波动率小于0.5%
                signals.append({
                    '信号类型': '洗盘',
                    '说明': f'价格在{round(price_mean, 2)}附近窄幅震荡，波动率仅{round(price_std, 4)}',
                    '震荡中枢': round(price_mean, 2),
                    '震荡幅度': round(price_std * 2, 2)
                })

            # 3. 对倒识别（买卖盘频繁交替）
            df['direction_change'] = df['direction'] != df['direction'].shift()
            change_rate = df['direction_change'].sum() / len(df)
            if change_rate > 0.5:  # 超过50%的记录方向发生变化
                signals.append({
                    '信号类型': '疑似对倒',
                    '说明': f'买卖盘方向频繁切换，切换率{round(change_rate * 100, 2)}%',
                    '风险提示': '可能存在人为操纵迹象'
                })

            # 4. 尾盘拉升/砸盘
            df['hour'] = df['time'].dt.hour
            df['minute'] = df['time'].dt.minute
            tail_period = df[(df['hour'] == 14) & (df['minute'] >= 30) | (df['hour'] == 15)]
            if not tail_period.empty and len(tail_period) > 1:
                tail_change = tail_period['price'].iloc[-1] - tail_period['price'].iloc[0]
                tail_change_pct = (tail_change / tail_period['price'].iloc[0] * 100) if tail_period['price'].iloc[0] > 0 else 0

                if abs(tail_change_pct) > 1:  # 尾盘波动超过1%
                    signals.append({
                        '信号类型': '尾盘拉升' if tail_change > 0 else '尾盘砸盘',
                        '时间段': '14:30-15:00',
                        '价格变化': round(tail_change, 2),
                        '变化幅度(%)': round(tail_change_pct, 2),
                        '成交量': int(tail_period['volume'].sum())
                    })

            return {
                '信号总数': len(signals),
                '信号明细': signals
            }
        except Exception as e:
            self.log_error(f"交易策略信号识别失败: {e}")
            return {}

    def _generate_summary(self, df, analysis_result):
        """生成综合摘要"""
        try:
            total_records = len(df)
            time_span = (df['time'].max() - df['time'].min()).total_seconds() / 60  # 分钟

            # 提取关键指标
            capital_flow = analysis_result.get('capital_flow', {})
            price_volatility = analysis_result.get('price_volatility', {})
            trading_power = analysis_result.get('trading_power', {})
            big_orders = analysis_result.get('big_orders', {})
            strategy_signals = analysis_result.get('strategy_signals', {})

            # 生成结论
            conclusions = []

            # 资金流向结论
            if capital_flow.get('净流入', 0) > 0:
                conclusions.append(f"资金净流入{capital_flow.get('净流入', 0):.2f}元，买盘占比{capital_flow.get('买盘占比', 0)}%")
            else:
                conclusions.append(f"资金净流出{abs(capital_flow.get('净流入', 0)):.2f}元，卖盘占比{capital_flow.get('卖盘占比', 0)}%")

            # 价格走势结论
            conclusions.append(f"价格{price_volatility.get('价格趋势', '震荡')}，涨跌幅{price_volatility.get('涨跌幅(%)', 0)}%")

            # 盘口强弱结论
            conclusions.append(f"盘口表现：{trading_power.get('盘口强弱', '未知')}")

            # 大单结论
            if big_orders.get('大单总数', 0) > 0:
                conclusions.append(f"发现{big_orders.get('大单总数', 0)}笔大单，{big_orders.get('大单资金流向', '平衡')}")

            # 信号结论
            if strategy_signals.get('信号总数', 0) > 0:
                conclusions.append(f"识别到{strategy_signals.get('信号总数', 0)}个交易信号")

            return {
                '数据记录数': total_records,
                '时间跨度(分钟)': round(time_span, 2),
                '分析时间': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                '综合结论': conclusions,
                '市场状态': self._determine_market_state(analysis_result)
            }
        except Exception as e:
            self.log_error(f"生成综合摘要失败: {e}")
            return {}

    def _determine_market_state(self, analysis_result):
        """判断市场状态"""
        try:
            capital_flow = analysis_result.get('capital_flow', {})
            price_volatility = analysis_result.get('price_volatility', {})
            trading_power = analysis_result.get('trading_power', {})

            net_inflow = capital_flow.get('净流入', 0)
            price_trend = price_volatility.get('价格趋势', '震荡')
            power_state = trading_power.get('盘口强弱', '多空平衡')

            # 综合判断
            if net_inflow > 0 and price_trend == '上涨' and '买方' in power_state:
                return '强势上涨'
            elif net_inflow < 0 and price_trend == '下跌' and '卖方' in power_state:
                return '弱势下跌'
            elif price_trend == '震荡':
                return '震荡整理'
            elif net_inflow > 0 and price_trend != '上涨':
                return '资金流入但价格未涨，可能在吸筹'
            elif net_inflow < 0 and price_trend != '下跌':
                return '资金流出但价格未跌，可能在出货'
            else:
                return '市场分歧'
        except Exception as e:
            self.log_error(f"判断市场状态失败: {e}")
            return '未知'