import akshare as ak
import pandas as pd
from datetime import datetime, timedelta
from .financial_instruments import FinancialInstrument


class Index(FinancialInstrument):
    """指数类"""
    
    def get_instrument_type(self):
        return "指数"
    
    def get_all_instruments(self):
        """获取所有指数列表"""
        try:
            boards_df = ak.index_csindex_all()
            return [{'code': row['指数代码'], 'name': row['指数简称']} for _, row in boards_df.iterrows()]
        except Exception as e:
            print(f"获取概指数列表列表失败: {e}")
            return []
    
    def get_historical_5min_data(self, symbol, period="5"):
        """获取指数历史5分钟数据"""
        try:
            # 设置结束时间为当前时间，开始时间为两个月前
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)
            
            # 格式化为API需要的字符串格式
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")
            
            hist_data = ak.index_zh_a_hist_min_em(symbol=symbol, period=period, start_date=start_date_str, end_date=end_date_str)
            print(hist_data)
            if not hist_data.empty:
                hist_data['日期时间'] = pd.to_datetime(hist_data['时间'])
                hist_data = hist_data.rename(columns={
                    '时间': '日期时间',
                    '开盘': '开盘',
                    '收盘': '收盘',
                    '最高': '最高',
                    '最低': '最低',
                    '成交量': '成交量',
                    '成交额': '成交额'
                })
            return hist_data
        except Exception as e:
            print(f"获取{symbol}指数历史5分钟数据失败: {e}")
            return None
    
    def get_realtime_1min_data(self):
        """获取指数实时1分钟数据"""
        try:
            realtime_df = ak.stock_zh_index_spot_em(symbol="中证系列指数")
            if not realtime_df.empty:
                realtime_df = realtime_df.rename(columns={
                    '代码': 'code',
                    '名称': 'name',
                    '最新价': 'close',
                    '成交量': 'volume',
                    '成交额': 'amount'
                })
            return realtime_df
        except Exception as e:
            print(f"获取指数实时1分钟数据失败: {e}")
            return None
    
    def get_daily_data(self, symbol, start_date=None, end_date=None):
        """获取指数日K数据"""
        try:
            daily_data = ak.stock_zh_index_daily_em(symbol=symbol, start_date=start_date, end_date=end_date)
            if not daily_data.empty:
                daily_data['日期时间'] = pd.to_datetime(daily_data['日期'])
                daily_data = daily_data.rename(columns={
                    '日期': '日期时间',
                    '开盘': '开盘',
                    '收盘': '收盘',
                    '最高': '最高',
                    '最低': '最低',
                    '成交量': '成交量',
                    '成交额': '成交额'
                })
            return daily_data
        except Exception as e:
            print(f"获取{symbol}指数日K数据失败: {e}")
            return None
    
    def _get_data_api_params(self, symbol):
        """指数API参数"""
        return {'symbol': symbol}