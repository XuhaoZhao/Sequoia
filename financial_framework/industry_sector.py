import akshare as ak
import pandas as pd
from .financial_instruments import FinancialInstrument


class IndustrySector(FinancialInstrument):
    """行业板块类"""
    
    def get_instrument_type(self):
        return "行业板块"
    
    def get_all_instruments(self):
        """获取所有行业板块列表"""
        try:
            boards_df = ak.stock_board_industry_name_em()
            return [{'code': row['板块代码'], 'name': row['板块名称']} for _, row in boards_df.iterrows()]
        except Exception as e:
            print(f"获取行业板块列表失败: {e}")
            return []
    
    def get_historical_5min_data(self, symbol, period="5"):
        """获取行业板块历史5分钟数据"""
        try:
            hist_data = ak.stock_board_industry_hist_min_em(symbol=symbol, period=period)
            if not hist_data.empty:
                hist_data['日期时间'] = pd.to_datetime(hist_data['日期时间'])
            return hist_data
        except Exception as e:
            print(f"获取{symbol}行业板块历史5分钟数据失败: {e}")
            return None
    
    def get_realtime_1min_data(self):
        """获取行业板块实时1分钟数据"""
        try:
            realtime_df = ak.stock_board_industry_name_em()
            if not realtime_df.empty:
                realtime_df = realtime_df.rename(columns={
                    '板块代码': 'code',
                    '板块名称': 'name',
                    '最新价': 'close',
                    '成交量': 'volume',
                    '成交额': 'amount'
                })
            return realtime_df
        except Exception as e:
            print(f"获取行业板块实时1分钟数据失败: {e}")
            return None
    
    def get_daily_data(self, symbol, start_date=None, end_date=None):
        """获取行业板块日K数据"""
        try:
            # 行业板块暂无直接日K数据接口，可以通过分钟数据聚合
            print(f"行业板块{symbol}暂不支持直接获取日K数据")
            return None
        except Exception as e:
            print(f"获取{symbol}行业板块日K数据失败: {e}")
            return None
    
    def _get_data_api_params(self, symbol):
        """行业板块API参数"""
        return {'symbol': symbol}