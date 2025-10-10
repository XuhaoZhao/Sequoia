import akshare as ak
import pandas as pd
from .financial_instruments import FinancialInstrument
from .logger_config import log_data_operation, log_method_call


class ETF(FinancialInstrument):
    """ETF类"""
    
    def get_instrument_type(self):
        return "ETF"
    
    @log_method_call(include_args=False)
    def get_all_instruments(self):
        """获取所有ETF列表"""
        try:
            self.log_info("开始获取所有ETF列表")
            etf_df = ak.fund_etf_spot_em()
            result = [{'code': row['代码'], 'name': row['名称']} for _, row in etf_df.iterrows()]
            self.log_info(f"成功获取{len(result)}个ETF")
            return result
        except Exception as e:
            self.log_error(f"获取ETF列表失败: {e}", exc_info=True)
            return []
    
    def get_historical_5min_data(self, symbol, period="5"):
        """获取ETF历史5分钟数据"""
        try:
            hist_data = ak.fund_etf_hist_min_em(symbol=symbol, period=period)
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
            print(f"获取{symbol}ETF历史5分钟数据失败: {e}")
            return None
    
    def get_realtime_1min_data(self):
        """获取ETF实时1分钟数据"""
        try:
            realtime_df = ak.fund_etf_spot_em()
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
            print(f"获取ETF实时1分钟数据失败: {e}")
            return None
    
    def get_daily_data(self, symbol, start_date=None, end_date=None):
        """获取ETF日K数据"""
        try:
            daily_data = ak.fund_etf_hist_em(symbol=symbol, period="daily", start_date=start_date, end_date=end_date)
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
            print(f"获取{symbol}ETF日K数据失败: {e}")
            return None
    
    def _get_data_api_params(self, symbol):
        """ETF API参数"""
        return {'symbol': symbol}