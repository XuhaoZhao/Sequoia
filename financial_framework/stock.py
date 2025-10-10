import akshare as ak
import pandas as pd
from .financial_instruments import FinancialInstrument
from .logger_config import log_data_operation, log_method_call


class Stock(FinancialInstrument):
    """股票类"""
    
    def get_instrument_type(self):
        return "股票"
    
    @log_method_call(include_args=False)
    def get_all_instruments(self):
        """获取所有股票列表"""
        try:
            self.log_info("开始获取所有股票列表")
            stocks_df = ak.stock_info_a_code_name()
            result = [{'code': row['code'], 'name': row['name']} for _, row in stocks_df.iterrows()]
            self.log_info(f"成功获取{len(result)}个股票")
            return result
        except Exception as e:
            self.log_error(f"获取股票列表失败: {e}", exc_info=True)
            return []
    
    @log_data_operation('获取股票历史5分钟数据')
    def get_historical_5min_data(self, symbol, period="5"):
        """获取股票历史5分钟数据"""
        try:
            self.log_debug(f"开始获取股票{symbol}的5分钟历史数据")
            # 使用股票代码获取分钟级历史数据
            hist_data = ak.stock_zh_a_hist_min_em(symbol=symbol, period=period, adjust="")
            if not hist_data.empty:
                hist_data['日期时间'] = pd.to_datetime(hist_data['时间'])
                # 重命名列以匹配标准格式
                hist_data = hist_data.rename(columns={
                    '时间': '日期时间',
                    '开盘': '开盘',
                    '收盘': '收盘',
                    '最高': '最高',
                    '最低': '最低',
                    '成交量': '成交量',
                    '成交额': '成交额'
                })
                self.log_info(f"成功获取股票{symbol}历史数据{len(hist_data)}条")
            return hist_data
        except Exception as e:
            self.log_error(f"获取{symbol}股票历史5分钟数据失败: {e}", exc_info=True)
            return None
    
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
    def get_daily_data(self, symbol, start_date=None, end_date=None):
        """获取股票日K数据"""
        try:
            self.log_debug(f"开始获取股票{symbol}日K数据")
            # 获取股票日K线数据
            daily_data = ak.stock_zh_a_hist(symbol=symbol, period="daily", start_date=start_date, end_date=end_date, adjust="")
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
                self.log_info(f"成功获取股票{symbol}日K数据{len(daily_data)}条")
            return daily_data
        except Exception as e:
            self.log_error(f"获取{symbol}股票日K数据失败: {e}", exc_info=True)
            return None
    
    def _get_data_api_params(self, symbol):
        """股票API参数"""
        return {'symbol': symbol}