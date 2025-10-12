import akshare as ak
from .financial_instruments import FinancialInstrument
from .logger_config import log_data_operation, log_method_call


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
            stocks_df = ak.stock_info_a_code_name()
            result = [{'code': row['code'], 'name': row['name']} for _, row in stocks_df.iterrows()]
            self.log_info(f"成功获取{len(result)}个股票")
            return result
        except Exception as e:
            self.log_error(f"获取股票列表失败: {e}", exc_info=True)
            return []
    
    @log_data_operation('获取股票历史5分钟数据')
    def get_historical_5min_data(self, stock_info, period="5"):
        """获取股票历史5分钟数据"""
        try:
            self.log_debug(f"开始获取股票{stock_info['code']}的5分钟历史数据")
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
            self.log_info(f"成功获取股票{stock_info['code']}历史数据{len(result)}条")
            return result
        except Exception as e:
            self.log_error(f"获取{stock_info['code']}股票历史5分钟数据失败: {e}", exc_info=True)
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