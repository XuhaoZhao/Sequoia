import akshare as ak
from datetime import datetime, timedelta
from .financial_instruments import FinancialInstrument


class Index(FinancialInstrument):
    """指数类"""

    # 获取5分钟历史数据的延迟时间（秒），防止被封禁IP
    delay_seconds = 130

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
    
    def get_historical_5min_data(self, index_info, period="5"):
        """获取指数历史5分钟数据"""
        try:
            # 设置结束时间为当前时间，开始时间为两个月前
            end_date = datetime.now()
            start_date = end_date - timedelta(days=30)

            # 格式化为API需要的字符串格式
            start_date_str = start_date.strftime("%Y-%m-%d %H:%M:%S")
            end_date_str = end_date.strftime("%Y-%m-%d %H:%M:%S")

            hist_data = ak.index_zh_a_hist_min_em(symbol=index_info['code'], period=period, start_date=start_date_str, end_date=end_date_str)
            if hist_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in hist_data.iterrows():
                result.append({
                    'code': str(index_info['code']),
                    'name': index_info['name'],
                    'datetime': str(row['时间']),
                    'open': float(row['开盘']),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'close': float(row['收盘']),
                    'volume': int(row.get('成交量', 0)),
                    'amount': float(row.get('成交额', 0))
                })
            return result
        except Exception as e:
            print(f"获取{index_info['name']}指数历史5分钟数据失败: {e}")
            return []
    
    def get_realtime_1min_data(self):
        """获取指数实时1分钟数据"""
        try:
            realtime_df = ak.stock_zh_index_spot_sina()
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
    
    def get_daily_data(self, index_info, start_date=None, end_date=None):
        """获取指数日K数据"""
        try:
            # 设置默认时间范围：结束时间为当前时间，开始时间为30天前
            if start_date is None or end_date is None:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                # 格式化为API需要的字符串格式
                start_date = start_date.strftime("%Y%m%d")
                end_date = end_date.strftime("%Y%m%d")

            daily_data = ak.stock_zh_index_daily_em(symbol=index_info['code'], start_date=start_date, end_date=end_date)
            if daily_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in daily_data.iterrows():
                result.append({
                    'code': str(index_info['code']),
                    'name': index_info['name'],
                    'datetime': str(row['日期']),
                    'open': float(row['开盘']),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'close': float(row['收盘']),
                    'volume': int(row.get('成交量', 0)),
                    'amount': float(row.get('成交额', 0))
                })
            return result
        except Exception as e:
            print(f"获取{index_info['name']}指数日K数据失败: {e}")
            return []
    
    def _get_data_api_params(self, symbol):
        """指数API参数"""
        return {'symbol': symbol}
