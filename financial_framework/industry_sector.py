import akshare as ak
from .financial_instruments import FinancialInstrument


class IndustrySector(FinancialInstrument):
    """行业板块类"""

    # 获取5分钟历史数据的延迟时间（秒），防止被封禁IP
    delay_seconds = 80

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
    
    def get_historical_min_data(self, board_info, period="5", delay_seconds=1.0):
        """获取行业板块历史分时数据

        Args:
            board_info: 板块信息字典（包含 code 和 name）
            period: 数据周期（"1", "5", "15", "30", "60"等，单位：分钟）
            delay_seconds: 延迟时间（秒）

        Returns:
            字典列表格式的数据
        """
        try:
            hist_data = ak.stock_board_industry_hist_min_em(symbol=board_info['name'], period=period)
            if hist_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in hist_data.iterrows():
                result.append({
                    'code': str(board_info['code']),
                    'name': board_info['name'],
                    'datetime': str(row['日期时间']),
                    'open': float(row['开盘']),
                    'high': float(row['最高']),
                    'low': float(row['最低']),
                    'close': float(row['收盘']),
                    'volume': int(row.get('成交量', 0)),
                    'amount': float(row.get('成交额', 0))
                })
            return result
        except Exception as e:
            print(f"获取{board_info['name']}行业板块{period}分钟历史数据失败: {e}")
            return []
    
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
    
    def get_daily_data(self, board_info, start_date=None, end_date=None):
        """获取行业板块日K数据"""
        try:
            # 行业板块暂无直接日K数据接口
            print(f"行业板块{board_info['name']}暂不支持直接获取日K数据")
            return []
        except Exception as e:
            print(f"获取{board_info['name']}行业板块日K数据失败: {e}")
            return []
    
    def _get_data_api_params(self, symbol):
        """行业板块API参数"""
        return {'symbol': symbol}