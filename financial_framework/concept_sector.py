import akshare as ak
from .financial_instruments import FinancialInstrument
from datetime import datetime, timedelta

class ConceptSector(FinancialInstrument):
    """概念板块类"""

    # 获取5分钟历史数据的延迟时间（秒），防止被封禁IP
    delay_seconds = 80

    def get_instrument_type(self):
        return "概念板块"
    
    def get_all_instruments(self):
        """获取所有概念板块列表"""
        try:
            boards_df = ak.stock_board_concept_name_em()
            return [{'code': row['板块代码'], 'name': row['板块名称']} for _, row in boards_df.iterrows()]
        except Exception as e:
            print(f"获取概念板块列表失败: {e}")
            return []
    
    def get_historical_5min_data(self, board_info, period="5"):
        """获取概念板块历史5分钟数据"""
        try:
            hist_data = ak.stock_board_concept_hist_min_em(symbol=board_info['name'], period=period)
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
            print(f"获取{board_info['name']}概念板块历史5分钟数据失败: {e}")
            return []
    
    def get_realtime_1min_data(self):
        """获取概念板块实时1分钟数据"""
        try:
            realtime_df = ak.stock_board_concept_name_em()
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
            print(f"获取概念板块实时1分钟数据失败: {e}")
            return None
    
    def get_daily_data(self, board_info, start_date=None, end_date=None):
        """获取概念板块日K数据"""
        try:
            # 设置默认时间范围：结束时间为当前时间，开始时间为30天前
            if start_date is None or end_date is None:
                end_date = datetime.now()
                start_date = end_date - timedelta(days=30)
                # 格式化为API需要的字符串格式
                start_date = start_date.strftime("%Y-%m-%d")
                end_date = end_date.strftime("%Y-%m-%d")
            hist_data = ak.stock_board_concept_hist_em(board_info['name'], period="daily", start_date=start_date, end_date=end_date, adjust="")
            if hist_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in hist_data.iterrows():
                result.append({
                    'code': str(board_info['code']),
                    'name': board_info['name'],
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
            print(f"获取{board_info['name']}概念板块日K数据失败: {e}")
            return []
    
    def _get_data_api_params(self, symbol):
        """概念板块API参数"""
        return {'symbol': symbol}