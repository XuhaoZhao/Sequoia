import akshare as ak
from .financial_instruments import FinancialInstrument
from .logger_config import log_method_call
from financial_framework.file_path_generator import (
    FilePathGenerator,
    generate_etf_data_path,
    generate_stock_data_path,
    generate_industry_data_path,
    generate_concept_data_path
)
import pandas as pd
class ETF(FinancialInstrument):
    """ETF类"""

    # 获取5分钟历史数据的延迟时间（秒），防止被封禁IP
    delay_seconds = 80

    def get_instrument_type(self):
        return "ETF"
    
    @log_method_call(include_args=False)
    def get_all_instruments(self):
        """获取所有ETF列表"""
        try:
            
            self.log_info("开始获取所有ETF列表")
            # 获取ETF数据文件路径
            etf_path = generate_etf_data_path()
            self.log_info(f"读取ETF数据文件: {etf_path}")

            # 读取CSV文件
            df = pd.read_csv(etf_path)

            # 从CSV文件中获取股票名称和代码
            result = []
            for _, row in df.iterrows():
                etf_info = {
                    'code': str(row['SECURITY_CODE']),
                    'name': str(row['SECURITY_SHORT_NAME'])
                }
                result.append(etf_info)

            self.log_info(f"成功获取{len(result)}个ETF")
            return result
        except Exception as e:
            self.log_error(f"获取ETF列表失败: {e}", exc_info=True)
            return []
    
    def get_historical_min_data(self, etf_info, period="5", delay_seconds=1.0):
        """获取ETF历史分时数据

        Args:
            etf_info: ETF信息字典（包含 code 和 name）
            period: 数据周期（"1", "5", "15", "30", "60"等，单位：分钟）
            delay_seconds: 延迟时间（秒）

        Returns:
            字典列表格式的数据
        """
        try:
            hist_data = ak.fund_etf_hist_min_em(symbol=etf_info['code'], period=period)
            if hist_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in hist_data.iterrows():
                result.append({
                    'code': str(etf_info['code']),
                    'name': etf_info['name'],
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
            print(f"获取{etf_info['name']}ETF{period}分钟历史数据失败: {e}")
            return []
    
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
    
    def get_daily_data(self, etf_info, start_date=None, end_date=None):
        """获取ETF日K数据"""
        try:
            daily_data = ak.fund_etf_hist_em(symbol=etf_info['code'], period="daily", start_date=start_date, end_date=end_date)
            if daily_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in daily_data.iterrows():
                result.append({
                    'code': str(etf_info['code']),
                    'name': etf_info['name'],
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
            print(f"获取{etf_info['name']}ETF日K数据失败: {e}")
            return []
    
    def _get_data_api_params(self, symbol):
        """ETF API参数"""
        return {'symbol': symbol}