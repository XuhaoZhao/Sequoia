import akshare as ak
import adata as ad
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
from datetime import datetime, timedelta
from rewrite_ak_share.rewrite_fund_etf_em import fund_etf_hist_min_em
class ETF(FinancialInstrument):
    """ETF类"""

    # 获取5分钟历史数据的延迟时间（秒），防止被封禁IP
    delay_seconds = 80

    def get_instrument_type(self):
        return "etf"
    
    @log_method_call(include_args=False)
    def get_all_instruments(self):
        """获取所有ETF列表"""
        try:
            
            self.log_info("开始获取所有ETF列表")
            # 获取ETF数据文件路径
            etf_path = generate_etf_data_path()
            self.log_info(f"读取ETF数据文件: {etf_path}")

            # 读取CSV文件，将SECURITY_CODE列作为字符串读取以保持前导零
            df = pd.read_csv(etf_path, dtype={'SECURITY_CODE': str})

            # 从CSV文件中获取股票名称和代码
            result = []
            for _, row in df.iterrows():
                etf_info = {
                    'code': str(row['SECURITY_CODE']).zfill(6),  # 确保代码为6位，补齐前导零
                    'name': str(row['SECURITY_SHORT_NAME'])
                }
                result.append(etf_info)

            self.log_info(f"成功获取{len(result)}个ETF")
            return result
        except Exception as e:
            self.log_error(f"获取ETF列表失败: {e}", exc_info=True)
            return []
    
    def get_historical_min_data(self, etf_info, period="5", delay_seconds=1.0, start_date=None, end_date=None):
        """获取ETF历史分时数据

        Args:
            etf_info: ETF信息字典（包含 code 和 name）
            period: 数据周期（"1", "5", "15", "30", "60"等，单位：分钟）
            delay_seconds: 延迟时间（秒）
            start_date: 开始日期，格式 "2024-03-20 09:30:00"，如果为None则自动计算为一个月前
            end_date: 结束日期，格式 "2024-03-20 17:40:00"，如果为None则使用当前时间

        Returns:
            字典列表格式的数据
        """
        try:
            # 如果未指定日期范围，则自动计算最近一个月
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            if start_date is None:
                # 前推一个月
                one_month_ago = datetime.now() - timedelta(days=30)
                start_date = one_month_ago.strftime("%Y-%m-%d %H:%M:%S")

            self.log_info(f"获取{etf_info['name']}的{period}分钟数据，时间范围: {start_date} 至 {end_date}")
            code_id_dict = self.db.get_etf_info()
            hist_data = fund_etf_hist_min_em(
                symbol=etf_info['code'],
                period=period,
                adjust='qfq',
                start_date=start_date,
                end_date=end_date,
                code_id_dict = code_id_dict
            )

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
            self.log_error(f"获取{etf_info['name']}ETF{period}分钟历史数据失败: {e}", exc_info=True)
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
        """获取ETF日K数据

        Args:
            etf_info: ETF信息字典（包含 code 和 name）
            start_date: 开始日期，格式 "2025-01-01"，如果为None则自动计算为当前日期前推250个交易日（约350天）
            end_date: 结束日期，格式 "2025-01-01"，如果为None则使用当前日期

        Returns:
            字典列表格式的数据
        """
        try:
            # 如果未指定日期范围，则自动计算
            if end_date is None:
                end_date = datetime.now().strftime("%Y-%m-%d")

            if start_date is None:
                # 250个交易日大约是350个自然日（考虑周末和节假日）
                days_ago = datetime.now() - timedelta(days=350)
                start_date = days_ago.strftime("%Y-%m-%d")

            self.log_info(f"获取{etf_info['name']}的日K数据，时间范围: {start_date} 至 {end_date}")

            # 使用新的ad.fund.market.get_market_etf API
            # 参数: fund_code, period(1=日线), start_date, end_date
            daily_data = ad.fund.market.get_market_etf(
                etf_info['code'],
                1,  # 1表示日线
                start_date,
                end_date
            )

            if daily_data.empty:
                return []

            # 转换为标准格式的字典列表
            result = []
            for _, row in daily_data.iterrows():
                result.append({
                    'code': str(etf_info['code']),
                    'name': etf_info['name'],
                    'datetime': str(row['trade_date']),  # 使用 trade_date 列
                    'open': float(row['open']),
                    'high': float(row['high']),
                    'low': float(row['low']),
                    'close': float(row['close']),
                    'volume': int(row.get('volume', 0)),
                    'amount': float(row.get('amount', 0))
                })
            return result
        except Exception as e:
            self.log_error(f"获取{etf_info['name']}ETF日K数据失败: {e}", exc_info=True)
            return []
    
    def _get_data_api_params(self, symbol):
        """ETF API参数"""
        return {'symbol': symbol}