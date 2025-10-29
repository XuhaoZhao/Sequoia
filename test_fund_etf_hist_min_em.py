import unittest
from unittest.mock import patch, Mock
import pandas as pd
import sys
import os

# 添加项目根目录到路径
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from rewrite_ak_share.rewrite_fund_etf_em import fund_etf_hist_min_em
from db_manager import IndustryDataDB


class TestFundEtfHistMinEm(unittest.TestCase):
    """测试fund_etf_hist_min_em函数"""

    def setUp(self):
        """测试前准备"""
        self.db_manager = IndustryDataDB("industry_data.db")

        # 从数据库获取ETF信息并构建code_id_dict
        self.code_id_dict = self.db_manager.get_etf_info()

    @patch('rewrite_ak_share.rewrite_fund_etf_em.requests.get')
    def test_fund_etf_hist_min_em_1min(self, mock_get):
        """测试1分钟数据获取"""
        # 模拟API响应
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {
                "trends": [
                    "2024-01-15 09:30:00,1.000,1.010,1.015,0.995,1000000,1010000,1.005",
                    "2024-01-15 09:31:00,1.010,1.020,1.025,1.005,2000000,2020000,1.015"
                ]
            }
        }
        mock_get.return_value = mock_response

        # 使用setUp中已经获取的ETF代码进行测试
        if self.code_id_dict:
            test_symbol = list(self.code_id_dict.keys())[0]

            result = fund_etf_hist_min_em(
                symbol=test_symbol,
                start_date="2024-01-15 09:30:00",
                end_date="2024-01-15 09:35:00",
                period="1",
                code_id_dict=self.code_id_dict
            )

            self.assertIsInstance(result, pd.DataFrame)
            self.assertGreater(len(result), 0)
            self.assertIn("时间", result.columns)
            self.assertIn("开盘", result.columns)
            self.assertIn("收盘", result.columns)

            # 验证API调用参数
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            expected_secid = f"{self.code_id_dict[test_symbol]}.{test_symbol}"
            self.assertEqual(call_args[1]["params"]["secid"], expected_secid)

    @patch('rewrite_ak_share.rewrite_fund_etf_em.requests.get')
    def test_fund_etf_hist_min_em_5min(self, mock_get):
        """测试5分钟数据获取"""
        # 模拟API响应
        mock_response = Mock()
        mock_response.json.return_value = {
            "data": {
                "klines": [
                    "2024-01-15 09:30:00,1.000,1.010,1.015,0.995,1000000,1010000,2.0,1.0,0.010,0.5",
                    "2024-01-15 09:35:00,1.010,1.020,1.025,1.005,2000000,2020000,2.0,1.0,0.010,1.0"
                ]
            }
        }
        mock_get.return_value = mock_response

        # 使用setUp中已经获取的ETF代码进行测试
        if self.code_id_dict:
            test_symbol = list(self.code_id_dict.keys())[0]

            result = fund_etf_hist_min_em(
                symbol=test_symbol,
                start_date="2024-01-15 09:30:00",
                end_date="2024-01-15 09:40:00",
                period="5",
                code_id_dict=self.code_id_dict
            )

            self.assertIsInstance(result, pd.DataFrame)
            self.assertGreater(len(result), 0)
            expected_columns = [
                "时间", "开盘", "收盘", "最高", "最低", "涨跌幅", "涨跌额",
                "成交量", "成交额", "振幅", "换手率"
            ]
            for col in expected_columns:
                self.assertIn(col, result.columns)

            # 验证API调用参数
            mock_get.assert_called_once()
            call_args = mock_get.call_args
            self.assertEqual(call_args[1]["params"]["klt"], "5")
            expected_secid = f"{self.code_id_dict[test_symbol]}.{test_symbol}"
            self.assertEqual(call_args[1]["params"]["secid"], expected_secid)

    def test_code_id_dict_from_database(self):
        """测试从数据库构建的code_id_dict"""
        if self.code_id_dict:
            # 验证至少有一个ETF代码
            self.assertGreater(len(self.code_id_dict), 0)

            # 验证代码格式
            for code, market_id in self.code_id_dict.items():
                self.assertIn(market_id, ['0', '1'])
                self.assertTrue(code.isdigit())
                self.assertGreater(len(code), 0)


if __name__ == '__main__':
    unittest.main(verbosity=2)