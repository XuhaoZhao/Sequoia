import time
import akshare as ak

futures_hist_em_df = ak.futures_hist_em(symbol="沪铜2605", period="daily")
print(futures_hist_em_df)

xx = ak.futures_hist_table_em()
print(xx)