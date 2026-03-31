# import time
# import akshare as ak

# dce_text = ak.match_main_contract(symbol="dce")
# czce_text = ak.match_main_contract(symbol="czce")
# shfe_text = ak.match_main_contract(symbol="shfe")
# gfex_text = ak.match_main_contract(symbol="gfex")

# while True:
#     time.sleep(3)
#     futures_zh_spot_df = ak.futures_zh_spot(
#         symbol=",".join([dce_text, czce_text, shfe_text, gfex_text]),
#         market="CF",
#         adjust='0')
#     print(futures_zh_spot_df)

import akshare as ak

futures_comm_info_df = ak.futures_comm_info(symbol="所有")
print(futures_comm_info_df)


import akshare as ak

futures_zh_minute_sina_df = ak.futures_zh_minute_sina(symbol="RB0", period="1")
print(futures_zh_minute_sina_df)