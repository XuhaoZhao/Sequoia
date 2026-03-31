目的：期货5min数据收集，
    收集获取方法 
    import akshare as ak
    futures_zh_minute_sina_df = ak.futures_zh_minute_sina(symbol="RB0", period="1")
    print(futures_zh_minute_sina_df)
    1、从db_manager.py futures_contracts表获取所有的期货列表，参考_fix_all_contract_codes方法，转换正确的合约代码 
    2、遍历调用futures_zh_minute_sina 获取数据
    3、存储，db_manager.py期货5min数据存储表，要按月分表 存储数据的时候要注意定时任务有可能重跑，做好数据的去重。
 先新增一个main 函数，5min数据收集作为一个定时任务，只要main函数启动，定时任务就注册上去，每天凌晨5点开始运行，参考另一个main.py 保证任务能准时运行