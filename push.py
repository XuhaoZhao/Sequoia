# -*- encoding: UTF-8 -*-

import logging
import settings
from wxpusher import WxPusher


def push(msg):
    if settings.config['push']['enable']:
        print("jushsu")
        response = WxPusher.send_message(msg, uids=["UID_kVnwJ9qtNjtHQE62c575olKwi1LL"],
                                         token="AT_Yx4jtKrc2UqVwQ7a5yidYeTP36oZaMML")
        print(response)
        
    logging.info(msg)


def statistics(msg=None):
    push(msg)


def strategy(msg=None):
    if msg is None or not msg:
        msg = '今日没有符合条件的股票'
    push(msg)
