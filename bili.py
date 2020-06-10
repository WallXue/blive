# -*- encoding: utf-8 -*-

import json
import struct
import time
import traceback
import zlib
import sqlite3

from collections import namedtuple
from threading import Thread

import requests
import websocket


def get_sql_conn():
    conn = sqlite3.connect('danmu.sqlite')
    return conn


def save_welcome(msg, roomid):
    with get_sql_conn() as conn:
        data = msg.get('data')
        conn.cursor()
        conn.execute("insert into danmu_welcome(uid, uname, roomid) values(?, ?, ?)",
                     (data.get('uid'), data.get('uname'), roomid))
        conn.commit()


def save_danmu(msg, roomid):
    with get_sql_conn() as conn:
        info = msg.get('info')
        conn.cursor()
        conn.execute("INSERT INTO danmu ( uid, uname, danmu, send_time, roomid) VALUES (?,?,?,?,?);",
                     (info[2][0], info[2][1], info[1], info[9]['ts'], roomid))
        conn.commit()


def save_gift(msg, roomid):
    with get_sql_conn() as conn:
        data = msg.get('data')
        conn.cursor()
        conn.execute(
            "INSERT INTO danmu_gift ( uid, uname, giftname, giftid, coin_type, price, num, total_coin, gift_time, roomid) VALUES ( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            (data.get('uid'), data.get('uname'), data.get('giftName'), data.get('giftId'), data.get('coin_type'),
             data.get('price'), data.get('num'), data.get('total_coin'), data.get('timestamp'), roomid))
        conn.commit()


def save_guard(msg, roomid):
    with get_sql_conn() as conn:
        data = msg.get('data')
        conn.cursor()
        conn.execute(
            "INSERT INTO danmu_guard (uid, uname, guard_level, num, price, gift_id, gift_name, start_time, end_time, roomid) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);",
            (data.get('uid'), data.get('username'), data.get('guard_level'), data.get('num'), data.get('price'),
             data.get('gift_id'), data.get('gift_name'), data.get('start_time'), data.get('end_time'), roomid))
        conn.commit()


class BLiveDMClient:
    def __init__(self, roomid, key, url):
        self.roomid = roomid
        self.key = key
        self.url = url
        self.ws = None
        self.loop_thread = None

    HEADER_STRUCT = struct.Struct('>I2H2I')
    HEADER_TUPLE = namedtuple('HeaderTuple', ('pack_len', 'raw_header_size', 'ver', 'operation', 'seq_id'))

    def __on_data(self):
        def _data(ws, message, c, d):
            self.__unpack(message)

        return _data

    def __on_error(self):
        def _error(ws, error):
            print(error)

        return _error

    def __on_close(self):
        self.__stop_heartbeat()

        def _close(ws):
            print("### closed ###")

        return _close

    def __on_open(self):
        def _open(ws):
            auth_params = {
                'uid': 0,
                'roomid': self.roomid,
                'protover': 2,
                'platform': 'web',
                'clientver': '1.8.2',
                'type': 2,
                'key': self.key
            }
            packet = self.__pack(
                auth_params,
                7)
            ws.send(packet)

        return _open

    def __pack(self, payload, operation=2):
        body = json.dumps(payload).encode('utf-8')
        header = self.HEADER_STRUCT.pack(
            self.HEADER_STRUCT.size + len(body),
            self.HEADER_STRUCT.size,
            1,
            operation,
            1
        )
        return header + body

    def __unpack(self, data):
        offset = 0
        while offset < len(data):
            try:
                header = self.HEADER_TUPLE(*self.HEADER_STRUCT.unpack_from(data, offset))
            except struct.error:
                break
            print('header:', header)
            if header.operation == 8:
                self.__loop_heartbeat()
                return
            if header.pack_len == 20:
                return
            body = data[offset + self.HEADER_STRUCT.size: offset + header.pack_len]
            if header.ver == 2:
                body = zlib.decompress(body)
                self.__unpack(body)
                return
            try:
                b = body.decode('utf-8')
                msg = json.loads(b)
                print('body:', b)
                cmd = msg.get('cmd')
                if cmd == 'WELCOME':
                    save_welcome(msg, self.roomid)
                    pass
                elif cmd == 'DANMU_MSG':
                    save_danmu(msg, self.roomid)
                    pass
                elif cmd == 'SEND_GIFT':
                    save_gift(msg, self.roomid)
                    pass
                elif cmd == 'GUARD_BUY':
                    save_guard(msg, self.roomid)
                    pass
                else:
                    print('未知消息类型:', cmd)
            except Exception as e:
                print('异常', e)
                traceback.print_exc()
            offset += header.pack_len

    def __loop_heartbeat(self):
        self.__stop_heartbeat()

        def run(*args):
            while 1:
                print('发送心跳数据...')
                packet = self.__pack(
                    {},
                    2)
                self.ws.send(packet)
                time.sleep(30)

        self.loop_thread = Thread(target=run)
        self.loop_thread.start()

    def __stop_heartbeat(self):
        if self.loop_thread and self.loop_thread.isAlive():
            self.loop_thread.stop()

    def connect(self):
        def _c():
            websocket.enableTrace(True)
            self.ws = websocket.WebSocketApp(self.url,
                                             on_data=self.__on_data(),
                                             on_error=self.__on_error(),
                                             on_close=self.__on_close())
            self.ws.on_open = self.__on_open()
            self.ws.run_forever()

        t = Thread(target=_c)
        t.start()
        t.join()


def start(roomid):
    while True:
        print('正在连接弹幕服务器...')
        c = json.loads(requests.get(
            str(
                'https://api.live.bilibili.com/room/v1/Danmu/getConf?room_id=' + str(
                    roomid) + '&platform=pc&player=web')).text)
        print(c)
        key = c.get('data').get('token')
        host = c.get('data').get('host_server_list')[0].get('host')
        c = BLiveDMClient(roomid=roomid,
                          key=key,
                          url='wss://' + host + '/sub')
        c.connect()
        print('连接已断开，10秒后重连...')
        time.sleep(10)


if __name__ == "__main__":
    start(5414666)
