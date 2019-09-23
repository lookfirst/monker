#!/usr/bin/env python3
import requests, hashlib, hmac, time, base64
import json, urllib, pymongo, websocket, threading

from ..monker.orderbook import OrderBook

class Binance(OrderBook):
    key    = 'O9YNhkjieyoDY1xO0oLPcr4LIbTwv1xOBqisQbRnRNvuBlatj3zeKzZgcWxMtSMD'
    secret = 'Rzl9apnCoqJr6rKslB9pFnCsBRqFWeYgyVS9naDOckjaThb1fO2PkzqD1eTvbY9n'
    url    = 'https://api.binance.com'

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            'Accept'       : 'application/json',
            'User-Agent'   : 'monker',
            'X-MBX-APIKEY' : self.key,
        })
        self.is_stop_thread = threading.Event()
        self.thread = threading.Thread(target=self.thread_entry_point)

    def sign(self, **params):
        q = urllib.parse.urlencode(params)
        m = hmac.new(self.secret.encode(), q.encode(), hashlib.sha256)
        return m.hexdigest()

    def get(self, uri, is_signed=False, **params):
        if is_signed:
            params['timestamp'] = int(time.time() * 1000)
            params['signature'] = self.sign(**params)
        params = urllib.parse.urlencode(params)
        return self.s.get(self.url + uri, params=params)

    def stop_thread(self):
        self.is_stop_thread.set()

    def start_thread(self):
        self.thread.start()

    def thread_entry_point(self):
        #client = pymongo.MongoClient('mongodb://localhost:27017/')
        #db = client.monker
        #ws = websocket.create_connection("wss://stream.binance.com:9443/ws/ethbtc@depth@100ms")
        while not self.is_stop_thread.is_set():
            result = ws.recv()
            obj = json.loads(result)
            print(obj)
            obj = {
                '_localtime': time.time(),
                'obj' : obj,
            }
            db.binance.insert_one(obj)
        client.close()


if __name__ == '__main__':
    b = Binance()
    #r = b.get('/api/v3/allOrders', is_signed=True, symbol="LTCBTC")
    #print(r.json())
    b.listen()

