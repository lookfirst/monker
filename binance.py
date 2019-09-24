import requests, hashlib, hmac, time, base64
import json, urllib, pymongo, websocket, threading

from orderbook import OrderBook, Order

class Binance(OrderBook):
    key    = 'O9YNhkjieyoDY1xO0oLPcr4LIbTwv1xOBqisQbRnRNvuBlatj3zeKzZgcWxMtSMD'
    secret = 'Rzl9apnCoqJr6rKslB9pFnCsBRqFWeYgyVS9naDOckjaThb1fO2PkzqD1eTvbY9n'
    url    = 'https://api.binance.com'

    def __init__(self):
        super().__init__()
        self.s = requests.Session()
        self.s.headers.update({
            'Accept'       : 'application/json',
            'User-Agent'   : 'monker',
            'X-MBX-APIKEY' : self.key,
        })
        self.stop_flag = threading.Event()
        self.thread = threading.Thread(target=self.thread_entry_point)
        self.db_client = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.db_client.monker
        self.thread.start()

    def sign(self, **params):
        q = urllib.parse.urlencode(params)
        m = hmac.new(self.secret.encode(), q.encode(), hashlib.sha256)
        return m.hexdigest()

    def get(self, uri, is_signed, **params):
        if is_signed:
            params['timestamp'] = int(time.time() * 1000)
            params['signature'] = self.sign(**params)
        params = urllib.parse.urlencode(params)
        return self.s.get(self.url + uri, params=params)

    def thread_entry_point(self):
        def merge(book, buff):
            full_book = {
                'b': [ order for order in book['bids'] ],
                'a': [ order for order in book['asks'] ],
            }
            last_updated_id = int(book['lastUpdateId'])
            for obj in buff:
                u = int(obj['u'])
                U = int(obj['U'])
                if u <= last_updated_id:
                    continue
                assert(U <= last_updated_id+1 and u >= last_updated_id+1)
                book['b'].extend(obj['b'])
                book['a'].extend(obj['a'])
            return full_book
        server = "wss://stream.binance.com:9443/ws/btcusdt@depth@100ms"
        ws = websocket.create_connection(server)
        buff = []
        while not self.stop_flag.is_set() and len(buff) < 10:
            buff.append(json.loads(ws.recv()))
        r = self.get('/api/v1/depth', False, symbol="BTCUSDT", limit=100)
        objs = merge(r.json(), buff)
        self.update_book(objs)
        prv_u = None
        while not self.stop_flag.is_set():
            obj = json.loads(ws.recv())
            if prv_u is not None:
                if int(obj['U']) != prv_u + 1:
                    break
            prv_u = int(obj['u'])
            self.update_book(obj)
    
    def save(self):
        self.db.binance.insert_one(self.dump(ts=time.time()))

    def update_book(self, objs):
        for bid in objs['b']:
            self.bids.add(Order(*bid))
        for ask in objs['a']:
            self.asks.add(Order(*ask))

    def stop(self):
        self.stop_flag.set()
        self.thread.join()
        
if __name__ == '__main__':
    b = Binance()
    try:
        from pprint import pprint
        while True:
            time.sleep(1)
            if not b.thread.is_alive():
                print('thread has died')
            print('Bids:')
            pprint(b.bids)
            print('Asks:')
            pprint(b.asks)
            if b.bids or b.asks:
                b.save()
    except KeyboardInterrupt:
        b.stop()
        print('\nquitting politely')

