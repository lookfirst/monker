import signalr_aio, base64, zlib, hashlib, hmac
import time, urllib, threading, json, pymongo
import requests, asyncio, websockets
from pdb import set_trace as trace

from orderbook import OrderBook, Order, Trade

class Bittrex(OrderBook):
    key    = '15f7f668f7b548e08ebd7b50e8e1b544'
    secret = '7225f7821ff54835a963a445e9e2375c'
    url    = 'https://api.bittrex.com/v3'

    def __init__(self):
        super().__init__()
        self.s = requests.Session()
        self.stop_flag = threading.Event()
        self.thread = threading.Thread(target=self.thread_entry_point)
        self.conn = None
        self.initial_nonce = None
        self.buff_deltas = []
        self.db_client = pymongo.MongoClient('mongodb://localhost:27017/')
        self.db = self.db_client.monker
        self.thread.start()

    def process_message(self, msg):
        deflated_msg = zlib.decompress(base64.b64decode(msg, validate=True), -zlib.MAX_WBITS)
        return json.loads(deflated_msg.decode())

    async def on_receive(self, **msg):
        if 'R' in msg and type(msg['R']) is not bool:
            book = self.process_message(msg['R'])
            self.initial_nonce = book['N']
            self.flush()
            self.update_book(book)

    async def on_exchange_deltas(self, msg):
        delta = self.process_message(msg[0])
        self.update_delta(delta)
        r = self.get('/markets/ETH-BTC/trades', False)
        self.update_trades(r.json())

    def get(self, uri, is_signed, **params):
        if is_signed:
            ## TODO
            raise NotImplemented
        params = urllib.parse.urlencode(params)
        return self.s.get(self.url + uri, params=params)

    def save(self):
        self.db.bittrex.insert_one(self.dump(ts=time.time()))

    def thread_entry_point(self):
        while not self.stop_flag.is_set():
            try:
                self.initial_nonce = None
                self.conn = signalr_aio.Connection('https://beta.bittrex.com/signalr', session=None)
                hub = self.conn.register_hub('c2')
                self.conn.received += self.on_receive
                hub.client.on('uE', self.on_exchange_deltas)
                hub.server.invoke('SubscribeToExchangeDeltas', 'BTC-ETH')
                hub.server.invoke('QueryExchangeState', 'BTC-ETH')
                self.conn.start()
            except websockets.exceptions.ConnectionClosedOK:
                pass
            except websockets.exceptions.ConnectionClosedError:
                pass

    def update_book(self, book):
        bids = book['Z']
        for bid in bids:
            self.bids.add(Order(bid['R'], bid['Q']))
        asks = book['S']
        for ask in asks:
            self.asks.add(Order(ask['R'], ask['Q']))

    def update_trades(self, trades):
        for trade in trades:
            dotnet_ts = trade['executedAt']
            if '.' not in dotnet_ts:
                dotnet_ts = dotnet_ts.replace('Z', '.00Z')
            ts = time.mktime(time.strptime(dotnet_ts, '%Y-%m-%dT%H:%M:%S.%fZ'))
            price = trade['rate']
            qnty = trade['quantity']
            self.trades.append(Trade(ts, price, qnty))

    def update_delta(self, delta):
        if self.initial_nonce is None:
            self.buff_deltas.append(delta)
        else:
            if self.buff_deltas:
                for delta in self.buff_deltas:
                    if delta['N'] > self.initial_nonce:
                        self.update_book(delta)
                self.buff_deltas = []
            self.update_book(delta)

    def stop(self):
        self.stop_flag.set()
        self.conn.close()
        self.thread.join()

if __name__ == '__main__':
    b = Bittrex()
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
            print('Trades:')
            pprint(b.trades)
            #if b.bids or b.asks or b.trades:
            #    b.save()
    except KeyboardInterrupt:
        b.stop()
        print('\nquitting politely')

