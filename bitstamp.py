import requests, hashlib, hmac, time, json
import urllib, uuid, pymongo, websocket, threading

from orderbook import OrderBook, Order

class Bitstamp(OrderBook):
    key    = 'Mki3wDj0koY516iMKw6DXH88DeX0wN9K'
    secret = 'mutSyNjOR61rBhUP8vZuFNKbGN48z2YI'
    url    = 'https://www.bitstamp.net'

    def __init__(self):
        super().__init__(is_absolute=False)
        self.s = requests.Session()
        self.s.headers.update({
            'Accept'       : 'application/json',
            'User-Agent'   : 'monker',
        })
        self.stop_flag = threading.Event()
        self.thread = threading.Thread(target=self.thread_entry_point)

    def post(self, uri, is_signed, **params):
        kargs = { 'data' : urllib.parse.urlencode(params) }
        if is_signed:
            kargs.setdefault('headers', self.sign(uri, **params))
        return self.s.post(self.url + uri, **kargs)

    def sign(self, uri, **params):
        timestamp = str(int(time.time() * 1000))
        nonce = str(uuid.uuid4())
        content_type = 'application/x-www-form-urlencoded'
        params_url = urllib.parse.urlencode(params)
        url = 'www.bitstamp.net' + uri
        message = (
            'BITSTAMP ' + self.key + 'POST' + url + content_type +
            nonce + timestamp + 'v2' + params_url
        )
        signature = hmac.new(Bitstamp.secret.encode(),
                             msg=message.encode(),
                             digestmod=hashlib.sha256).hexdigest()
        headers = {
                'X-Auth'           : 'BITSTAMP ' + Bitstamp.key,
                'X-Auth-Signature' : signature,
                'X-Auth-Nonce'     : nonce,
                'X-Auth-Timestamp' : timestamp,
                'X-Auth-Version'   : 'v2',
                'Content-Type'     : content_type,
        }
        return headers

    def thread_entry_point(self):
        server = "wss://ws.bitstamp.net"
        ws = websocket.create_connection(server)
        ws.send(json.dumps({
            "event": "bts:subscribe",
            "data": {
                "channel": "live_orders_ethbtc"
            }
        }))
        while not self.stop_flag.is_set():
            self.update_book(json.loads(ws.recv()))

    def update_book(self, obj):
        data = obj['data']
        if data:
            price      = data.get('price')
            qnty       = data.get('amount')
            order_type = data.get('order_type')
            event      = 1 if obj.get('event') == 'order_created' else -1
            if order_type  == 0:
                self.bids.add(Order(price, qnty*event))
            else:
                self.asks.add(Order(price, qnty*event))

if __name__ == '__main__':
    b = Bitstamp()
    b.thread.start()
    try:
        from pprint import pprint
        while True:
            time.sleep(2)
            if not b.thread.is_alive():
                print('thread has died')
            print('Bids:')
            pprint(b.bids)
            print('Asks:')
            pprint(b.asks)
    except KeyboardInterrupt:
        b.stop_flag.set()
        b.thread.join()
        print('\nquitting politely')

