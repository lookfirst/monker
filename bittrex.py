import hashlib, hmac, time, base64
import json, urllib, threading

from orderbook import OrderBook, Order

## the imports below should be in this order
import gevent.monkey
gevent.monkey.patch_all()
import requests, signalr

# QueryExchangeState
# get full order book
# params
# market: string
# the market identifier (e.g. BTC-ETH)
# 
# SubscribeToExchangeDeltas
# Allows the caller to receive real-time updates to the state of a single market
# marketName: string
# the market identifier (e.g. BTC-ETH)
# 
# Success
# Boolean indicating whether the user was subscribed to the feed
# 
# Drop existing websocket connections and flush accumulated data and state (e.g. market nonces).
# Re-establish websocket connection.
# Subscribe to BTC-ETH market deltas, cache received data keyed by nonce.
# Query BTC-ETH market state.
# Apply cached deltas sequentially, starting with nonces greater than that received in step 4.


class Bittrex(OrderBook):
    key    = '15f7f668f7b548e08ebd7b50e8e1b544'
    secret = '7225f7821ff54835a963a445e9e2375c'
    url    = 'https://api.bittrex.com'

    def __init__(self):
        super().__init__()
        self.s = requests.Session()
        self.stop_flag = threading.Event()
        self.thread = threading.Thread(target=self.thread_entry_point)

    def market_data(self, *args, **kwargs):
        print ('mkt', (args, kwargs))

    def thread_entry_point(self):
        server = "https://socket.bittrex.com/signalr"
        conn = signalr.Connection(server, self.s)
        corehub = conn.register_hub('corehub')

        #conn.received += self.market_data
        #conn.error += self.market_data

        conn.start()
        #corehub.client.on('uE', self.market_data)
        corehub.client.on('updateExchangeState', self.market_data)
        corehub.server.invoke('SubscribeToExchangeDeltas', 'BTC-ETH')
        conn.wait(500)

        # You missed this part

        while not self.stop_flag.is_set():
            time.sleep(1)
        #    self.update_book(json.loads(ws.recv()))

    def update_book(self, obj):
        if obj['event'] == 'data':
            data = obj['data']
            self.flush()
            for bid in data['bids']:
                self.bids.add(Order(*bid))
            for ask in data['asks']:
                self.asks.add(Order(*ask))

if __name__ == '__main__':
    b = Bittrex()
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

