import requests, hashlib, hmac, time, base64
import json, urllib, pymongo, websocket

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


class Bittrex:
    key    = '15f7f668f7b548e08ebd7b50e8e1b544'
    secret = '7225f7821ff54835a963a445e9e2375c'
    url    = 'https://api.bittrex.com'

    def __init__(self):
        self.s = requests.Session()
        self.s.headers.update({
            'Accept'       : 'application/json',
            'User-Agent'   : 'mmm',
        })

    def listen(self):
        try:
            client = pymongo.MongoClient('mongodb://localhost:27017/')
            db = client.mmm
            ws = websocket.create_connection("wss://stream.binance.com:9443/ws/ethbtc@depth@100ms")
            while True:
                result = ws.recv()
                obj = json.loads(result)
                print(obj)
                obj = {
                    '_localtime': time.time(),
                    'obj' : obj,
                }
                db.binance.insert_one(obj)
        except KeyboardInterrupt:
            #print('Reading from db:')
            #for item in db.binance.find():
            #    print(item)
            print('Done.')
            client.close()


if __name__ == '__main__':
    b = Binance()
    #r = b.get('/api/v3/allOrders', is_signed=True, symbol="LTCBTC")
    #print(r.json())
    b.listen()

