#!/usr/bin/env python3
from IPython import embed
import requests
import hashlib
import hmac
import time
import urllib.parse
import base64
import json
import websocket

class Kraken:
  key    = 'ICjU+mmu2Uyj3Oxrfny0jhTz3hde1LrUtPsI3nhq+f1HlNdKXfIW61J7'
  secret = 'jSFq64Gzve0szD2YUdqwxj+AVmuRT6DFBRz5xOaTV/ZNgqA/+4wkgBhBxwXuTKdCAEkcO3qEEBoKF22u4U8DGw=='
  passwd = '7a90rK68eEbAScyx4pvXqb7$gO3vP^'
  url    = 'https://api.kraken.com'
  def __init__(self):
    self.s = requests.Session()
    self.s.headers.update({
      'Accept'       : 'application/json',
      'User-Agent'   : 'mmm/kraken',
    })
  def gensig(self, uri, **params):
    params_url = urllib.parse.urlencode(params)
    encoded    = (params['nonce'] + params_url).encode()
    message    = uri.encode() + hashlib.sha256(encoded).digest()
    signature  = hmac.new(base64.b64decode(self.secret), message, hashlib.sha512)
    sigdigest  = base64.b64encode(signature.digest())
    return sigdigest.decode()

  def post(self, uri, is_signed=False, **params):
    if is_signed:
      params['nonce'] = str(int(1000*time.time()))
      params['otp'] = self.passwd
      headers = {
        'API-Key':  self.key,
        'API-Sign': self.gensig(uri, **params)
      }
    return self.s.post(self.url + uri, data=params, headers=headers)

  def get_ws_token(self):
    r = self.post('/0/private/GetWebSocketsToken', True)
    data = r.json()
    return data['result']['token']

  def listen(self):
    ws = websocket.create_connection("wss://ws.kraken.com")
    ws.send(json.dumps({
      "event": "subscribe",
      #"event": "ping",
      "pair": ["XBT/USD", "XBT/EUR"],
      #"subscription": {"name": "ticker"}
      #"subscription": {"name": "spread"}
      "subscription": {
        "name": "ticker",
      }
      #"subscription": {"name": "book", "depth": 10}
      #"subscription": {"name": "ohlc", "interval": 5}
    }))
    while True:
      result = ws.recv()
      print(json.loads(result))

if __name__ == '__main__':
  k = Kraken()
  embed()