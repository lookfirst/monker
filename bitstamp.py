#!/usr/bin/env python3
from IPython import embed
import requests
import hashlib
import hmac
import time
import json
import urllib
import uuid
from websocket import create_connection

class Bitstamp:
  key    = 'Mki3wDj0koY516iMKw6DXH88DeX0wN9K'
  secret = 'mutSyNjOR61rBhUP8vZuFNKbGN48z2YI'
  url    = 'https://www.bitstamp.net'

  def __init__(self):
    self.s = requests.Session()

  def post(self, uri, is_signed=False, **params):
    kargs = { 'data' : urllib.parse.urlencode(params) }
    if is_signed:
      kargs.setdefault('headers', self.gensig(uri, **params))
    return self.s.post(self.url + uri, **kargs)

  def listen(self):
    ws = create_connection("wss://ws.bitstamp.net")
    ws.send(json.dumps({
      "event": "bts:subscribe",
      "data": {
        "channel": "live_orders_ethbtc"
      }
    }))
    while True:
      result = ws.recv()
      result = json.loads(result)

  def gensig(self, uri, **params):
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
        'X-Auth'            : 'BITSTAMP ' + Bitstamp.key,
        'X-Auth-Signature'  : signature,
        'X-Auth-Nonce'      : nonce,
        'X-Auth-Timestamp'  : timestamp,
        'X-Auth-Version'    : 'v2',
        'Content-Type'      : content_type,
    }
    return headers

if __name__ == '__main__':
  b = Bitstamp()
  r = b.post('/api/v2/user_transactions/', is_signed=True, offset='1')
  #r = b.post('/api/v2/ticker/btcusd/')
  print(r.json())


