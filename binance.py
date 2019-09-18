#!/usr/bin/env python3
from IPython import embed
import requests
import hashlib
import hmac
import time
import base64
import json
import urllib
import pymongo
from websocket import create_connection

class Binance:
  key    = 'O9YNhkjieyoDY1xO0oLPcr4LIbTwv1xOBqisQbRnRNvuBlatj3zeKzZgcWxMtSMD'
  secret = 'Rzl9apnCoqJr6rKslB9pFnCsBRqFWeYgyVS9naDOckjaThb1fO2PkzqD1eTvbY9n'
  url    = 'https://api.binance.com'

  def __init__(self):
    self.s = requests.Session()
    self.s.headers.update({
      'Accept'       : 'application/json',
      'User-Agent'   : 'mmm',
      'X-MBX-APIKEY' : self.key,
    })

  def gensig(self, **params):
    q = urllib.parse.urlencode(params)
    m = hmac.new(self.secret.encode(), q.encode(), hashlib.sha256)
    return m.hexdigest()

  def get(self, uri, is_signed=False, **params):
    if is_signed:
      params['timestamp'] = int(time.time() * 1000)
      params['signature'] = self.gensig(**params)
    params = urllib.parse.urlencode(params)
    return self.s.get(self.url + uri, params=params)

  def listen(self):
    try:
      client = pymongo.MongoClient('mongodb://localhost:27017/')
      db = client.mmm
      ws = create_connection("wss://stream.binance.com:9443/ws/bnbbtc@depth")
      while True:
        result = ws.recv()
        obj = json.loads(result)
        db.binance.insert_one(obj)
        print(obj)
    except KeyboardInterrupt:
      print('Reading from db:')
      for item in db.binance.find():
        print(item)
      print('Done.')
      client.close()


if __name__ == '__main__':
  b = Binance()
  #r = b.get('/api/v3/allOrders', is_signed=True, symbol="LTCBTC")
  #print(r.json())
  b.listen()

