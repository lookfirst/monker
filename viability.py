from IPython import embed
import pymongo
import matplotlib.pyplot as plt
import numpy as np
import math
from sys import stdout

def get_binance_values(db, filterdict):
  ask_price, ask_volume = float('nan'), 0.0 
  bid_price, bid_volume = float('nan'), 0.0
  try:
    for obj in db.binance.find(filterdict):
      for p, v in obj['obj']['a']:
        p, v = float(p), float(v)
        if p < ask_price or math.isnan(ask_price):
          ask_price = p
        ask_volume += v
      for p, v in obj['obj']['b']:
        p, v = float(p), float(v)
        if p > bid_price or math.isnan(bid_price):
          bid_price = p
        bid_volume += v
  except KeyError:
    pass
  price = (ask_price+bid_price)/2
  return price, ask_volume, bid_volume

def get_bitstamp_values(db, filterdict):
  ask_price, ask_volume = float('nan'), 0.0 
  bid_price, bid_volume = float('nan'), 0.0
  try:
    for obj in db.bitstamp.find(filterdict):
      for p, v in obj['obj']['data']['asks']:
        p, v = float(p), float(v)
        if p < ask_price or math.isnan(ask_price):
          ask_price = p
        ask_volume += v
      for p, v in obj['obj']['data']['bids']:
        p, v = float(p), float(v)
        if p > bid_price or math.isnan(bid_price):
          bid_price = p
        bid_volume += v
  except KeyError:
    pass
  price = (ask_price+bid_price)/2
  return price, ask_volume, bid_volume

def fill_nan(arr):
  for i in range(len(arr)):
    if math.isnan(arr[i]):
      if i > 0:
        ## Forward fill the nan is the default
        arr[i] = arr[i-1]
      else:
        ## If nan is first item, use backward fill
        for v in arr:
          if not math.isnan(v):
            arr[i] = v
            break
  return np.array(arr)

TIME_WINDOW = 500

if __name__ == '__main__':
  ## Setup db
  client = pymongo.MongoClient('mongodb://localhost:27017/')
  db = client.mmm
  ## Get first and last timestamps for analysis
  for obj in db.binance.find().sort([("_localtime", pymongo.ASCENDING)]).limit(1):
    time_begin = math.floor(obj['_localtime'])
  for obj in db.binance.find().sort([("_localtime", pymongo.DESCENDING)]).limit(1):
    time_end = math.floor(obj['_localtime'])
  ## Collect data, one second at a time
  while time_begin < time_end:
    time = time_begin
    time_pause = time_begin+TIME_WINDOW
    ## Init arrays
    times = []
    binance  = { 'prices' : [], 'ask_volumes' : [], 'bid_volumes' : [] }
    bitstamp = { 'prices' : [], 'ask_volumes' : [], 'bid_volumes' : [] }
    while time < time_pause:
      fltr = {'_localtime' : { '$gt' : time, '$lt' : time+1}}
      ## Get the price and volumes for this second (exception if time not avail)
      binance_tuple  = get_binance_values(db, fltr)
      bitstamp_tuple = get_bitstamp_values(db, fltr)
      ## Store values in the arrays for ploting
      binance['prices'     ].append(10000*binance_tuple[0])
      binance['ask_volumes'].append(binance_tuple[1])
      binance['bid_volumes'].append(binance_tuple[2])
      bitstamp['prices'     ].append(10000*bitstamp_tuple[0])
      bitstamp['ask_volumes'].append(bitstamp_tuple[1])
      bitstamp['bid_volumes'].append(bitstamp_tuple[2])
      times.append(time)
      ## Increment time in a second
      time += 1
      stdout.write('.')
      stdout.flush()
    stdout.write('\n')
    ## Fix the NaN values
    ax = plt.subplot(7,1,1)
    idx = 1
    for key in ['prices', 'ask_volumes', 'bid_volumes']:
      for name, exchange in zip(['bin', 'bit'], [binance, bitstamp]):
        exchange[key]  = fill_nan(exchange[key])
        ax.plot(times, exchange[key], 'o', label=f'{key} {name}')
        ax.legend()
        idx += 1
        ax = plt.subplot(7,1,idx, sharex=ax)
    embed()
    ax.plot(times, binance['prices']-bitstamp['prices'], 'o', label='diff prices')
    ax.legend()
    plt.show()
    time_begin += TIME_WINDOW

