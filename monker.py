import requests, hashlib, hmac, time, base64
import json, urllib, pymongo, threading
from datetime import datetime
from sys import stdout
from traceback import format_exc as exc

## Print more output to the terminal
VERBOSE = True

## Market and budget definition
MRKT          = 'ethtusd' ## market to trade on
MRKT_BUDGET   = 0.0       ## budget for this market (zero for entire balance)

## Internal state parameters
TRDN_CHUNK    = 90.0      ## every trade uses the same investiment
FEES          = 0.50      ## fees to buy/sell
DTHR_MINIMAL  = FEES*5    ## minimal dip threshold for any transaction
DTHR_STEP     = FEES      ## dip threshold step to increment/decrement
TICK_INTERVAL = '5m'      ## window size for market analysis
TICK_PER_OPER = 5         ## target ticks per operation (1 op every 25min)

## Binance keys and url
KEY    = 'ZADfFZTF0Djk5HozzmbbPhK1TWqz9SROYaivOcQPbJPIEscP24Rhc8RzMGx7pvdz'
SECRET = '5SNpXT5wRqDBgEfvl7b2gTLq1fKnNqDmteFZMwXfrbOBKDLSt4QHA7Vu1UcIejYx'
URL    = 'https://api.binance.com'

## Mongo DB connection
db_client = pymongo.MongoClient('mongodb://localhost:27017/')
db = db_client.monker

def logbuy(id, price, qty, target):
    stdout.write('BUY: ')
    obj = {
        'time'   : datetime.now(),
        'id'     : id,
        'market' : MARKET,
        'price'  : price,
        'target' : target,
        'qty'    : qty,
    }
    if VERBOSE: print(obj)
    db.buy.insert_one(obj)

def logstate(dthr, bln, expsr):
    stdout.write('STATE: ')
    obj = {
        'time'   : datetime.now(),
        'dthr'   : dthr,
        'bln'    : bln,
        'expsr'  : expsr,
    }
    if VERBOSE: print(obj)
    db.state.insert_one(obj)

def logsell(id, price, qty):
    stdout.write('SELL: ')
    obj = {
        'time'   : datetime.now(),
        'id'     : id,
        'market' : MARKET,
        'price'  : price,
        'qty'    : qty,
    }
    if VERBOSE: print(obj)
    db.sell.insert_one(obj)

def logtext(level, text):
    stdout.write('LOGGING: ')
    obj = {
        'time'   : datetime.now(),
        'market' : MARKET,
        'level'  : level,
        'text'   : text,
    }
    if VERBOSE: print(obj)
    db.logging.insert_one(obj)

def loginfo(text):
    logtext('info', text)

def logwarn(text):
    logtext('warn', text)

def logerror(text):
    logtext('error', text)

def sign(self, **params):
    q = urllib.parse.urlencode(params)
    m = hmac.new(secret.encode(), q.encode(), hashlib.sha256)
    return m.hexdigest()

def get(s, uri, is_signed, **params):
    if is_signed:
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = sign(**params)
    params = urllib.parse.urlencode(params)
    return s.get(url + uri, params=params)

def get_price(s):
    ## TODO
    return 160

def get_balance(s):

    return 100

def calculate_exposure(s):

def 


def buy_thread(stop_event):
    try:
        loginfo('buy thread started')
        s = requests.Session()
        s.headers.update({
            'Accept'       : 'application/json',
            'User-Agent'   : 'monker',
            'X-MBX-APIKEY' : key,
        })
        while not stop_event.is_set():
            ## buyer loop
            time.sleep(1)
    except Exception:
        logerror(exc())

def sell_thread(stop_event):
    try:
        loginfo('sell thread started')
        s = requests.Session()
        s.headers.update({
            'Accept'       : 'application/json',
            'User-Agent'   : 'monker',
            'X-MBX-APIKEY' : key,
        })
        while not stop_event.is_set():
            ## seller loop
            time.sleep(1)
    except Exception:
        logerror(exc())

def start_buy_thread()
    stop_event = threading.Event()
    thread = threading.Thread(target=buy_thread, args=(stop_event,))
    thread.start()
    return thread, stop_event

def start_sell_thread()
    stop_event = threading.Event()
    thread = threading.Thread(target=buy_thread, args=(stop_event,))
    thread.start()
    return thread, stop_event

if __name__ == '__main__':
    try:
        buy_th, buy_stop_ev   = start_buy_thread()
        sell_th, sell_stop_ev = start_sell_thread()
    except KeyboardInterrupt:
        buy_stop_ev.set()
        sell_stop_ev.set()
        buy_th.join()
        sell_th.join()

