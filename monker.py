import requests, hashlib, hmac, time, base64
import json, urllib, pymongo, threading
from datetime import datetime
from sys import stdout
from traceback import format_exc as exc

## print more output to the terminal
VERBOSE = True

## market and budget definition
MRKT_PAIR   = 'eth', 'tusd' ## market to trade on (asset, market)
MRKT_BUDGET = 0.0           ## budget for market (zero for entire balance)

## internal state parameters
TRDN_CHUNK    = 90.0      ## every trade uses the same investiment
FEES          = 0.50      ## fees to buy/sell
DTHR_MINIMAL  = FEES*5    ## minimal dip threshold for any transaction
DTHR_STEP     = FEES      ## dip threshold step to increment/decrement
TICK_INTERVAL = '5m'      ## window size for market analysis
TICK_PER_OPER = 5         ## target ticks per operation (1 op every 25min)

## binance keys and url
KEY    = 'ZADfFZTF0Djk5HozzmbbPhK1TWqz9SROYaivOcQPbJPIEscP24Rhc8RzMGx7pvdz'
SECRET = '5SNpXT5wRqDBgEfvl7b2gTLq1fKnNqDmteFZMwXfrbOBKDLSt4QHA7Vu1UcIejYx'
URL    = 'https://api.binance.com'

## mongo db connection
db_client = pymongo.MongoClient('mongodb://localhost:27017/')
db = db_client.monker

def logbuy(price, qty, target):
    stdout.write('BUY: ')
    obj = {
        'time'   : datetime.now(),
        'market' : MARKET,
        'price'  : price,
        'target' : target,
        'qty'    : qty,
    }
    if VERBOSE: print(obj)
    db.buy.insert_one(obj)

def logstate():
    stdout.write('STATE: ')
    obj = {
        'time'   : datetime.now(),
        'market' : MARKET,
        'dthr'   : dthr,
        'blnc'   : blnc,
        'exps'   : exps,
    }
    if VERBOSE: print(obj)
    db.state.insert_one(obj)

def logsell(buy_id, price, qty):
    stdout.write('SELL: ')
    obj = {
        'time'   : datetime.now(),
        'market' : MARKET,
        'buy_id' : buy_id,
        'price'  : price,
        'qty'    : qty,
    }
    if VERBOSE: print(obj)
    db.sell.insert_one(obj)

def logdip(dip, dthr, price):
    stdout.write('DIP: ')
    obj = {
        'time'   : datetime.now(),
        'market' : MARKET,
        'dip'    : dip,
        'dthr'   : dthr,
        'price'  : price,
    }
    if VERBOSE: print(obj)
    db.dip.insert_one(obj)

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

def get_accm_diff(s):
    ## get last five ticks to calculate price
    ## sum the derivatives..
    # returns tuple dip, price
    return -3.0, 160

def get_mrkt_blnc(s):
    ## get balance on target market
    ## ensure it is larger then current market budget 
    return 100

def cancel_open_buy_orders(s):
    ## cancel buy open order
    pass

def cancel_open_sell_orders(s):
    ## cancel sell open order
    pass

def get_opnd_posns(s):
    ## read open positions from db
    ## get asset balance from binance
    ## ensure the asset balance agrees with open positions in db
    ## TODO I need to decide what will happen when balances dont match
    pass

def get_mrkt_exps(s):
    ## TODO I need to decide what will happen when balances dont match
    pass

def try_buy(price, qty):
    ## return boolean, true if bought, false if not
    pass

def try_sell(price, qty):
    ## return boolean, true if sold, false if not
    pass

## dip threshold is managed by the main thread
dthr = None

def buy_thread(stop_event):
    try:
        loginfo('buy thread started')
        s = requests.Session()
        s.headers.update({
            'Accept'       : 'application/json',
            'User-Agent'   : 'monker',
            'X-MBX-APIKEY' : key,
        })
        cancel_open_buy_orders()
        while not stop_event.is_set():
            time.sleep(10)
            dip, price = get_accm_diff(s)
            if dip < (-dthr):
                logdip(dip, dthr, price)
                exps = get_mrkt_exps(s)
                blnc = get_mrkt_blnc(s)
                if (exps + blnc >= MRKT_BUDGET and
                           blnc >= TRDN_CHUNK):
                    try_buy(price, TRDN_CHUNK):
                    target = price + dthr
                    logbuy(price, TRDN_CHUNK, target):
                else:
                    logwarn('not enough resources to buy on market')
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
        cancel_open_sell_orders()
        while not stop_event.is_set():
            time.sleep(10)
            posns = get_opnd_posns(s)
            quote = get_ask_quote(s)
            for posn in posns:
                if quote > posn['target']:
                    try_sell(quote, posn['qty']):
                    logsell(posn['_id'], quote, posn['qty'])
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
    logingo('monker main started')
    try:
        dthr = DTHR_MINIMAL
        buy_th, buy_stop_ev   = start_buy_thread()
        sell_th, sell_stop_ev = start_sell_thread()
        while True:
            time.sleep(10)
            ## TODO:
            ## the main thread should do the throtling of dthr up/down
            ## depending on the throughput of transactions
            logstate()
    except KeyboardInterrupt:
        buy_stop_ev.set()
        sell_stop_ev.set()
        buy_th.join()
        sell_th.join()
    logingo('monker main terminated')

