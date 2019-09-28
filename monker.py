import requests, hashlib, hmac, time
import urllib, pymongo, threading, uuid
import argparse, fcntl

from traceback import format_exc as exc
from datetime import datetime
from sys import stdout

## print more output to the terminal
VERBOSE = True

try:
    from IPython import embed as trace
except ImportError:
    from pdb import set_trace as trace

## market definition
ASTMKT = None, None         ## tuple with pair asset,market
SYMBOL = ''                 ## market to trade on (joined asset,market pair)

MIN_NOTIONAL   = None       ## minimum price*qty of an order
MAX_NUM_ORDERS = 5          ## binance 'apparently' supports up to 25

## binance keys and url
KEY    = 'ZADfFZTF0Djk5HozzmbbPhK1TWqz9SROYaivOcQPbJPIEscP24Rhc8RzMGx7pvdz'
SECRET = '5SNpXT5wRqDBgEfvl7b2gTLq1fKnNqDmteFZMwXfrbOBKDLSt4QHA7Vu1UcIejYx'
URL    = 'https://api.binance.com'
DFT_API_HDRS = {
    'Accept'       : 'application/x-www-form-urlencoded',
    'User-Agent'   : 'monker',
    'X-MBX-APIKEY' : KEY,
}

KLINES_LABELS = [
    'open_time',
    'open',
    'high',
    'low',
    'close',
    'volume',
    'close_time',
    'quote_volume',
    'n_of_trades',
    'taker_base_volume',
    'taker_quote_volume',
    'ignore',
]

## metadata and thread names 
MAESTRO   = 'maestro'
DIPSEEKER = 'dipseeker'
BUYER     = 'buyer'
SELLER    = 'seller'

## global variables
DB_CLIENT = pymongo.MongoClient('mongodb://localhost:27017/')
DB        = DB_CLIENT.monker

class MetaNotFound   (Exception): pass
class FileLockFailed (Exception): pass
class BadAPIResponse (Exception): pass
class MarketNotFound (Exception): pass
class SymbolNotFound (Exception): pass

def logbuy(buy_id, price, qty, proft):
    stdout.write('BUY: ')
    obj = {
        'open_time' : datetime.now(), ## open time
        'close_time': '',             ## close time (updated later)
        'symbol'    : SYMBOL,         ## symbol name
        'buy_id'    : buy_id,         ## exchange order buy id 
        'sell_id'   : '',             ## exchange order sell id
        'status'    : 'OPENED',       ## [OPENED, CLOSED]
        'orig_price': price,          ## original order price
        'price'     : 0.0,            ## final order price (updated later)
        'proft'     : proft,          ## offset on price to sell
        'orig_qty'  : qty,            ## original order quantity
        'qty'       : 0.0,            ## final order quantity (updated later)
    }
    if VERBOSE: print(obj)
    DB.buy.insert_one(obj)

def logsell(sell_id, buy_id, price, qty):
    stdout.write('SELL: ')
    obj = {
        'open_time' : datetime.now(), ## open time
        'close_time': '',             ## close time (updated later)
        'symbol'    : SYMBOL,         ## symbol name
        'buy_id'    : buy_id,         ## exchange order buy id
        'sell_id'   : sell_id,        ## exchange order sell id
        'sell_id2'  : '',             ## next try sell id (if any, updated later)
        'status'    : 'OPENED',       ## [OPENED, CLOSED]
        'orig_price': price,          ## original order price
        'price'     : 0.0,            ## final order price (updated later)
        'orig_qty'  : qty,            ## original order quantity
        'qty'       : 0.0,            ## final order quantity (updated later)
    }
    if VERBOSE: print(obj)
    DB.sell.insert_one(obj)

def logstate(dthr, dip, exps, blnc, lqdy, price):
    stdout.write('STATE: ')
    obj = {
        'time'   : datetime.now(),
        'symbol' : SYMBOL,
        'dthr'   : dthr,
        'dip'    : dip,
        'exps'   : exps,
        'blnc'   : blnc,
        'lqdy'   : lqdy,
        'price'  : price,
    }
    if VERBOSE: print(obj)
    DB.dip.insert_one(obj)

def logtext(level, text):
    stdout.write('LOGGING: ')
    obj = {
        'time'   : datetime.now(),
        'symbol' : SYMBOL,
        'level'  : level,
        'text'   : text,
    }
    if VERBOSE: print(obj)
    DB.logging.insert_one(obj)

def loginfo(text):
    logtext('info', text)

def logwarn(text):
    logtext('warn', text)

def logerror(text):
    logtext('error', text)

def sign(**params):
    q = urllib.parse.urlencode(params)
    m = hmac.new(SECRET.encode(), q.encode(), hashlib.sha256)
    return m.hexdigest()

def api(method, uri, is_signed, **params):
    if is_signed:
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = sign(**params)
    params = urllib.parse.urlencode(params)
    r = method(URL + uri, params=params)
    obj = r.json()
    if r.status_code >= 500:
        text = f'server error ({r.status_code}, {obj["code"]}): {obj["msg"]}'
        raise BadAPIResponse(text)
    elif r.status_code >= 400:
        text = f'bad request ({r.status_code}, {obj["code"]}): {obj["msg"]}'
        raise BadAPIResponse(text)
    return obj

def get(s, uri, is_signed, **params):
    return api(s.get, uri, is_signed, **params)

def post(s, uri, is_signed, **params):
    return api(s.post, uri, is_signed, **params)

def delete(s, uri, is_signed, **params):
    return api(s.delete, uri, is_signed, **params)

def post_order(s, side, id, price, qty):
    return post(s, '/api/v3/order', True,
                symbol=SYMBOL,
                side=side,
                type='LIMIT',
                timeInForce='GTC',
                newOrderRespType='RESULT',
                quantity=qty,
                price=price,
                recvWindow=5000,
                newClientOrderId=id)

def get_order(s, id):
    try:
        return get(s, '/api/v3/order', True,
                   symbol=SYMBOL,
                   origClientOrderId=id)
    except BadAPIResponse:
        return None

def delete_order(s, id):
    return delete(s, '/api/v3/order', True,
                  symbol=SYMBOL,
                  origClientOrderId=id)

def get_klines(s, interval, limit):
    r = get(s, '/api/v1/klines', False,
            symbol=SYMBOL,
            interval=interval,
            limit=limit)
    klines = []
    for kline_values in r:
        kline_values = [float(v) for v in kline_values]
        klines.append(dict(zip(KLINES_LABELS, kline_values)))
    return klines

def get_exchange_info(s):
    return get(s, '/api/v1/exchangeInfo', False)

def get_mrkt_info(s):
    r = get(s, '/api/v3/account', True)
    asset, market = ASTMKT
    exps, blnc = {}, {}
    for balance in r['balances']:
        if balance['asset'] == asset:
            exps['free'  ] = float(balance['free'])
            exps['locked'] = float(balance['locked'])
            exps['total' ] = exps['free'] + exps['locked']
        if balance['asset'] == market:
            blnc['free'  ] = float(balance['free'])
            blnc['locked'] = float(balance['locked'])
            blnc['total' ] = blnc['free'] + blnc['locked']
        if exps and blnc:
            break
    else:
        raise MarketNotFound
    return exps, blnc

def get_bid_ask_price(s):
    obj = get(s, '/api/v3/ticker/bookTicker', False, symbol=SYMBOL)
    return float(obj['bidPrice']), float(obj['askPrice'])

def get_min_notional():
    s = requests.Session()
    s.headers.update(DFT_API_HDRS)
    r = get_exchange_info(s)
    for symbol in r['symbols']:
        if symbol['symbol'] == SYMBOL:
            for filter in symbol['filters']:
                if filter['filterType'] == 'MIN_NOTIONAL':
                    return float(filter['minNotional'])
    else:
        raise SymbolNotFound

def calculate_metric(s, interval, limit):
    klines = get_klines(s, interval, limit)
    diff = 0.0
    for i in range(0, limit-1):
        diff += (klines[i]['close'] - klines[i+1]['close'])
    return diff

def debug():
    s = requests.Session()
    s.headers.update(DFT_API_HDRS)
    print(get_min_notional())
    exit(0)

def maestro(s):
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': DIPSEEKER})
    if M is None:
        dipseeker = {
            'symbol'        : SYMBOL,
            'name'          : DIPSEEKER,
            'INTERVAL'      : '1m',
            'LIMIT'         : 5,
            'BUDGET'        : 70.0,
            'DTHR'          : 0.0,
            'BUY_QTY'       : 0.0,
            'MAX_NUM_ORDERS': MAX_NUM_ORDERS,
        }
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': BUYER})
    if M is None:
        buyer = {
            'symbol'        : SYMBOL,
            'name'          : BUYER,
            'BUY_TIMEOUT'   : 5*60,
        }
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': SELLER})
    if M is None:
        seller = {
            'symbol'        : SYMBOL,
            'name'          : SELLER,
            'SELL_TIMEOUT'  : 60*60,
        }

def dipseeker(s):
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': DIPSEEKER})
    if M is None: raise MetaNotFound
    dip = get_accm_diff(s, M['INTERVAL'], M['LIMIT'])
    exps, blnc = get_mrkt_info(s)
    bid_price, ask_price = get_bid_ask_price(s)
    lqdy = M['BUDGET'] - exps['total']
    logstate(M['DTHR'], dip, exps['total'], blnc['total'], lqdy, ask_price)
    if dip < (-M['DTHR']):
        if lqdy < M['BUY_QTY']:
            logwarn('not enough liquidity')
        elif blnc['total'] < lqdy:
            logwarn('not enough balance')
        elif blnc['free'] < lqdy:
            logwarn('not enough free balance')
        else:
            logbuy(str(uuid.uuid4()), ask_price, M['BUY_QTY'], M['DTHR'])

def buyer(s):
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': BUYER})
    if M is None: raise MetaNotFound
    for buy in DB.buy.find({'symbol': SYMBOL, 'status':'OPENED'}):
        buy_id = buy['buy_id']
        r = get_order(s, buy_id)
        if r is None:
            post_order(s, 'BUY', buy_id, buy['orig_price'], buy['orig_qty'])
        elif r is not None:
            executedQty         = float(r['executedQty'])
            cummulativeQuoteQty = float(r['cummulativeQuoteQty'])
            age_in_seconds      = (datetime.now() - buy['open_time']).seconds
            if r['status'] == 'FILLED':
                sell_id = str(uuid.uuid4())
                upd_fields = {
                    'close_time' : datetime.now(),
                    'status'     : 'CLOSED',
                    'sell_id'    : sell_id,
                    'price'      : cummulativeQuoteQty/executedQty,
                    'qty'        : executedQty,
                }
                DB.buy.update({'buy_id' : buy_id}, {"$set": upd_fields})
                sell_price = cummulativeQuoteQty/executedQty+buy['proft']
                logsell(sell_id, buy_id, sell_price, executedQty)
            elif age_in_seconds > M['BUY_TIMEOUT']:
                sell_id = str(uuid.uuid4())
                delete_order(s, buy_id)
                if executedQty > 0.0:
                    sell_id = str(uuid.uuid4())
                    upd_fields = {
                        'close_time' : datetime.now(),
                        'status'     : 'CLOSED',
                        'sell_id'    : sell_id,
                        'price'      : cummulativeQuoteQty/executedQty,
                        'qty'        : executedQty,
                    }
                    sell_price = cummulativeQuoteQty/executedQty+buy['proft']
                    logsell(sell_id, buy_id, sell_price, executedQty)
                else:
                    upd_fields = {
                        'close_time' : datetime.now(),
                        'status'     : 'CLOSED',
                    }
                DB.buy.update({'buy_id' : buy_id}, {"$set": upd_fields})

def seller(s):
    ## read metadate from DB
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': SELLER})
    if M is None: raise MetaNotFound
    for sell in DB.sell.find({'symbol': SYMBOL, 'status':'OPENED'}):
        sell_id, buy_id = sell['sell_id'], sell['buy_id']
        r = get_order(s, sell_id)
        bid_price, ask_price = get_bid_ask_price(s)
        if r is None and bid_price > sell['orig_price']:
            post_order(s, 'SELL', sell_id, sell['orig_price'], sell['orig_qty'])
        elif r is not None:
            executedQty         = float(r['executedQty'])
            cummulativeQuoteQty = float(r['cummulativeQuoteQty'])
            age_in_seconds      = (datetime.now() - sell['open_time']).seconds
            if r['status'] == 'FILLED':
                upd_fields = {
                    'close_time' : datetime.now(),
                    'status'     : 'CLOSED',
                    'price'      : cummulativeQuoteQty/executedQty,
                    'qty'        : executedQty,
                }
                DB.sell.update({'sell_id' : sell_id}, {"$set": upd_fields})
            elif age_in_seconds > M['SELL_TIMEOUT']:
                delete_order(s, sell_id)
                sell_id2 = str(uuid.uuid4())
                upd_fields = {
                    'close_time' : datetime.now(),
                    'status'     : 'CLOSED',
                    'sell_id2'   : sell_id2,
                    'price'      : cummulativeQuoteQty/executedQty if executedQty > 0.0 else 0.0,
                    'qty'        : executedQty,
                }
                DB.sell.update({'sell_id' : sell_id}, {"$set": upd_fields})
                open_qty = sell['orig_qty'] - sell['qty']
                logsell(sell_id2, buy_id, sell['orig_price'], open_qty)

def thread_entry(stop_event, name, period):
    try:
        cnter = -1
        loginfo(f'thread {name} started')
        s = requests.Session()
        s.headers.update(DFT_API_HDRS)
        while not stop_event.is_set():
            ## ensure thread period is respected
            time.sleep(1)
            cnter = (cnter + 1) % period
            if cnter != 0: continue
            ## call appropriated function
            if   name == MAESTRO:   maestro(s)
            elif name == DIPSEEKER: dipseeker(s)
            elif name == BUYER:     buyer(s)
            elif name == SELLER:    seller(s)
            else: raise NotImplemented
    except Exception:
        logerror(exc())
    finally:
        loginfo(f'thread {name} ended')

def start_thread(name, period):
    lockfp = lock(f'{SYMBOL.lower()}.{name}')
    stop_event = threading.Event()
    thread = threading.Thread(target=thread_entry,
                              args=(stop_event, name, period))
    thread.start()
    return thread, stop_event, name, lockfp

def lock(name):
    lockfile = f'/var/lock/monker.{name}'
    fp = open(lockfile, 'w')
    fp.flush()
    try:
        fcntl.lockf(fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except IOError:
        raise FileLockFailed 
    return fp

def unlock(fp):
    fcntl.lockf(fp, fcntl.LOCK_UN)
    fp.close()

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("asset",  help="asset to buy/sell")
    parser.add_argument("market", help="market to trade on")
    parser.add_argument("--nomaestro", action='store_true',
                        help="disables maestro thread")
    parser.add_argument("--nodipseeker", action='store_true',
                        help="disables dip seeker thread")
    parser.add_argument("--nobuyer", action='store_true',
                        help="disables buyer thread")
    parser.add_argument("--noseller", action='store_true',
                        help="disables seller thread")
    return parser.parse_args()

if __name__ == '__main__':
    args = parse_args()
    loginfo('thread main started')
    try:
        ## set asset and market to operate on
        ASTMKT = args.asset, args.market
        SYMBOL = ''.join(ASTMKT)
        debug()
        ## set min price*qty of order, also checks connection and symbol 
        MIN_NOTIONAL = get_min_notional()
        ## starts the threads
        threads = []
        if not args.nomaestro:   threads.append(start_thread(MAESTRO,   30))
        time.sleep(1) ## ensures maestro can create meta collection first
        if not args.nodipseeker: threads.append(start_thread(DIPSEEKER, 30))
        if not args.nobuyer:     threads.append(start_thread(BUYER,     30))
        if not args.noseller:    threads.append(start_thread(SELLER,    30))
        ## monitor all threads, in case anyone of them die, finish execution
        while True:
            time.sleep(1)
            for thread, stop, name, lockfp in threads:
                if not thread.is_alive():
                    logerror('thread {name} died')
                    break
    except KeyboardInterrupt:
        loginfo('ctrl-c pressed')
    except Exception:
        logerror(exc())
    finally:
        for thread, stop, name, lockfp in threads:
            stop.set()
            thread.join()
            unlock(lockfp)
    loginfo('thread main ended')

