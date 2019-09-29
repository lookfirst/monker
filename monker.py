import requests, hashlib, hmac, time
import urllib, pymongo, threading, uuid
import argparse, fcntl

from traceback import format_exc as exc
from datetime import datetime
from pdb import set_trace as trace

## print more output to the terminal
VERBOSE = True

try:
    from IPython import embed
except ImportError:
    pass

## market definition
ASTQUT = None, None         ## tuple with pair asset,quote
SYMBOL = None               ## market to trade on (joined asset,quote pair)

## exchange information (precision, min/max qty, etc)
XCH = None

## binance keys and url
KEY    = 'ZADfFZTF0Djk5HozzmbbPhK1TWqz9SROYaivOcQPbJPIEscP24Rhc8RzMGx7pvdz'
SECRET = '5SNpXT5wRqDBgEfvl7b2gTLq1fKnNqDmteFZMwXfrbOBKDLSt4QHA7Vu1UcIejYx'
URL    = 'https://api.binance.com'
DFT_API_HDRS = {
    'Accept'       : 'application/x-www-form-urlencoded',
    'User-Agent'   : 'monker',
    'X-MBX-APIKEY' : KEY,
}

## keywords for kline data from binance
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

## color output for easy debugging on stdout
black   = lambda text: '\033[0;30m' + str(text) + '\033[0m'
red     = lambda text: '\033[0;31m' + str(text) + '\033[0m'
green   = lambda text: '\033[0;32m' + str(text) + '\033[0m'
yellow  = lambda text: '\033[0;33m' + str(text) + '\033[0m'
blue    = lambda text: '\033[0;34m' + str(text) + '\033[0m'
magenta = lambda text: '\033[0;35m' + str(text) + '\033[0m'
cyan    = lambda text: '\033[0;36m' + str(text) + '\033[0m'
white   = lambda text: '\033[0;37m' + str(text) + '\033[0m'

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
    if VERBOSE: print(magenta(obj))
    DB.buy.insert_one(obj)

def logsell(sell_id, buy_id, price, qty):
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
    if VERBOSE: print(green(obj))
    DB.sell.insert_one(obj)

def logmeta(dipseeker, buyer, seller):
    if VERBOSE:
        print(cyan(dipseeker))
        print(cyan(buyer))
        print(cyan(seller))
    DB.meta.update_one({'symbol': SYMBOL, 'name': DIPSEEKER},
                       {"$set": dipseeker}, upsert=True)
    DB.meta.update_one({'symbol': SYMBOL, 'name': BUYER},
                       {"$set": buyer}, upsert=True)
    DB.meta.update_one({'symbol': SYMBOL, 'name': SELLER},
                       {"$set": seller}, upsert=True)

def logstate(dthr, dip, exps, blnc, price):
    obj = {
        'time'   : datetime.now(),
        'symbol' : SYMBOL,
        'dthr'   : dthr,
        'dip'    : dip,
        'exps'   : exps,
        'blnc'   : blnc,
        'price'  : price,
    }
    if VERBOSE: print(blue(obj))
    ## TODO change collection name to state
    DB.dip.insert_one(obj)

def logtext(level, text, colorf=None):
    obj = {
        'time'   : datetime.now(),
        'symbol' : SYMBOL,
        'level'  : level,
        'text'   : text,
    }
    if VERBOSE:
        text = colorf(str(obj)) if colorf is not None else str(obj)
        print(text)
    DB.logging.insert_one(obj)

def loginfo(text):
    logtext('info', text)

def logwarn(text):
    logtext('warn', text, yellow)

def logerror(text):
    logtext('error', text, red)

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
                quantity=f'{qty:.{XCH["AST_PRECISION"]}f}',
                price=f'{price:.{XCH["QUT_PRECISION"]}f}',
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

def get_mrkt_info(s):
    r = get(s, '/api/v3/account', True)
    asset, quote = ASTQUT
    exps, blnc = {}, {}
    for balance in r['balances']:
        if balance['asset'] == asset:
            exps['free'  ] = float(balance['free'])
            exps['locked'] = float(balance['locked'])
            exps['total' ] = exps['free'] + exps['locked']
        if balance['asset'] == quote:
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

def get_exchange_info():
    info = {}
    s = requests.Session()
    s.headers.update(DFT_API_HDRS)
    r = get(s, '/api/v1/exchangeInfo', False)
    for symbol in r['symbols']:
        if symbol['symbol'] == SYMBOL:
            info['AST_PRECISION'] = int(symbol['baseAssetPrecision'])
            info['QUT_PRECISION'] = int(symbol['quotePrecision'])
            for filter in symbol['filters']:
                if filter['filterType'] == 'MIN_NOTIONAL':
                    info['MIN_NOTIONAL'] = float(filter['minNotional'])
                elif filter['filterType'] == 'LOT_SIZE':
                    info['MIN_QTY']   = float(filter["minQty"])
                    info['MAX_QTY']   = float(filter["maxQty"])
                    info['STEP_SIZE'] = float(filter["stepSize"])
                elif filter['filterType'] == 'PRICE_FILTER':
                    info['MIN_PRICE']  = float(filter['minPrice'])
                    info['MAX_PRICE']  = float(filter['maxPrice'])
                    info['TICK_SIZE']  = float(filter['tickSize'])
            break
    else:
        raise SymbolNotFound
    ## binance 'apparently' supports up to 25 orders in the same market
    info['MAX_NUM_ORDERS'] = 25
    return info

def get_mrkt_dip(s, interval, limit):
    klines = get_klines(s, interval, limit)
    diff = 0.0
    for i in range(0, limit-1):
        diff += (klines[i]['close'] - klines[i+1]['close'])
    return diff

def fix_order(price, qty):
    price = XCH['TICK_SIZE']*round(price/XCH['TICK_SIZE'])
    price = max(XCH['MIN_PRICE'], price)
    price = min(XCH['MAX_PRICE'], price)
    qty = XCH['STEP_SIZE']*round(qty/XCH['STEP_SIZE'])
    qty = max(XCH['MIN_QTY'], qty)
    qty = min(XCH['MAX_QTY'], qty)
    while (qty*price) < XCH['MIN_NOTIONAL']:
        qty += XCH['STEP_SIZE']
    return price, qty

def maestro(s):
    bid_price, ask_price = get_bid_ask_price(s)
    ## hardcoded values for testing on BTCUSDT
    dipseeker = {
        'symbol'        : SYMBOL,
        'name'          : DIPSEEKER,
        'INTERVAL'      : '1m',
        'LIMIT'         : 5,
        'BUDGET'        : 75.0,
        'DTHR'          : XCH['MIN_NOTIONAL']*2,
        'BUY_QTY'       : XCH['MIN_NOTIONAL']/ask_price,
        'MAX_NUM_ORDERS': XCH['MAX_NUM_ORDERS'],
    }
    buyer = {
        'symbol'        : SYMBOL,
        'name'          : BUYER,
        'BUY_TIMEOUT'   : 5*60,
    }
    seller = {
        'symbol'        : SYMBOL,
        'name'          : SELLER,
        'SELL_TIMEOUT'  : 60*60,
    }
    logmeta(dipseeker, buyer, seller)

def dipseeker(s):
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': DIPSEEKER})
    if M is None: raise MetaNotFound
    dip = get_mrkt_dip(s, M['INTERVAL'], M['LIMIT'])
    exps, blnc = get_mrkt_info(s)
    bid_price, ask_price = get_bid_ask_price(s)
    logstate(M['DTHR'], dip, exps['total'], blnc['total'], ask_price)
    if dip > M['DTHR']:
        if blnc['free'] < (M['BUY_QTY']*ask_price):
            logwarn('not enough free balance')
        else:
            logbuy(str(uuid.uuid4()), ask_price, M['BUY_QTY'], M['DTHR'])

def buyer(s):
    log = lambda text: logtext('info', text, magenta)
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': BUYER})
    if M is None: raise MetaNotFound
    for buy in DB.buy.find({'symbol': SYMBOL, 'status':'OPENED'}):
        buy_id = buy['buy_id']
        r = get_order(s, buy_id)
        if r is None:
            new_price, new_qty = fix_order(buy['orig_price'], buy['orig_qty'])
            log(f'post new buy order id={buy_id}')
            post_order(s, 'BUY', buy_id, new_price, new_qty)
        elif r is not None:
            executedQty         = float(r['executedQty'])
            cummulativeQuoteQty = float(r['cummulativeQuoteQty'])
            age_in_seconds      = (datetime.now() - buy['open_time']).seconds
            if r['status'] == 'FILLED':
                log(f'buy order filled id={buy_id}')
                sell_id = str(uuid.uuid4())
                upd_fields = {
                    'close_time' : datetime.now(),
                    'status'     : 'CLOSED',
                    'sell_id'    : sell_id,
                    'price'      : cummulativeQuoteQty/executedQty,
                    'qty'        : executedQty,
                }
                DB.buy.update_one({'buy_id' : buy_id}, {"$set": upd_fields})
                sell_price = cummulativeQuoteQty/executedQty+buy['proft']
                logsell(sell_id, buy_id, sell_price, executedQty)
            elif age_in_seconds > M['BUY_TIMEOUT']:
                log(f'buy order timeout executed qty={buy["qty"]} id={buy_id}')
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
                DB.buy.update_one({'buy_id' : buy_id}, {"$set": upd_fields})

def seller(s):
    log = lambda text: logtext('info', text, green)
    M = DB.meta.find_one({'symbol': SYMBOL, 'name': SELLER})
    if M is None: raise MetaNotFound
    for sell in DB.sell.find({'symbol': SYMBOL, 'status':'OPENED'}):
        sell_id, buy_id = sell['sell_id'], sell['buy_id']
        r = get_order(s, sell_id)
        bid_price, ask_price = get_bid_ask_price(s)
        if r is None and bid_price > sell['orig_price']:
            new_price, new_qty = fix_order(sell['orig_price'], sell['orig_qty'])
            log(f'post new sell order id={sell_id}')
            post_order(s, 'SELL', sell_id, new_price, new_qty)
        elif r is not None:
            executedQty         = float(r['executedQty'])
            cummulativeQuoteQty = float(r['cummulativeQuoteQty'])
            age_in_seconds      = (datetime.now() - sell['open_time']).seconds
            if r['status'] == 'FILLED':
                log(f'sell order filled id={sell_id}')
                upd_fields = {
                    'close_time' : datetime.now(),
                    'status'     : 'CLOSED',
                    'price'      : cummulativeQuoteQty/executedQty,
                    'qty'        : executedQty,
                }
                DB.sell.update_one({'sell_id' : sell_id}, {"$set": upd_fields})
            elif age_in_seconds > M['SELL_TIMEOUT']:
                log(f'sell order timeout executed qty={sell["qty"]} id={sell_id}')
                delete_order(s, sell_id)
                sell_id2 = str(uuid.uuid4())
                upd_fields = {
                    'close_time' : datetime.now(),
                    'status'     : 'CLOSED',
                    'sell_id2'   : sell_id2,
                    'price'      : cummulativeQuoteQty/executedQty if executedQty > 0.0 else 0.0,
                    'qty'        : executedQty,
                }
                DB.sell.update_one({'sell_id' : sell_id}, {"$set": upd_fields})
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
    parser.add_argument("asset", help="asset to buy/sell")
    parser.add_argument("quote", help="quote for asset")
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
    ## set market to operate on
    ASTQUT = args.asset, args.quote
    SYMBOL = ''.join(ASTQUT)
    loginfo('thread main started')
    try:
        ## set min price*qty of order, also checks connection and symbol 
        XCH = get_exchange_info()
        ## starts the threads
        threads = []
        if not args.nomaestro:   threads.append(start_thread(MAESTRO,   10))
        time.sleep(1) ## ensures maestro can create meta collection first
        if not args.nodipseeker: threads.append(start_thread(DIPSEEKER, 10))
        if not args.nobuyer:     threads.append(start_thread(BUYER,     10))
        if not args.noseller:    threads.append(start_thread(SELLER,    10))
        ## monitor all threads, in case anyone of them die, finish execution
        is_fatal_error = False
        while not is_fatal_error:
            time.sleep(1)
            for thread, stop, name, lockfp in threads:
                if not thread.is_alive():
                    logerror(f'thread {name} died')
                    is_fatal_error = True
    except KeyboardInterrupt:
        logwarn('ctrl-c pressed')
    except Exception:
        logerror(exc())
    finally:
        for thread, stop, name, lockfp in threads:
            stop.set()
            thread.join()
            unlock(lockfp)
    loginfo('thread main ended')

