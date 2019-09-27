import requests, hashlib, hmac, time
import urllib, pymongo, threading, uuid

from traceback import format_exc as exc
from pdb import set_trace as trace
from datetime import datetime
from sys import stdout

## print more output to the terminal
VERBOSE = True

## NOTE: the time difference between open/close of sell entries
##       can be a very good indicative of the market mood (TBC)

## market definition
MRKT_PAIR = None          ## tuple with pair asset,market (command line argument)
MRKT      = ''            ## market to trade on (joined market pair)

## binance keys and url
KEY    = 'ZADfFZTF0Djk5HozzmbbPhK1TWqz9SROYaivOcQPbJPIEscP24Rhc8RzMGx7pvdz'
SECRET = '5SNpXT5wRqDBgEfvl7b2gTLq1fKnNqDmteFZMwXfrbOBKDLSt4QHA7Vu1UcIejYx'
URL    = 'https://api.binance.com'
DFT_API_HDRS = {
    'Accept'       : 'application/x-www-form-urlencoded',
    'User-Agent'   : 'monker',
    'X-MBX-APIKEY' : KEY,
}

## global variables
DB_CLIENT = pymongo.MongoClient('mongodb://localhost:27017/')
DB        = DB_CLIENT.monker

def logbuy(buy_id, price, qty, proft):
    stdout.write('BUY: ')
    obj = {
        'open_time' : datetime.now(), ## open time
        'close_time': '',             ## close time (updated later)
        'market'    : MRKT,           ## market name
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
        'market'    : MRKT,           ## market name
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
        'market' : MRKT,
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
        'market' : MRKT,
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

def get(s, uri, is_signed, **params):
    if is_signed:
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = sign(**params)
    params = urllib.parse.urlencode(params)
    return s.get(URL + uri, params=params)

def post(s, uri, is_signed, **params):
    if is_signed:
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = sign(**params)
    params = urllib.parse.urlencode(params)
    return s.post(URL + uri, params=params)

def delete(s, uri, is_signed, **params):
    if is_signed:
        params['timestamp'] = int(time.time() * 1000)
        params['signature'] = sign(**params)
    params = urllib.parse.urlencode(params)
    return s.delete(URL + uri, params=params)

def post_order(s, side, id, price, qty):
    return post(s, '/api/v3/order', True,
                symbol=MRKT,
                side=side,
                type='LIMIT',
                timeInForce='GTC',
                newOrderRespType='RESULT',
                quantity=qty,
                price=price,
                recvWindow=5000,
                newClientOrderId=id)

def get_open_orders(s):
    return get(s, '/api/v3/openOrders', True,
               symbol=MRKT).json()

def get_order(s, id):
    ## TODO should return None if order not found
    return get(s, '/api/v3/order', True,
               symbol=MRKT,
               origClientOrderId=id).json()

def delete_order(s, id):
    return delete(s, '/api/v3/order', True,
                  symbol=MRKT,
                  origClientOrderId=id)

def get_accm_diff(s):
    ## TODO
    return -3.0

def get_mrkt_info(s):
    ## TODO
    ## exps is total amount of assets
    ## blnc is total amount of tusd 
    ## returns mrkt (exps, blnc)
    ## liquidity = budget - exps
    return 0.5, 80.0

def get_asset_price(s):
    ## TODO
    return 4

def dipseeker(s):
    M = DB.meta.find_one({'name':'DIPSEEKER'})
    dip = get_accm_diff(s, M['TICK_INTERVAL'])
    cur_exps, cur_blnc = get_mrkt_info(s)
    cur_price = get_asset_price(s)
    lqdy = M['BUDGET'] - cur_exps
    logstate(M['DTHR'], dip, cur_exps, cur_blnc, lqdy, cur_price)
    if dip < (-M['DTHR']):
        if lqdy < M['BUY_QTY']:
            logwarn('not enough liquidity')
        if cur_blnc < lqdy:
            logwarn('not enough balance')
        else:
            logbuy(str(uuid.uuid4()), cur_price, M['BUY_QTY'], M['DTHR'])

def buyer(s):
    M = DB.meta.find_one({'name':'BUYER'})
    for buy in DB.buy.find({'status':'OPENED'}):
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
    M = DB.meta.find_one({'name':'SELLER'})
    for sell in DB.sell.find({'status':'OPENED'}):
        sell_id, buy_id = sell['sell_id'], sell['buy_id']
        r = get_order(s, sell_id)
        cur_price = get_asset_price(s)
        if r is None and cur_price > sell['orig_price']:
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
            time.sleep(1)
            cnter = (cnter + 1) % period
            if cnter != 0: continue
            if   name == 'DIPSEEKER': dipseeker(s)
            elif name == 'BUYER':     buyer(s)
            elif name == 'SELLER':    seller(s)
            else: raise NotImplemented
    except Exception:
        logerror(exc())
    finally:
        loginfo(f'thread {name} ended')

def start_thread(name, period):
    stop_event = threading.Event()
    thread = threading.Thread(target=thread_entry, args=(stop_event, name, period))
    thread.start()
    return thread, stop_event

def debug():
    s = requests.Session()
    s.headers.update(DFT_API_HDRS)
    #post_order(s, 160, 1)
    exit(0)

if __name__ == '__main__':
    debug()
    loginfo('thread main started')
    try:
        threads = []
        if len(argv) != 3:
            stderr.write(f'USAGE: {argv[0]} <ASSET> <MARKET>\n')
            exit(1)
        MRKT_PAIR = (argv[0].upper(), argv[1].upper())
        MRKT      = (argv[0] + argv[1]).upper()
        threads.append(start_thread('DIPSEEKER', 30))
        threads.append(start_thread('BUYER',     30))
        threads.append(start_thread('SELLER',    30))
        while True: time.sleep(1)
    except KeyboardInterrupt:
        loginfo('ctrl-c pressed')
    except Exception:
        logerror(exc())
    finally:
        for thread, stop in threads:
            stop.set()
            thread.join()
    loginfo('thread main ended')

