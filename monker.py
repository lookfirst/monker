import requests, hashlib, hmac, time, base64
import json, urllib, pymongo, threading, uuid

from traceback import format_exc as exc
from pdb import set_trace as trace
from datetime import datetime
from sys import stdout

## print more output to the terminal
VERBOSE = True

## NOTE: it would be nice if I could update these variables using a REST
##       interface. the other program could be running ML to improve the
##       profitability. Also interesthing if there was a webserver to
##       vizualise the state variables.

## market and budget definition
MRKT   = 'ETHTUSD'        ## market to trade on
BUDGET = 90.0             ## budget for market (zero for entire balance)

## internal state parameters
BUY_QTY       = 90.0      ## every trade uses the same investiment
BUY_TIMEOUT   = 5*60      ## max time it insists in buying an asset (sec)
SELL_TIMEOUT  = 60*60     ## max time before it cancels sell order (sec)
FEES          = 0.50      ## fees to buy/sell
DTHR          = FEES*5    ## dip threshold for buy/sell (abs value)
TICK_INTERVAL = '5m'      ## window size for market analysis
TICK_PER_OPER = 5         ## target ticks per operation (1 op every 25min)

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
db_client = pymongo.MongoClient('mongodb://localhost:27017/')
db        = db_client.monker

def logbuy(buy_id, price):
    stdout.write('BUY: ')
    obj = {
        'time'    : datetime.now(), ## creation time
        'market'  : MRKT,           ## market name
        'buy_id'  : buy_id,         ## exchange order buy id 
        'sell_id' : '',             ## exchange order sell id
        'status'  : 'OPENED',       ## [OPENED, CLOSED]
        'price'   : price,          ## payed price (updated later)
        'target'  : 0.0,            ## target sales price (updated later)
        'qty'     : BUY_QTY,        ## total quantity purchased (updated later)
    }
    if VERBOSE: print(obj)
    db.buy.insert_one(obj)

def logsell(sell_id, buy_id, qty):
    stdout.write('SELL: ')
    obj = {
        'time'    : datetime.now(), ## creation time
        'market'  : MRKT,           ## market name
        'buy_id'  : buy_id,         ## exchange order buy id
        'sell_id' : sell_id,        ## exchange order sell id
        'sell_id2': '',             ## next try sell id (if any, updated later)
        'status'  : 'OPENED',       ## [OPENED, CLOSED]
        'price'   : 0.0,            ## sold price (updated later)
        'orig_qty': qty,            ## total quantity for sale
        'qty'     : 0.0,            ## total quantity sold (updated later)
    }
    if VERBOSE: print(obj)
    db.sell.insert_one(obj)

def logstate(dip, exps, blnc, lqdy, price):
    stdout.write('STATE: ')
    obj = {
        'time'   : datetime.now(),
        'market' : MRKT,
        'dthr'   : DTHR,
        'dip'    : dip,
        'exps'   : exps,
        'blnc'   : blnc,
        'lqdy'   : lqdy,
        'price'  : price,
    }
    if VERBOSE: print(obj)
    db.dip.insert_one(obj)

def logtext(level, text):
    stdout.write('LOGGING: ')
    obj = {
        'time'   : datetime.now(),
        'market' : MRKT,
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

def delete_order(s, id)
    return delete(s, '/api/v3/order', True,
                  symbol=MRKT,
                  origClientOrderId=id)

def get_accm_diff(s):
    return -3.0

def get_mrkt_info(s):
    ## returns mrkt (exps, bln, price)
    ## liquidity = budget - exps
    return 0.5, 80.0, 100

interval_cnter = 0
def is_interval_edge(period):
    global interval_cnter
    isedge = interval_cnter == 0
    interval_cnter = (interval_cnter+1) % 60
    return isedge

def thread_entry(stop_event):
    try:
        loginfo('thread started')
        s = requests.Session()
        s.headers.update(DFT_API_HDRS)
        while not stop_event.is_set():
            time.sleep(1)
            if not is_interval_edge(30):
                continue
            dip = get_accm_diff(s)
            exps, blnc, price = get_mrkt_info(s)
            lqdy = BUDGET - exps
            logstate(dip, exps, blnc, lqdy, price)
            ## dip price monitor and buy order db creator
            if dip < (-DTHR):
                if lqdy < BUY_QTY:
                    logwarn('not enough liquidity')
                if blnc < lqdy:
                    logwarn('not enough balance')
                else:
                    buy_id = str(uuid.uuid4())
                    logbuy(buy_id, price)
            ## post/delete buy orders and update buy db entries when completed
            for buy in db.buy.find({'status':'OPENED'}):
                buy_id = buy['buy_id']
                r = get_order(s, buy_id)
                if r is None:
                    post_order(s, 'BUY', buy_id, price, BUY_QTY)
                elif r is not None:
                    executedQty         = float(r['executedQty'])
                    cummulativeQuoteQty = float(r['cummulativeQuoteQty'])
                    age_in_seconds      = (datetime.now() - buy['time']).seconds
                    if r['status'] == 'FILLED':
                        sell_id = str(uuid.uuid4())
                        upd_fields = {
                            'status' : 'CLOSED',
                            'sell_id': sell_id,
                            'price'  : cummulativeQuoteQty/executedQty,
                            'target' : cummulativeQuoteQty/executedQty+DTHR,
                            'qty'    : executedQty,
                        }
                        db.buy.update({'buy_id' : buy_id}, {"$set": upd_fields})
                        logsell(sell_id, buy_id, executedQty)
                    elif age_in_seconds > BUY_TIMEOUT:
                        delete_order(s, buy_id)
                        if executedQty > 0.0:
                            sell_id = str(uuid.uuid4())
                            upd_fields = {
                                'status' : 'CLOSED',
                                'sell_id': sell_id,
                                'price'  : cummulativeQuoteQty/executedQty,
                                'qty'    : executedQty,
                            }
                            logsell(sell_id, buy_id, executedQty)
                        else:
                            upd_fields = { 'status' : 'CLOSED', }
                        db.buy.update({'buy_id' : buy_id}, {"$set": upd_fields})
            ## post/delete sell orders and update sell db entries when completed
            for sell in db.sell.find({'status':'OPENED'}):
                sell_id, buy_id = sell['sell_id'], sell['buy_id']
                buy = db.buy.find_one({'buy_id': buy_id})
                target = buy['target']
                r = get_order(s, sell_id)
                if r is None and price > target:
                    post_order(s, 'SELL', sell_id, target, sell['orig_qty'])
                elif r is not None:
                    executedQty         = float(r['executedQty'])
                    cummulativeQuoteQty = float(r['cummulativeQuoteQty'])
                    age_in_seconds      = (datetime.now() - sell['time']).seconds
                    if r['status'] == 'FILLED':
                        upd_fields = {
                            'status' : 'CLOSED',
                            'price'  : cummulativeQuoteQty/executedQty,
                            'qty'    : executedQty,
                        }
                        db.sell.update({'sell_id' : sell_id}, {"$set": upd_fields})
                    elif age_in_seconds > SELL_TIMEOUT:
                        delete_order(s, sell_id)
                        sell_id2 = str(uuid.uuid4())
                        upd_fields = {
                            'status'   : 'CLOSED',
                            'sell_id2' : sell_id2,
                            'price'    : cummulativeQuoteQty/executedQty if executedQty > 0.0 else 0.0,
                            'qty'      : executedQty,
                        }
                        db.sell.update({'sell_id' : sell_id}, {"$set": upd_fields})
                        open_qty = sell['orig_qty'] - sell['qty']
                        logsell(sell_id2, buy_id, open_qty)
    except Exception:
        logerror(exc())
    finally:
        loginfo('thread ended')

def start_thread():
    stop_event = threading.Event()
    thread = threading.Thread(target=thread_entry, args=(stop_event,))
    thread.start()
    return thread, stop_event

def __debug__():
    s = requests.Session()
    s.headers.update(DFT_API_HDRS)
    #post_order(s, 160, 1)
    exit(0)

if __name__ == '__main__':
    __debug__()
    loginfo('monker main started')
    try:
        t, stop = start_buy_thread()
        while True:
            time.sleep(1)
            if not t.is_alive():
                logerror('thread died')
                break
    except KeyboardInterrupt:
        loginfo('ctrl-c pressed')
    except Exception:
        logerror(exc())
    finally:
        stop.set()
        t.join()
    loginfo('monker main terminated')

