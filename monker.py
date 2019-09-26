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

def logbuy(orderid, price):
    stdout.write('BUY: ')
    obj = {
        'time'    : datetime.now(),
        'orderid' : orderid,
        'market'  : MRKT,
        'price'   : price,
        'status'  : 'NEW', ## [NEW, FILLED, PARTIALLY_FILLED, EXPIRED]
        'isliqd'  : False, ## true when asset is sold (liquidated)
        'target'  : 0.0,   ## zero while buy is not completed
        'qty'     : 0.0,   ## zero while buy is not completed
    }
    if VERBOSE: print(obj)
    db.buy.insert_one(obj)

def logsell(orderid, buy_orderid):
    stdout.write('SELL: ')
    obj = {
        'time'        : datetime.now(),
        'market'      : MRKT,
        'orderid'     : orderid,
        'buy_orderid' : buy_orderid, ## buy order id that it refers
        'price'       : 0.0, ## zero while sell is not completed
        'qty'         : 0.0, ## zero while sell is not completed
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

def post_order(s, orderid, side, price, qty):
    return post(s, '/api/v3/order', True,
                symbol=MRKT,
                side=side,
                type='LIMIT',
                timeInForce='GTC',
                newOrderRespType='RESULT',
                quantity=qty,
                price=price,
                recvWindow=5000,
                newClientOrderId=orderid)

def get_open_orders(s):
    return get(s, '/api/v3/openOrders', True,
               symbol=MRKT).json()

def get_order(s, orderid):
    return get(s, '/api/v3/order', True,
               symbol=MRKT,
               origClientOrderId=orderid).json()

def delete_order(s, orderid)
    return delete(s, '/api/v3/order', True,
                  symbol=MRKT,
                  origClientOrderId=orderid)

def get_accm_diff(s):
    return -3.0

def get_mrkt_info(s):
    ## returns mrkt (exps, bln, price)
    ## liquidity = budget - exps
    return 0.5, 80.0, 100

def buy_thread(stop_event):
    try:
        loginfo('buy thread started')
        s = requests.Session()
        s.headers.update(DFT_API_HDRS)
        while not stop_event.is_set():
            time.sleep(10)
            dip = get_accm_diff(s)
            exps, blnc, price = get_mrkt_info(s)
            lqdy = BUDGET - exps
            logstate(dip, exps, blnc, lqdy, price)
            if dip < (-DTHR):
                if lqdy < BUY_QTY:
                    logwarn('not enough liquidity')
                if blnc < lqdy:
                    logwarn('not enough balance')
                else:
                    orderid = str(uuid.uuid4())
                    post_order(s, orderid, 'BUY', price, BUY_QTY)
                    logbuy(orderid, price)
            for buy in db.buy.find({'status':'NEW'}):
                orderid = buy['orderid']
                r = get_order(s, orderid)
                executedQty         = float(r['executedQty'])
                cummulativeQuoteQty = float(r['cummulativeQuoteQty'])
                new_price           = cummulativeQuoteQty/executedQty
                if r['status'] == 'FILLED':
                    upd_fields = {
                        'status' : 'FILLED',
                        'price'  : new_price,
                        'target' : new_price+DTHR,
                        'qty'    : executedQty,
                    }
                    db.buy.update({'orderid' : orderid}, {"$set": upd_fields})
                else:
                    age = datetime.now() - buy['time']
                    if age.seconds > BUY_TIMEOUT:
                        if executedQty > 0.0:
                            upd_fields = {
                                'status' : 'PARTIALLY_FILLED',
                                'price'  : new_price,
                                'qty'    : executedQty,
                            }
                        else:
                            upd_fields = {
                                'status' : 'EXPIRED',
                                'isliqd' : True, ## makes filter easier to sell
                            }
                        db.buy.update({'orderid' : orderid}, {"$set": upd_fields})
    except Exception:
        logerror(exc())

## TODO join the two threads and remove is liqudated flag...

def sell_thread(stop_event):
    try:
        loginfo('sell thread started')
        s = requests.Session()
        s.headers.update(DFT_API_HDRS)
        while not stop_event.is_set():
            time.sleep(10)
            exps, blnc, price = get_mrkt_info(s)
            for buy in db.buy.find({'isliqd':False}):
                target = buy['target']
                if price > target:
                    buy_orderid = buy['orderid']
                    sell = db.sell.find_one({'buy_orderid':buy_orderid})
                    if sell is not None:
                        orderid = sell['orderid']
                        r = get_order(s, orderid)
                        ## a db entry exists and an order was issued already
                        ## check status of the order and update db, maybe issue
                        ## order again or cancel it..
                        ## db.sell.update({'orderid' : orderid}, {"$set": upd_fields})
                        ## in case sell succeed update isliqd on buy table...
                        ## db.buy.update({'orderid' : buy_orderid}, {"$set": {'isliqd':True}})
                    else:
                        orderid = str(uuid.uuid4())
                        post_order(s, orderid, 'SELL', target, buy['qty'])
                        logsell(orderid, buy_orderid)
    except Exception:
        logerror(exc())

def start_buy_thread():
    stop_event = threading.Event()
    thread = threading.Thread(target=buy_thread, args=(stop_event,))
    thread.start()
    return thread, stop_event

def start_sell_thread():
    stop_event = threading.Event()
    thread = threading.Thread(target=buy_thread, args=(stop_event,))
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
        buy_th, buy_stop_ev   = start_buy_thread()
        sell_th, sell_stop_ev = start_sell_thread()
        while True:
            time.sleep(1)
            if not buy_th.is_alive():
                logerror('buy thread died')
                break
            if not sell_th.is_alive():
                logerror('sell thread died')
                break
    except KeyboardInterrupt:
        loginfo('ctrl-c pressed')
    except Exception:
        logerror(exc())
    finally:
        buy_stop_ev.set()
        sell_stop_ev.set()
        buy_th.join()
        sell_th.join()
    loginfo('monker main terminated')

