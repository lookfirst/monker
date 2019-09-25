from sys import stdout
from pdb import set_trace as trace

import time

from binance import Binance
from bitstamp import Bitstamp
from bittrex import Bittrex

AMOUNT = 0.5

def putc(c):
    stdout.write(c)
    stdout.flush()

def monker():
    try:
        binance = Binance()
        bitstamp = Bitstamp()
        bittrex = Bittrex()
       
        print(f'binance_bid,binance_ask,binance_quote,bittrex_bid,bittrex_ask,bittrex_quote')

        while not (binance.asks  and
                   bitstamp.asks and
                   bittrex.asks):
            time.sleep(1)

        while True:
            time.sleep(1)

            binance_bid  = binance.get_bid_for(AMOUNT)
            binance_ask  = binance.get_ask_for(AMOUNT)
            binance_quote  = binance.get_trading_quote()

            #bitstamp_price = bitstamp.get_ask_for(AMOUNT)

            bittrex_bid  = bittrex.get_bid_for(AMOUNT)
            bittrex_ask  = bittrex.get_ask_for(AMOUNT)
            bittrex_quote  = bittrex.get_trading_quote()

            print(f'{binance_bid:.06f},{binance_ask:.06f},{binance_quote:.06f},{bittrex_bid:.06f},{bittrex_ask:.06f},{bittrex_quote:.06f}')

    except KeyboardInterrupt:
        print('\nexiting politely')
        binance.stop()
        bitstamp.stop()
        bittrex.stop()

if __name__ == '__main__':
    monker()

