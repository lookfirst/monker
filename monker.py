from sys import stdout
from pdb import set_trace as trace

import time

from binance import Binance
from bitstamp import Bitstamp
from bittrex import Bittrex

AMOUNT = 0.2

def putc(c):
    stdout.write(c)
    stdout.flush()

def monker():
    try:
        binance = Binance()
        bitstamp = Bitstamp()
        bittrex = Bittrex()
       
        print('loading order book')
        while not (binance.asks  and
                   bitstamp.asks and
                   bittrex.asks):
            time.sleep(1)
            putc('.')
        stdout.write('\n')

        print('starting monitoring prices')

        while True:
            time.sleep(1)
            putc('.')

            binance_price  = binance.get_ask_for(AMOUNT)
            bitstamp_price = bitstamp.get_ask_for(AMOUNT)
            bittrex_price  = bittrex.get_ask_for(AMOUNT)
            print('\n----------\nprices:')
            print(f'  {binance_price:.2f}')
            print(f'  {bitstamp_price:.2f}')
            print(f'  {bittrex_price:.2f}')

            #d = binance_price - bitstamp_price 
            #if abs(d) > 1.5:
            #    print(f'\nbinance_price - bitstamp_price = {d:.2f}')

            #d = binance_price - bittrex_price 
            #if abs(d) > 1.5:
            #    print(f'\nbinance_price - bittrex_price = {d:.2f}')

    except KeyboardInterrupt:
        print('\nexiting politely')
        binance.stop()
        bitstamp.stop()
        bittrex.stop()

if __name__ == '__main__':
    monker()

