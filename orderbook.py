class Order:
    def __init__(self, price, qnty):
        self.price = float(price)
        self.qnty = float(qnty)
    def __str__(self):
        return repr(self)
    def __repr__(self):
        return f'Order({self.price:.2f}, {self.qnty})'

class Book(dict):
    def __init__(self, is_absolute):
        super().__init__()
        self.is_absolute = is_absolute

    def add(self, order):
        if self.is_absolute:
            if order.qnty > 0.0:
                self[order.price] = order
            else:
                try:
                    del self[order.price]
                except KeyError:
                    pass
        else:
            if order.price not in self:
                self[order.price] = order
            else:
                self[order.price].qnty += order.qnty
                if abs(self[order.price].qnty) < 1e-15:
                    del self[order.price]

class OrderBook:
    def __init__(self, is_absolute):
        self.asks = Book(is_absolute)
        self.bids = Book(is_absolute)

    def _get_price_for(self, book, qnty):
        total_price = 0.0
        for price in sorted(book):
            total_price += price * book[price].qnty
            qnty -= book[price].qnty
            if qnty < 0:
                total_price -= price * qnty
                break
        return total_price

    def get_ask_for(self, qnty):
        return self._get_price_for(self.asks, qnty)

    def get_bid_for(self, qnty):
        return self._get_price_for(self.bids, qnty)

    def update_book(self, objs):
        ## implement this on extended classes...
        raise NotImplemented

