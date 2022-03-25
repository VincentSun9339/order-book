import enum
import queue
import time
from collections import defaultdict


class Type(enum.Enum):
    BUY = 0
    SELL = 1


def get_timestamp():
    """ Microsecond timestamp """
    return int(1e6 * time.time())


class OrderBook(object):
    def __init__(self):
        """ Orders stored as two defaultdicts of {price:[orders at price]}
            Orders sent to OrderBook through OrderBook.unprocessed_orders queue
        """
        self.bid_prices = []
        self.bid_quantity = []
        self.offer_prices = []
        self.offer_quantity = []
        self.bids = defaultdict(list)
        self.offers = defaultdict(list)
        self.unprocessed_orders = queue.Queue()
        self.trades = queue.Queue()
        self.order_id = 0

    def new_order_id(self):
        self.order_id += 1
        return self.order_id

    @property
    def max_bid(self):
        if self.bids:
            return max(self.bids.keys())
        else:
            return 0.

    @property
    def min_offer(self):
        if self.offers:
            return min(self.offers.keys())
        else:
            return float('inf')
    '''判断订单是否能够成交'''
    def process_order(self, incoming_order):
        """ Main processing function. If incoming_order matches delegate to process_match."""
        incoming_order.timestamp = get_timestamp()
        incoming_order.order_id = self.new_order_id()
        if incoming_order.type == Type.BUY:
            if incoming_order.price >= self.min_offer and self.offers:
                self.process_match(incoming_order)
            else:
                self.bids[incoming_order.price].append(incoming_order)
        else:
            if incoming_order.price <= self.max_bid and self.bids:
                self.process_match(incoming_order)
            else:
                self.offers[incoming_order.price].append(incoming_order)

    def process_match(self, incoming_order):
        """ Match an incoming order against orders on the other side of the book, in price-time priority."""
        levels = self.bids if incoming_order.type == Type.SELL else self.offers
        prices = sorted(levels.keys(), reverse=(incoming_order.type == Type.SELL))

        def price_doesnt_match(book_price):
            if incoming_order.type == Type.BUY:
                return incoming_order.price < book_price
            else:
                return incoming_order.price > book_price

        for (i, price) in enumerate(prices):
            if (incoming_order.quantity == 0) or (price_doesnt_match(price)):
                break
            orders_at_level = levels[price]
            for (j, book_order) in enumerate(orders_at_level):
                if incoming_order.quantity == 0:
                    break
                trade = self.execute_match(incoming_order, book_order)
                incoming_order.quantity = max(0, incoming_order.quantity - trade.quantity)
                book_order.quantity = max(0, book_order.quantity - trade.quantity)
                self.trades.put(trade)
            levels[price] = [o for o in orders_at_level if o.quantity > 0]
            if len(levels[price]) == 0:
                levels.pop(price)
        # If the incoming order has not been completely matched, add the remainder to the order book
        if incoming_order.quantity > 0:
            same_type = self.bids if incoming_order.type == Type.BUY else self.offers
            same_type[incoming_order.price].append(incoming_order)

    def execute_match(self, incoming_order, book_order):
        trade_quantity = min(incoming_order.quantity, book_order.quantity)
        return Trade(incoming_order.type, book_order.price, trade_quantity, incoming_order.order_id, book_order.order_id)

    def book_summary(self):
        self.bid_prices = sorted(self.bids.keys(), reverse=True)
        self.offer_prices = sorted(self.offers.keys())
        self.bid_quantity = [sum(o.quantity for o in self.bids[p]) for p in self.bid_prices]
        self.offer_quantity = [sum(o.quantity for o in self.offers[p]) for p in self.offer_prices]

    def show_book(self):
        self.book_summary()
        print('Sell side:')
        if len(self.offer_prices) == 0:
            print('EMPTY')
        for i, price in reversed(list(enumerate(self.offer_prices))):
            print('({0}) Price={1}, Total units={2}'.format(i + 1, self.offer_prices[i], self.offer_quantity[i]))
        print('Buy side:')
        if len(self.bid_prices) == 0:
            print('EMPTY')
        for i, price in enumerate(self.bid_prices):
            print('({0}) Price={1}, Total units={2}'.format(i + 1, self.bid_prices[i], self.bid_quantity[i]))
        print()


class Order(object):
    def __init__(self, type, price, quantity, timestamp=None, order_id=None, underlying=None, instrument=None, product=None, status=None):
        self.type = type
        self.price = price
        self.quantity = quantity
        self.timestamp = timestamp
        self.order_id = order_id
        self.underlying = underlying
        self.instrument = instrument
        self.product = product
        self.status = status


    def __repr__(self):
        return '{0} {1} units at {2}; ID:{4}; Underlying:{5}; Instrument:{6}; Product：{7}; Status:{8}'.format(self.type, self.quantity, self.price, self.timestamp, self.order_id, self.underlying, self.instrument, self.product, self.status)


class Trade(object):
    def __init__(self, incoming_type, incoming_price, trade_quantity, incoming_order_id, book_order_id):
        self.type = incoming_type
        self.price = incoming_price
        self.quantity = trade_quantity
        self.incoming_order_id = incoming_order_id
        self.book_order_id = book_order_id

    def __repr__(self):
        return 'Executed: {0} {1} units at {2}'.format(self.type, self.quantity, self.price)


if __name__ == '__main__':
    # print('Example 1:')
    # ob = OrderBook()
    # orders = [Order(Type.BUY, 1., 2),
    #           Order(Type.BUY, 2., 3, 2),
    #           Order(Type.BUY, 1., 4, 3)]
    # print('We receive these orders:')
    # for order in orders:
    #     print(order)
    #     ob.unprocessed_orders.put(order)
    # while not ob.unprocessed_orders.empty():
    #     ob.process_order(ob.unprocessed_orders.get())
    # print()
    # print('Resulting order book:')
    # ob.show_book()

    print('Example:')
    ob = OrderBook()
    orders = [Order(Type.BUY, 12.23, 10,20210917,8,'BTCUSD','BTC-USD-210917','future','completed'),
              Order(Type.BUY, 12.31, 20,20210916,7,'BTCUSD','BTC-USD-210916','forward','completed'),
              Order(Type.SELL, 13.55, 5,20210915,6,'BTCUSD','BTC-USD-210915','future','pending'),
              Order(Type.BUY, 12.23, 5,20210914,5,'BTCUSD','BTC-USD-210914','forward','completed'),
              Order(Type.BUY, 12.25, 15,20210913,4,'BTCUSD','BTC-USD-210913','future','completed'),
              Order(Type.SELL, 13.31, 5,20210912,3,'BTCUSD','BTC-USD-210912','future','pending'),
              Order(Type.BUY, 12.25, 30,20210911,2,'BTCUSD','BTC-USD-210911','swap','completed'),
              Order(Type.SELL, 13.31, 5,20210910,1,'BTCUSD','BTC-USD-210910','future','completed')]
    print('We receive these orders:')
    for order in orders:
        print(order)
        ob.unprocessed_orders.put(order)
    while not ob.unprocessed_orders.empty():
        ob.process_order(ob.unprocessed_orders.get())
    print()
    print('Resulting order book:')
    ob.show_book()

    offer_order = Order(Type.SELL, 12.25, 100, 20211003,9,'BTCUSD','BTC-USD-211003','future','completed')
    print('Now we get a sell order： {}'.format(offer_order))
    print('This removes the first two buy orders and creates a new price level on the sell side')
    ob.unprocessed_orders.put(offer_order)
    ob.process_order(ob.unprocessed_orders.get())
    ob.show_book()
