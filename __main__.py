from Queue import Queue
from collections import defaultdict
from threading import Lock
from threading import Thread
from json import dumps
from json import loads
from time import sleep
from time import strptime
from time import time

from websocket import create_connection
from websocket import WebSocketTimeoutException
from websocket import WebSocketConnectionClosedException

DEFAULT_DELAY = 1
DEFAULT_POSITION_LIMIT = 100
DEFAULT_ORDER_QUANTITY = 50

EWMA_FACTOR = 0.2
ENTER_THRESHOLD = 0.05

class Options():
    def __init__(self, default=None):
        self.data = default or {}
        self.mu = Lock()

    def get(self, key):
        return self.data.get(key)

    def set(self, key, val):
        if key not in self.data:
            print "Invalid option: %s" % key
        else:
            print "Set %s to %s" % (key, val)
            self.data[key] = val


class SampleBot():
    # XXX change me for actual running
    trader_id = 'trader0'

    def __init__(self):
        self.options = Options({
            'position_limit': DEFAULT_POSITION_LIMIT,
            'order_quantity': DEFAULT_ORDER_QUANTITY,
        })

        self.ws = create_connection('ws://localhost:10914/%s' % self.trader_id,
                                    timeout=0.5)
        self.outbox = Queue()

        self.started = False
        self.done = False
        self.lastActionTime = time()

        self.topBid = {}
        self.topAsk = {}
        self.lastPrices = {}
        self.positions = {}
        self.priceChange = {}

        register_msg = dumps({
            'message_type': 'REGISTER',
        })
        self.outbox.put(register_msg)


    def makeThreads(self):
        parser_t = Thread(target=self.parser, args=())
        reader_t = Thread(target=self.ws_reader, args=())
        writer_t = Thread(target=self.ws_writer, args=())

        return parser_t, reader_t, writer_t


    def parser(self):
        while True:
            key, val = raw_input().strip().split(':')
            self.options.set(key, float(val))


    def ws_reader(self):
        while True:
            try:
                msg = loads(self.ws.recv())
            except WebSocketTimeoutException:
                msg = None
            except WebSocketConnectionClosedException:
                self.outbox.put(None)
                return

            output = self.process(msg)
            if output is not None:
                self.outbox.put(output)


    def ws_writer(self):
        while True:
            msg = self.outbox.get()
            if msg is None:
                break
            else:
                self.ws.send(msg)
        self.ws.close()


    def process(self, msg):
        if msg is not None:
            if msg.get('trader_state'):
                self.positions = msg['trader_state']['positions']

            # ack register
            if msg.get('market_states'):
                for ticker, state in msg['market_states'].iteritems():
                    if len(state['bids']):
                        self.topBid[ticker] = max(map(float, state['bids'].keys()))
                    if len(state['asks']):
                        self.topAsk[ticker] = min(map(float, state['asks'].keys()))
                    self.lastPrices[ticker] = state['last_price']
                    self.priceChange[ticker] = 0

            if msg.get('market_state'):
                state = msg['market_state']
                ticker = state['ticker']

                if len(state['bids']):
                    self.topBid[ticker] = max(map(float, state['bids'].keys()))
                if len(state['asks']):
                    self.topAsk[ticker] = min(map(float, state['asks'].keys()))

                lastPrice = self.lastPrices[ticker]

                self.priceChange[ticker] *= (1-EWMA_FACTOR)
                self.priceChange[ticker] += EWMA_FACTOR * (state['last_price'] - lastPrice)

                self.lastPrices[ticker] = state['last_price']

            if msg.get('message_type') == 'START':
                self.started = True
            elif msg.get('end_time'):
                if not msg.get('end_time').startswith('0001'):
                    self.started = True


        orders = []
        if self.started and time() - self.lastActionTime > DEFAULT_DELAY:
            self.lastActionTime = time()

            # orders.extend(self.marketMake())
            orders.extend(self.momentum())

        if len(orders) > 0:
            action = {
                'message_type': 'MODIFY ORDERS',
                'orders': orders,
            }
            return dumps(action)
        return None


    def marketMake(self):
        # this market making attempt turns out to not be that profitable
        # because of the strong trends in sample case
        orders = []

        for ticker, position in self.positions.iteritems():
            lastPrice = self.lastPrices[ticker]

            if abs(position) > self.options.get('position_limit'):
                orders.append({
                    'ticker': ticker,
                    'buy': position < 0,
                    'quantity': abs(position),
                    'price': lastPrice * (1.5 if position < 0 else 0.5),
                })
            else:
                if ticker in self.topBid:
                    orders.append({
                        'ticker': ticker,
                        'buy': True,
                        'quantity': self.options.get('order_quantity'),
                        'price': self.topBid[ticker] + 0.01,
                    })
                if ticker in self.topAsk:
                    orders.append({
                        'ticker': ticker,
                        'buy': False,
                        'quantity': self.options.get('order_quantity'),
                        'price': self.topAsk[ticker] - 0.01,
                    })
        return orders


    def momentum(self):
        orders = []

        for ticker, change in self.priceChange.iteritems():
            lastPrice = self.lastPrices[ticker]
            if change > ENTER_THRESHOLD:
                orders.append({
                    'ticker': ticker,
                    'buy': True,
                    'quantity': 100,
                    'price': lastPrice * 1.5,
                })
            elif change < -ENTER_THRESHOLD:
                orders.append({
                    'ticker': ticker,
                    'buy': False,
                    'quantity': 100,
                    'price': lastPrice * .5,
                })

        return orders

if __name__ == '__main__':
    bot = SampleBot()
    print "options are", bot.options.data

    for t in bot.makeThreads():
        t.daemon = True
        t.start()

    while not bot.done:
        sleep(0.01)

