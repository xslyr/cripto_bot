import random, os
from datetime import datetime

import numpy as np
from binance.spot import Spot
from .logger import RegistrarLog
from .conector_bd import DataBase

# TODO: Implementar método buy com a possibilidade de ordem LIMIT
# TODO: Implementar método sell com a possibilidade de ordem LIMIT

class TraderBinance:

    def __init__(self, operation_mode = 'live'):
        '''
        :param operation_mode: options available: /
            'live': normal operation, doing real trades on binance exchange.
            'livetest': real time test with data received by websocket
            'backtest': test with past data
        '''
        self.operation_mode = operation_mode # expected str: 'live','livetest' or 'backtest'
        apikey = os.environ['APIKEY']
        secretkey = os.environ['SECRETKEY']
        self.trader = Spot(apikey, secretkey)

    def get_price(self, symbol):
        try: return np.float64(self.trader.ticker_price(symbol)['price'])
        except Exception as e: RegistrarLog(e)


# **************************************** BUY ****************************************
    def buy(self, strategy_id:int, symbol:str, quantity:float, data_backtest=None):
        # TODO: A ocorrência de uma lista de preços no JSON de ordens é por operarmos pouco capital. Melhorar isso para que no futuro visualizarmos o vwap dessa lista, que é o mais correto.
        order = {
            'transactTime': datetime.now().timestamp() if data_backtest==None else data_backtest['opentime'],
            'orderId': -1*random.randrange(00000000000,99999999999),
            'symbol': symbol,
            'side': 'BUY',
            'price': 0,
            'origQty': quantity,
            'cummulativeQuoteQty': 0,
            'type': 'MARKET'
        }

        if self.operation_mode == 'livetest':
            order['price'] = self.get_price(symbol)
            order['cummulativeQuoteQty'] = order['price'] * quantity
        elif self.operation_mode == 'backtest':
            order['price'] = data_backtest['close']
            order['cummulativeQuoteQty'] = order['price'] * quantity
        else:
            response = self.trader.new_order(symbol=symbol, side="BUY", type="MARKET", quantity=quantity)
            order['transactTime'] = np.int64(response['transactTime'])
            order['orderId'] = np.int64(response['orderId'])
            order['price'] = np.float64(response['fills'][0]['price'])
            order['cummulativeQuoteQty'] = np.float64(response['cummulativeQuoteQty'])

        RegistrarLog('order: '+str(order))
        DataBase().insert_info('orders', order)

        performance = {
            'strategy_id': strategy_id,
            'status': 'open',
            'symbol': symbol,
            'buy_order': order['orderId'],
            'sell_order': 0,
            'delta_time_s': order['transactTime'],
            'delta_assetprice': order['price'],
            'delta_quoteprice': order['cummulativeQuoteQty'],
            'trade_qty': quantity}

        RegistrarLog('performance: ' + str(performance))
        DataBase().insert_info('trade_performance', dictionary=performance)

        return order['orderId']

# **************************************** SELL ****************************************
    def sell(self, order_pair:int, symbol:str, quantity:float, data_backtest=None):
        order = {
            'transactTime': datetime.now().timestamp() if data_backtest==None else data_backtest['opentime'],
            'orderId': -1*random.randrange(00000000000, 99999999999),
            'symbol': symbol,
            'side': 'SELL',
            'price': 0,
            'origQty': quantity,
            'cummulativeQuoteQty': 0,
            'type': 'MARKET'
        }

        if self.operation_mode=='livetest':
            order['price'] = self.get_price(symbol)
            order['cummulativeQuoteQty'] = order['price'] * quantity
        elif self.operation_mode == 'backtest':
            order['price'] = data_backtest['close']
            order['cummulativeQuoteQty'] = order['price'] * quantity
        else:
            response = self.trader.new_order(symbol=symbol, side="SELL", type="MARKET", quantity=quantity)
            order['transactTime'] = np.int64(response['transactTime'])
            order['orderId'] = np.int64(response['orderId'])
            order['price'] = np.float64(response['fills'][0]['price'])
            order['cummulativeQuoteQty'] = np.float64(response['cummulativeQuoteQty'])

        RegistrarLog('order: ' + str(order))
        DataBase().insert_info('orders', order)

        RegistrarLog(f'order_pair: {order_pair}')
        trade = DataBase().get_table('trade_performance')
        trade = trade.loc[ trade.buy_order == int(order_pair) ].iloc[-1]

        delta_time = datetime.fromtimestamp(np.float64(order['transactTime'])/1000) - datetime.fromtimestamp(np.float64(trade['delta_time_s'])/1000)
        delta_assetprice = np.float64(order['price']) - np.float64(trade['delta_assetprice'])
        delta_quoteprice = np.float64(order['cummulativeQuoteQty']) - np.float64(trade['delta_quoteprice'])

        performance = {
            'strategy_id': int(trade.strategy_id),
            'status': 'closed',
            'symbol': symbol,
            'buy_order': int(trade.buy_order),
            'sell_order': int(order['orderId']),
            'delta_time_s': delta_time.seconds,
            'delta_assetprice': delta_assetprice,
            'delta_quoteprice': delta_quoteprice,
            'trade_qty': quantity
        }

        RegistrarLog('performance: ' + str(tuple(performance)))
        DataBase().update_info('trade_performance', performance, f'buy_order={order_pair}')

        return delta_quoteprice
