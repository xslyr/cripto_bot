import gc
from tqdm import tqdm
from .estrategias import *
from .trader import TraderBinance

class BackTest:

    def __init__(self, monitor):
        self.monitor = monitor.copy()


    def test_strategy(self, class_name:str, fake_qtd:float=0.0004, timeview='1m'):
        RegistrarLog(f'Starting Backtest to Strategy {class_name}')
        dataframe = self.monitor[timeview]
        trade_register = TraderBinance(operation_mode='backtest')
        rowlimit = dataframe.shape[0]
        progress = tqdm(total=rowlimit-25)
        context_class = globals()[class_name]()
        order_pairs = {}

        for idx in range(25, rowlimit):
            stats_generator = Gerador_Estatistico( { timeview: dataframe.head(idx) } )
            result = context_class.verificar_condicoes(stats_generator)
            data_backtest = {}
            data_backtest['opentime'] = dataframe.head(idx).iloc[-1]['opentime']
            data_backtest['close'] = dataframe.head(idx).iloc[-1]['close']
            if result == 'BUY':
                order_pairs[context_class.id] = trade_register.buy(context_class.id, 'BTCUSDT', fake_qtd, data_backtest)
            elif result == 'SELL':
                trade_register.sell(order_pairs[context_class.id], 'BTCUSDT', fake_qtd, data_backtest)

            stats_generator = None
            gc.collect()
            progress.update(1)


