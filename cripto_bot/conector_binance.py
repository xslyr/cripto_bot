import numpy as np
import pandas as pd
from datetime import datetime
import requests, json, time, asyncio
from .logger import RegistrarLog
from .conector_bd import DataBase
from binance.websocket.websocket_client import BinanceWebsocketClient



KLINECOLS = ['ticker', 'interval', 'opentime', 'closetime', 'open', 'close', 'high', 'low', 'base_asset_volume',
             'quote_asset_volume', 'number_of_trades', 'taker_asset_volume', 'taker_quote_volume']
             
KLDUMP_RENAME = {0: 'opentime', 6: 'closetime', 1: 'open', 4: 'close', 2: 'high', 3: 'low', 5: 'base_asset_volume',
                 7: 'quote_asset_volume', 8: 'number_of_trades', 9: 'taker_asset_volume', 10: 'taker_quote_volume'}
                 
KLMSG_RENAME = klrename = {'s': 'ticker', 'i': 'interval', 't': 'opentime', 'T': 'closetime', 'o': 'open', 'c': 'close',
                           'h': 'high', 'l': 'low', 'v': 'base_asset_volume', 'q': 'quote_asset_volume',
                           'n': 'number_of_trades', 'V': 'taker_asset_volume', 'Q': 'taker_quote_volume'}
                           
WIDEFLOATS = ['pct_price', 'var_price', 'vwap', 'open', 'high', 'low', 'close', 'base_asset_volume',
              'quote_asset_volume']
WIDECOLS = {
    # 'e': 'etype',				#Event type (tipo de evento).
    'E': 'timestamp',  # Event time (tempo do evento) em milissegundos desde a Época Unix.
    's': 'ticker',  # Símbolo do par de negociação.
    'p': 'pct_price',  # Variação percentual do preço (price change percentage) no período.
    'P': 'var_price',  # Variação de preço (price change) no período.
    'w': 'vwap',  # Preço médio ponderado no período.
    'o': 'open',  # Preço de abertura no período.
    'h': 'high',  # Preço mais alto no período.
    'l': 'low',  # Preço mais baixo no período.
    'c': 'close',  # Preço de fechamento no período.
    'v': 'base_asset_volume',  # Volume total negociado no período.
    'q': 'quote_asset_volume',  # Volume total em dólares negociado no período.
    'O': 'last_open',  # Preço de abertura no período anterior.
    'C': 'last_close',  # Preço de fechamento no período anterior.
    'F': 'first_trade',  # Primeiro trade (primeiro negócio) no período.
    'L': 'last_trade',  # Último trade (último negócio) no período.
    'n': 'number_of_trades'  # Número total de trades no período.
}


class Conector_Binance:
    '''A classe Conector_Binance é responsável por conectar, adquirir e disponibilizar as informações de criptoativos.
    Atributes:
        ticker_monitor: fornece um dataframe atualizado segundo a segundo com a cotação de um determinado criptoativo de um intervalo. (1s, 1m, 5m, 15m, 1h, ...).
        wide_monitor: fornece um dataframe atualizado segundo a segundo com a cotação de vários ativos.

    Methods:
        save_monitor: rotina para salvar os dados de cotacoes no banco de dados. O intuito de fazer isso é ter inputs para os modelos de predição.

    TODO:
        - Implementar o salvamento de parte do widemonitor no banco para o caso de reestartar aplicação rapidamente e já voltar em condições próximas.
        - Implementar outros intervalos gráficos para a rotina save_tickermonitor.
    '''

    _ticker_monitor: dict = {}
    _wide_monitor: pd.DataFrame = None
    _parameters: dict = {}
    _last_data: dict = {}

    def __init__(self):
        DataBase().check_fix_db_integrity()

    @property
    def wide_monitor(self):
        return self._wide_monitor

    @property
    def ticker_monitor(self):
        return self._ticker_monitor

    @property
    def last_data(self):
        return self._last_data

    def start_ticker_monitor(self, ticker: str, intervals=['1m'], websocket: bool = True, memory_size: int = 1000, save_data=False):
        if ticker == '': raise Exception('Ticker cannot be empty.')
        KLINES_INTERVAL_AVAILABLE = DataBase().get_param('KLINES_INTERVAL_AVAILABLE')
        if any([i not in KLINES_INTERVAL_AVAILABLE for i in intervals]):
            raise Exception(f'The graph times available for single monitor is {KLINES_INTERVAL_AVAILABLE}')
        if memory_size <= 50: raise Exception('Memory size must be number greater than 50.')
        self.memory_size = memory_size
        self._ticker_monitor = {str(t): pd.DataFrame([], columns=KLINECOLS) for t in intervals}
        self._ticker_monitor = self.__initial_dump(ticker, intervals)
        if websocket:
            URL = DataBase().get_param('SOCKET_BASE_URL')
            prefix = ''
            if len(intervals) > 1:
                prefix += '/stream?streams='
                for occur in intervals:
                    prefix += '{ticker}@kline_{time}/'.format(ticker=ticker.lower(), time=occur)
            else:
                prefix += '/{ticker}@kline_{time}'.format(ticker=ticker.lower(), time=intervals[0])
            URL += prefix
            #RegistrarLog(f'Url da conexao binance: {URL}')
            self.__tickersocket = BinanceWebsocketClient(stream_url=URL, on_message=self.__get_single)

        if save_data: self.save_monitor(monitor_type='ticker_monitor', ticker=ticker, interval=intervals[0],
                                        websocket=websocket)

    def start_wide_monitor(self, interval='1h', save_data=True, saves_interval=25, line_limit=10000000):
        print('Starting wide monitor ...')
        WIDE_INTERVAL_AVAILABLE = DataBase().get_param('WIDE_INTERVAL_AVAILABLE')
        self.line_limit = line_limit
        if (interval not in WIDE_INTERVAL_AVAILABLE):
            raise Exception(f'The graph times available for wide monitor is {WIDE_INTERVAL_AVAILABLE}')

        if save_data:
            try: self._wide_monitor = DataBase().get_table('wide_monitor')
            except: self._wide_monitor = pd.DataFrame([], columns=list(WIDECOLS.values()))

        URL = DataBase().get_param('SOCKET_BASE_URL')
        URL += '/!ticker_{time}@arr'.format(time=interval)
        self.__widesocket = BinanceWebsocketClient(stream_url=URL, on_message=self.__get_wide)
        time.sleep(2)
        # asyncio.create_task(self.__save_wide_monitor(saves_interval))
        print('Done!')

    def __initial_dump(self, ticker: str, intervals: list):
        for time in intervals:
            r = requests.get(
                f'https://api.binance.com/api/v3/klines?symbol={ticker}&interval={time}&limit={self.memory_size}')
            klines = pd.DataFrame(json.loads(r.text))
            klines = klines.iloc[:, [0, 6, 1, 4, 2, 3, 5, 7, 8, 9, 10]]
            klines = klines.rename(columns=KLDUMP_RENAME)
            klines.index = klines['opentime'].apply(lambda t: datetime.fromtimestamp(t / 1000))
            klines.index.name = 'index'
            klines = klines.astype(np.float64)
            self._ticker_monitor[time] = pd.concat(
                [pd.DataFrame([[ticker, time]] * klines.shape[0], index=klines.index, columns=['ticker', 'interval']),
                 klines], axis=1)
        return self._ticker_monitor

    def __get_single(self, ws, message):
        response = json.loads(message)
        interval = response['k']['i']
        kline = pd.DataFrame(response['k'], index=[0])
        kline = kline[['s', 'i', 't', 'T', 'o', 'c', 'h', 'l', 'v', 'q', 'n', 'V', 'Q']].rename(columns=KLMSG_RENAME)
        self._last_data[interval] = kline.iloc[-1].to_dict()
        kline.index = [datetime.fromtimestamp(kline.at[0, 'opentime'] / 1000)]
        kline.index.name = 'index'
        kline.iloc[:, 2:] = kline.iloc[:, 2:].astype(np.float64)
        self._ticker_monitor[interval] = kline.combine_first(self._ticker_monitor[interval].tail(self.memory_size))

    def __get_wide(self, ws, message):
        new_info = pd.DataFrame(json.loads(message))
        new_info = new_info.drop(['e'], axis=1)
        new_info = new_info.rename(columns=WIDECOLS)
        new_info[WIDEFLOATS] = new_info[WIDEFLOATS].astype(np.float64)
        new_info.index = new_info['timestamp'].apply(lambda r: datetime.fromtimestamp(r / 1000))
        self._wide_monitor = new_info.combine_first(self._wide_monitor.tail(self.line_limit))

    def __save_wide_monitor(self, save_interval):
        pass

    def save_monitor(self, monitor_type='ticker_monitor', **kwargs):
        try:
            ticker = kwargs.get('ticker', 'BTCUSDT')
            interval = kwargs.get('interval', '1m')
            websocket = kwargs.get('websocket', False)

            if monitor_type == 'ticker_monitor':
                asyncio.create_task(self.save_tickermonitor(ticker, interval, websocket))
            else:
                asyncio.create_task((self.__save_wide_monitor(interval)))
        except:
            pass

    async def save_tickermonitor(self, ticker='BTCUSDT', interval='1m', websocket=False):
        try:
            now = round(datetime.now().timestamp() * 1000)
            data = self._ticker_monitor[interval]
            data = data.loc[data.closetime < now]
            registered = DataBase().get_table('single_monitor', f'ticker=\"{ticker}\" and interval=\"{interval}\"')
            tuples_to_insert = list(
                data.loc[~data.opentime.isin(registered.opentime.values)].itertuples(index=False, name=None))

            if DataBase().insert_many('single_monitor', tuples_to_insert):
                last_opentime_registred = tuples_to_insert[-1][2]
            else:
                raise Exception('Some error occured on saving database. The routine save_tickermonitor will quitting now.')

            while websocket:
                await asyncio.sleep(2)
                now = round(datetime.now().timestamp() * 1000)
                data = self._ticker_monitor[interval]
                data = data.loc[(data.closetime < now) & (data.opentime > last_opentime_registred)]
                if data.shape[0] > 0:
                    tuples_to_insert = list(data.itertuples(index=False, name=None))
                    if DataBase().insert_many('single_monitor', tuples_to_insert):
                        last_opentime_registred = tuples_to_insert[-1][2]
                    else:
                        raise Exception('Some error occured on saving database. The routine save_tickermonitor will quitting now.')

        except Exception as e:
            RegistrarLog(e)
