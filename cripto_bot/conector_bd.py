import sqlite3
from enum import Enum
import pandas as pd
from typing import List, Tuple
from .logger import RegistrarLog

k = Enum('keywords',['int','float','str','text','key','not_null'])

SQL_TYPES = { k.int: 'INTEGER', k.float: 'REAL', k.str: 'TEXT', k.text: 'BLOB' }
SQL_OPTIONS = { k.key: 'PRIMARY KEY', k.not_null: 'NOT NULL' }

DATABASE_STRUCTURE = [ {
    'name': 'app_parameters',
    'columns': [
        ('id', k.int , k.key),
        ('name', k.str , k.not_null),
        ('value', k.str , k.not_null),
        ('description', k.text , None)
    ]
}, {
    'name': 'single_monitor',
    'columns': [
        ('ticker', k.str, k.key ),
        ('interval', k.str, k.key),
        ('opentime', k.int , k.key),
        ('closetime', k.int , k.not_null),
        ('open', k.float , k.not_null),
        ('close', k.float , k.not_null),
        ('high', k.float , k.not_null),
        ('low', k.float , k.not_null),
        ('base_asset_volume', k.float , k.not_null),
        ('quote_asset_volume', k.float , k.not_null),
        ('number_of_trades', k.float , k.not_null),
        ('taker_asset_volume', k.float , k.not_null),
        ('taker_quote_volume', k.float , k.not_null)
    ]
}, {
    'name': 'wide_monitor',
    'columns': [
        ('timestamp', k.int , k.key),
        ('ticker', k.str , k.key),
        ('pct_price', k.float , k.not_null),
        ('var_price', k.float , k.not_null),
        ('vwap', k.float , k.not_null),
        ('open', k.float , k.not_null),
        ('close', k.float , k.not_null),
        ('high', k.float , k.not_null),
        ('low', k.float , k.not_null),
        ('base_asset_volume', k.float , k.not_null),
        ('quote_asset_volume', k.float , k.not_null),
        ('number_of_trades', k.float , k.not_null),
        ('taker_asset_volumes', k.float , k.not_null),
        ('taker_quote_volume', k.float , k.not_null)
    ]
}, {
    'name': 'strategies',
    'columns': [
        ('strategy_id', k.int , k.key),
        ('name', k.str , k.not_null),
        ('description', k.text , None)
    ]
}, {
    'name': 'orders',
    'columns': [
        ('transactTime', k.int , k.key),
        ('orderId', k.int , k.key),
        ('symbol', k.str, k.not_null),
        ('side', k.str, k.not_null),
        ('price', k.float, k.not_null),
        ('origQty', k.float, k.not_null),
        ('cummulativeQuoteQty', k.float, k.not_null),
        ('type', k.str, k.not_null)
    ]
}, {
    'name': 'trade_performance',
    'columns': [
        ('id', k.int, k.key),
        ('strategy_id', k.int , k.not_null),
        ('status', k.str , k.not_null),
        ('symbol', k.str , k.not_null),
        ('buy_order', k.int , k.not_null),
        ('sell_order', k.int , None),
        ('delta_time_s', k.float , None),
        ('delta_assetprice', k.float , None),
        ('delta_quoteprice', k.float , None),
        ('trade_qty', k.float , None)
    ]
} ]


class DataBase(object):
    _instance = None

    def __new__(cls, db_file='appdata.db', mode='live'):
        if cls._instance is None:
            cls._instance = super(DataBase, cls).__new__(cls)
            if mode == 'test': cls._db_file = 'appdata_test.db'
            else: cls._db_file = db_file
            cls.logtask = False
        return cls._instance


    @property
    def conn(self):
        return sqlite3.connect(self._db_file)


    def check_fix_db_integrity(self):
        conn = self.conn
        cur = conn.cursor()
        for item in DATABASE_STRUCTURE:
            try: pd.read_sql_query(f"SELECT * FROM {item['name']}", conn)
            except:
                query = f"CREATE TABLE {item['name']} ("
                columns, keys = '',''
                for col in item['columns']:
                    if col[2] == k.key:
                        columns += f"{col[0]} {SQL_TYPES[col[1]]},"
                        keys += f"{col[0]},"
                    elif col[2] == None: columns += f"{col[0]} {SQL_TYPES[col[1]]},"
                    else: columns += f"{col[0]} {SQL_TYPES[col[1]]} {SQL_OPTIONS[col[2]]},"

                query += columns+ f" PRIMARY KEY ({keys[:-1]}))"
                if self.logtask: RegistrarLog(query)
                cur.execute(query)
                conn.commit()
                print(f"Table {item['name']} was sucessful created.")

        initial_info = [

            ('SOCKET_BASE_URL','wss://stream.binance.com:9443/ws','','SOCKET_BASE_URL'),
            ('KLINES_INTERVAL_AVAILABLE', "['1s','1m','5m','15m']", '','KLINES_INTERVAL_AVAILABLE'),
            ('WIDE_INTERVAL_AVAILABLE',"['1h','4h','1d']",'','WIDE_INTERVAL_AVAILABLE')
        ]

        for param in initial_info:
            cur.execute('INSERT INTO app_parameters (name,value,description) select ?,?,? WHERE NOT EXISTS (SELECT 1 FROM app_parameters WHERE name=?)',param)
            conn.commit()
        conn.close()


    def get_param(self, param: str):
        c = self.conn.cursor()
        r = c.execute('SELECT value FROM app_parameters WHERE name=:name',{'name': param}).fetchone()[0]
        try: return eval(r)
        except: return r


    def get_table(self, table: str, conditions=None):
        try:
            if conditions == None: return pd.read_sql_query(f'SELECT * FROM {table}', self.conn)
            else: return pd.read_sql_query(f'SELECT * FROM {table} WHERE {conditions}', self.conn)
        except Exception as e:
            RegistrarLog(e)


    def insert_info(self, table: str, dictionary: dict):
        columns, values = '', ''
        for item in dictionary:
            columns += str(item) + ','
            values += ':'+str(item) + ','
        try:
            conn = self.conn
            cur = conn.cursor()
            command = f'INSERT INTO {table} ({columns[:-1]}) VALUES ({values[:-1]})'
            if self.logtask: RegistrarLog('insert_info: '+str(command))
            cur.execute(command, dictionary)
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            RegistrarLog(e)
            return False


    def insert_info(self, table: str, tuple: tuple):
        values = '?,'*len(tuple)
        try:
            conn = self.conn
            cur = conn.cursor()
            command = f'INSERT INTO {table} VALUES ({values[:-1]})'
            if self.logtask: RegistrarLog('insert_info: '+str(command))
            cur.execute(command, tuple)
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            RegistrarLog(e)
            return False


    def insert_many(self, table: str, data: List[Tuple]):
        some_error = False
        values = ''
        for idx, item in enumerate(DATABASE_STRUCTURE):
            if DATABASE_STRUCTURE[idx]['name'] == table:
                values = '?,'*len(DATABASE_STRUCTURE[idx]['columns'])
        conn = self.conn
        cur = conn.cursor()
        command = f'INSERT INTO {table} VALUES ({ values[:-1] })'

        try:
            cur.executemany(command , data)
            conn.commit()
        except sqlite3.Error as e:
            RegistrarLog(f'Error {e} on insert_many: \nCommand: {command}\nValues: {data}')
            conn.rollback()
            some_error = True

        conn.close()
        return False if some_error else True


    def update_info(self, table: str, dictionary: dict, conditions: str):
        columns = ''
        for item in dictionary:
            columns += '{}=:{},'.format(item,item)

        command = f'UPDATE {table} SET {columns[:-1]} WHERE {conditions}'
        if self.logtask: RegistrarLog('update_info: '+str(command))
        try:
            conn = self.conn
            cur = conn.cursor()
            cur.execute(command, dictionary )
            conn.commit()
            conn.close()
            return True
        except Exception as e:
            RegistrarLog(e)
            return False