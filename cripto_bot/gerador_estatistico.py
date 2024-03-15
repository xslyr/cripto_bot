import itertools

import pandas as pd
import pandas_ta as ta
from enum import Enum
from typing import List, Tuple
from datetime import datetime
from .conector_binance import Conector_Binance
from .logger import RegistrarLog

class Estatisticas(Enum):
	PADRAO = 'self._retornar_monitor(tempo=\'{}\', formato=\'{}\', {})'
	VOLUME = 'self._calcular_volumes( tempo=\'{}\', formato=\'{}\', {} )'
	STD_ROLL = 'self._calcular_stdroll( tempo=\'{}\', formato=\'{}\', {} )'
	STD_INNER = 'self._calcular_stdinner( tempo=\'{}\', formato=\'{}\', {} )'
	SMA = 'self._calcular_sma( tempo=\'{}\', formato=\'{}\', {})'
	RSI = 'self._calcular_rsi( tempo=\'{}\', formato=\'{}\', {})'
	STOCHRSI = 'self._calcular_stochrsi( tempo=\'{}\', formato=\'{}\', {})'
	MACD = 'self._calcular_macd( tempo=\'{}\', formato=\'{}\', {})'
	ANIMUS = 'self._calcular_animus( tempo=\'{}\', formato=\'{}\', {})'


class Gerador_Estatistico:

	_monitor: dict = None
	
	def __init__(self, conexao: Conector_Binance=None, monitor_auxiliar:dict=None):
		if conexao is not None:
			self.conexao = conexao
			self._monitor = conexao.ticker_monitor
		elif monitor_auxiliar is not None:
			self._monitor = monitor_auxiliar
		else:
			raise Exception('É necessário um objeto de conexão binance ou um monitor auxilar para que o Gerador Estatístico opere.')

	def get_all_stats(self, tempo:list=['1m'], formato:str='dataframe', lista_configuracoes:List[Tuple[Estatisticas,str]]=[]):
		"""
		:param tempo
		:param metodos_configuracoes: Uma lista de tuplas no formato ( Estatisticas, 'configuracoes' ) a qual será usada para personalizar a execução das funções.\
		Deixe este parâmetro vazio para receber os caculos com as configurações padrão.
		:return: Retorna um dataframe com
		"""

		# TODO: Implementar a solicitação de multiplas configuracoes para as estratégias chamadas no método get_all_stats
		if lista_configuracoes == []:
			lista_configuracoes = [ (Estatisticas.__members__[item], '') for item in Estatisticas.__members__ ]

		retorno = {}
		for t in tempo:
			retorno[t] = []
			for item in lista_configuracoes:
				funcao_a_ser_executada = item[0].value.format(t, 'dataframe', item[1])
				retorno[t].append( eval( funcao_a_ser_executada ) )

			retorno[t] = pd.concat(retorno[t], axis=1) if formato == 'dataframe' else pd.concat(retorno[t], axis=1).iloc[-1].to_dict()

		return retorno

	def get_data(self, funcao:Estatisticas, tempo_grafico:str, formato:str, configuracoes:str='', **kwargs ):
		"""
		:param funcao: Enum da classe Estatisticas a qual nos retornará as opções de cálculos disponíveis.
		:param tempo_grafico: Tempo gráfico a onde será aplicado os cálculos. Este valor deve coincidir com a chave do dicionário monitor.
		:param formato: Formato de retorno desejado 'dict' para obter um dicionário com o último dado ou 'dataframe' para obter o dataframe.
		:param configuracoes: Configurações deve ser um um texto com os parametros de entrada para função anunciada no primeiro parâmetro.
		:return: Devolve um dicionário ou dataframe com os valores de cálculo estatístico configurados pelo ParametroGerador.
		"""
		# TODO: De que maneira posso informar o 'caller' sobre as opcoes a serem preenchidas em configuracoes? um get na classe enum?
		funcao_a_ser_executada = funcao.value.format(tempo_grafico, formato, configuracoes)

		return eval( funcao_a_ser_executada )

	def set_monitor(self, dataframe:pd.DataFrame, tempo_grafico='1m'):
		info = "Monitor de tempo grafico {} foi atualizado. O último índice passou de {} para {}."
		RegistrarLog(info.format(tempo_grafico,
			self.monitor[tempo_grafico].index[-1],
			dataframe.index[-1]
		))
		self.monitor[tempo_grafico] = dataframe

	@property
	def monitor(self):
		return self._monitor

# Padrao ***************************************************************************************************************
	def _retornar_monitor(self, tempo, formato, **kwargs):
		aux = self.monitor[tempo].copy()
		drop = kwargs.get('drop', ['ticker','interval'])
		apply_pct = kwargs.get('apply_pct',[])

		if len(drop) > 0:
			for col in drop: aux.pop(col)

		if len(apply_pct) > 0:
			for col in apply_pct:
				aux[col] = aux[col].pct_change().fillna(0)

		if formato=='dataframe': return aux
		else: return aux.iloc[-1].to_dict()

	def _ultimos_dados(self, tempo, formato, **kwargs):
		if self.conexao == None: raise Exception('Para resgatar os últimos dados é necessário informar uma conexão ao criar a classe Gerador Estatístico')
		if formato == 'dataframe':
			aux = pd.DataFrame(self.conexao._last_data[tempo])
			aux.index = [ datetime.fromtimestamp(self.conexao._last_data[tempo]['opentime']/1000) ]
			aux.index.name = 'index'
			return aux
		else: self.conexao._last_data[tempo]

# Volume ***************************************************************************************************************
	def _calcular_volumes(self, tempo, formato, **kwargs):
		drop = kwargs.get('drop',['base_asset_volume','taker_asset_volume','number_of_trades'])

		# taker é que de fato chegou a realizar compras, maker é quem pois certo volume a venda
		aux = pd.DataFrame(self.monitor[tempo][['base_asset_volume','taker_asset_volume','number_of_trades']]).copy()
		aux['taker%'] = aux['taker_asset_volume']/aux['base_asset_volume']
		aux['maker_asset_volume'] = aux['base_asset_volume'] - aux['taker_asset_volume']
		aux['maker%'] = aux['maker_asset_volume'] / aux['base_asset_volume']
		aux['taker/maker'] = aux['taker_asset_volume'] / aux['maker_asset_volume']
		aux['maker/taker'] = aux['maker_asset_volume'] / aux['taker_asset_volume']
		aux['signed_volume'] = aux['base_asset_volume'] * ( aux['maker%'] - 0.5)
		aux['mean_vol_by_trade'] = aux['taker_asset_volume']/aux['number_of_trades']

		# volume taker tem forte relacao com fundo
		# volume maker tem forte relacao com topo

		if len(drop) > 0:
			for col in drop: aux.pop(col)

		if formato=='dataframe': return aux
		else: return aux.iloc[-1].to_dict()


# RSI ******************************************************************************************************************
	def _calcular_rsi(self, tempo, formato, **kwargs):
		target = kwargs.get('target', 'close')
		aux = pd.DataFrame(self.monitor[tempo][target]).copy()
		window = kwargs.get('window',7)

		if isinstance(window, List):
			for periodo in window:
				column_name = f'rsi_{target}_{periodo}p'
				aux[column_name] = ta.rsi(aux[target], length=periodo).fillna(0)
		else:
			column_name = f'rsi_{target}_{window}p'
			aux[column_name] = ta.rsi(aux[target], length=window).fillna(0)

		if formato=='dataframe': return pd.DataFrame(aux.iloc[:,1:])
		else: return aux.iloc[-1,1:].to_dict()

		
# RSI Estocástico ******************************************************************************************************
	def _calcular_stochrsi(self, tempo, formato, **kwargs):
		target = kwargs.get('target', 'close')
		aux = pd.DataFrame(self.monitor[tempo][target]).copy()
		length = kwargs.get('length',14)
		rsi_length = kwargs.get('rsi_length',14)
		k = kwargs.get('k',3)
		d = kwargs.get('d',3)

		combinacoes_parametros = [x if isinstance(x, list) else [x] for x in [rsi_length, k, d]]

		# 'k' é a linha mais sensível, 'd' a mais suave
		for item in list(itertools.product(*combinacoes_parametros)):
			column_name = [f'stochrsi_k_{item[0]}.{item[1]}.{item[2]}',
						   f'stochrsi_d_{item[0]}.{item[1]}.{item[2]}']
			aux[column_name] = ta.momentum.stochrsi(aux[target], length=length, rsi_length=item[0], k=item[1], d=item[2], fillna=0)

		if formato=='dataframe': return pd.DataFrame(aux.iloc[:,1:])
		else: return pd.DataFrame(aux.iloc[:,1:]).iloc[-1].to_dict()


# SMA ******************************************************************************************************************
	def _calcular_sma(self, tempo, formato, **kwargs):
		target = kwargs.get('target', 'close')
		aux = pd.DataFrame(self.monitor[tempo][target]).copy()
		window = kwargs.get('window',7)

		if isinstance(window, List ):
			for periodo in window:
				column_name = f'sma_{target}_{periodo}p'
				aux[column_name] = aux[target].rolling(min_periods=0, window=periodo).mean()
		else:
			column_name = f'sma_{target}_{window}p'
			aux[column_name] = aux[target].rolling(min_periods=0, window=window).mean()

		if formato=='dataframe': return pd.DataFrame(aux.iloc[:,1:])
		else: return aux.iloc[-1,1:].to_dict()


# MACD *****************************************************************************************************************
	def _calcular_macd(self, tempo, formato, **kwargs):
		target = kwargs.get('target', 'close')
		aux = pd.DataFrame(self.monitor[tempo][target]).copy()
		# fast=2, slow=11, signal=9
		fast = kwargs.get('fast', 2)
		slow = kwargs.get('slow', 11)
		signal = kwargs.get('signal', 9)

		combinacoes_parametros = [ x if isinstance(x,list) else [x] for x in [fast, slow, signal]]

		# 'macd' é a linha mais sensível, 'sign' a mais suave e 'hist' o gráfico de barras ao fundo.
		for item in itertools.product(*combinacoes_parametros):
			column_name = [f'macd_{str(item[0])}.{str(item[1])}.{str(item[2])}',
						   f'hist_{str(item[0])}.{str(item[1])}.{str(item[2])}',
						   f'sign_{str(item[0])}.{str(item[1])}.{str(item[2])}']
			aux[column_name] = ta.macd(aux[target], fast=item[0], slow=item[1], signal=item[2]).fillna(0)

		if formato=='dataframe': return pd.DataFrame(aux.iloc[:,1:])
		else: return pd.DataFrame(aux.iloc[:,1:]).iloc[-1].to_dict()


# Desvio Padrão ********************************************************************************************************
	def _calcular_stdroll(self, tempo, formato, **kwargs):
		'''
		:param tempo: Tempo gráfico presente no monitor o qual será usado para os cálculos.
		:param formato: Formato de retorno dos dados.
		:param kwargs:
			window: Define a quantidade de períodos que será usada para os cálculos de desvio padrão. O valor padrão é 7.
			target: Define qual será o alvo para o cálculo do desvio padrão:
					O valor padrão é 'close'.
					Os alvos disponíveis são:
						open, high, low, base_asset_volume, taker_asset_volume, maker_asset_volume, taker/maker
		:return: pandas.DataFrame | dict
		'''
		aux = pd.DataFrame(self.monitor[tempo][['open','close','high','low','base_asset_volume','taker_asset_volume']]).copy()
		window = kwargs.get('window',7)
		target = kwargs.get('target','close')

		if isinstance(window, List):
			for period in window:
				column_name = f'std_{target}_{period}p'
				if target == 'taker/maker' or target == 'maker_asset_volume':
					aux['maker_asset_volume'] = aux['base_asset_volume'] - aux['taker_asset_volume']
					aux['taker/maker'] = aux['taker_asset_volume'] / aux['maker_asset_volume']
					aux[column_name] = aux[target].rolling(min_periods=0, window=period).std()
				else:
					aux[column_name] = aux[target].rolling(min_periods=0, window=period).std()
		else:
			column_name = f'std_{target}_{window}p'
			if target == 'taker/maker' or target == 'maker_asset_volume':
				aux['maker_asset_volume'] = aux['base_asset_volume'] - aux['taker_asset_volume']
				aux['taker/maker'] = aux['taker_asset_volume'] / aux['maker_asset_volume']
				aux[column_name] = aux[target].rolling(min_periods=0, window=window).std()
			else:
				aux[column_name] = aux[target].rolling(min_periods=0, window=window).std()

		if formato=='dataframe': return pd.DataFrame(aux.iloc[:,6:])
		else: return pd.DataFrame(aux.iloc[:,6:]).iloc[-1].to_dict()


# Desvio Padrão Inner **************************************************************************************************
	def _calcular_stdinner(self, tempo, formato, **kwargs):
		aux = self.monitor[tempo][['open','high','low','close']].copy()

		column_name='std_inner'
		aux[column_name] = aux.std(axis=1)

		if formato=='dataframe': return aux[column_name]
		else: return aux[column_name].iloc[-1].to_dict()


# Animus ****************************************************************************************************************

	def _calcular_animus(self, tempo, formato, **kwargs):
		'''
		:param tempo: Tempo gráfico presente no monitor o qual será usado para os cálculos.
		:param formato: Formato de retorno dos dados.
		:param kwargs:
			window: Define a quantidade de períodos que será usado para o cálculo do momentum e deviation.
					O valor padrão é 21.
		:return:
		'''
		def _anima(row):
			return (row['close'] - row['low']) * row['taker%'] * row['base_asset_volume'] - (
						row['high'] - row['close']) * row['maker%'] * row['base_asset_volume']

		aux = self.monitor[tempo][['open', 'close', 'high', 'low', 'base_asset_volume', 'taker_asset_volume']].copy()
		window = kwargs.get('window', 21)

		if formato == 'dataframe': column_name = ['animus_score','animus_momentum','animus_deviation']
		else: column_name = ['score','momentum','deviation']

		aux['taker%'] = aux['taker_asset_volume'] / aux['base_asset_volume']
		aux['maker%'] = 1 - aux['taker%']
		aux[ column_name[0] ] = aux.apply(_anima, axis=1)
		aux[ column_name[1] ] = aux[ column_name[0] ].rolling(min_periods=0, window=window).mean().fillna(0)
		aux[ column_name[2] ] = aux[column_name[0]].rolling(min_periods=0, window=window).std().fillna(0)

		if formato=='dataframe': return aux[column_name]
		else: return aux[column_name].iloc[-1].to_dict()
	
	
		
		
		
		
		
		
		
