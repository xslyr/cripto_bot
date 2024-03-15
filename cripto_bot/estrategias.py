import asyncio
from abc import ABC, abstractmethod
from typing import Type

import pandas as pd
from .conector_bd import DataBase
from .logger import RegistrarLog
from .gerador_estatistico import Gerador_Estatistico, Estatisticas
from sklearn.ensemble import GradientBoostingRegressor, ExtraTreesRegressor, HistGradientBoostingRegressor
from xgboost import XGBRegressor
from lightgbm import LGBMRegressor

# TODO: Implementar urgente uma trava many-loss pra ser usada em períodos de teste da estratégia
# TODO: Cadastrar estratégia na tabela de estratégias do banco de dados

class Estrategia(ABC):
	@abstractmethod
	def verificar_condicoes(self, gerador:Gerador_Estatistico): pass


# ************************* Estrategia_MACRSI *************************
class Estrategia_MACRSI(Estrategia):
	
	def __init__(self):
		self.id = 1
		self.nome = 'MACRSI'
		self.LOGTASK = True
		self.position = 0


	def verificar_condicoes(self, gerador:Gerador_Estatistico):

		# TODO: D0011 : abstrair o tempo e formato para os parâmetros da funcao, após criar o mecanismo D0010
		timeview, response_format, action = '1m', 'dict', 'WAIT'

		volume = gerador.get_data( Estatisticas.VOLUME, timeview, response_format)
		rsi = gerador.get_data( Estatisticas.RSI,timeview, response_format, 'window=[3,7]')
		macd = gerador.get_data( Estatisticas.MACD, timeview, response_format, 'fast=3, slow=11, signal=9')

		# buy conditions
		if self.position == 1 and (rsi['7p'] > 50 and rsi['7p'] > rsi['3p'] and macd['hist'] <= 0.5):
			if self.LOGTASK: RegistrarLog(f'Estrategia:{self.nome}: SELL: rsi{rsi}, macd: {macd}, volume: {volume}')
			self.position, action = 0, 'SELL'

		# sell conditions
		if self.position == 0 and (rsi['7p'] < 50 and rsi['3p'] > rsi['7p'] and (macd['hist'] >= 0 or volume['taker/maker'] >= 2.5)):
			if self.LOGTASK: RegistrarLog(f'Estrategia:{self.nome}: BUY: rsi{rsi}, macd: {macd}, volume: {volume}')
			self.position, action = 1, 'BUY'

		return action

class Estrategia_AIMix(Estrategia):
	_regressor = None

	def __init__(self):
		self.id = 2
		self.nome = 'AIMix'
		self.LOGTASK = True
		self.position = 0
		self.models = {}


	async def model_fit(self, funcao: Type, x: pd.DataFrame, y: pd.DataFrame = None, size_prediction: int = -1, params:dict= {}):
		if not any([ funcao.__name__ == modelo for modelo in self.models.keys() ]):
			self.models[funcao.__name__] = funcao(**params)
			if y == None: y = x.pop('close')
			self.models[funcao.__name__].fit( x.iloc[50:size_prediction], y.shift(-1).iloc[50:size_prediction] )
		else:
			if y == None: y = x.pop('close')
			self.models[funcao.__name__].fit(x.iloc[50:size_prediction], y.shift(-1).iloc[50:size_prediction])


	async def model_predict(self, funcao: Type, x: pd.DataFrame) -> float:
		return self.models[funcao.__name__].predict(x.iloc[-1].values)

	async def fit_all_models(self, model_params:dict, data_train:pd.DataFrame):
		tasks = [ self.model_fit(m, data_train, model_params[m]) for m in model_params ]
		await asyncio.gather( *tasks )

	def verificar_condicoes(self, gerador: Gerador_Estatistico):
		tempo_grafico = '1m'
		last_data = gerador.conexao.last_data[tempo_grafico]
		if self._regressor == None:
			conditions = 'ticker=\'{}\' and interval=\'{}\' order by opentime'.format(last_data['ticker'], last_data['interval'])
			ticker_monitor = DataBase().get_table('single_monitor', conditions )

			gerador_aux = Gerador_Estatistico(monitor_auxiliar = ticker_monitor)
			configuracoes = [
				(Estatisticas.PADRAO, 'drop=["ticker","interval","opentime","closetime"]'),
				(Estatisticas.VOLUME, ''),
				(Estatisticas.STD_ROLL, 'target="taker_asset_volume", window=[3,7,14,21]'),
				(Estatisticas.STD_ROLL, 'target="open", window=[3,7,9]'),
				(Estatisticas.STD_INNER, ''),
				(Estatisticas.SMA, 'window=[3,7,9], target="open"'),
				(Estatisticas.RSI, 'window=[3,7,9], target="open"'),
				(Estatisticas.RSI, 'window=[3,7,9], target="high"'),
				(Estatisticas.RSI, 'window=[3,7,9], target="low"'),
				(Estatisticas.STOCHRSI, 'target="open", rsi_length=14, k=[3,5], d=[3,5]'),
				(Estatisticas.MACD, 'taget="open", fast=3, slow=[11,13], signal=[7,9]')
			]
			x_train = gerador_aux.get_all_stats([tempo_grafico], 'dataframe', configuracoes)
			model_params = {
				GradientBoostingRegressor : {'random_state': 0, 'max_depth': None},
				XGBRegressor : {},
				ExtraTreesRegressor : {'random_state': 0},
				LGBMRegressor : {'random_state': 0, 'force_col_wise': True},
				HistGradientBoostingRegressor : {'random_state': 0}
			}

			asyncio.run( self.fit_all_models(model_params, x_train) )

		else:
			# resgatar parte do tickermonitor, agregar o last data, calcular o ultimo x_train pra prever o proximo close
			# combinar com outros calculos para estimar a acao de compra ou venda
			pass
