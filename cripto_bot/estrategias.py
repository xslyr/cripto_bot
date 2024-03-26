import asyncio
from abc import ABC, abstractmethod
from typing import Type
import pandas as pd

from .conector_bd import DataBase
from .logger import RegistrarLog
from .gerador_estatistico import Gerador_Estatistico, Estatisticas
from enum import Enum


class TradePosition(Enum):
    OutMarket = 0
    InMarket = 1

class Estrategia(ABC):

	def set_position(self, value: TradePosition): self.position = value
	def set_orderpair(self, value: int): self.orderpair = value
	def set_quantity(self, value:float): self.quantity = value
	def set_min_quantity(self, gerador:Gerador_Estatistico): self.money_to_trade/float(gerador.conexao.last_data['close'])

	def get_id(self): return self.id
	def get_position(self): return self.position
	def get_orderpair(self): return self.orderpair
	def get_quantity(self): return self.quantity


	@abstractmethod
	def verificar_condicoes(self, gerador:Gerador_Estatistico): pass



# ************************* Estrategia_MACRSI *************************
class Estrategia_SMAC(Estrategia):
	
	def __init__(self, money_to_trade):
		self.id = 1
		self.nome = 'MACRSI'
		self.LOGTASK = True
		self.position = TradePosition.OutMarket
		self.orderpair = None
		self.qty = None
		self.money_to_trade = money_to_trade


	def verificar_condicoes(self, gerador:Gerador_Estatistico):

		timeview, response_format, action = '1m', 'dict', 'WAIT'

		volume = gerador.get_data( Estatisticas.VOLUME, timeview, response_format)
		sma = gerador.get_data( Estatisticas.SMA, timeview, response_format, 'window=[3,9]')
		macd = gerador.get_data( Estatisticas.MACD, timeview, response_format, 'fast=3, slow=11, signal=9')

		# buy conditions
		if self.position == TradePosition.OutMarket and (sma['9p'] > sma['3p'] and macd['sign'] > 0):
			if self.LOGTASK: RegistrarLog(f'Estrategia:{self.nome}: SELL: sma{sma}, macd: {macd}, volume: {volume}')
			self.set_min_quantity(gerador)
			action = 'BUY'

		# sell conditions
		if self.position == TradePosition.InMarket and (sma['3p'] > sma['9p'] and (macd['sign'] < 0 or volume['taker/maker'] >= 2.5)):
			if self.LOGTASK: RegistrarLog(f'Estrategia:{self.nome}: BUY: sma{sma}, macd: {macd}, volume: {volume}')
			self.set_quantity(0)
			action = 'SELL'

		return action


class Estrategia_Exemplo(Estrategia):
	def __init__(self, money_to_trade):
		# id e nome será utilizada no momento de guardar as informações sobre a performance das estratégias
		self.id = 2
		self.nome = 'Exemplo'

		# a variavel logtask é usada para controlar se a classe como um irá guardar log no arquivo de texto
		self.LOGTASK = True

		self.position = TradePosition.OutMarket # position indica se estamos dentro ou fora do mercado spot
		self.orderpair = None # oderpair armazena o id da ordem de compra pra registrarmos a performance entre compra/venda como um par
		self.qty = None # o qty guarda a quantidade de criptomoedas que está sendo negociada no trade aberto
		self.money_to_trade = money_to_trade

	def verificar_condicoes(self, gerador: Gerador_Estatistico):

		# Aqui você utiliza o objeto gerador para recuperar o dado de análise técnica necessária a sua estratégia.
		# Os dados disponíveis para ser recuperados estão em um enum chamado Estatisticas.
		# O tempo_grafico deve ser 1 das chaves que está sendo carregada pelo conector binance.

		exemplo_sma = gerador.get_data( Estatisticas.SMA, tempo_grafico='1m', formato='dict', configuracoes='window=[3,7]')

		# No exemplo acima carregamos em formato de dicionario o SMA( Simple Moving Average = Média Móvel Simples) do tempo gráfico 1 minuto.
		# É importante lembrar que há também disponível o formato dataframe, o que facilitará utilizar esses dados com algoritmos de IA por exemplo.

		# O intuito geral desse método é retornar para o Acionador_Estrategia a informação se irá comprar, vender ou nenhuma delas.
		action = 'WAIT'

		# É impressindível que ao retornar 'BUY' a quantidade de criptomoedas seja definida.
		# Caso deseje operar com a quantidade definida em money_to_trade, basta chamar o método set_min_quantity passando gerador como parâmetro.
		# Por exemplo:

		if self.position == TradePosition.OutMarket and (exemplo_sma['7p'] > exemplo_sma['3p']):
			self.set_min_quantity(gerador)
			action = 'BUY'

		if self.position == TradePosition.InMarket and (exemplo_sma['3p'] > exemplo_sma['7p']):
			self.set_quantity(0)
			action = 'SELL'

		return action

