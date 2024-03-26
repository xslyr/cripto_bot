import time
from typing import List, Dict
from .trader import TraderBinance
from .estrategias import *
from IPython.display import HTML,display, clear_output


class BasesAcionador(ABC):

	@abstractmethod
	def adicionar_estrategia(self, estrategia: Estrategia): pass

	@abstractmethod
	def remover_estrategia(self, estrategia: Estrategia): pass
		
	@abstractmethod
	def notificar_estrategias(self): pass


class Acionador_Estrategias(BasesAcionador):

	_lista_estrategias: List[Estrategia] = []
	_parametros_monitoramento:Dict = {}

	def __init__(self, gerador_estatistico: Gerador_Estatistico, operation_mode = 'livetest', **kwargs):
		self.gerador_estatistico = gerador_estatistico
		self.trader = TraderBinance(operation_mode)

		if len(gerador_estatistico.conexao.last_data) > 0:
			tempos = [item for item in gerador_estatistico.conexao.last_data.keys()]
			colunas_monitor = gerador_estatistico.conexao.last_data[tempos[0]].keys()
			self.painel_dados = pd.DataFrame( [[0]*len(colunas_monitor)], columns=colunas_monitor )

		self._parametros_monitoramento['tempo_notificacao'] = 1
		for item in kwargs:
			self._parametros_monitoramento[item] = kwargs[item]

	def adicionar_estrategia(self, estrategia: Estrategia):
		self._lista_estrategias.append(estrategia)
	
	def remover_estrategia(self, estrategia: Estrategia):
		self._lista_estrategias.remove(estrategia)
		
	def notificar_estrategias(self):
		for estrategia in self._lista_estrategias:
			action = estrategia.verificar_condicoes(self.gerador_estatistico)
			if action == 'BUY': self.trader.buy(estrategia, 'BTCUSDT')
			elif action == 'SELL': self.trader.sell( estrategia, 'BTCUSDT' )

	def mostrar_paineis(self):
		dados = []
		for tempo in self.gerador_estatistico.conexao.last_data:
			aux = {'interval': tempo}
			aux.update(self.gerador_estatistico.conexao.last_data[tempo])
			dados.append(aux)

		self.painel_dados = pd.DataFrame( dados, columns=dados[0].keys() ).T
		clear_output(wait=True)
		display(self.painel_dados)
		# TODO: montar um painel de posicoes com os nomes das estratégias e o status de cada 1, streamlit?
		# TODO: acho conveniente montar um painel com histórico das ultimas ordens?

	# TODO: D0010 : criar uma conexão entre a view do usuario e o acionador, para que eles escolham quais estrategias, tickers e quantidades usar

	def executar_trades(self, money_to_trade:float=12.5):
		self.adicionar_estrategia(Estrategia_SMAC(money_to_trade=money_to_trade))
		while True:			
			self.notificar_estrategias()
			self.mostrar_paineis()
			time.sleep(self._parametros_monitoramento['tempo_notificacao'])



	


	
