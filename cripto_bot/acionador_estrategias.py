import time
from typing import List,Dict
from .trader import TraderBinance
from .estrategias import *
from IPython.display import HTML, display, clear_output


class BasesAcionador(ABC):

	@abstractmethod
	def adicionar_estrategia(self, estrategia: Estrategia)->None: pass

	@abstractmethod
	def remover_estrategia(self, estrategia: Estrategia)->None: pass
		
	@abstractmethod
	def notificar_estrategias(self)->None: pass


class Acionador_Estrategias(BasesAcionador):

	_lista_estrategias: List[Estrategia] = []
	_parametros_monitoramento:Dict = {}

	def __init__(self, gerador_estatistico: Gerador_Estatistico, operation_mode = 'livetest', **kwargs):
		self.gerador_estatistico = gerador_estatistico
		self.trader = TraderBinance(operation_mode)
		self.order_pairs = {}

		if len(gerador_estatistico.conexao.last_data) > 0:
			tempos = [item for item in gerador_estatistico.conexao.last_data.keys()]
			colunas_monitor = gerador_estatistico.conexao.last_data[tempos[0]].keys()
			self.painel_dados = pd.DataFrame( [[0]*len(colunas_monitor)], columns=colunas_monitor )

		self._parametros_monitoramento['tempo_notificacao'] = 1
		for item in kwargs:
			self._parametros_monitoramento[item] = kwargs[item]

	def adicionar_estrategia(self, estrategia: Estrategia)->None:
		self._lista_estrategias.append(estrategia)
	
	def remover_estrategia(self, estrategia: Estrategia)->None:
		self._lista_estrategias.remove(estrategia)
		
	def notificar_estrategias(self)->None:
		for estrategia in self._lista_estrategias:
			result = estrategia.verificar_condicoes(self.gerador_estatistico)
			if result == 'BUY':
				self.order_pairs[estrategia.id] = self.trader.buy(estrategia.id, 'BTCUSDT', self.lot_size)
			elif result == 'SELL':
				self.trader.sell( self.order_pairs[estrategia.id], 'BTCUSDT', self.lot_size )

	def mostrar_paineis(self):
		dados = [list(item.values()) for item in self.gerador_estatistico.conexao.last_data.values()]
		self.painel_dados = pd.DataFrame( dados, columns=self.painel_dados.columns )


		clear_output(wait=True)
		display(self.painel_dados)
		# TODO: montar um painel de posicoes com os nomes das estratégias e o status de cada 1
		# TODO: montar um painel com histórico das ultimas ordens?


	# TODO: D0010 : criar uma conexão entre a view do usuario e o acionador, para que eles escolham quais estrategias, tickers e quantidades usar
	def executar_trades(self, lot_size:float):
		self.adicionar_estrategia(Estrategia_MACRSI())
		self.lot_size = lot_size
		while True:			
			self.notificar_estrategias()
			self.mostrar_paineis()
			time.sleep(self._parametros_monitoramento['tempo_notificacao'])



	


	
