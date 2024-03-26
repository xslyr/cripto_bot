# cripto_bot

Objetivo: bot para realizar compra e venda de criptomoedas no mercado Spot, segundo padrões pré-definidos de análise técnica.

Status: (em desenvolvimento)

Requisitos:
	Ter uma conta na corretora Binance e configurar a BINANCE_APIKEY e BINANCE_SECRETKEY no arquivo .env na raiz do projeto.
	
---

Resumo da Estrutura: 
	- O pacote conector_binance é responsável por trazer as informações da Binance e armazená-las em um dataframe.

	- O gerador_estatistico é responsável por pegar os dados de um dataframe e realizar os cálculos de análise técnica.

	-  O arquivo de estrategias é responsável por definir as estratégias a serem aplicadas para compra ou venda.
	
	- O acionador_estrategias funciona como um Observer para acionar as estratégias.
	
	- O objeto trader é quem realiza as operações de compra ou venda.
	
