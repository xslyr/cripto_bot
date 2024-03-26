import cripto_bot as cb

conexao_binance = cb.Conector_Binance()
conexao_binance.start_ticker_monitor(ticker='BTCUSDT')
gerador_estatistico = cb.Gerador_Estatistico(conexao_binance)
execucao = cb.Acionador_Estrategias( gerador_estatistico )
execucao.executar_trades()
