import pandas as pd
from datetime import datetime
from typing import Dict

LOG_NAME = 'log_execucao.log'
LOG_OPERACAO = 'log_operacao.log'
COLUNAS_LOG_OPERACOES = ['datetime','acao','preco']

class RegistrarLog:

    def __init__(cls, informacao, **kwargs):
        if isinstance(informacao, pd.DataFrame): cls._registrar_dataframe(informacao, **kwargs)
        else: cls._registrar_texto(informacao)

    def _registrar_texto(self, texto):
        with open(LOG_NAME,'a') as log:
            log.write('{} : {}{}'.format(str(datetime.now())[:-7], str(texto),'\n'))

    def _registrar_dataframe(self, dataframe, **kwargs):
        nome_dataframe = kwargs.get('nome_dataframe','dataframe_'+str(datetime.now())+'.csv')
        with open(LOG_NAME,'a') as log:
            log.write('{} : {}{}'.format(str(datetime.now())[:-7], f'Dataframe registrado com o nome {nome_dataframe}','\n'))
        pd.DataFrame(dataframe).to_csv(nome_dataframe)


class RegistrarTrade:

    def __init__(cls, nome_estratagia:str, dicionario_informacoes:Dict):
        try:
            datalog = pd.read_csv(f'log_{nome_estratagia}.csv')
            datalog = pd.concat([datalog, pd.DataFrame(dicionario_informacoes, index=[datalog.index[-1] + 1])], axis=0)
            datalog.to_csv(f'log_{nome_estratagia}.csv', index=False)
        except:
            datalog = pd.DataFrame(dicionario_informacoes, index=[0])
            datalog.to_csv(f'log_{nome_estratagia}.csv', index=False)





